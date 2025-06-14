#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pump.Fun Server - Ультра быстрая серверная часть
Только получение токенов с комьюнити и админами
+ ЗАЩИТА ОТ ДУБЛИКАТОВ КОМЬЮНИТИ С ПЕРСИСТЕНТНЫМ ХРАНЕНИЕМ
"""
import asyncio
import websockets
import json
import aiohttp
import time
import re
import requests
from typing import Dict, List, Optional
import logging
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import threading
from collections import deque
import socket
import os
from pathlib import Path

# Настройки логирования - минимум для скорости
logging.basicConfig(level=logging.ERROR)
for logger_name in ["aiohttp", "websockets", "asyncio"]:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)

# =============================================================================
# КОНФИГУРАЦИЯ СЕРВЕРА
# =============================================================================

# WebSocket настройки
WS_URL = "wss://pumpportal.fun/api/data"
SERVER_HOST = "0.0.0.0"  # Слушать на всех интерфейсах
SERVER_PORT = 80

# Twitter API (быстрые настройки)
TWITTER_API_URL = "https://api.twitterapi.io/twitter/community/moderators"
TWITTER_API_KEYS = [
    "337c6f9fe7114c1884cd56b158c15878",
    # Добавьте больше ключей для скорости
]

# Производительность
MAX_CONCURRENT_REQUESTS = 200
TWITTER_API_CONCURRENT_REQUESTS = 50
HTTP_TIMEOUT = 1  # Очень быстрые таймауты
CONNECT_TIMEOUT = 0.5

# 🗄️ НАСТРОЙКИ ПЕРСИСТЕНТНОГО ХРАНЕНИЯ
PROCESSED_COMMUNITIES_FILE = "processed_communities.json"
AUTOSAVE_INTERVAL = 60  # Автосохранение каждые 60 секунд


# =============================================================================
# МОДЕЛИ ДАННЫХ
# =============================================================================

@dataclass
class TokenResult:
    mint: str
    name: str
    symbol: str
    community_url: str
    admin_name: str
    admin_username: str
    timestamp: float

    def to_dict(self) -> Dict:
        return {
            "mint": self.mint,
            "name": self.name,
            "symbol": self.symbol,
            "community_url": self.community_url,
            "admin_name": self.admin_name,
            "admin_username": self.admin_username,
            "timestamp": self.timestamp
        }


# =============================================================================
# 🗄️ ПЕРСИСТЕНТНОЕ ХРАНЕНИЕ КОМЬЮНИТИ
# =============================================================================

class PersistentCommunityStorage:
    """Класс для сохранения и загрузки обработанных комьюнити"""

    def __init__(self, filename: str = PROCESSED_COMMUNITIES_FILE):
        self.filename = filename
        self.data_lock = threading.Lock()

    def load_communities(self) -> set:
        """Загрузка обработанных комьюнити из файла"""
        try:
            if os.path.exists(self.filename):
                with open(self.filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        communities = set(data.get('communities', []))
                        print(f"📂 Загружено {len(communities)} обработанных комьюнити из {self.filename}")
                        return communities
                    elif isinstance(data, list):
                        # Совместимость со старым форматом
                        communities = set(data)
                        print(f"📂 Загружено {len(communities)} обработанных комьюнити (старый формат)")
                        return communities
        except Exception as e:
            print(f"⚠️ Ошибка загрузки файла {self.filename}: {e}")

        print(f"📂 Создан новый файл для хранения комьюнити: {self.filename}")
        return set()

    def save_communities(self, communities: set, stats: dict = None):
        """Сохранение обработанных комьюнити в файл"""
        try:
            with self.data_lock:
                data = {
                    'communities': list(communities),
                    'last_updated': time.time(),
                    'total_count': len(communities)
                }

                if stats:
                    data['stats'] = stats

                # Атомарное сохранение
                temp_filename = f"{self.filename}.tmp"
                with open(temp_filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                # Переименовываем файл только после успешной записи
                os.replace(temp_filename, self.filename)

        except Exception as e:
            print(f"⚠️ Ошибка сохранения файла {self.filename}: {e}")

    def get_file_info(self) -> dict:
        """Получение информации о файле"""
        try:
            if os.path.exists(self.filename):
                stat = os.stat(self.filename)
                with open(self.filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return {
                        'exists': True,
                        'size_bytes': stat.st_size,
                        'size_kb': round(stat.st_size / 1024, 2),
                        'modified': time.ctime(stat.st_mtime),
                        'communities_count': len(data.get('communities', [])),
                        'last_updated': data.get('last_updated'),
                        'stats': data.get('stats', {})
                    }
        except:
            pass

        return {'exists': False}


# =============================================================================
# БЫСТРЫЙ TWITTER API ПУЛ
# =============================================================================

class FastTwitterAPIPool:
    def __init__(self, api_keys: List[str]):
        self.api_keys = api_keys
        self.api_url = TWITTER_API_URL
        self._key_index = 0
        self._lock = threading.Lock()
        self.executor = ThreadPoolExecutor(
            max_workers=TWITTER_API_CONCURRENT_REQUESTS,
            thread_name_prefix="twitter_api"
        )
        self.cache = {}

    def get_next_api_key(self) -> str:
        with self._lock:
            api_key = self.api_keys[self._key_index % len(self.api_keys)]
            self._key_index += 1
            return api_key

    async def get_admin_info(self, community_url: str) -> Optional[Dict[str, str]]:
        """Получение информации об админе комьюнити"""
        community_id = self._extract_community_id(community_url)
        if not community_id or community_id == "unknown":
            return None

        if community_id in self.cache:
            return self.cache[community_id]

        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                self._get_admin_sync,
                community_id
            )

            if result:
                self.cache[community_id] = result
                return result

        except Exception:
            pass

        return None

    def _extract_community_id(self, community_url: str) -> str:
        """Быстрое извлечение ID комьюнити"""
        try:
            idx = community_url.find('/i/communities/')
            if idx != -1:
                start = idx + 15
                end = len(community_url)
                for char in ['/', '?', '#']:
                    pos = community_url.find(char, start)
                    if pos != -1:
                        end = min(end, pos)
                community_id = community_url[start:end]
                if community_id.isdigit():
                    return community_id
        except:
            pass
        return "unknown"

    def _get_admin_sync(self, community_id: str) -> Optional[Dict[str, str]]:
        """Синхронное получение админа"""
        try:
            api_key = self.get_next_api_key()

            headers = {
                'x-api-key': api_key,
                'Accept': 'application/json',
                'User-Agent': 'PumpServer/1.0'
            }

            params = {'community_id': community_id}

            response = requests.get(
                self.api_url,
                headers=headers,
                params=params,
                timeout=HTTP_TIMEOUT
            )

            if response.status_code == 200:
                result = response.json()
                return self._parse_admin_from_result(result)

        except Exception:
            pass

        return None

    def _parse_admin_from_result(self, result: dict) -> Optional[Dict[str, str]]:
        """Парсинг админа из результата API"""
        try:
            admin_name = "Unknown"
            admin_username = "unknown"

            if "moderators" in result and result["moderators"]:
                moderators = result["moderators"]
                if isinstance(moderators, list) and len(moderators) > 0:
                    first_mod = moderators[0]
                    admin_name = first_mod.get("name", first_mod.get("display_name", "Unknown"))
                    admin_username = first_mod.get("username", first_mod.get("screen_name", "unknown"))

            elif "admin" in result:
                admin = result["admin"]
                if isinstance(admin, dict):
                    admin_name = admin.get("name", admin.get("display_name", "Unknown"))
                    admin_username = admin.get("username", admin.get("screen_name", "unknown"))

            if admin_username.startswith('@'):
                admin_username = admin_username[1:]

            return {
                "admin_name": admin_name,
                "admin_username": admin_username
            }

        except Exception:
            return None


# =============================================================================
# БЫСТРЫЙ МОНИТОР ТОКЕНОВ С ПЕРСИСТЕНТНЫМ ХРАНЕНИЕМ
# =============================================================================

class FastTokenMonitor:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.twitter_api = FastTwitterAPIPool(TWITTER_API_KEYS)
        self.running = False
        self.tokens_seen = set()
        self.metadata_cache = {}
        self.pump_cache = {}

        # 🗄️ ПЕРСИСТЕНТНОЕ ХРАНЕНИЕ
        self.storage = PersistentCommunityStorage()
        self.processed_communities = self.storage.load_communities()
        self.last_save_time = time.time()

        # Статистика
        self.processed_count = 0
        self.community_found_count = 0
        self.duplicate_communities_skipped = 0
        self.start_time = time.time()

        # Список подключенных клиентов
        self.connected_clients = set()

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=500,
            limit_per_host=200,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
            ttl_dns_cache=3600,
            use_dns_cache=True
        )

        timeout = aiohttp.ClientTimeout(
            total=HTTP_TIMEOUT,
            connect=CONNECT_TIMEOUT,
            sock_read=0.5
        )

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'PumpServer/1.0',
                'Accept': 'application/json'
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        # 🗄️ Сохраняем данные при выходе
        self.save_processed_communities()

    def normalize_community_url(self, url: str) -> str:
        """Нормализация URL комьюнити для правильного сравнения"""
        if not url:
            return ""

        # Убираем протокол и www
        normalized = url.lower()
        for prefix in ['https://', 'http://', 'www.']:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):]

        # Заменяем twitter.com на x.com для единообразия
        if normalized.startswith('twitter.com'):
            normalized = normalized.replace('twitter.com', 'x.com', 1)

        # Убираем trailing slash и параметры
        normalized = normalized.split('?')[0].split('#')[0].rstrip('/')

        return normalized

    def is_twitter_community_url(self, url: str) -> bool:
        """Быстрая проверка URL комьюнити"""
        return (isinstance(url, str) and
                len(url) > 25 and
                '/i/communities/' in url and
                ('x.com' in url or 'twitter.com' in url))

    def extract_twitter_url(self, data: Dict) -> Optional[str]:
        """Быстрое извлечение Twitter URL"""
        if not data:
            return None

        # Прямые поля
        for field in ['twitter', 'twitter_url']:
            value = data.get(field)
            if isinstance(value, str) and self.is_twitter_community_url(value):
                return value

        # Из описания
        description = data.get('description', '')
        if isinstance(description, str) and '/i/communities/' in description:
            start = description.find('/i/communities/')
            if start != -1:
                end = description.find(' ', start)
                if end == -1:
                    end = len(description)
                potential_url = description[start:end].rstrip('.,!?')
                if self.is_twitter_community_url(potential_url):
                    # Добавляем протокол если нужно
                    if not potential_url.startswith('http'):
                        potential_url = 'https://x.com' + potential_url
                    return potential_url

        return None

    async def get_pump_data(self, mint_address: str) -> Optional[Dict]:
        """Быстрое получение данных с pump.fun"""
        if mint_address in self.pump_cache:
            return self.pump_cache[mint_address]

        try:
            url = f"https://frontend-api.pump.fun/coins/{mint_address}"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    self.pump_cache[mint_address] = data
                    return data
        except:
            pass
        return None

    async def get_metadata(self, uri: str) -> Optional[Dict]:
        """Быстрое получение метаданных"""
        if uri in self.metadata_cache:
            return self.metadata_cache[uri]

        try:
            async with self.session.get(uri) as response:
                if response.status == 200:
                    data = await response.json()
                    self.metadata_cache[uri] = data
                    return data
        except:
            pass
        return None

    async def find_community_url(self, token_data: Dict) -> Optional[str]:
        """Поиск URL комьюнити в токене"""
        mint_address = token_data.get('mint', '')

        # Сначала проверяем прямо в данных токена
        community_url = self.extract_twitter_url(token_data)
        if community_url:
            return community_url

        # Собираем задачи для параллельного выполнения
        tasks = []

        if mint_address:
            tasks.append(self.get_pump_data(mint_address))

        for field in ['uri', 'metadata_uri', 'metadataUri']:
            uri = token_data.get(field)
            if uri and uri.startswith('http'):
                tasks.append(self.get_metadata(uri))

        if not tasks:
            return None

        try:
            # Быстрый таймаут для максимальной скорости
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=1.5
            )

            for result in results:
                if isinstance(result, dict):
                    community_url = self.extract_twitter_url(result)
                    if community_url:
                        return community_url

        except asyncio.TimeoutError:
            pass

        return None

    def save_processed_communities(self):
        """🗄️ Сохранение обработанных комьюнити"""
        stats = {
            'processed_count': self.processed_count,
            'community_found_count': self.community_found_count,
            'duplicate_communities_skipped': self.duplicate_communities_skipped,
            'uptime_seconds': time.time() - self.start_time,
            'connected_clients': len(self.connected_clients)
        }
        self.storage.save_communities(self.processed_communities, stats)
        self.last_save_time = time.time()

    async def process_token(self, token_data: Dict):
        """Быстрая обработка токена"""
        mint_address = token_data.get('mint')
        if not mint_address or mint_address in self.tokens_seen:
            return

        self.tokens_seen.add(mint_address)
        self.processed_count += 1

        # 🗄️ Автосохранение
        if time.time() - self.last_save_time > AUTOSAVE_INTERVAL:
            self.save_processed_communities()

        # Создаем отдельную задачу для неблокирующей обработки
        asyncio.create_task(self._process_token_async(token_data))

    async def _process_token_async(self, token_data: Dict):
        """Асинхронная обработка токена"""
        try:
            # Ищем комьюнити
            community_url = await self.find_community_url(token_data)
            if not community_url:
                return

            # 🛡️ ПРОВЕРКА НА ДУБЛИКАТ КОМЬЮНИТИ (с персистентным хранением)
            normalized_community = self.normalize_community_url(community_url)
            if normalized_community in self.processed_communities:
                self.duplicate_communities_skipped += 1
                print(f"🔄 Дубликат комьюнити пропущен: {community_url}")
                return

            # Получаем админа
            admin_info = await self.twitter_api.get_admin_info(community_url)
            if not admin_info:
                return

            # 🛡️ ДОБАВЛЯЕМ КОМЬЮНИТИ В ОБРАБОТАННЫЕ (персистентно)
            self.processed_communities.add(normalized_community)
            self.community_found_count += 1

            # Создаем результат
            result = TokenResult(
                mint=token_data.get('mint', ''),
                name=token_data.get('name', 'Unknown'),
                symbol=token_data.get('symbol', 'UNK'),
                community_url=community_url,
                admin_name=admin_info['admin_name'],
                admin_username=admin_info['admin_username'],
                timestamp=token_data.get('timestamp', time.time())
            )

            print(f"✅ Новый токен с комьюнити: {result.symbol} ({result.admin_username})")

            # Отправляем всем подключенным клиентам
            await self.broadcast_to_clients(result)

        except Exception:
            pass

    async def broadcast_to_clients(self, token_result: TokenResult):
        """Отправка результата всем клиентам"""
        if not self.connected_clients:
            return

        message = json.dumps({
            "type": "token",
            "data": token_result.to_dict()
        })

        # Отправляем всем клиентам
        disconnected = set()
        for client in self.connected_clients:
            try:
                await client.send(message)
            except:
                disconnected.add(client)

        # Удаляем отключенных клиентов
        self.connected_clients -= disconnected

    async def handle_websocket_message(self, message: str):
        """Обработка сообщения от pump.fun"""
        try:
            data = json.loads(message)

            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and 'mint' in item:
                        await self.process_token(item)
            elif isinstance(data, dict):
                if 'mint' in data:
                    await self.process_token(data)
                elif 'txType' in data and data.get('txType') == 'create':
                    await self.process_token(data)

        except Exception:
            pass

    async def connect_to_pump(self):
        """Подключение к pump.fun WebSocket"""
        while self.running:
            try:
                print(f"🔌 Подключение к {WS_URL}...")

                async with websockets.connect(
                        WS_URL,
                        ping_interval=None,
                        ping_timeout=None,
                        close_timeout=2,
                        max_size=2 ** 22,
                        compression=None
                ) as websocket:

                    print("✅ Подключен к pump.fun!")

                    # Подписываемся на новые токены
                    subscribe_msg = {"method": "subscribeNewToken"}
                    await websocket.send(json.dumps(subscribe_msg))

                    print("📡 Подписка на новые токены активна")

                    async for message in websocket:
                        if not self.running:
                            break
                        await self.handle_websocket_message(message)

            except Exception as e:
                print(f"❌ Ошибка WebSocket: {e}")
                if self.running:
                    print("🔄 Переподключение через 3 секунды...")
                    await asyncio.sleep(3)

    async def handle_client(self, websocket, path):
        """Обработка подключения клиента"""
        self.connected_clients.add(websocket)
        client_address = websocket.remote_address
        print(f"👤 Клиент подключен: {client_address}")

        try:
            # Отправляем приветственное сообщение с информацией о хранении
            file_info = self.storage.get_file_info()
            welcome = json.dumps({
                "type": "welcome",
                "message": "Подключен к Pump.Fun Server (с персистентным хранением)",
                "persistent_storage": {
                    "file_exists": file_info['exists'],
                    "communities_loaded": len(self.processed_communities),
                    "file_size_kb": file_info.get('size_kb', 0)
                }
            })
            await websocket.send(welcome)

            # Ждем сообщения от клиента
            async for message in websocket:
                try:
                    data = json.loads(message)
                    if data.get("type") == "ping":
                        await websocket.send(json.dumps({"type": "pong"}))
                    elif data.get("type") == "get_stats":
                        # Отправляем статистику хранения
                        stats = {
                            "type": "storage_stats",
                            "data": {
                                "communities_in_memory": len(self.processed_communities),
                                "file_info": self.storage.get_file_info(),
                                "autosave_interval": AUTOSAVE_INTERVAL,
                                "last_save": self.last_save_time
                            }
                        }
                        await websocket.send(json.dumps(stats))
                except:
                    pass

        except Exception:
            pass
        finally:
            self.connected_clients.discard(websocket)
            print(f"👤 Клиент отключен: {client_address}")

    async def start_server(self):
        """Запуск сервера"""
        self.running = True
        self.start_time = time.time()

        print("🚀 Pump.Fun Fast Server v1.2 (Persistent Storage)")
        print("⚡ Ультра-быстрая обработка токенов")
        print("🛡️ Защита от дубликатов комьюнити")
        print("🗄️ Персистентное хранение дубликатов")
        print(f"🔑 Twitter API ключей: {len(TWITTER_API_KEYS)}")
        print(f"🌐 Сервер: {SERVER_HOST}:{SERVER_PORT}")
        print(f"📂 Файл хранения: {PROCESSED_COMMUNITIES_FILE}")

        # Информация о загруженных данных
        file_info = self.storage.get_file_info()
        if file_info['exists']:
            print(f"📊 Загружено комьюнити: {len(self.processed_communities)}")
            print(f"📏 Размер файла: {file_info['size_kb']} KB")
            if file_info.get('last_updated'):
                last_update = time.ctime(file_info['last_updated'])
                print(f"🕒 Последнее обновление: {last_update}")
        else:
            print("📁 Новый файл хранения будет создан")

        print(f"💾 Автосохранение каждые {AUTOSAVE_INTERVAL} секунд")
        print()

        # Запускаем сервер для клиентов
        server = await websockets.serve(
            self.handle_client,
            SERVER_HOST,
            SERVER_PORT,
            ping_interval=None,
            ping_timeout=10
        )
        print(f"🌐 WebSocket сервер запущен на {SERVER_HOST}:{SERVER_PORT}")

        # Запускаем статистику
        stats_task = asyncio.create_task(self._print_stats_periodically())

        # Подключаемся к pump.fun
        await self.connect_to_pump()

        # Останавливаем статистику
        stats_task.cancel()
        server.close()
        await server.wait_closed()

    async def _print_stats_periodically(self):
        """Периодический вывод статистики"""
        while self.running:
            try:
                await asyncio.sleep(30)
                if self.running:
                    elapsed = time.time() - self.start_time
                    rate = self.processed_count / max(elapsed, 1)
                    print(f"📊 {rate:.1f} tok/s | Обработано: {self.processed_count} | "
                          f"С комьюнити: {self.community_found_count} | "
                          f"Дубликатов пропущено: {self.duplicate_communities_skipped} | "
                          f"В хранилище: {len(self.processed_communities)} | "
                          f"Клиентов: {len(self.connected_clients)}")
            except asyncio.CancelledError:
                break
            except:
                pass

    def stop(self):
        """Остановка сервера"""
        self.running = False
        # 🗄️ Финальное сохранение при остановке
        print("💾 Сохранение данных перед остановкой...")
        self.save_processed_communities()


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

async def main():
    """Главная функция сервера"""
    monitor = FastTokenMonitor()

    try:
        async with monitor:
            await monitor.start_server()
    except KeyboardInterrupt:
        print("\n⏹️ Получен сигнал остановки")
        monitor.stop()
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    asyncio.run(main())
