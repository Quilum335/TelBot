import asyncio
from typing import Optional, Dict
from pyrogram import Client
from config import Config


class PyrogramClientFactory:
    """Reusable cache/factory for Pyrogram clients keyed by session_string.

    Ensures a single started Client per session_string and provides
    graceful stopping/cleanup.
    """

    def __init__(self) -> None:
        self._client_cache: Dict[str, Client] = {}
        self._lock = asyncio.Lock()

    async def get_client(self, session_string: str, name_hint: str) -> Optional[Client]:
        if not session_string:
            return None
        async with self._lock:
            if session_string in self._client_cache:
                return self._client_cache[session_string]
            client = Client(
                name_hint,
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                session_string=session_string,
                in_memory=True,
            )
            await client.start()
            self._client_cache[session_string] = client
            return client

    async def stop_all(self) -> None:
        async with self._lock:
            for client in list(self._client_cache.values()):
                try:
                    await client.stop()
                except Exception:
                    pass
            self._client_cache.clear()


# Singleton instance for convenience
pyrogram_clients = PyrogramClientFactory()

