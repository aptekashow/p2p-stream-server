import uuid

import httpx
from app.config import settings

class AceStreamClient:
    def __init__(self):
        self.base = settings.acestream_base_url.rstrip("/")

    async def health(self):
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{self.base}/webui/api/service", params={"method": "get_version"})
            r.raise_for_status()
            return r.json().get("result", {})

    async def search(self, query, category=None, page=0, page_size=50):
        params = {"query": query, "page": page, "page_size": min(max(page_size, 1), 200)}
        if category:
            params["category"] = category
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(f"{self.base}/search", params=params)
            r.raise_for_status()
            return r.json()

    async def start_hls(self, infohash):
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(f"{self.base}/ace/manifest.m3u8",
                params={"infohash": infohash, "format": "json", "pid": uuid.uuid4().hex, "use_api_events": 1, "use_stop_notifications": 1})
            r.raise_for_status()
            return r.json()

    async def stop_session(self, command_url):
        if not command_url:
            return {"response": "no_command_url", "error": None}
        parsed = httpx.URL(command_url)
        if parsed.host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("Недопустимый адрес команды AceStream")
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(command_url, params={"method": "stop"})
            response.raise_for_status()
            return response.json()

    async def session_stats(self, stat_url):
        if not stat_url:
            return {}
        parsed = httpx.URL(stat_url)
        if parsed.host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("Недопустимый адрес статистики AceStream")
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(stat_url)
            response.raise_for_status()
            payload = response.json()
            if payload.get("error"):
                raise RuntimeError(payload["error"])
            return payload.get("response") or {}
