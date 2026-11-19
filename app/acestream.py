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
                params={"infohash": infohash, "format": "json", "use_api_events": 1, "use_stop_notifications": 1})
            r.raise_for_status()
            return r.json()
