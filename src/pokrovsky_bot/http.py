from typing import Optional
import aiohttp

from .config import HEADERS

async def fetch_text(url: str, session: Optional[aiohttp.ClientSession] = None) -> str:
    timeout = aiohttp.ClientTimeout(total=35)
    if session is None:
        async with aiohttp.ClientSession(timeout=timeout, headers=HEADERS) as s:
            async with s.get(url) as r:
                r.raise_for_status()
                return await r.text()
    async with session.get(url) as r:
        r.raise_for_status()
        return await r.text()
