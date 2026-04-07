import asyncio
from curl_cffi.requests import AsyncSession

async def dump():
    async with AsyncSession(impersonate="chrome124") as s:
        r = await s.get("https://adidas.kz/muzhchiny/obuv/")
        with open("adidas_dump.html", "w", encoding="utf-8") as f:
            f.write(r.text)
        print(f"Status: {r.status_code}")
        print(f"Title: {r.text[:500]}")

asyncio.run(dump())
