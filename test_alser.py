import asyncio
from curl_cffi.requests import AsyncSession

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9"
}

async def check(session, name, url, method="GET", json_data=None):
    try:
        if method == "POST":
            r = await session.post(url, headers=HEADERS, json=json_data, timeout=15)
        else:
            r = await session.get(url, headers=HEADERS, timeout=15)
        print(f"\n[{name}] Status: {r.status_code}")
        if r.status_code == 200:
            text = r.content.decode("utf-8", errors="ignore")
            print(f"[{name}] Length: {len(text)} bytes")
            print(f"[{name}] Preview: {text[:300]}")
        else:
            print(f"[{name}] BLOCKED! Response: {r.text[:200]}")
    except Exception as e:
        print(f"[{name}] Exception: {e}")

async def main():
    async with AsyncSession(impersonate="chrome124") as session:
        # Alser frontend
        await check(session, "ALSER", "https://alser.kz/c/smartfony/_payload.js")

        # Technodom frontend (известно, что заблокирован)
        await check(session, "TECHNODOM FRONTEND", "https://www.technodom.kz/catalog/smartfony-i-gadzhety/smartfony-i-telefony")

        # Technodom API v1 (GET)
        await check(session, "TECHNODOM API v1", "https://api.technodom.kz/katalog/api/v1/products?categoryId=smartfony-i-telefony&cityId=5f5f1e3b4c8a49e692fefd70&limit=10")

        # Technodom API v2 (GET)
        await check(session, "TECHNODOM API v2", "https://api.technodom.kz/katalog/api/v2/products?categoryId=smartfony-i-telefony&cityId=5f5f1e3b4c8a49e692fefd70&limit=10&offset=0")

asyncio.run(main())
