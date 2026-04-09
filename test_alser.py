import asyncio
from curl_cffi.requests import AsyncSession

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9"
}

async def check(session, name, url):
    try:
        r = await session.get(url, headers=HEADERS, timeout=15)
        print(f"\n[{name}] Status: {r.status_code}")
        if r.status_code == 200:
            text = r.content.decode("utf-8", errors="ignore")
            print(f"[{name}] Length: {len(text)} bytes")
            print(f"[{name}] Preview: {text[:200]}")
        else:
            print(f"[{name}] BLOCKED! Response: {r.text[:150]}")
    except Exception as e:
        print(f"[{name}] Exception: {e}")

async def main():
    async with AsyncSession(impersonate="chrome124") as session:
        await check(session, "ALSER", "https://alser.kz/c/smartfony/_payload.js")
        await check(session, "TECHNODOM", "https://www.technodom.kz/catalog/smartfony-i-gadzhety/smartfony-i-telefony")

asyncio.run(main())
