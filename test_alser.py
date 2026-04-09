import asyncio
import json
from curl_cffi.requests import AsyncSession

CITY_ID = "5f5f1e3b4c8a49e692fefd70"

API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Content-Language": "ru-RU",
    "Affiliation": "web",
    "Origin": "https://www.technodom.kz",
    "Referer": "https://www.technodom.kz/",
}

async def post_check(session, name, url, payload):
    try:
        r = await session.post(url, headers=API_HEADERS, json=payload, timeout=15)
        print(f"\n[{name}] POST {url}")
        print(f"  Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            items = data.get("items") or data.get("products") or data.get("data") or []
            print(f"  Items found: {len(items)}")
            print(f"  Keys: {list(data.keys())}")
            if items:
                print(f"  First item keys: {list(items[0].keys())}")
        else:
            print(f"  Error: {r.text[:300]}")
    except Exception as e:
        print(f"  Exception: {e}")

async def main():
    async with AsyncSession(impersonate="chrome124") as session:
        # Вариант 1 — с categoryId
        await post_check(session, "Search v2 (categoryId)", 
            "https://api.technodom.kz/katalog/api/v2/products/search",
            {"categoryId": "smartfony-i-telefony", "city_id": CITY_ID, "limit": 24, "offset": 0, "sorting": "score", "type": "category"}
        )
        # Вариант 2 — с categories как массив
        await post_check(session, "Search v2 (categories array)",
            "https://api.technodom.kz/katalog/api/v2/products/search",
            {"categories": ["smartfony-i-telefony"], "city_id": CITY_ID, "limit": 24, "page": 1, "sort_by": "score", "type": "category"}
        )
        # Вариант 3 — с query пустой и categoryId
        await post_check(session, "Search v2 (empty query + categoryId)",
            "https://api.technodom.kz/katalog/api/v2/products/search",
            {"categories": ["smartfony-i-gadzhety/smartfony-i-telefony"], "city_id": CITY_ID, "limit": 24, "page": 1, "sort_by": "score", "type": "full_search", "query": ""}
        )

asyncio.run(main())
