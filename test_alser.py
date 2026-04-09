import asyncio
from curl_cffi.requests import AsyncSession

CITY_ID = "5f5f1e3b4c8a49e692fefd70"
BASE = "https://api.technodom.kz"

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
        print(f"\n[{name}]  Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            products = data.get("products") or []
            total = data.get("total", 0)
            print(f"  total={total}, products={len(products)}")
            if products:
                p = products[0]
                print(f"  First: {p.get('name') or p.get('title')} | price={p.get('price')} oldPrice={p.get('old_price') or p.get('oldPrice')}")
        else:
            print(f"  Error: {r.text[:200]}")
    except Exception as e:
        print(f"  Exception: {e}")

async def get_check(session, name, url):
    try:
        r = await session.get(url, headers=API_HEADERS, timeout=15)
        print(f"\n[{name}]  Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            products = data.get("products") or data.get("items") or []
            print(f"  Keys: {list(data.keys())}")
            print(f"  Products: {len(products)}")
        else:
            print(f"  Error: {r.text[:200]}")
    except Exception as e:
        print(f"  Exception: {e}")

async def main():
    async with AsyncSession(impersonate="chrome124") as session:
        # Keyword search (старый подход)
        await post_check(session, "Keyword: смартфон",
            f"{BASE}/katalog/api/v2/products/search",
            {"categories": [""], "city_id": CITY_ID, "query": "смартфон", "limit": 10, "page": 1, "sort_by": "popular", "type": "full_search"}
        )
        # Keyword search с фильтром скидок
        await post_check(session, "Keyword: смартфон + discount",
            f"{BASE}/katalog/api/v2/products/search",
            {"categories": [""], "city_id": CITY_ID, "query": "смартфон", "limit": 10, "page": 1, "sort_by": "popular", "type": "full_search", "filter": "discount"}
        )
        # Попытка найти category endpoint
        await get_check(session, "Category GET v1",
            f"{BASE}/katalog/api/v1/catalog/smartfony-i-telefony?cityId={CITY_ID}&limit=10"
        )
        await get_check(session, "Category GET v2",
            f"{BASE}/katalog/api/v2/catalog?categoryId=smartfony-i-telefony&cityId={CITY_ID}&limit=10"
        )
        await get_check(session, "Products by category GET",
            f"{BASE}/katalog/api/v2/products?categorySlug=smartfony-i-telefony&cityId={CITY_ID}&limit=10"
        )

asyncio.run(main())
