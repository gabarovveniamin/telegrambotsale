import asyncio
from curl_cffi.requests import AsyncSession
import json

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

async def post_check(session, name, payload):
    r = await session.post(f"{BASE}/katalog/api/v2/products/search", headers=API_HEADERS, json=payload, timeout=15)
    print(f"\n[{name}] Status: {r.status_code}, total={r.json().get('total')}, products={len(r.json().get('products', []))}")
    prods = r.json().get("products", [])
    # Показываем товары со скидками
    discounted = [p for p in prods if p.get("old_price") and p.get("old_price") != p.get("price")]
    print(f"  С разными ценами: {len(discounted)}")
    if prods:
        print(f"  Все ключи 1-го товара: {list(prods[0].keys())}")
        p = prods[0]
        print(f"  name={p.get('name')}")
        print(f"  price={p.get('price')}, old_price={p.get('old_price')}, discount={p.get('discount')}")
        print(f"  is_sale={p.get('is_sale')}, stickers={p.get('stickers')}")

async def main():
    async with AsyncSession(impersonate="chrome124") as session:
        # Базовый keyword поиск
        await post_check(session, "смартфон базовый",
            {"categories": [""], "city_id": CITY_ID, "query": "смартфон", "limit": 30, "page": 1, "sort_by": "popular", "type": "full_search"}
        )
        # С сортировкой по скидке
        await post_check(session, "смартфон sort=discount",
            {"categories": [""], "city_id": CITY_ID, "query": "смартфон", "limit": 30, "page": 1, "sort_by": "discount", "type": "full_search"}
        )
        # С фильтром is_sale
        await post_check(session, "смартфон + filter promotions",
            {"categories": [""], "city_id": CITY_ID, "query": "смартфон", "limit": 30, "page": 1, "sort_by": "popular", "type": "full_search", "promotions": True}
        )
        # Попробуем ноутбуки
        await post_check(session, "ноутбук базовый",
            {"categories": [""], "city_id": CITY_ID, "query": "ноутбук", "limit": 30, "page": 1, "sort_by": "popular", "type": "full_search"}
        )

asyncio.run(main())
