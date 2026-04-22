import asyncio
import logging
import sys
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(name)s - %(message)s')

sys.path.insert(0, '/home/ubuntu/telegrambotsale')

from curl_cffi.requests import AsyncSession
import random
import re

async def test_kaspi_api():
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    city_id = "750000000"
    
    categories = [
        {"code": "Smartphones", "label": "tech"},
        {"code": "Televisions", "label": "tech"},
        {"code": "Notebooks", "label": "tech"},
    ]
    
    headers = {
        "Accept": "application/json, text/*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "User-Agent": user_agent,
        "Referer": "https://kaspi.kz/shop/",
        "Origin": "https://kaspi.kz",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Ks-City": city_id,
    }
    
    total = 0
    async with AsyncSession(impersonate="chrome124") as session:
        for cat in categories:
            url = (
                f"https://kaspi.kz/yml/product-view/pl/results"
                f"?q=%3AavailableInZones%3A{city_id}%3Acategory%3A{cat['code']}"
                f"&sort=relevance&sc=&ui=d&i=-1&c={city_id}"
                f"&page=0&limit=20"
            )
            r = await session.get(url, headers=headers, timeout=20)
            print(f"\n=== {cat['code']} ===")
            print(f"Status: {r.status_code}")
            
            if r.status_code == 200:
                data = r.json()
                products = data.get("data", [])
                print(f"Products count: {len(products)}")
                total += len(products)
                
                for p in products[:3]:
                    pid = p.get("id")
                    title = p.get("title", "")[:60]
                    price = p.get("unitSalePrice") or p.get("unitPrice")
                    sale_price = p.get("unitSalePrice")
                    orig_price = p.get("unitPrice")
                    print(f"  [{pid}] {title}  price={price} (sale={sale_price}, orig={orig_price})")
            
            await asyncio.sleep(1)
    
    print(f"\n=== TOTAL: {total} products from {len(categories)} categories ===")

asyncio.run(test_kaspi_api())
