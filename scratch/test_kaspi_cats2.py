import asyncio
from curl_cffi.requests import AsyncSession
import urllib.parse

async def test():
    city_id = "750000000"
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    
    headers = {
        "Accept": "application/json, text/*",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "User-Agent": ua,
        "Referer": "https://kaspi.kz/shop/",
        "Origin": "https://kaspi.kz",
        "X-Ks-City": city_id,
    }
    
    # From kaspi.kz/shop/ URLs
    test_categories = [
        "Smartphones",
        "Smartphones and gadgets",
        "Notebooks",
        "Tablets",
        "Headphones",
        "TV",
        "tv_audio",
        "Refrigerators",
        "Vacuum Cleaners",
        "Monitors",
        "Computers",
        "Desktop Computers",
        "Game consoles",
        "Furniture",
        "Car Goods",
        "home equipment",
        "beauty care",
        "child goods",
        "pharmacy",
        "construction and repair",
        "sports and outdoors",
        "leisure",
        "jewelry and bijouterie",
        "fashion accessories",
        "fashion",
        "shoes",
        "home",
        "gifts and party supplies",
        "pet goods",
        "office and school supplies",
    ]
    
    async with AsyncSession(impersonate="chrome124") as session:
        for cat in test_categories:
            cat_encoded = urllib.parse.quote(cat)
            url = (
                f"https://kaspi.kz/yml/product-view/pl/results"
                f"?q=%3AavailableInZones%3A{city_id}%3Acategory%3A{cat_encoded}"
                f"&sort=relevance&sc=&ui=d&i=-1&c={city_id}"
                f"&page=0&limit=5"
            )
            r = await session.get(url, headers=headers, timeout=15)
            data = r.json() if r.status_code == 200 else {}
            count = len(data.get("data", []))
            status = "OK" if r.status_code == 200 and count > 0 else f"FAIL({r.status_code})"
            # check unitSalePrice vs unitPrice
            has_discount = False
            if count > 0:
                for p in data["data"]:
                    if p.get("unitSalePrice") and p.get("unitPrice") and p["unitSalePrice"] != p["unitPrice"]:
                        has_discount = True
                        break
            disc = "DISC!" if has_discount else ""
            print(f"  {cat:35s} -> {status:10s} items={count} {disc}")
            await asyncio.sleep(0.3)

asyncio.run(test())
