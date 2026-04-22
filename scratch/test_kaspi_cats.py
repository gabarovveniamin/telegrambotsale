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
    
    # Try to get category codes from Kaspi
    test_categories = [
        "Smartphones", "Notebooks", "Tablets", 
        "Televisions", "TV", "Television",
        "Refrigerators", "WashingMachines", "Washing Machines",
        "Vacuum Cleaners", "VacuumCleaners",
        "Conditioners", "AirConditioners",
        "Monitors", "Headphones",
        "Smart watches and bracelets", "Smartwatches",
        "Game consoles", "GameConsoles",
        "GPS Navigation",
        "Furniture", "Beauty and Health",
        "Car Goods", "Sport Goods",
        "Computers", "Desktop Computers",
        "Cameras", "Photo", "Cameras and Photo",
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
            total = data.get("total", 0)
            status = "OK" if r.status_code == 200 and count > 0 else f"FAIL({r.status_code})"
            print(f"  {cat:40s} -> {status:10s} items={count} total={total}")
            await asyncio.sleep(0.3)

asyncio.run(test())
