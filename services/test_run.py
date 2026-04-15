import asyncio
from scraper import scraper_service

async def main():
    urls = [
        "https://kaspi.kz/shop/p/apple-iphone-14-128gb-nanosim-esim-chernyi-106363023/?c=750000000",
        "https://kaspi.kz/shop/p/apple-iphone-14-128gb-nanosim-esim-bezhevyi-106363144/?c=750000000",
        "https://www.sulpak.kz/g/smartfon_apple_iphone_13_128gb_midnight_mlpf3rua"
    ]
    for url in urls:
        print(f"Testing {url}...")
        price = await scraper_service.fetch_price(url)
        print(f"Price: {price}")

asyncio.run(main())
