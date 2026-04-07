import asyncio
import logging
from parser import parser
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
import json

logging.basicConfig(level=logging.INFO)

async def test():
    async with AsyncSession(impersonate="chrome124") as session:
        heads = {
            **parser.base_headers,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "https://www.meloman.kz/",
        }
        
        print("Testing Meloman Category: books (with AJAX heads)")
        url = "https://www.meloman.kz/books/"
        r = await session.get(url, headers=heads)
        print(f"Status: {r.status_code}")
        
        try:
            data = r.json()
            html = data.get("categoryProducts") or data.get("products") or r.text
            print("Successfully parsed JSON response")
        except:
            html = r.text
            print("Response was not JSON (or parsing failed)")

        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select(".product-item")
        print(f"Total cards found: {len(cards)}")
        
        if cards:
            pid = cards[0].get("data-product-id") or cards[0].select_one("[data-product-id], [data-id-product]").get("data-product-id")
            print(f"Sample card PID: {pid}")

        print("\nRunning full fetch_meloman...")
        res = await parser.fetch_meloman(session)
        print(f"Meloman finished. Found: {len(res)} discounts")
        for item in res[:5]:
            print(f"  - {item['title']}: {item['old_price']} -> {item['new_price']} ({item['discount']}%)")

if __name__ == "__main__":
    asyncio.run(test())
