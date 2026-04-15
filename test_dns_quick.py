#!/usr/bin/env python3
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from curl_cffi.requests import AsyncSession
from parser import DiscountParser

logging.basicConfig(level=logging.INFO, format='%(message)s')

async def test_dns():
    parser = DiscountParser()
    print("🚀 Тестирую DNS парсер...\n")
    
    async with AsyncSession(impersonate="chrome124") as session:
        products = await parser.fetch_dns(session)
        
        if products:
            print(f"✅ DNS: Найдено {len(products)} товаров")
            print("\n📦 Топ 3 товара:")
            for i, p in enumerate(products[:3], 1):
                print(f"  {i}. {p['title'][:50]} - {p['discount']}% скидка - {p['new_price']}")
            return True
        else:
            print("❌ DNS: Товаров не найдено")
            return False

if __name__ == "__main__":
    result = asyncio.run(test_dns())
    sys.exit(0 if result else 1)
