import asyncio
import logging
from parser import parser
from curl_cffi.requests import AsyncSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def debug_test():
    async with AsyncSession(impersonate="chrome124") as session:
        print("\n--- Testing Adidas ---")
        try:
            adidas = await parser.fetch_adidas(session)
            print(f"Adidas found: {len(adidas)} discounts")
            if adidas:
                for item in adidas[:3]:
                    print(f"  - {item['title']}: {item['old_price']} -> {item['new_price']} ({item['discount']}%)")
        except Exception as e:
            print(f"Adidas failed: {e}")

        print("\n--- Testing Freedom Mobile ---")
        try:
            freedom = await parser.fetch_freedom(session)
            print(f"Freedom found: {len(freedom)} discounts")
            if freedom:
                for item in freedom[:3]:
                    print(f"  - {item['title']}: {item['old_price']} -> {item['new_price']} ({item['discount']}%)")
        except Exception as e:
            print(f"Freedom failed: {e}")

        print("\n--- Testing Sulpak ---")
        try:
            sulpak = await parser.fetch_sulpak(session)
            print(f"Sulpak found: {len(sulpak)} discounts")
            if sulpak:
                for item in sulpak[:3]:
                    print(f"  - {item['title']}: {item['old_price']} -> {item['new_price']} ({item['discount']}%)")
        except Exception as e:
            print(f"Sulpak failed: {e}")

        print("\n--- Testing Intertop ---")
        try:
            intertop = await parser.fetch_intertop(session)
            print(f"Intertop found: {len(intertop)} discounts")
            if intertop:
                for item in intertop[:3]:
                    print(f"  - {item['title']}: {item['old_price']} -> {item['new_price']} ({item['discount']}%)")
        except Exception as e:
            print(f"Intertop failed: {e}")

if __name__ == "__main__":
    asyncio.run(debug_test())
