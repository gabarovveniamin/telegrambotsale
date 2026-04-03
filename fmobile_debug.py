import asyncio
from curl_cffi.requests import AsyncSession

BASE_URL = "https://fmobile.kz/api/v1/products/listing"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Referer": "https://fmobile.kz/category/smartfony",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "x-channel": "ONLINE",
}

# Варианты параметров, которые нужно проверить
VARIANTS = [
    # Вариант 1 — оригинальный
    {"channel": "ONLINE", "city_id": 1, "category_slug": "smartfony", "page": 1, "size": 20},
    # Вариант 2 — slug вместо category_slug
    {"channel": "ONLINE", "city_id": 1, "slug": "smartfony", "page": 1, "size": 20},
    # Вариант 3 — category вместо category_slug
    {"channel": "ONLINE", "city_id": 1, "category": "smartfony", "page": 1, "size": 20},
    # Вариант 4 — без channel
    {"city_id": 1, "category_slug": "smartfony", "page": 1, "size": 20},
    # Вариант 5 — channel в заголовке, не в params
    {"city_id": 1, "slug": "smartfony", "page": 1, "size": 20},
    # Вариант 6 — per_page вместо size
    {"channel": "ONLINE", "city_id": 1, "category_slug": "smartfony", "page": 1, "per_page": 20},
    # Вариант 7 — cityId camelCase
    {"channel": "ONLINE", "cityId": 1, "category_slug": "smartfony", "page": 1, "size": 20},
    # Вариант 8 — без city_id
    {"channel": "ONLINE", "category_slug": "smartfony", "page": 1, "size": 20},
]

# Также проверим альтернативные URL
ALT_URLS = [
    "https://fmobile.kz/api/v1/products/listing",
    "https://fmobile.kz/api/v1/catalog/listing",
    "https://fmobile.kz/api/v1/product/listing",
    "https://fmobile.kz/api/v1/products/catalog",
    "https://fmobile.kz/api/v1/products",
    "https://fmobile.kz/api/v2/products/listing",
]


async def main():
    async with AsyncSession(impersonate="chrome124") as session:
        print("=" * 60)
        print(" ПРОВЕРЯЕМ РАЗНЫЕ ПАРАМЕТРЫ (один URL)")
        print("=" * 60)
        for i, params in enumerate(VARIANTS, 1):
            r = await session.get(BASE_URL, params=params, headers=HEADERS)
            status = r.status_code
            icon = "✅" if status == 200 else "❌"
            print(f"{icon} Вариант {i} [{status}]: {dict(params)}")
            if status == 200:
                data = r.json()
                print(f"   🎉 УСПЕХ! Ключи: {list(data.keys())}")
                break
            await asyncio.sleep(0.3)

        print()
        print("=" * 60)
        print(" ПРОВЕРЯЕМ РАЗНЫЕ URL")
        print("=" * 60)
        base_params = {"channel": "ONLINE", "city_id": 1, "category_slug": "smartfony", "page": 1, "size": 20}
        for url in ALT_URLS:
            r = await session.get(url, params=base_params, headers=HEADERS)
            status = r.status_code
            icon = "✅" if status == 200 else "❌"
            print(f"{icon} [{status}] {url}")
            if status == 200:
                data = r.json()
                print(f"   🎉 ПРАВИЛЬНЫЙ URL! Ответ: {list(data.keys())}")
            await asyncio.sleep(0.3)


if __name__ == "__main__":
    asyncio.run(main())
