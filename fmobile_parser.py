import asyncio
from curl_cffi.requests import AsyncSession

# Cloudflare IP из DevTools (Remote Address: 104.18.23.0:443)
# DNS не резолвится локально, используем IP напрямую с заголовком Host
BASE_URL = "https://104.18.23.0/catalog/api/v2/catalog/listing"

CITY_SLUGS = {
    "almaty": "Алматы",
    "astana": "Астана",
    "shymkent": "Шымкент",
}

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Host": "apx.fmobile.kz",
    "Origin": "https://fmobile.kz",
    "Referer": "https://fmobile.kz/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}


async def fetch_page(session, page=1, city_slug="almaty", category_slug="smartfony"):
    params = {
        "channel": "ONLINE",
        "city_slug": city_slug,
        "size": 20,
        "page": page,
        "category_slug": category_slug,
        "availability": "is_available",
    }
    r = await session.get(BASE_URL, params=params, headers=HEADERS, verify=False)
    print(f"  [DEBUG] Страница {page} -> {r.status_code}")
    if r.status_code != 200:
        print(f"  Ошибка: {r.text[:200]}")
        return None
    return r.json()


async def get_discounts(city_slug="almaty", category_slug="smartfony", max_pages=10):
    all_discounts = []
    city_name = CITY_SLUGS.get(city_slug, city_slug)

    async with AsyncSession(impersonate="chrome124") as session:
        for page in range(1, max_pages + 1):
            data = await fetch_page(session, page=page, city_slug=city_slug, category_slug=category_slug)

            if not data:
                break

            # Структура из скриншота: result -> items / total
            result = data.get("result", data)  # на случай если ответ сразу items
            items = result.get("items", [])
            total = result.get("total", 0)

            if not items:
                print(f"  Страница {page}: товары закончились.")
                break

            print(f"  Страница {page}: {len(items)} товаров (всего {total})")

            for item in items:
                name = item.get("model_stock_name") or item.get("model_name", "N/A")
                model_id = item.get("model_id")
                model_stock_id = item.get("model_stock_id")
                price = item.get("price", 0)
                discount_price = item.get("discount_price", 0)
                discount_pct = item.get("discount", 0)
                kaspi_amount = item.get("kaspi_amount", 0)
                cashback_fb = item.get("cashback_freedom_bank", 0)
                sku = item.get("model_stock_sku", "")

                # Вычислить discount_price если только процент задан
                if not discount_price and discount_pct and price:
                    discount_price = int(price * (1 - discount_pct / 100))

                if discount_price and price and discount_price < price:
                    pct = round((1 - discount_price / price) * 100)
                    benefit_vs_kaspi = kaspi_amount - discount_price if kaspi_amount > 0 else 0
                    all_discounts.append({
                        "name": name,
                        "sku": sku,
                        "model_id": model_id,
                        "model_stock_id": model_stock_id,
                        "price": price,
                        "discount_price": discount_price,
                        "pct": pct,
                        "kaspi_amount": kaspi_amount,
                        "cashback_fb": cashback_fb,
                        "benefit_vs_kaspi": benefit_vs_kaspi,
                    })

            # Если все страницы пройдены
            if len(items) < 20 or page * 20 >= total:
                break

            await asyncio.sleep(0.5)

    return all_discounts, city_name


async def main():
    print("🚀 Freedom Mobile (apx.fmobile.kz) — парсим скидки...\n")

    discounts, city_name = await get_discounts(city_slug="almaty", category_slug="smartfony")

    print(f"\n{'=' * 60}")
    print(f"  🏙️  Город: {city_name} | 📱 Категория: Смартфоны")
    print(f"  🔥 Найдено акций: {len(discounts)}")
    print(f"{'=' * 60}\n")

    if discounts:
        for d in discounts:
            print(f"📱 {d['name']}")
            print(f"   SKU: {d['sku']}")
            print(f"   💰 Цена: {d['price']:,} ₸  →  {d['discount_price']:,} ₸  (-{d['pct']}%)")
            if d['kaspi_amount'] > 0:
                sign = "✅ Выгоднее Каспи" if d['benefit_vs_kaspi'] > 0 else "❌ На Каспи дешевле"
                print(f"   🏦 Каспи: {d['kaspi_amount']:,} ₸  |  {sign}" +
                      (f" на {d['benefit_vs_kaspi']:,} ₸" if d['benefit_vs_kaspi'] > 0 else ""))
            if d['cashback_fb'] > 0:
                print(f"   🎁 Кэшбэк Freedom Bank: {d['cashback_fb']:,} ₸")
            print()
    else:
        print("🤷 Акций не найдено.")


if __name__ == "__main__":
    asyncio.run(main())
