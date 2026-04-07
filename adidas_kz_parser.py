"""
Adidas KZ Parser
Парсит все категории adidas.kz: обувь, одежда, аксессуары
Извлекает: название, цена, скидка, ссылка
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
from datetime import datetime


# ─── НАСТРОЙКИ ───────────────────────────────────────────────────────────────

BASE_URL = "https://adidas.kz"

CATEGORIES = {
    "muzhchina/obuv":          "Мужская обувь",
    "zhenshchina/obuv":        "Женская обувь",
    "deti/obuv":               "Детская обувь",
    "muzhchina/odezhda":       "Мужская одежда",
    "zhenshchina/odezhda":     "Женская одежда",
    "deti/odezhda":            "Детская одежда",
    "aksessuary":              "Аксессуары",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://adidas.kz/",
}


# ─── ПАРСИНГ СТРАНИЦЫ КАТЕГОРИИ ───────────────────────────────────────────────

def fetch_html(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        print(f"  ❌ Ошибка запроса {url}: {e}")
        return None


def parse_json_ld(html: str) -> list[dict]:
    """
    adidas.kz встраивает данные в JSON-LD (<script type='application/ld+json'>)
    Там есть ItemList с продуктами и ценами.
    """
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", {"type": "application/ld+json"})

    products = []

    for script in scripts:
        try:
            content = script.string or ""
            if not content: continue
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            continue

        # Может быть массив или один объект
        if isinstance(data, list):
            items = data
        else:
            items = [data]

        for item in items:
            # ItemList — список товаров на странице категории
            if item.get("@type") == "ItemList":
                for list_item in item.get("itemListElement", []):
                    product = extract_product(list_item)
                    if product:
                        products.append(product)

            # Один продукт
            elif item.get("@type") == "Product":
                product = extract_product(item)
                if product:
                    products.append(product)

    return products


def extract_product(item: dict) -> dict | None:
    """Извлекает поля из одного элемента JSON-LD."""

    # ListItem содержит вложенный item
    if item.get("@type") == "ListItem":
        inner = item.get("item", {})
        url_from_list = item.get("url", "")
    else:
        inner = item
        url_from_list = ""

    if not inner:
        return None

    name = inner.get("name", "").strip()
    if not name:
        return None

    url = inner.get("url") or url_from_list or ""
    if url and not url.startswith("http"):
        url = BASE_URL + url

    brand = inner.get("brand", {})
    if isinstance(brand, dict):
        brand = brand.get("name", "adidas")

    # Цены из offers
    offers = inner.get("offers", {})
    if isinstance(offers, list):
        offers = offers[0] if offers else {}

    price = None
    original_price = None
    sku = None
    availability = None

    if isinstance(offers, dict):
        price_raw = offers.get("price") or offers.get("lowPrice")
        try:
            if price_raw:
                price = int(float(str(price_raw).replace(",", ".").replace(" ", "")))
        except (ValueError, TypeError):
            price = None

        original_price_raw = offers.get("highPrice")
        try:
            if original_price_raw:
                original_price = int(float(str(original_price_raw).replace(",", ".").replace(" ", "")))
        except (ValueError, TypeError):
            original_price = None

        sku = offers.get("sku") or inner.get("sku", "")
        availability_raw = offers.get("availability", "")
        availability = "В наличии" if "InStock" in str(availability_raw) else "Нет в наличии"

    # Считаем скидку
    discount_percent = None
    discount_amount = None
    if price and original_price and original_price > price:
        discount_amount = original_price - price
        discount_percent = round((discount_amount / original_price) * 100)

    return {
        "name":             name,
        "brand":            brand,
        "price":            price,
        "original_price":   original_price,
        "discount_percent": discount_percent,
        "discount_amount":  discount_amount,
        "sku":              sku,
        "availability":     availability,
        "url":              url,
        "parsed_at":        datetime.now().isoformat(),
    }


# ─── ПАРСИНГ КАРТОЧЕК ИЗ HTML (запасной способ) ──────────────────────────────

def parse_cards_from_html(html: str, category_url: str) -> list[dict]:
    """
    Если JSON-LD не дал результатов — парсим карточки из HTML напрямую.
    Ищем типичные структуры adidas.kz.
    """
    soup = BeautifulSoup(html, "html.parser")
    products = []

    # Карточки товаров (селекторы могут измениться при редизайне)
    cards = (
        soup.select("article[data-auto-id='product-card']") or
        soup.select("[class*='product-card']") or
        soup.select("[class*='ProductCard']") or
        soup.select("li[class*='product']")
    )

    for card in cards:
        name_el = (
            card.select_one("[class*='name']") or
            card.select_one("[class*='title']") or
            card.select_one("h3") or
            card.select_one("h2")
        )
        name = name_el.get_text(strip=True) if name_el else ""

        price_el = card.select_one("[class*='sale-price'], [class*='salePrice'], [class*='price']")
        original_el = card.select_one("[class*='original-price'], [class*='originalPrice'], [class*='crossed']")

        price = extract_price_text(price_el.get_text() if price_el else "")
        original_price = extract_price_text(original_el.get_text() if original_el else "")

        link_el = card.select_one("a[href]")
        url = ""
        if link_el:
            href = link_el["href"]
            url = href if href.startswith("http") else BASE_URL + (href if href.startswith("/") else "/" + href)

        discount_percent = None
        discount_amount = None
        if price and original_price and original_price > price:
            discount_amount = original_price - price
            discount_percent = round((discount_amount / original_price) * 100)

        if name and price:
            products.append({
                "name":             name,
                "brand":            "adidas",
                "price":            price,
                "original_price":   original_price,
                "discount_percent": discount_percent,
                "discount_amount":  discount_amount,
                "sku":              "",
                "availability":     "В наличии",
                "url":              url,
                "parsed_at":        datetime.now().isoformat(),
            })

    return products


def extract_price_text(text: str) -> int | None:
    """'109 990 ₸' → 109990"""
    import re
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


# ─── ПАГИНАЦИЯ ────────────────────────────────────────────────────────────────

def parse_category(slug: str, label: str, delay: float = 1.0) -> list[dict]:
    """Парсит все страницы одной категории."""
    print(f"\n📂 Категория: {label}")
    all_products = []
    page = 1

    while True:
        # adidas.kz использует ?start=N для пагинации (по 48 товаров)
        start = (page - 1) * 48
        
        if page == 1:
            url = f"{BASE_URL}/{slug}/"
        else:
            url = f"{BASE_URL}/{slug}/?start={start}"

        print(f"  📄 Страница {page} (start={start})...", end=" ")

        html = fetch_html(url)
        if not html:
            break

        # Сначала пробуем JSON-LD
        products = parse_json_ld(html)

        # Если не нашли — парсим HTML карточки
        if not products:
            products = parse_cards_from_html(html, url)

        if not products:
            print("товары не найдены, стоп.")
            break

        # Добавляем категорию к каждому товару
        for p in products:
            p["category"] = label

        all_products.extend(products)
        print(f"найдено {len(products)} (всего: {len(all_products)})")

        # Проверяем есть ли кнопка "Показать ещё" / следующая страница
        soup = BeautifulSoup(html, "html.parser")
        has_next = (
            soup.select_one("[class*='load-more'], [class*='loadMore'], [class*='next-page']") or
            soup.select_one("a[href*='start=']")
        )

        if not has_next or len(products) < 10:
            break

        page += 1
        time.sleep(delay)

    return all_products


# ─── СОХРАНЕНИЕ ──────────────────────────────────────────────────────────────

def save_json(products: list, filename: str):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON: {filename}")


def save_csv(products: list, filename: str):
    if not products:
        return
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=products[0].keys())
        writer.writeheader()
        writer.writerows(products)
    print(f"✅ CSV:  {filename}")


# ─── ФОРМАТИРОВАНИЕ ДЛЯ БОТА ─────────────────────────────────────────────────

def format_for_bot(product: dict) -> str:
    """Форматирует товар для отправки в Telegram бота."""
    lines = [f"👟 *{product['name']}*"]

    if product.get("price"):
        lines.append(f"💰 Цена: *{product['price']:,} ₸*".replace(",", " "))

    if product.get("discount_percent"):
        lines.append(f"🔥 Скидка: *{product['discount_percent']}%* "
                     f"(-{product['discount_amount']:,} ₸)".replace(",", " "))
        if product.get("original_price"):
            lines.append(f"~~{product['original_price']:,} ₸~~".replace(",", " "))

    if product.get("url"):
        lines.append(f"🔗 [Ссылка]({product['url']})")

    return "\n".join(lines)


# ─── ГЛАВНАЯ ФУНКЦИЯ ──────────────────────────────────────────────────────────

def main():
    print("🚀 Adidas KZ Parser запущен\n")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    all_products = []

    for slug, label in CATEGORIES.items():
        try:
            products = parse_category(slug, label, delay=1.5)
            all_products.extend(products)
        except Exception as e:
            print(f"  ⚠️  Ошибка в категории {label}: {e}")
        time.sleep(2)

    if all_products:
        save_json(all_products, f"adidas_kz_{timestamp}.json")
        save_csv(all_products,  f"adidas_kz_{timestamp}.csv")

        # Статистика
        with_discount = [p for p in all_products if p.get("discount_percent")]
        print(f"\n📊 Итого товаров: {len(all_products)}")
        print(f"🔥 Со скидкой: {len(with_discount)}")

        # Топ скидок
        if with_discount:
            print("\n🏆 Топ-5 скидок:")
            for p in sorted(with_discount, key=lambda x: x["discount_percent"], reverse=True)[:5]:
                print(f"  {p['discount_percent']}% | {p['price']:>8} ₸ | {p['name'][:45]}")

        # Пример форматирования для бота
        print("\n─── Пример для Telegram бота ───")
        if with_discount:
            print(format_for_bot(with_discount[0]))
    else:
        print("❌ Товары не найдены. Возможно сайт требует JS-рендеринг.")
        print("   Попробуй запустить с Playwright: pip install playwright")


if __name__ == "__main__":
    main()
