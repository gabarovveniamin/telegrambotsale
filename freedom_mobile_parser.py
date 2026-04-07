"""
Freedom Mobile Parser
Парсит цены и данные товаров с api.fmobile.kz
"""

import requests
import json
import csv
import time
from datetime import datetime


# ─── НАСТРОЙКИ ───────────────────────────────────────────────────────────────

BASE_URL = "https://api.fmobile.kz/catalog/api/v2/catalog/listing"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://fmobile.kz/",
    "Origin": "https://fmobile.kz",
}

PARAMS_DEFAULT = {
    "channel": "ONLINE",
    "city_slug": "almaty",   # можно менять: astana, shymkent и т.д.
    "page": 1,
    "size": 50,              # максимум за раз
}


# ─── ФУНКЦИИ ─────────────────────────────────────────────────────────────────

def fetch_page(page: int, city_slug: str = "almaty", model_ids: list = None) -> dict:
    """Загружает одну страницу листинга."""
    params = {**PARAMS_DEFAULT, "page": page, "city_slug": city_slug}
    if model_ids:
        params["model_ids"] = ",".join(map(str, model_ids))

    response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=15)
    response.raise_for_status()
    return response.json()


def parse_product(item: dict) -> dict:
    """Извлекает нужные поля из одного товара."""
    # Обработка цен с учетом структуры API
    price = item.get("price")
    old_price = item.get("old_price") or 0
    
    return {
        "name":              item.get("model_stock_name") or item.get("name") or "",
        "brand":             item.get("brand_name", ""),
        "category":          item.get("category_name", ""),
        "sku":               item.get("sku", ""),
        "model_id":          item.get("model_id"),
        "model_stock_id":    item.get("model_stock_id"),
        "price":             price,
        "old_price":         old_price,
        "kaspi_amount":      item.get("kaspi_amount"),     # цена Kaspi
        "cashback":          item.get("cashback_freedom_bank"),
        "slug":              item.get("model_stock_slug", ""),
        "image":             item.get("image_path", ""),
        "parsed_at":         datetime.now().isoformat(),
    }


def fetch_all_products(city_slug: str = "almaty", model_ids: list = None,
                       delay: float = 0.5) -> list[dict]:
    """
    Пагинирует все страницы и возвращает список товаров.
    delay — пауза между запросами в секундах (чтобы не банили).
    """
    products = []
    page = 1

    while True:
        print(f"  📦 Страница {page}...", end=" ")
        try:
            data = fetch_page(page, city_slug, model_ids)
        except Exception as e:
            print(f"ошибка: {e}")
            break

        # Структура из API: data['result']['items']
        result_node = data.get("result") or {}
        items = result_node.get("items") or []

        if not items:
            print("пусто, стоп.")
            break

        for item in items:
            products.append(parse_product(item))

        print(f"получено {len(items)} товаров (всего: {len(products)})")

        # Проверяем есть ли следующая страница
        total = result_node.get("total") or 0
        if total and len(products) >= total:
            break
        if len(items) < PARAMS_DEFAULT["size"]:
            break

        page += 1
        time.sleep(delay)

    return products


def save_to_json(products: list[dict], filename: str = "freedom_products.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON сохранён: {filename} ({len(products)} товаров)")


def save_to_csv(products: list[dict], filename: str = "freedom_products.csv"):
    if not products:
        print("⚠️  Нет данных для сохранения.")
        return
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=products[0].keys())
        writer.writeheader()
        writer.writerows(products)
    print(f"✅ CSV сохранён: {filename} ({len(products)} товаров)")


# ─── ГЛАВНАЯ ФУНКЦИЯ ──────────────────────────────────────────────────────────

def main():
    print("🚀 Freedom Mobile Parser запущен\n")

    city = "almaty"
    print(f"🏙️  Город: {city}")
    products = fetch_all_products(city_slug=city, delay=0.5)

    if products:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        save_to_json(products, f"freedom_{city}_{timestamp}.json")
        save_to_csv(products,  f"freedom_{city}_{timestamp}.csv")

        # Быстрая сводка по ценам
        print("\n📊 Топ-5 по скидке:")
        # Фильтруем товары со скидкой
        discounted = [p for p in products if p["old_price"] and p["old_price"] > p["price"]]
        discounted.sort(key=lambda x: (x["old_price"] - x["price"]) / x["old_price"], reverse=True)
        
        for p in discounted[:5]:
            diff = p["old_price"] - p["price"]
            percent = round(diff / p["old_price"] * 100)
            print(f"  -{percent}% | {p['name'][:45]:<45} | {p['old_price']:>8} -> {p['price']:>8} ₸")
    else:
        print("❌ Товары не найдены.")


if __name__ == "__main__":
    main()
