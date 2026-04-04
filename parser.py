import asyncio
import logging
import json
import re
from typing import List, Dict, Any, Optional

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

# ─── Константы городов ────────────────────────────────────────────────────────
CITY_ID_TECHNODOM = "5f5f1e3b4c8a49e692fefd70"  # Алматы (MongoDB ObjectId)
CITY_ID_SULPAK    = "1"                           # Алматы
CITY_SLUG_MECHTA  = "almaty"
CITY_ID_KASPI     = "750000000"                   # Алматы (Kaspi geo id)
CITY_SLUG_ALSER   = "almaty"

# ─── Настройки ────────────────────────────────────────────────────────────────
MAX_PAGES      = 5    # максимум страниц на магазин
PAGE_SIZE      = 30   # товаров на страницу
RETRY_COUNT    = 3    # попыток при ошибке
RETRY_DELAY    = 2.0  # секунд между попытками


def fmt_price(value) -> str:
    """Форматирует цену в строку с ₸ или возвращает '—'."""
    if value is None:
        return "—"
    val = str(value).strip()
    if not val or val == "0":
        return "—"
    val = re.sub(r"\s+", " ", val)
    return f"{val} ₸"


def calc_discount(old: Any, new: Any) -> int:
    """Считает процент скидки. Возвращает 0 если не удалось."""
    try:
        o = float(re.sub(r"[^\d.]", "", str(old)))
        n = float(re.sub(r"[^\d.]", "", str(new)))
        if o > 0 and n < o:
            return round((o - n) / o * 100)
    except Exception:
        pass
    return 0


async def safe_request(
    session: AsyncSession,
    method: str,
    url: str,
    *,
    headers: Optional[Dict] = None,
    json: Optional[Dict] = None,
    content: Optional[str] = None,
    params: Optional[Dict] = None,
    timeout: int = 20,
) -> Optional[Any]:
    """
    Выполняет HTTP-запрос с повторными попытками.
    Возвращает объект ответа или None при неудаче.
    """
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            if method.upper() == "POST":
                r = await session.post(
                    url, headers=headers, json=json,
                    data=content, timeout=timeout
                )
            else:
                r = await session.get(
                    url, headers=headers, params=params, timeout=timeout
                )

            if r.status_code == 200:
                return r
            logger.warning(
                f"[attempt {attempt}/{RETRY_COUNT}] {url} → HTTP {r.status_code}"
            )
        except Exception as e:
            logger.warning(f"[attempt {attempt}/{RETRY_COUNT}] {url} → {e}")

        if attempt < RETRY_COUNT:
            await asyncio.sleep(RETRY_DELAY)

    logger.error(f"Все {RETRY_COUNT} попытки исчерпаны: {url}")
    return None


class DiscountParser:
    def __init__(self):
        self.impersonate = "chrome"
        self.base_headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        }

    # ─────────────────────────────────────────────────────────────────────────
    #  TECHNODOM
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_technodom(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """POST /katalog/api/v2/products/search — фильтр discount_products."""
        url = "https://api.technodom.kz/katalog/api/v2/products/search"
        headers = {
            **self.base_headers,
            "Content-Type": "application/json",
            "affiliation": "web",
            "content-language": "ru-RU",
            "Origin": "https://www.technodom.kz",
            "Referer": "https://www.technodom.kz/",
        }
        result: List[Dict[str, Any]] = []

        queries = [
            "смартфон", "ноутбук", "телевизор", "холодильник", 
            "стиральная", "наушники", "пылесос", "часы", "печь", "iphone", "samsung", "пк", "монитор"
        ]
        seen: set = set()

        for q in queries:
            for page in range(1, 5):
                payload = {
                    "categories": [""],
                    "city_id": CITY_ID_TECHNODOM,
                    "query": q,
                    "limit": PAGE_SIZE,
                    "page": page,
                    "sort_by": "popular",
                    "type": "full_search",
                }
                r = await safe_request(session, "POST", url, headers=headers, json=payload)
                if r is None:
                    break

                try:
                    data = r.json()
                except Exception as e:
                    logger.error(f"Technodom JSON parse error (page {page}): {e}")
                    break

                products = data.get("products") or data.get("data") or data.get("items") or []
                if not products:
                    break

                for p in products:
                    pid   = str(p.get("id") or p.get("sku") or "").strip()
                    if pid in seen:
                        continue
                    seen.add(pid)
                    title = (p.get("name") or p.get("title") or "").strip()
                    slug  = (p.get("slug") or p.get("url_key") or p.get("uri") or "").strip()
                    old_p = p.get("old_price") or p.get("price_old") or p.get("crossed_price")
                    new_p = p.get("price") or p.get("price_new") or p.get("sell_price")

                    if not (pid and title and slug):
                        continue
                    if not old_p or str(old_p) == str(new_p):
                        continue

                    result.append({
                        "id":        f"td_{pid}",
                        "title":     title,
                        "old_price": fmt_price(old_p),
                        "new_price": fmt_price(new_p),
                        "discount":  calc_discount(old_p, new_p),
                        "link":      f"https://www.technodom.kz/p/{slug}",
                        "shop":      "Technodom",
                    })

                if len(products) < PAGE_SIZE:
                    break
                    
                await asyncio.sleep(0.3)

        logger.info(f"Technodom: найдено {len(result)} акций")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  SULPAK
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_sulpak(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """
        GET /SaleLoadProducts/{page}/...
        Данные берём из data-атрибутов карточек — стабильно.
        """
        result: List[Dict[str, Any]] = []

        for page in range(1, MAX_PAGES + 1):
            url = (
                f"https://www.sulpak.kz/SaleLoadProducts/{page}"
                f"/~/~/0-2147483647/~/~/popularitydesc/tiles"
            )
            headers = {
                **self.base_headers,
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.sulpak.kz/sale/",
            }
            r = await safe_request(session, "POST", url, headers=headers)
            if r is None:
                break

            try:
                data = r.json()
            except Exception as e:
                logger.error(f"Sulpak JSON parse error (page {page}): {e}")
                break

            html = data.get("products", "")
            if not html:
                break

            soup  = BeautifulSoup(html, "html.parser")
            cards = soup.select("div.product__item-js")
            if not cards:
                logger.warning(f"Sulpak page {page}: карточки не найдены")
                break

            for card in cards:
                title  = card.get("data-name", "").strip()
                code   = card.get("data-code", "").strip()
                new_p  = card.get("data-price", "").strip()
                a_tag  = card.select_one("a.product__item-images")
                slug   = a_tag["href"] if a_tag else ""
                old_tag = card.select_one(".product__item-price-old")
                old_p   = re.sub(r"[^\d]", "", old_tag.get_text()) if old_tag else ""

                if not (title and code and slug and old_p):
                    continue

                new_p_clean = str(int(float(new_p))) if new_p else ""

                result.append({
                    "id":        f"sp_{code}",
                    "title":     title,
                    "old_price": fmt_price(old_p),
                    "new_price": fmt_price(new_p_clean),
                    "discount":  calc_discount(old_p, new_p_clean),
                    "link":      f"https://www.sulpak.kz{slug}",
                    "shop":      "Sulpak",
                })

            paginator = data.get("paginator", "")
            if f"/{page + 1}/" not in paginator:
                break

        logger.info(f"Sulpak: найдено {len(result)} акций")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  MECHTA
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_mechta(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """GET /api/v3/catalog/products -> GET offers"""
        result: List[Dict[str, Any]] = []

        headers = {
            **self.base_headers,
            "x-city": CITY_SLUG_MECHTA, 
            "Referer": "https://www.mechta.kz/"
        }

        # Самые популярные категории для парсинга
        categories = ["smartfony", "noutbuki", "televizory", "holodilniki-i-morozilniki", "stiralnye-i-sushilnye-mashiny"]

        for category in categories:
            for page in range(1, 4): # Ограничим глубину
                # Шаг 1: Получаем список ID товаров
                url_products = f"https://www.mechta.kz/api/v3/catalog/products?filter%5Bslug%5D={category}&page={page}&page_size=24"
                
                r_prod = await safe_request(session, "GET", url_products, headers=headers)
                if r_prod is None: break
                
                try: prods_data = r_prod.json()
                except: break
                    
                # В ответе обычно JSON массив на верхнем уровне или в data
                items = prods_data if isinstance(prods_data, list) else prods_data.get("data", [])
                if not items: break
                    
                product_ids = []
                prod_map = {}
                
                for item in items:
                    pid = str(item.get("productId") or "").strip()
                    if not pid: continue
                    product_ids.append(pid)
                    prod_map[pid] = {
                        "title": item.get("name") or item.get("title") or "Без названия",
                        "slug": item.get("slug") or pid
                    }
                    
                if not product_ids: break
                    
                # Шаг 2: Запрашиваем цены для этих ID 
                import urllib.parse
                q_string = "&".join([f"productIds%5B%5D={urllib.parse.quote(pid)}" for pid in product_ids])
                url_offers = f"https://www.mechta.kz/api/v3/catalog/offers?{q_string}"
                
                r_off = await safe_request(session, "GET", url_offers, headers=headers)
                if r_off is None: continue
                
                try: offers_data = r_off.json()
                except: continue
                    
                offers_list = offers_data if isinstance(offers_data, list) else offers_data.get("data", [])
                
                for offer in offers_list:
                    pid = str(offer.get("productId"))
                    if pid not in prod_map: continue
                    
                    prices = offer.get("prices") or {}
                    base = prices.get("basePrice")
                    final = prices.get("finalPrice")
                    
                    # Проверяем на наличие реальной скидки
                    if not base or not final or float(base) <= float(final):
                        continue
                        
                    meta = prod_map[pid]
                    result.append({
                        "id":        f"mc_{pid}",
                        "title":     meta["title"],
                        "old_price": fmt_price(base),
                        "new_price": fmt_price(final),
                        "discount":  calc_discount(base, final),
                        "link":      f"https://www.mechta.kz/product/{meta['slug']}",
                        "shop":      "Mechta 🔵",
                    })

        logger.info(f"Mechta: найдено {len(result)} акций")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  KASPI
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_kaspi(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """
        Kaspi.kz — товары со скидкой, сортировка по убыванию скидки.

        Эндпоинт (нашли через DevTools):
          GET /yml/offer-service/api/v1/offers
              ?q=:discountDesc&cityId=750000000&page=0&pageSize=30

        Сначала прогреваем сессию чтобы получить куки.

        Если получаешь 403 — открой kaspi.kz в DevTools,
        найди запрос к /yml/offer-service/ и скопируй
        актуальные заголовки X-KS-City и Cookie.
        """
        result: List[Dict[str, Any]] = []

        # Прогрев — получаем куки сессии
        await safe_request(
            session, "GET", "https://kaspi.kz/",
            headers={**self.base_headers, "Accept": "text/html,*/*"}
        )

        headers = {
            **self.base_headers,
            "Referer":          "https://kaspi.kz/shop/subdomain/all/categories/all/products/?q=%3AdiscountDesc",
            "X-KS-City":        CITY_ID_KASPI,
            "X-Requested-With": "XMLHttpRequest",
        }
        for page in range(0, MAX_PAGES):  # Kaspi нумерует с 0
            params = {
                "q":        ":discountDesc",
                "cityId":   CITY_ID_KASPI,
                "page":     page,
                "pageSize": PAGE_SIZE,
            }
            r = await safe_request(
                session, "GET",
                "https://kaspi.kz/yml/offer-service/api/v1/offers",
                headers=headers, params=params,
            )
            if r is None: break

            try:
                data = r.json()
            except: break

            inner  = data.get("data") or data
            offers = inner.get("offers") or inner.get("items") or []
            total  = inner.get("total") or 0
            if not offers: break

            for o in offers:
                pid   = str(o.get("id") or o.get("offerId") or "").strip()
                title = (o.get("name") or o.get("title") or "").strip()
                slug  = (o.get("slug") or o.get("productCode") or pid).strip()
                p_i   = o.get("unitPrice") or o
                new_p = p_i.get("price") or p_i.get("sellPrice") or o.get("price")
                old_p = p_i.get("basePrice") or p_i.get("oldPrice") or p_i.get("priceBeforeDiscount")

                if not (pid and title and old_p and str(old_p) != str(new_p)): continue

                result.append({
                    "id": f"kp_{pid}", "title": title, "old_price": fmt_price(old_p),
                    "new_price": fmt_price(new_p), "discount": calc_discount(old_p, new_p),
                    "link": f"https://kaspi.kz/shop/p/{slug}-{pid}/", "shop": "Kaspi",
                })

            if total and (page + 1) * PAGE_SIZE >= total: break

        logger.info(f"Kaspi: найдено {len(result)} акций")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  ALSER
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_category_alser(self, session: AsyncSession, category: str, sem: asyncio.Semaphore, seen_ids: set) -> List[Dict[str, Any]]:
        """Парсинг категории Alser с глубоким поиском."""
        result = []
        async with sem:
            for page in range(1, 4): 
                url = f"https://alser.kz/c/{category}/_payload.js" if page == 1 else f"https://alser.kz/c/{category}/_payload.js?page={page}"
                headers = {**self.base_headers, "Accept": "*/*", "Referer": "https://alser.kz/", "Sec-Fetch-Dest": "script"}
                try:
                    r = await session.get(url, headers=headers, timeout=12)
                    if r.status_code != 200 or not r.text: break
                    raw = r.text
                except: break

                blocks = re.split(r'\{\s*"?id"?\s*:', raw)
                found_on_page = 0
                for block in blocks:
                    if "oldPrice" not in block and "old_price" not in block: continue
                    
                    try:
                        title_m = re.search(r'title\s*:\s*"(.*?)"', block)
                        link_m = re.search(r'link_url\s*:\s*"(.*?)"', block)
                        sku_m = re.search(r'sku\s*:\s*"(.*?)"', block)
                        price_m = re.search(r'price\s*:\s*(\d+)', block)
                        old_p_m = re.search(r'(?:oldPrice|old_price)\s*:\s*(\d+)', block)

                        if not (title_m and price_m and old_p_m): continue

                        title = title_m.group(1).replace('\\u002f', '/').replace('\\u002F', '/').replace('\\"', '"')
                        link = link_m.group(1).replace('\\u002f', '/').replace('\\u002F', '/') if link_m else ""
                        sku = sku_m.group(1) if sku_m else str(hash(title))
                        new_p, old_p = price_m.group(1), old_p_m.group(1)

                        new_f, old_f = float(new_p), float(old_p)
                        if old_f <= new_f: continue

                        uid = f"al_{sku}"
                        if uid in seen_ids: continue
                        seen_ids.add(uid)

                        result.append({
                            "id": uid, "title": title, "old_price": fmt_price(int(old_f)),
                            "new_price": fmt_price(int(new_f)), "discount": calc_discount(old_f, new_f),
                            "link": f"https://alser.kz{link}" if link.startswith("/") else link or f"https://alser.kz/c/{category}",
                            "shop": "Alser"
                        })
                        found_on_page += 1
                    except: continue
                if found_on_page == 0: break
        return result

    async def fetch_alser(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """ДИНАМИЧЕСКИЙ сбор категорий Alser."""
        catalog_url = "https://alser.kz/api/v2/catalog-full?location_id=8&lang=ru"
        categories = set()
        try:
            r = await session.get(catalog_url, headers=self.base_headers, timeout=15)
            if r.status_code == 200:
                def collect_keywords(items):
                    for item in items:
                        kw = item.get("keyword")
                        if kw: categories.add(kw)
                        if "subcategories" in item: collect_keywords(item["subcategories"])
                        if "children" in item: collect_keywords(item["children"])
                collect_keywords(r.json().get("data", []))
        except:
            categories = {"smartfony-i-planshety", "noutbuki-i-kompyutery", "televizory", "bytovaya-tehnika"}

        logger.info(f"Alser: обнаружено {len(categories)} динамических категорий")
        seen_ids, sem = set(), asyncio.Semaphore(5)
        
        # Берем первые 60 категорий (самые интересные обычно в начале/середине)
        tasks = [self.fetch_category_alser(session, cat, sem, seen_ids) for cat in list(categories)[:60]]
        results = await asyncio.gather(*tasks)
        
        all_items = [item for sublist in results for item in sublist]
        logger.info(f"Alser: итого собрано {len(all_items)} предложений")
        all_items.sort(key=lambda x: x.get("discount", 0), reverse=True)
        return all_items


    # ─────────────────────────────────────────────────────────────────────────
    #  БЕЛЫЙ ВЕТЕР (SHOP.KZ)
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_shopkz(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """
        Парсинг Белого Ветра (shop.kz) через официальную YML (XML) выгрузку.
        Самый надежный способ, так как магазин предоставляет этот файл сам.
        """
        url = "https://shop.kz/bitrix/catalog_export/yandex.php"
        result: List[Dict[str, Any]] = []

        r = await safe_request(session, "GET", url, timeout=60)
        if r is None:
            logger.error("Shop.kz: не удалось скачать YML файл")
            return []

        try:
            import xml.etree.ElementTree as ET
            # Читаем XML из ответа
            root = ET.fromstring(r.content)
            offers = root.findall(".//offer")
            
            for offer in offers:
                # Пропускаем, если нет в наличии
                if offer.get("available") == "false":
                    continue
                    
                price_elem = offer.find("price")
                oldprice_elem = offer.find("oldprice")
                
                # Если старой цены нет, значит нет скидки
                if price_elem is None or oldprice_elem is None:
                    continue
                    
                try:
                    new_p = float(price_elem.text)
                    old_p = float(oldprice_elem.text)
                except Exception:
                    continue
                    
                # Игнорируем фейковые скидки или если цена стала выше
                if old_p <= new_p:
                    continue
                    
                # Ищем название товара
                name_elem = offer.find("model")
                if name_elem is None:
                    name_elem = offer.find("name")
                
                title_text = name_elem.text if name_elem is not None else ""
                title = title_text.strip() if title_text else "Без названия"
                
                url_elem = offer.find("url")
                link = url_elem.text if url_elem is not None else "https://shop.kz/"
                
                uid = str(offer.get("id"))
                
                result.append({
                    "id":        f"bw_{uid}",
                    "title":     title,
                    "old_price": fmt_price(int(old_p)),
                    "new_price": fmt_price(int(new_p)),
                    "discount":  calc_discount(old_p, new_p),
                    "link":      link,
                    "shop":      "Белый Ветер 🌪",
                })
                
            logger.info(f"Белый Ветер: найдено {len(result)} товаров со скидкой из YML")
            return result
        except Exception as e:
            logger.error(f"Shop.kz XML parse error: {e}")
            return []

    # ─────────────────────────────────────────────────────────────────────────
    #  АГРЕГАТОР
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_discounts(self) -> List[Dict[str, Any]]:
        """
        Параллельный запуск всех парсеров.
        Результат дедуплицирован и отсортирован по скидке (от большей к меньшей).
        """
        async with AsyncSession(impersonate=self.impersonate) as session:
            results = await asyncio.gather(
                self.fetch_technodom(session),
                self.fetch_sulpak(session),
                self.fetch_alser(session),
                self.fetch_shopkz(session),
                return_exceptions=True,
            )

        all_items: List[Dict[str, Any]] = []
        seen_ids: set = set()

        for r in results:
            if isinstance(r, list):
                for item in r:
                    uid = item["id"]
                    if uid not in seen_ids:
                        seen_ids.add(uid)
                        all_items.append(item)
            else:
                logger.error(f"Parser raised exception: {r}")

        # Самые выгодные скидки — первыми
        all_items.sort(key=lambda x: x.get("discount", 0), reverse=True)

        logger.info(f"Всего найдено акций по Алматы: {len(all_items)}")
        return all_items


    # ─────────────────────────────────────────────────────────────────────────
    #  ТОЧЕЧНАЯ СЛЕДИЛКА (SINGULAR ITEM PARSING)
    # ─────────────────────────────────────────────────────────────────────────
    async def get_single_product_price(self, url: str, shop: str) -> Optional[int]:
        """Парсинг цены одного товара по прямой ссылке (без API каталога)."""
        async with AsyncSession(impersonate=self.impersonate) as session:
            try:
                r = await session.get(url, headers=self.base_headers, timeout=20)
                if r.status_code != 200:
                    return None
                
                soup = BeautifulSoup(r.text, "html.parser")
                price_text = ""

                if shop == "Kaspi":
                    # На Kaspi цена часто лежит в meta tag или специфичном классе
                    price_meta = soup.find("meta", property="product:price:amount")
                    if price_meta:
                        price_text = price_meta.get("content", "")
                    else:
                        # Запасной вариант - ищем класс цены
                        price_el = soup.select_one(".item-card__de-price, .product-item__price")
                        if price_el: price_text = price_el.get_text()

                elif shop == "Sulpak":
                    price_el = soup.select_one(".product__price, [data-price], .price")
                    if price_el:
                        price_text = price_el.get("data-price") or price_el.get_text()

                elif shop == "Mechta":
                    # У Мечты цена часто в скриптах или просто в div
                    price_el = soup.select_one(".product-item__price, [class*='price'], .finalPrice")
                    if price_el: price_text = price_el.get_text()

                if price_text:
                    # Очищаем от мусора и конвертируем в int
                    clean_price = re.sub(r"[^\d]", "", price_text)
                    return int(clean_price) if clean_price else None
                
            except Exception as e:
                logger.error(f"Error parsing single product {url}: {e}")
            
            return None




# ─── Singleton ────────────────────────────────────────────────────────────────
parser = DiscountParser()