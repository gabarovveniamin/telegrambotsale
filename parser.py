import asyncio
import logging
import re
import xml.etree.ElementTree as ET
import urllib.parse
from typing import List, Dict, Any, Optional
import time

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

# ─── Константы городов ────────────────────────────────────────────────────────
CITY_ID_TECHNODOM = "5f5f1e3b4c8a49e692fefd70"  # Алматы
CITY_ID_SULPAK    = "1"                           # Алматы
CITY_SLUG_MECHTA  = "almaty"
CITY_ID_KASPI     = "750000000"                   # Алматы

# ─── Настройки ────────────────────────────────────────────────────────────────
MAX_PAGES      = 5
PAGE_SIZE      = 30
RETRY_COUNT    = 3
RETRY_DELAY    = 2.0


def fmt_price(value) -> str:
    if value is None:
        return "—"
    val = str(value).strip()
    if not val or val == "0":
        return "—"
    val = re.sub(r"\s+", " ", val)
    return f"{val} ₸"


def calc_discount(old: Any, new: Any) -> int:
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
    json_data: Optional[Dict] = None,
    content: Optional[str] = None,
    params: Optional[Dict] = None,
    timeout: int = 20,
) -> Optional[Any]:
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            if method.upper() == "POST":
                r = await session.post(
                    url, headers=headers, json=json_data,
                    data=content, timeout=timeout
                )
            else:
                r = await session.get(
                    url, headers=headers, params=params, timeout=timeout
                )

            if r.status_code == 200:
                return r
            logger.warning(f"[attempt {attempt}/{RETRY_COUNT}] {url} → HTTP {r.status_code}")
        except Exception as e:
            logger.warning(f"[attempt {attempt}/{RETRY_COUNT}] {url} → {e}")

        if attempt < RETRY_COUNT:
            await asyncio.sleep(RETRY_DELAY)

    logger.error(f"Все {RETRY_COUNT} попытки исчерпаны: {url}")
    return None


class DiscountParser:
    def __init__(self):
        self.impersonate = "chrome124"
        self.base_headers = {
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
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
        url = "https://api.technodom.kz/katalog/api/v2/products/search"
        headers = {
            **self.base_headers,
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "affiliation": "web",
            "content-language": "ru-RU",
            "Origin": "https://www.technodom.kz",
            "Referer": "https://www.technodom.kz/",
        }
        result: List[Dict[str, Any]] = []
        queries = [
            "смартфон", "ноутбук", "телевизор", "холодильник",
            "стиральная", "наушники", "пылесос", "часы", "iphone", "samsung", "монитор"
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
                r = await safe_request(session, "POST", url, headers=headers, json_data=payload)
                if r is None:
                    break
                try:
                    data = r.json()
                except Exception as e:
                    logger.error(f"Technodom JSON error (page {page}): {e}")
                    break

                products = data.get("products") or data.get("data") or data.get("items") or []
                if not products:
                    break

                for p in products:
                    pid = str(p.get("id") or p.get("sku") or "").strip()
                    if pid in seen:
                        continue
                    seen.add(pid)
                    title = (p.get("name") or p.get("title") or "").strip()
                    slug  = (p.get("slug") or p.get("url_key") or p.get("uri") or "").strip()
                    old_p = p.get("old_price") or p.get("price_old") or p.get("crossed_price")
                    new_p = p.get("price") or p.get("price_new") or p.get("sell_price")

                    if not (pid and title and slug and old_p):
                        continue
                    if str(old_p) == str(new_p):
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
    async def fetch_category_sulpak(self, session: AsyncSession, category: str, seen_ids: set) -> List[Dict[str, Any]]:
        """Парсинг конкретной категории Sulpak."""
        result = []
        for page in range(1, 4):
            # Для первой страницы не всегда нужен параметр page=1
            if page == 1:
                url = f"https://www.sulpak.kz/f/{category}/"
            else:
                url = f"https://www.sulpak.kz/f/{category}/?page={page}"
            headers = {
                **self.base_headers,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://www.sulpak.kz/",
            }
            r = await safe_request(session, "GET", url, headers=headers)
            if r is None: break
            
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("div.product__item-js")
            if not cards: break
            
            for card in cards:
                title   = card.get("data-name", "").strip()
                code    = card.get("data-code", "").strip()
                new_p   = card.get("data-price", "").strip()
                old_tag = card.select_one(".product__item-price-old")
                old_p   = re.sub(r"[^\d]", "", old_tag.get_text()) if old_tag else ""
                
                if not (title and code and new_p and old_p): continue
                if code in seen_ids: continue
                seen_ids.add(code)
                
                try:
                    if float(old_p) <= float(new_p): continue
                except: continue

                a_tag = card.select_one("a.product__item-images")
                link = f"https://www.sulpak.kz{a_tag['href']}" if a_tag else ""
                
                result.append({
                    "id":        f"sp_{code}",
                    "title":     title,
                    "old_price": fmt_price(old_p),
                    "new_price": fmt_price(new_p),
                    "discount":  calc_discount(old_p, new_p),
                    "link":      link,
                    "shop":      "Sulpak",
                })
            
            if len(cards) < 10: break
            await asyncio.sleep(0.3)
        return result

    async def fetch_sulpak(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """Сбор акций со всего каталога Sulpak по категориям."""
        result: List[Dict[str, Any]] = []
        seen_ids = set()
        
        # Сначала чекаем саму страницу акций
        for page in range(1, 4):
            url = f"https://www.sulpak.kz/SaleLoadProducts/{page}/~/~/0-2147483647/~/~/popularitydesc/tiles"
            r = await safe_request(session, "POST", url, headers={"X-Requested-With": "XMLHttpRequest"})
            if r is None: break
            try:
                data = r.json()
                html = data.get("products", "")
                if not html: break
                soup = BeautifulSoup(html, "html.parser")
                cards = soup.select("div.product__item-js")
                for card in cards:
                    title   = card.get("data-name", "").strip()
                    code    = card.get("data-code", "").strip()
                    new_p   = card.get("data-price", "").strip()
                    old_tag = card.select_one(".product__item-price-old")
                    old_p   = re.sub(r"[^\d]", "", old_tag.get_text()) if old_tag else ""
                    
                    if not (title and code and new_p and old_p): continue
                    if code in seen_ids: continue
                    seen_ids.add(code)
                    
                    try:
                        if float(old_p) <= float(new_p): continue
                    except: continue
                    
                    a_tag = card.select_one("a.product__item-images")
                    link = f"https://www.sulpak.kz{a_tag['href']}" if a_tag else ""
                    
                    result.append({
                        "id": f"sp_{code}", "title": title, "old_price": fmt_price(old_p),
                        "new_price": fmt_price(new_p), "discount": calc_discount(old_p, new_p),
                        "link": link, "shop": "Sulpak",
                    })
            except: break

        # Затем добавляем обход по популярным категориям
        # Правильные сегменты Sulpak (версия Алматы)
        categories = [
            "smartfoniy", "noutbukiy", "led_oled_televizoriy", 
            "holodilnikiy", "stiralniye_mashiniy", "pyilesosyi"
        ]
        for cat in categories:
            res = await self.fetch_category_sulpak(session, cat, seen_ids)
            result.extend(res)
            
        logger.info(f"Sulpak: итого со всех категорий {len(result)} акций")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  MECHTA  — теперь через _payload.js (обход 403 на API)
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_mechta(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """
        Mechta заблокировала /api/v3/ для серверных IP (403).
        Переходим на парсинг _payload.js — тот же подход что и у Alser/Nuxt.
        """
        result: List[Dict[str, Any]] = []
        seen_ids: set = set()

        categories = [
            "smartfony", "noutbuki", "televizory",
            "holodilniki-i-morozilniki", "stiralnye-i-sushilnye-mashiny",
            "naushniki-i-garnitury", "planshety",
        ]

        headers = {
            **self.base_headers,
            "Accept": "*/*",
            "Referer": "https://www.mechta.kz/",
            "Sec-Fetch-Dest": "script",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Site": "same-origin",
        }

        for category in categories:
            for page in range(1, 4):
                if page == 1:
                    url = f"https://www.mechta.kz/category/{category}/_payload.js"
                else:
                    url = f"https://www.mechta.kz/category/{category}/_payload.js?page={page}"

                r = await safe_request(session, "GET", url, headers=headers, timeout=20)
                if r is None:
                    break

                raw = r.text
                found_on_page = 0

                # Ищем товары с oldPrice/old_price
                blocks = re.split(r'(?="?id"?\s*:)', raw)
                for block in blocks:
                    if "oldPrice" not in block and "old_price" not in block:
                        continue
                    try:
                        title_m = re.search(r'"?(?:name|title)"?\s*:\s*"(.*?)"', block)
                        price_m = re.search(r'"?(?:finalPrice|price)"?\s*:\s*(\d+)', block)
                        old_m   = re.search(r'"?(?:basePrice|oldPrice|old_price)"?\s*:\s*(\d+)', block)
                        slug_m  = re.search(r'"?slug"?\s*:\s*"(.*?)"', block)

                        if not (title_m and price_m and old_m):
                            continue

                        title = title_m.group(1).encode().decode("unicode_escape", errors="ignore")
                        new_f = float(price_m.group(1))
                        old_f = float(old_m.group(1))

                        if old_f <= new_f:
                            continue

                        slug = slug_m.group(1) if slug_m else ""
                        uid  = f"mc_{slug or title[:30]}"
                        if uid in seen_ids:
                            continue
                        seen_ids.add(uid)

                        result.append({
                            "id":        uid,
                            "title":     title,
                            "old_price": fmt_price(int(old_f)),
                            "new_price": fmt_price(int(new_f)),
                            "discount":  calc_discount(old_f, new_f),
                            "link":      f"https://www.mechta.kz/product/{slug}" if slug else "https://www.mechta.kz/",
                            "shop":      "Mechta 🔵",
                        })
                        found_on_page += 1
                    except Exception:
                        continue

                logger.info(f"Mechta [{category}] page {page}: найдено {found_on_page}")
                if found_on_page == 0:
                    break
                await asyncio.sleep(0.3)

        logger.info(f"Mechta: найдено {len(result)} акций")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  KASPI
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_kaspi(self, session: AsyncSession) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []

        api_headers = {
            **self.base_headers,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://kaspi.kz/",
            "X-KS-City": CITY_ID_KASPI,
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

        for page in range(0, MAX_PAGES):
            params = {
                "q":        f":availableInZones:{CITY_ID_KASPI}:all:relevance:all",
                "page":     page,
                "pageSize": PAGE_SIZE,
                "c":        CITY_ID_KASPI,
                "sc":       "",
            }
            r = await safe_request(
                session, "GET",
                "https://kaspi.kz/yml/product-view/pl/filters",
                headers=api_headers, params=params, timeout=20
            )
            if r is None:
                break

            try:
                data = r.json()
            except Exception as e:
                logger.error(f"Kaspi JSON error: {e}")
                break

            inner = data.get("data")
            if not isinstance(inner, dict):
                break

            cards = inner.get("cards") or []
            total = inner.get("total") or 0

            if not cards:
                break

            for o in cards:
                if not isinstance(o, dict):
                    continue
                try:
                    pid      = str(o.get("id") or "").strip()
                    title    = (o.get("title") or o.get("name") or "").strip()
                    shop_link = o.get("shopLink") or ""

                    # unitPrice и unitSalePrice — это числа (int), не объекты!
                    old_p = o.get("unitPrice")
                    new_p = o.get("unitSalePrice")

                    if not (pid and title and old_p and new_p):
                        continue
                    if int(old_p) <= int(new_p):
                        continue

                    # Строим ссылку из shopLink
                    link = f"https://kaspi.kz{shop_link}" if shop_link else f"https://kaspi.kz/shop/p/{pid}/"

                    result.append({
                        "id":        f"kp_{pid}",
                        "title":     title,
                        "old_price": fmt_price(old_p),
                        "new_price": fmt_price(new_p),
                        "discount":  calc_discount(old_p, new_p),
                        "link":      link,
                        "shop":      "Kaspi",
                    })
                except Exception as e:
                    logger.error(f"Kaspi item error: {e}")
                    continue

            logger.info(f"Kaspi page {page}: {len(cards)} карточек, добавлено {len(result)}")

            if total and (page + 1) * PAGE_SIZE >= total:
                break
            await asyncio.sleep(0.5)

        logger.info(f"Kaspi: найдено {len(result)} акций")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  ALSER
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_category_alser(
        self,
        session: AsyncSession,
        category: str,
        sem: asyncio.Semaphore,
        seen_ids: set,
    ) -> List[Dict[str, Any]]:
        result = []
        headers = {
            **self.base_headers,
            "Accept": "*/*",
            "Referer": "https://alser.kz/",
            "Sec-Fetch-Dest": "script",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Site": "same-origin",
        }

        async with sem:
            for page in range(1, 4):
                url = (
                    f"https://alser.kz/c/{category}/_payload.js"
                    if page == 1
                    else f"https://alser.kz/c/{category}/_payload.js?page={page}"
                )
                try:
                    r = await session.get(url, headers=headers, timeout=15)
                    if r.status_code != 200 or not r.text:
                        break
                    raw = r.text
                except Exception:
                    break

                found_on_page = 0
                blocks = re.split(r'\{\s*"?id"?\s*:', raw)
                for block in blocks:
                    if "oldPrice" not in block and "old_price" not in block:
                        continue
                    try:
                        title_m = re.search(r'title\s*:\s*"(.*?)"', block)
                        link_m  = re.search(r'link_url\s*:\s*"(.*?)"', block)
                        sku_m   = re.search(r'sku\s*:\s*"(.*?)"', block)
                        price_m = re.search(r'(?<!"old)[Pp]rice\s*:\s*(\d+)', block)
                        old_m   = re.search(r'(?:oldPrice|old_price)\s*:\s*(\d+)', block)

                        if not (title_m and price_m and old_m):
                            continue

                        title = (
                            title_m.group(1)
                            .replace("\\u002f", "/").replace("\\u002F", "/")
                            .replace('\\"', '"')
                        )
                        link  = (
                            link_m.group(1)
                            .replace("\\u002f", "/").replace("\\u002F", "/")
                            if link_m else ""
                        )
                        sku   = sku_m.group(1) if sku_m else str(hash(title))
                        new_f = float(price_m.group(1))
                        old_f = float(old_m.group(1))

                        if old_f <= new_f:
                            continue

                        uid = f"al_{sku}"
                        if uid in seen_ids:
                            continue
                        seen_ids.add(uid)

                        result.append({
                            "id":        uid,
                            "title":     title,
                            "old_price": fmt_price(int(old_f)),
                            "new_price": fmt_price(int(new_f)),
                            "discount":  calc_discount(old_f, new_f),
                            "link":      (
                                f"https://alser.kz{link}"
                                if link.startswith("/")
                                else link or f"https://alser.kz/c/{category}"
                            ),
                            "shop":      "Alser",
                        })
                        found_on_page += 1
                    except Exception:
                        continue

                if found_on_page == 0:
                    break
                await asyncio.sleep(0.2)

        return result

    async def fetch_alser(self, session: AsyncSession) -> List[Dict[str, Any]]:
        catalog_url = "https://alser.kz/api/v2/catalog-full?location_id=8&lang=ru"
        categories: set = set()

        try:
            r = await session.get(
                catalog_url,
                headers={**self.base_headers, "Accept": "application/json"},
                timeout=15,
            )
            if r.status_code == 200:
                def collect(items):
                    for item in items:
                        kw = item.get("keyword")
                        if kw:
                            categories.add(kw)
                        collect(item.get("subcategories") or item.get("children") or [])
                collect(r.json().get("data", []))
        except Exception as e:
            logger.warning(f"Alser catalog fetch failed: {e}")

        if not categories:
            categories = {
                "smartfony-i-planshety", "noutbuki-i-kompyutery",
                "televizory", "bytovaya-tehnika", "audio-i-video",
            }

        logger.info(f"Alser: обнаружено {len(categories)} категорий")
        seen_ids = set()
        sem      = asyncio.Semaphore(4)
        tasks    = [
            self.fetch_category_alser(session, cat, sem, seen_ids)
            for cat in list(categories)[:60]
        ]
        results = await asyncio.gather(*tasks)

        all_items = [item for sub in results for item in sub]
        all_items.sort(key=lambda x: x.get("discount", 0), reverse=True)
        logger.info(f"Alser: итого {len(all_items)} предложений")
        return all_items

    # ─────────────────────────────────────────────────────────────────────────
    #  БЕЛЫЙ ВЕТЕР (SHOP.KZ)
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_shopkz(self, session: AsyncSession) -> List[Dict[str, Any]]:
        url    = "https://shop.kz/bitrix/catalog_export/yandex.php"
        result: List[Dict[str, Any]] = []

        r = await safe_request(session, "GET", url, timeout=60)
        if r is None:
            logger.error("Shop.kz: не удалось скачать YML")
            return []

        try:
            root   = ET.fromstring(r.content)
            offers = root.findall(".//offer")

            for offer in offers:
                if offer.get("available") == "false":
                    continue

                price_el    = offer.find("price")
                oldprice_el = offer.find("oldprice")
                if price_el is None or oldprice_el is None:
                    continue

                try:
                    new_p = float(price_el.text)
                    old_p = float(oldprice_el.text)
                except Exception:
                    continue

                if old_p <= new_p:
                    continue

                name_el = offer.find("model") or offer.find("name")
                title   = (name_el.text or "Без названия").strip() if name_el is not None else "Без названия"
                url_el  = offer.find("url")
                link    = url_el.text if url_el is not None else "https://shop.kz/"
                uid     = str(offer.get("id"))

                result.append({
                    "id":        f"bw_{uid}",
                    "title":     title,
                    "old_price": fmt_price(int(old_p)),
                    "new_price": fmt_price(int(new_p)),
                    "discount":  calc_discount(old_p, new_p),
                    "link":      link,
                    "shop":      "Белый Ветер 🌪",
                })

            logger.info(f"Белый Ветер: найдено {len(result)} товаров из YML")
            return result
        except Exception as e:
            logger.error(f"Shop.kz XML error: {e}")
            return []

    # ─────────────────────────────────────────────────────────────────────────
    #  MELOMAN.KZ  — Книги, комиксы, настолки
    # ─────────────────────────────────────────────────────────────────────────

    def _meloman_extract_price(self, html: str) -> Optional[int]:
        if not html or html == "-":
            return None
        
        soup = BeautifulSoup(html, "html.parser")
        # Ищем специальную цену, если она есть
        special = soup.select_one(".special-price [data-price-amount]")
        if special:
            amt = special.get("data-price-amount")
            if amt and amt.replace(".", "").isdigit():
                return int(float(amt))

        # Если специальной нет, ищем финальную цену (которая может быть единственной)
        # Но важно не зацепить oldPrice, если мы ищем текущую цену.
        # В Meloman финальная цена обычно в .price-container.price-final_price
        # но если есть скидка, их там ДВЕ.
        final = soup.select_one(".special-price [data-price-amount]") or \
                soup.select_one("[data-price-type='finalPrice']") or \
                soup.select_one(".price-final_price [data-price-amount]")
        
        if final:
            amt = final.get("data-price-amount")
            if amt and amt.replace(".", "").isdigit():
                return int(float(amt))
            
        return None

    def _meloman_extract_old_price(self, html: str) -> Optional[int]:
        if not html or html == "-":
            return None

        soup = BeautifulSoup(html, "html.parser")
        # Обычная цена (старая)
        el = soup.select_one(".old-price [data-price-amount]") or \
             soup.select_one("[data-price-type='oldPrice']")
        
        if el:
            amt = el.get("data-price-amount")
            if amt and amt.replace(".", "").isdigit():
                return int(float(amt))

        return None

    async def _meloman_fetch_prices(
        self,
        session: AsyncSession,
        product_ids: List[str],
        headers: Dict,
    ) -> Dict[str, str]:
        """
        Запрашивает loyalty/products/prices/ батчами по 100 ID.

        Сайт ожидает параметры вида ids%5B%5D=111&ids%5B%5D=222
        (%5B%5D — это URL-encoded []).
        Параметр _ — Unix-timestamp в миллисекундах (cache-buster).
        """
        CHUNK = 100
        prices: Dict[str, str] = {}
        api_url = "https://www.meloman.kz/loyalty/products/prices/"

        chunks = [product_ids[i:i + CHUNK] for i in range(0, len(product_ids), CHUNK)]
        for chunk in chunks:
            ids_qs = "&".join(f"ids%5B%5D={pid}" for pid in chunk)
            ts = int(time.time() * 1000)
            url = f"{api_url}?{ids_qs}&_={ts}"

            r = await safe_request(session, "GET", url, headers=headers, timeout=20)
            if r is None:
                continue
            try:
                prices.update(r.json().get("prices", {}))
            except Exception as e:
                logger.error(f"Meloman prices JSON error: {e}")

            await asyncio.sleep(0.3)

        return prices

    async def fetch_meloman(self, session: AsyncSession) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        seen_ids: set = set()

        categories = [
            "books", "videogames", "toys-and-entertainment",
            "books/fiction", "books/graphic-literature",
            "shkola-kancelyariya-19236",
            "baby-and-mom", "food-items", "suvenirnaya-produkciya-19235",
            "tvorchestvo-19692", "music", "127-audiotehnika-28213",
            "digital-technique"
        ]
        headers = {
            **self.base_headers,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.meloman.kz/",
        }

        for category in categories:
            for page in range(1, 4):
                url = (
                    f"https://www.meloman.kz/{category}/"
                    if page == 1
                    else f"https://www.meloman.kz/{category}/?p={page}"
                )

                r = await safe_request(session, "GET", url, headers=headers)
                if r is None:
                    break

                soup = BeautifulSoup(r.text, "html.parser")

                # ── Собираем ID и мета-информацию со страницы ──────────────
                page_ids: List[str] = []
                info_map: Dict[str, Dict] = {}

                for card in soup.select(".product-item"):
                    # product-id живёт на разных элементах — проверяем оба
                    pid_el = card.select_one(
                        "[data-product-id]"
                    ) or card.select_one("form[data-product-id]")
                    if not pid_el:
                        continue
                    pid = str(pid_el.get("data-product-id", "")).strip()
                    if not pid:
                        continue

                    link_el = card.select_one("a.product-item-link")
                    if not link_el:
                        continue

                    title = link_el.get_text(strip=True)
                    link  = link_el.get("href", "")

                    if pid not in info_map:
                        page_ids.append(pid)
                        info_map[pid] = {"title": title, "link": link}

                if not page_ids:
                    break

                # ── Запрашиваем цены через loyalty API ─────────────────────
                prices_data = await self._meloman_fetch_prices(session, page_ids, headers)

                for pid, html_fragment in prices_data.items():
                    if pid not in info_map:
                        continue

                    new_p = self._meloman_extract_price(html_fragment)
                    old_p = self._meloman_extract_old_price(html_fragment)

                    # Товар без скидки — пропускаем
                    if not new_p or not old_p or old_p <= new_p:
                        if old_p:
                            logger.info(f"Meloman DEBUG: pid={pid}, np={new_p}, op={old_p}, frag_len={len(html_fragment)}")
                        continue

                    uid = f"ml_{pid}"
                    if uid in seen_ids:
                        continue
                    seen_ids.add(uid)

                    result.append({
                        "id":        uid,
                        "title":     info_map[pid]["title"],
                        "old_price": fmt_price(old_p),
                        "new_price": fmt_price(new_p),
                        "discount":  calc_discount(old_p, new_p),
                        "link":      info_map[pid]["link"],
                        "shop":      "Meloman 📚",
                    })

                logger.info(
                    f"Meloman [{category}] page {page}: "
                    f"{len(page_ids)} товаров, "
                    f"{sum(1 for p in prices_data if p in info_map and self._meloman_extract_old_price(prices_data[p]))} со скидкой"
                )

                if len(page_ids) < 15:
                    break
                await asyncio.sleep(0.3)

        logger.info(f"Meloman: найдено {len(result)} акций")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  АГРЕГАТОР
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_discounts(self) -> List[Dict[str, Any]]:
        async with AsyncSession(impersonate=self.impersonate) as session:
            results = await asyncio.gather(
                self.fetch_technodom(session),
                self.fetch_sulpak(session),
                # self.fetch_mechta(session),
                self.fetch_kaspi(session),
                self.fetch_alser(session),
                self.fetch_shopkz(session),
                self.fetch_meloman(session),
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
                logger.error(f"Parser exception: {r}")

        all_items.sort(key=lambda x: x.get("discount", 0), reverse=True)
        logger.info(f"Всего найдено акций по Алматы: {len(all_items)}")
        return all_items

    # ─────────────────────────────────────────────────────────────────────────
    #  ТОЧЕЧНАЯ СЛЕДИЛКА
    # ─────────────────────────────────────────────────────────────────────────
    async def get_single_product_price(self, url: str, shop: str) -> Optional[int]:
        async with AsyncSession(impersonate=self.impersonate) as session:
            try:
                r = await session.get(
                    url,
                    headers={**self.base_headers, "Accept": "text/html,*/*"},
                    timeout=20,
                )
                if r.status_code != 200:
                    return None

                soup       = BeautifulSoup(r.text, "html.parser")
                price_text = ""

                if shop == "Kaspi":
                    meta = soup.find("meta", property="product:price:amount")
                    if meta:
                        price_text = meta.get("content", "")
                    else:
                        el = soup.select_one(".item-card__de-price, .product-item__price")
                        if el:
                            price_text = el.get_text()

                elif shop == "Sulpak":
                    el = soup.select_one(".product__price, [data-price], .price")
                    if el:
                        price_text = el.get("data-price") or el.get_text()

                elif shop == "Mechta":
                    el = soup.select_one(".product-item__price, [class*='price'], .finalPrice")
                    if el:
                        price_text = el.get_text()

                elif shop == "Alser":
                    # Alser — пробуем _payload.js страницы товара
                    path  = url.replace("https://alser.kz", "")
                    p_url = f"https://alser.kz{path}/_payload.js"
                    pr = await session.get(
                        p_url,
                        headers={**self.base_headers, "Accept": "*/*"},
                        timeout=10,
                    )
                    if pr.status_code == 200:
                        m = re.search(r'"?price"?\s*:\s*(\d+)', pr.text)
                        if m:
                            return int(m.group(1))

                elif shop == "Meloman":
                    # ── Используем тот же loyalty API что и в fetch_meloman ──
                    pid = None

                    # Пробуем вытащить ID из HTML страницы товара
                    pid_el = soup.select_one("[data-product-id]")
                    if pid_el:
                        pid = str(pid_el.get("data-product-id", "")).strip()

                    # Fallback: ID часто стоит последним числом в URL
                    # /catalog/product/view/id/12345/  или  /slug-12345.html
                    if not pid or not pid.isdigit():
                        m = re.search(r"/id/(\d+)", url) or re.search(r"-(\d+)\.html$", url)
                        if m:
                            pid = m.group(1)

                    if pid and pid.isdigit():
                        ts = int(time.time() * 1000)
                        api_url = (
                            f"https://www.meloman.kz/loyalty/products/prices/"
                            f"?ids%5B%5D={pid}&_={ts}"
                        )
                        pr = await session.get(
                            api_url,
                            headers={
                                **self.base_headers,
                                "Accept": "application/json, text/javascript, */*; q=0.01",
                                "X-Requested-With": "XMLHttpRequest",
                                "Referer": url,
                            },
                            timeout=15,
                        )
                        if pr.status_code == 200:
                            html_fragment = pr.json().get("prices", {}).get(pid, "")
                            return self._meloman_extract_price(html_fragment)

                if price_text:
                    clean = re.sub(r"[^\d]", "", price_text)
                    return int(clean) if clean else None

            except Exception as e:
                logger.error(f"get_single_product_price {url}: {e}")

        return None


# ─── Singleton ────────────────────────────────────────────────────────────────
parser = DiscountParser()