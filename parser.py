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
                        "category":  "tech",
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
                    "category":  "tech",
                })
            
            if len(cards) < 10: break
            await asyncio.sleep(0.3)
        return result

    async def fetch_sulpak(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """Сбор акций со всего каталога Sulpak по категориям."""
        result: List[Dict[str, Any]] = []
        seen_ids = set()
        
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
                        "link": link, "shop": "Sulpak", "category": "tech",
                    })
            except: break

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
    #  MECHTA
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_mechta(self, session: AsyncSession) -> List[Dict[str, Any]]:
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
                            "category":  "tech",
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

                    old_p = o.get("unitPrice")
                    new_p = o.get("unitSalePrice")

                    if not (pid and title and old_p and new_p):
                        continue
                    if int(old_p) <= int(new_p):
                        continue

                    link = f"https://kaspi.kz{shop_link}" if shop_link else f"https://kaspi.kz/shop/p/{pid}/"

                    result.append({
                        "id":        f"kp_{pid}",
                        "title":     title,
                        "old_price": fmt_price(old_p),
                        "new_price": fmt_price(new_p),
                        "discount":  calc_discount(old_p, new_p),
                        "link":      link,
                        "shop":      "Kaspi",
                        "category":  "tech",
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
                            "category":  "tech",
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
                    "category":  "tech",
                })

            logger.info(f"Белый Ветер: найдено {len(result)} товаров из YML")
            return result
        except Exception as e:
            logger.error(f"Shop.kz XML error: {e}")
            return []

    # ─────────────────────────────────────────────────────────────────────────
    #  MELOMAN.KZ
    # ─────────────────────────────────────────────────────────────────────────

    def _meloman_extract_price(self, html: str) -> Optional[int]:
        if not html or html == "-":
            return None
        
        soup = BeautifulSoup(html, "html.parser")
        special = soup.select_one(".special-price [data-price-amount]")
        if special:
            amt = special.get("data-price-amount")
            if amt and amt.replace(".", "").isdigit():
                return int(float(amt))

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

                page_ids: List[str] = []
                info_map: Dict[str, Dict] = {}

                for card in soup.select(".product-item"):
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

                prices_data = await self._meloman_fetch_prices(session, page_ids, headers)

                for pid, html_fragment in prices_data.items():
                    if pid not in info_map:
                        continue

                    new_p = self._meloman_extract_price(html_fragment)
                    old_p = self._meloman_extract_old_price(html_fragment)

                    if not new_p or not old_p or old_p <= new_p:
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
                        "category":  "other",
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
    #  FREEDOM MOBILE
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_freedom(self, session: AsyncSession) -> List[Dict[str, Any]]:
        url = "https://api.fmobile.kz/catalog/api/v2/catalog/listing"
        headers = {
            **self.base_headers,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://fmobile.kz/",
            "Origin": "https://fmobile.kz",
        }
        result: List[Dict[str, Any]] = []
        seen_ids = set()

        for page in range(1, 51):
            params = {
                "channel": "ONLINE",
                "city_slug": CITY_SLUG_MECHTA, # almaty
                "page": page,
                "size": 50,
            }
            r = await safe_request(session, "GET", url, headers=headers, params=params)
            if r is None:
                break
            
            try:
                data = r.json()
            except Exception as e:
                logger.error(f"Freedom JSON error (page {page}): {e}")
                break
            
            res_node = data.get("result") or {}
            items = res_node.get("items") or []
            if not items:
                break
                
            for item in items:
                sku = str(item.get("sku") or item.get("model_stock_id") or "")
                if not sku or sku in seen_ids:
                    continue
                seen_ids.add(sku)
                
                title = item.get("model_stock_name") or item.get("name") or ""
                new_p = item.get("price")
                old_p = item.get("old_price") or 0
                slug  = item.get("model_stock_slug") or ""
                
                if not (title and new_p and old_p):
                    continue
                if old_p <= new_p:
                    continue
                    
                cat_slug = item.get("category_slug") or "catalog"
                slug     = item.get("model_stock_slug") or ""
                
                result.append({
                    "id":        f"fm_{sku}",
                    "title":     title,
                    "old_price": fmt_price(old_p),
                    "new_price": fmt_price(new_p),
                    "discount":  calc_discount(old_p, new_p),
                    "link":      f"https://fmobile.kz/category/{cat_slug}/{slug}",
                    "shop":      "Freedom Mobile 🟢",
                    "category":  "tech",
                })
            
            if len(items) < 50:
                break
            await asyncio.sleep(0.5)
            
        logger.info(f"Freedom Mobile: найдено {len(result)} акций")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  ADIDAS KZ
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_adidas(self, session: AsyncSession) -> List[Dict[str, Any]]:
        base_url = "https://adidas.kz"
        categories = {
            "muzhchiny/obuv":    "Мужская обувь",
            "zhenshhiny/obuv":   "Женская обувь",
            "deti/obuv":         "Детская обувь",
            "muzhchiny/odezhda": "Мужская одежда",
            "zhenshhiny/odezhda": "Женская одежда",
            "deti/odezhda":      "Детская одежда",
            "aksessuary":        "Аксессуары",
        }
        headers = {
            **self.base_headers,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://adidas.kz/",
        }
        result: List[Dict[str, Any]] = []
        seen_ids = set()

        for slug, label in categories.items():
            # Парсим только первую страницу каждой категории для мониторинга новинок
            url = f"{base_url}/{slug}/"
            r = await safe_request(session, "GET", url, headers=headers)
            if r is None:
                continue

            # Используем JSON-LD парсинг (как в adidas_kz_parser.py)
            soup = BeautifulSoup(r.text, "html.parser")
            scripts = soup.find_all("script", {"type": "application/ld+json"})

            for script in scripts:
                try:
                    content = script.string or ""
                    if not content: continue
                    data = json.loads(content)
                except: continue

                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") == "ItemList":
                        for list_item in item.get("itemListElement", []):
                            prod = list_item.get("item", {}) if list_item.get("@type") == "ListItem" else list_item
                            if not prod: continue
                            
                            name = prod.get("name", "").strip()
                            sku  = prod.get("sku") or ""
                            if not name or sku in seen_ids: continue
                            
                            offers = prod.get("offers", {})
                            if isinstance(offers, list): offers = offers[0] if offers else {}
                            
                            new_p = None
                            old_p = None
                            if isinstance(offers, dict):
                                try:
                                    raw_new = offers.get("price") or offers.get("lowPrice")
                                    if raw_new: new_p = int(float(str(raw_new).replace(" ","")))
                                    raw_old = offers.get("highPrice")
                                    if raw_old: old_p = int(float(str(raw_old).replace(" ","")))
                                except: pass

                            if not (new_p and old_p and old_p > new_p):
                                continue

                            seen_ids.add(sku)
                            p_url = prod.get("url") or list_item.get("url") or ""
                            if p_url and not p_url.startswith("http"): p_url = base_url + p_url

                            result.append({
                                "id":        f"ad_{sku}",
                                "title":     f"[{label}] {name}",
                                "old_price": fmt_price(old_p),
                                "new_price": fmt_price(new_p),
                                "discount":  calc_discount(old_p, new_p),
                                "link":      p_url,
                                "shop":      "Adidas KZ 👟",
                                "category":  "fashion",
                            })
            
            await asyncio.sleep(0.5)

        logger.info(f"Adidas KZ: найдено {len(result)} акций")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  INTERTOP.KZ
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_intertop(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """
        Парсинг intertop.kz: Многостраничный режим с обходом Cloudflare.
        Категория: fashion
        """
        base_url = "https://intertop.kz"
        categories = {
            "men/shoes":         "Мужская обувь",
            "men/clothing":      "Мужская одежда",
            "women/shoes":       "Женская обувь",
            "women/clothing":    "Женская одежда",
            "kids/shoes":        "Детская обувь",
            "kids/clothing":     "Детская одежда",
        }
        
        all_results = []
        # Дедупликация по ссылке внутри одного прохода
        seen_links = set()

        for slug, label in categories.items():
            for pg in range(1, 3): # Парсим по 2 страницы для каждой категории
                url = f"{base_url}/ru-kz/shopping/catalog/{slug}/"
                if pg > 1: url += f"?page={pg}"
                
                # Запрос с таймаутом и обходом
                try:
                    r = await safe_request(session, "GET", url, timeout=30)
                    if not r: break
                    
                    soup = BeautifulSoup(r.text, "html.parser")
                    products = []
                    
                    # 1. JSON-LD (с поддержкой @graph)
                    for script in soup.find_all("script", {"type": "application/ld+json"}):
                        try:
                            data = json.loads(script.string or "")
                            items = []
                            if isinstance(data, dict):
                                if "@graph" in data: items = data["@graph"]
                                else: items = [data]
                            
                            for item in items:
                                if item.get("@type") == "ItemList":
                                    for li in item.get("itemListElement", []):
                                        p_raw = li.get("item", li)
                                        if p_raw.get("@type") != "Product": continue
                                        
                                        u = p_raw.get("url") or li.get("url")
                                        if not u: continue
                                        full_u = u if u.startswith("http") else base_url + u
                                        if full_u in seen_links: continue
                                        
                                        brand_d = p_raw.get("brand", {})
                                        brand = brand_d.get("name") if isinstance(brand_d, dict) else str(brand_d)
                                        name = p_raw.get("name")
                                        
                                        offers = p_raw.get("offers", {})
                                        if isinstance(offers, list): offers = offers[0]
                                        
                                        pc = self._parse_price_val(offers.get("price") or offers.get("lowPrice"))
                                        po = self._parse_price_val(offers.get("highPrice"))
                                        
                                        if pc:
                                            seen_links.add(full_u)
                                            products.append({
                                                "id": f"it_{re.sub(r'[^a-z0-9]', '', full_u[-15:].lower())}",
                                                "title": f"[{brand}] {name}" if brand else name,
                                                "new_price": fmt_price(pc),
                                                "old_price": fmt_price(po) if po else None,
                                                "discount": calc_discount(po, pc),
                                                "link": full_u,
                                                "shop": "Intertop 👟",
                                                "category": "fashion"
                                            })
                        except: continue

                    # 2. Если JSON-LD дал мало результатов, добираем из HTML
                    if len(products) < 10:
                        cards = soup.select("[class*='in-product-tile']")
                        for card in cards:
                            try:
                                l_el = card.select_one("a[href*='/product/']")
                                if not l_el: continue
                                href = l_el.get("href")
                                full_u = href if href.startswith("http") else base_url + href
                                if full_u in seen_links: continue
                                
                                # Текст карточки для цен
                                text = re.sub(r"-\d+%", "", card.get_text(" ", strip=True))
                                matches = re.findall(r"(?:₸\s*([\d\s]{4,8}))|(([\d\s]{4,8})\s*₸)", text)
                                found_v = []
                                for m in matches:
                                    for part in m:
                                        v = self._parse_price_val(part)
                                        if v: found_v.append(v)
                                
                                vals = sorted(list(set(found_v)))
                                if not vals: continue
                                pc = vals[0]
                                po = vals[-1] if len(vals) > 1 else None
                                
                                if not pc: continue
                                seen_links.add(full_u)
                                brand = card.select_one("[class*='brand']").get_text(strip=True) if card.select_one("[class*='brand']") else "Бренд"
                                name = card.select_one("[class*='name']").get_text(strip=True) if card.select_one("[class*='name']") else "Товар"
                                
                                products.append({
                                    "id": f"it_{re.sub(r'[^a-z0-9]', '', full_u[-15:].lower())}",
                                    "title": f"[{brand}] {name}",
                                    "new_price": fmt_price(pc),
                                    "old_price": fmt_price(po) if po else None,
                                    "discount": calc_discount(po, pc),
                                    "link": full_u,
                                    "shop": "Intertop 👟",
                                    "category": "fashion"
                                })
                            except: continue
                    
                    if not products: break # Конец страниц
                    all_results.extend(products)
                    
                    # Пауза между страницами
                    await asyncio.sleep(5)
                except Exception as e:
                    logger.error(f"Intertop error {url}: {e}")
                    break
            
            # Пауза между категориями
            await asyncio.sleep(10)
            
        logger.info(f"Intertop KZ: найдено {len(all_results)} акций")
        return all_results

    def _parse_price_val(self, raw) -> Optional[int]:
        if raw is None: return None
        digits = re.sub(r"[^\d]", "", str(raw))
        val = int(digits) if digits else None
        if val and (val < 1000 or val > 1000000): return None
        return val

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
                self.fetch_freedom(session),
                self.fetch_adidas(session),
                return_exceptions=True,
            )

            # Intertop запускаем отдельно и последовательно, так как он тяжелый и часто 524-тит
            try:
                intertop_results = await self.fetch_intertop(session)
                results.append(intertop_results)
            except Exception as e:
                logger.error(f"Intertop sequential fetch error: {e}")
                results.append([])

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

                elif shop == "Freedom Mobile":
                    # Для Freedom Mobile вытаскиваем категорию и слаг из URL
                    # URL: .../category/some_slug/product_slug
                    m = re.search(r"category/([^/]+)/([^/?#]+)", url)
                    if m:
                        cat_slug = m.group(1)
                        slug = m.group(2)
                        api_url = f"https://api.fmobile.kz/catalog/api/v2/catalog/listing"
                        params = {
                            "channel": "ONLINE",
                            "city_slug": CITY_SLUG_MECHTA,
                            "category_slug": cat_slug,
                            "model_stock_slug": slug,
                        }
                        pr = await session.get(api_url, params=params, timeout=10)
                        if pr.status_code == 200:
                            data = pr.json()
                            res_node = data.get("result") or {}
                            items = res_node.get("items") or []
                            if items:
                                return items[0].get("price")

                elif shop == "Adidas KZ":
                    # Adidas KZ — используем JSON-LD со страницы товара
                    scripts = soup.find_all("script", {"type": "application/ld+json"})
                    for script in scripts:
                        try:
                            content = script.string or ""
                            if not content: continue
                            data = json.loads(content)
                            items = data if isinstance(data, list) else [data]
                            for item in items:
                                if item.get("@type") == "Product":
                                    offers = item.get("offers", {})
                                    if isinstance(offers, list): offers = offers[0] if offers else {}
                                    price = offers.get("price") or offers.get("lowPrice")
                                    if price: return int(float(str(price).replace(" ","")))
                        except: pass

                if price_text:
                    clean = re.sub(r"[^\d]", "", price_text)
                    return int(clean) if clean else None

            except Exception as e:
                logger.error(f"get_single_product_price {url}: {e}")

        return None


# ─── Singleton ────────────────────────────────────────────────────────────────
parser = DiscountParser()