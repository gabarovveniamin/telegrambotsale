import asyncio
import logging
import re
import xml.etree.ElementTree as ET
import urllib.parse
from typing import List, Dict, Any, Optional
import json
import time
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from services.scraper import scraper_service
logger = logging.getLogger(__name__)
CITY_ID_TECHNODOM = "5f5f1e3b4c8a49e692fefd70"
CITY_ID_SULPAK    = "1"
CITY_SLUG_MECHTA  = "almaty"
CITY_ID_KASPI     = "750000000"
MAX_PAGES      = 10
PAGE_SIZE      = 30
RETRY_COUNT    = 3
RETRY_DELAY    = 2.0
def fmt_price(value) -> str:
    if value is None:
        return "—"
    try:
        digits = re.sub(r"[^\d]", "", str(value))
        if not digits or digits == "0":
            return "—"
        val = int(digits)
        formatted = f"{val:,}".replace(",", " ")
        return f"{formatted} ₸"
    except Exception:
        return str(value) + " ₸"
def calc_discount(old: Any, new: Any) -> int:
    try:
        if not old or not new: return 0
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
            elif r.status_code >= 400:
                logger.warning(f"[{url}] HTTP {r.status_code}, прекращаем повторы")
                return None
            else:
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
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "max-age=0",
            "Sec-Ch-Ua": '"Not-A.Brand";v="99", "Chromium";v="124", "Google Chrome";v="124"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }
    async def fetch_technodom_category(self, session: AsyncSession, query: str, seen: set) -> List[Dict[str, Any]]:
        url = "https://api.technodom.kz/katalog/api/v2/products/search"
        headers = {
            **self.base_headers,
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Content-Language": "ru-RU",
            "Affiliation": "web",
            "Origin": "https://www.technodom.kz",
            "Referer": "https://www.technodom.kz/",
        }
        results = []
        for page in range(1, MAX_PAGES + 1):
            payload = {
                "categories": [""],
                "city_id": CITY_ID_TECHNODOM,
                "query": query,
                "limit": PAGE_SIZE,
                "page": page,
                "sort_by": "discount",
                "type": "full_search",
            }
            r = await safe_request(session, "POST", url, headers=headers, json_data=payload)
            if not r:
                break
            try:
                data = r.json()
                products = data.get("products") or []
                if not products:
                    break
                found_discount = False
                for p in products:
                    try:
                        discount = int(p.get("discount") or 0)
                    except (ValueError, TypeError):
                        discount = 0
                    if discount <= 0:
                        continue
                    found_discount = True
                    sku = str(p.get("sku") or "").strip()
                    if not sku or sku in seen:
                        continue
                    seen.add(sku)
                    title = (p.get("title") or "").strip()
                    uri = (p.get("uri") or "").strip()
                    price = p.get("price")
                    old_price = p.get("old_price")
                    if not (title and uri and price and old_price and old_price > price):
                        continue
                    results.append({
                        "id": f"td_{sku}",
                        "title": title,
                        "old_price": fmt_price(old_price),
                        "new_price": fmt_price(price),
                        "discount": discount,
                        "link": f"https://www.technodom.kz/p/{uri}",
                        "shop": "Technodom",
                        "category": "tech",
                    })
                if not found_discount:
                    break
                if len(products) < PAGE_SIZE:
                    break
            except Exception as e:
                logger.error(f"[Technodom] Ошибка при парсинге '{query}' стр.{page}: {e}")
                break
            await asyncio.sleep(0.3)
        return results
    async def fetch_technodom(self, session: AsyncSession) -> List[Dict[str, Any]]:
        queries = [
            "смартфон", "ноутбук", "телевизор", "планшет", "наушники",
            "холодильник", "стиральная машина", "пылесос", "кондиционер",
            "микроволновая печь", "посудомоечная машина", "кофемашина",
            "фен", "утюг", "электробритва", "умные часы", "фитнес браслет",
            "игровая приставка", "монитор", "видеокарта", "процессор",
            "фотоаппарат", "колонка", "электросамокат", "робот пылесос",
        ]
        all_products = []
        seen = set()
        semaphore = asyncio.Semaphore(5)
        async def fetch_with_sem(q):
            async with semaphore:
                try:
                    res = await self.fetch_technodom_category(session, q, seen)
                    if res:
                        logger.info(f"[Technodom] '{q}' -> {len(res)} скидок")
                    return res
                except Exception as e:
                    logger.error(f"[Technodom] Ошибка '{q}': {e}")
                    return []
        tasks = [fetch_with_sem(q) for q in queries]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, list):
                all_products.extend(res)
        logger.info(f"[Technodom] Всего собрано: {len(all_products)} товаров со скидками")
        return all_products
    async def fetch_category_sulpak(self, session: AsyncSession, category: str, seen_ids: set) -> List[Dict[str, Any]]:
        result = []
        for page in range(1, 11):
            url = f"https://www.sulpak.kz/f/{category}/" if page == 1 else f"https://www.sulpak.kz/f/{category}/?page={page}"
            r = await safe_request(session, "GET", url, headers=self.base_headers)
            if r is None: break
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("div.product__item-js")
            if not cards: break
            for card in cards:
                title = card.get("data-name", "").strip()
                code = card.get("data-code", "").strip()
                new_p = card.get("data-price", "").strip()
                old_tag = card.select_one(".product__item-price-old")
                old_p = re.sub(r"[^\d]", "", old_tag.get_text()) if old_tag else ""
                if not (title and code and new_p and old_p): continue
                if code in seen_ids: continue
                seen_ids.add(code)
                if float(old_p) <= float(new_p): continue
                a_tag = card.select_one("a.product__item-images")
                link = f"https://www.sulpak.kz{a_tag['href']}" if a_tag else ""
                result.append({
                    "id": f"sp_{code}", "title": title, "old_price": fmt_price(old_p),
                    "new_price": fmt_price(new_p), "discount": calc_discount(old_p, new_p),
                    "link": link, "shop": "Sulpak", "category": "tech",
                })
            if len(cards) < 10: break
            await asyncio.sleep(0.3)
        return result
    async def fetch_sulpak(self, session: AsyncSession) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        seen_ids = set()
        for page in range(1, 11):
            url = f"https://www.sulpak.kz/SaleLoadProducts/{page}/~/~/0-2147483647/~/~/popularitydesc/tiles"
            r = await safe_request(session, "POST", url, headers={"X-Requested-With": "XMLHttpRequest"})
            if r is None: break
            try:
                html = r.json().get("products", "")
                if not html: break
                soup = BeautifulSoup(html, "html.parser")
                for card in soup.select("div.product__item-js"):
                    title = card.get("data-name", "").strip()
                    code = card.get("data-code", "").strip()
                    new_p = card.get("data-price", "").strip()
                    old_tag = card.select_one(".product__item-price-old")
                    old_p = re.sub(r"[^\d]", "", old_tag.get_text()) if old_tag else ""
                    if not (title and code and new_p and old_p): continue
                    if code in seen_ids: continue
                    seen_ids.add(code)
                    if float(old_p) <= float(new_p): continue
                    a_tag = card.select_one("a.product__item-images")
                    result.append({
                        "id": f"sp_{code}", "title": title, "old_price": fmt_price(old_p),
                        "new_price": fmt_price(new_p), "discount": calc_discount(old_p, new_p),
                        "link": f"https://www.sulpak.kz{a_tag['href']}" if a_tag else "",
                        "shop": "Sulpak", "category": "tech",
                    })
            except: break
        for cat in ["smartfoniy", "noutbuki", "led_oled_televizoriy", "holodilniki", "stiralniye_mashiniy", "planshetiy"]:
            result.extend(await self.fetch_category_sulpak(session, cat, seen_ids))
        return result
    async def fetch_mechta(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """
        Парсинг Мечты через ScraperService (Playwright + API).
        """
        logger.info("[Mechta] Запуск парсинга через ScraperService...")
        from services.scraper import scraper_service
        raw_items = await scraper_service.fetch_mechta_discounts()
        results = []
        for item in raw_items:
            results.append({
                "id": item["id"],
                "title": item["title"],
                "old_price": fmt_price(item["old_price"]),
                "new_price": fmt_price(item["new_price"]),
                "discount": calc_discount(item["old_price"], item["new_price"]),
                "link": item["link"],
                "image": item.get("image"),
                "shop": "Mechta 🔵",
                "category": item["category"]
            })
        logger.info(f"[Mechta] Сбор завершен. Найдено скидок: {len(results)}")
        return results
    async def fetch_kaspi(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """
        Умный мониторинг цен Kaspi: сравнивает текущие цены с историей в БД.
        Позволяет находить скидки, даже если магазин их не афиширует (без старой цены).
        """
        logger.info("[Kaspi] Запуск глобального мониторинга цен...")
        from services.scraper import scraper_service
        from database import db
        try:
            raw_items = await scraper_service.fetch_kaspi_discounts()
        except Exception as e:
            logger.error(f"[Kaspi] Критическая ошибка при загрузке товаров: {e}")
            return []
        logger.info(f"[Kaspi] Получено {len(raw_items)} товаров из API, начинаем сверку с БД...")
        results = []
        updated = 0
        for item in raw_items:
            try:
                product_id = item["id"]
                current_price = int(item["new_price"])
                last_price = await db.get_product_price(product_id)
                if last_price:
                    if current_price < last_price:
                        drop_pct = (last_price - current_price) / last_price
                        if drop_pct >= 0.02:
                            results.append({
                                "id": product_id,
                                "title": item["title"],
                                "old_price": fmt_price(last_price),
                                "new_price": fmt_price(current_price),
                                "discount": calc_discount(last_price, current_price),
                                "link": item["link"],
                                "image": item.get("image"),
                                "shop": "Kaspi",
                                "category": item["category"]
                            })
                            logger.info(f"[Kaspi] Скидка: {item['title']} {last_price} -> {current_price} (-{round(drop_pct*100)}%)")
                await db.update_product_price(product_id, current_price)
                updated += 1
            except Exception as e:
                logger.warning(f"[Kaspi] Ошибка обработки товара {item.get('id', '?')}: {e}")
                continue
        logger.info(f"[Kaspi] Обработка завершена. Обновлено: {updated}, Найдено скидок: {len(results)}")
        return results
    async def fetch_alser(self, session: AsyncSession) -> List[Dict[str, Any]]:
        result = []
        seen_ids = set()
        def unescape(s):
            if not s or "\\" not in s: return s
            try:
                return re.sub(r'\\u([0-9a-fA-F]{4})', lambda m: chr(int(m.group(1), 16)), s)
            except:
                return s
        categories = [
            "smartfony", "planshety", "umnye-chasy", "fitnes-braslety", "baby-watch",
            "naushniki-dlja-smartfonov", "portativnoe-audio", "aksessuary-dlja-smartfonov",
            "noutbuki", "vneshnie-nakopiteli", "komplektujuschie-dlja-noutbukov",
            "monitory", "aksessuary-dlja-pk", "setevoe-oborudovanie", "komplektujuschie",
            "igrovye-konsoli", "igrovye-manipuljatory-i-aksessuary", "mebel",
            "printery", "mfu", "rashodnye-materialy", "stacionarnye-telefony",
            "pitanie", "stiralnye-i-susilnye-masiny", "malaa-bytovaa-tehnika",
            "vse-tovary-dla-uborki", "jge-arnalgan-kerek-zaraktar", "vse-holodilniki",
            "morozil-nye-kamery", "plity", "vinnye-shkafy", "ydys-zugys-masinalar",
            "prigotovlenie-i-obrabotka-produktov", "prigotovlenie-napitkov",
            "kuhonnaja-posuda", "plitalar", "duhovye-shkafy", "vytjazhki",
            "posudomoechnye-mashiny", "vstraivaemye-holodilniki",
            "vstraivaemye-mikrovolnovye-peci", "pribory-dlja-ukladki-volos",
            "brite-i-strizka", "dlya-zdorovya", "uhod-za-polostu-rta", "dla-krasoty",
            "televizory", "vse-saundbari", "proekcionnoe-oborudovanie",
            "stacionarnoe-audio", "aksessuary-dlja-tv", "vse-kondicioneri",
            "ventiljatory", "vodonagrevateli", "uvlazhniteli-vozduha", "obogrevateli",
            "gadzhety-dlja-umnogo-doma", "umnyj-dom-sistemy-bezopastnosti",
            "akyldy-dinamikter", "velosipedy", "otdih-i-sport",
            "jekshn-kamery-accessory", "igrushki", "shini"
        ]
        heads = {
            "Host": "alser.kz",
            "Accept": "*/*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://alser.kz/",
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Requested-With": "XMLHttpRequest",
        }
        await safe_request(session, "GET", "https://alser.kz/", headers=heads)
        await asyncio.sleep(1)
        for cat in categories:
            for page in range(1, 3):
                url = f"https://alser.kz/c/{cat}/_payload.js" + (f"?page={page}" if page > 1 else "")
                r = await safe_request(session, "GET", url, headers=heads)
                if not r: break
                content_text = r.text
                try:
                    arg_names_match = re.search(r'function\((.*?)\)', content_text)
                    arg_values_match = re.search(r'}\((.*)\)\)\s*$', content_text)
                    val_map = {}
                    if arg_names_match and arg_values_match:
                        names = [n.strip() for n in arg_names_match.group(1).split(',')]
                        raw_vals = arg_values_match.group(1)
                        json_ready = "[" + raw_vals.replace("void 0", "null") + "]"
                        try:
                            values = json.loads(json_ready)
                            val_map = dict(zip(names, values))
                        except: pass
                except:
                    val_map = {}
                def get_val(raw):
                    if not raw: return None
                    raw = raw.strip()
                    if raw.startswith('"') and raw.endswith('"'):
                        return unescape(raw[1:-1])
                    if raw in val_map:
                        return val_map[raw]
                    try: return float(raw)
                    except: return raw
                blocks = re.split(r'\{\s*"?id"?\s*:', content_text)
                for b in blocks:
                    if "oldPrice" not in b: continue
                    try:
                        title_m = re.search(r'title\s*:\s*("(.*?)"|[\w$]+)', b)
                        sku_m = re.search(r'sku\s*:\s*("(.*?)"|[\w$]+)', b)
                        price_m = re.search(r'(?<!"old)[Pp]rice\s*:\s*("(.*?)"|[\w$]+)', b)
                        old_m = re.search(r'oldPrice\s*:\s*("(.*?)"|[\w$]+)', b)
                        link_m = re.search(r'link_url\s*:\s*("(.*?)"|[\w$]+)', b)
                        if not (title_m and sku_m and price_m and old_m and link_m): continue
                        title = get_val(title_m.group(1))
                        sku = str(get_val(sku_m.group(1)))
                        price = float(get_val(price_m.group(1)))
                        old = float(get_val(old_m.group(1)))
                        link = get_val(link_m.group(1))
                        if not link.startswith("http"):
                            link = f"https://alser.kz{link}"
                        if old > price and sku not in seen_ids:
                            seen_ids.add(sku)
                            result.append({
                                "id": f"al_{sku}", "title": title, "old_price": fmt_price(int(old)),
                                "new_price": fmt_price(int(price)), "discount": calc_discount(old, price),
                                "link": link, "shop": "Alser", "category": "tech",
                            })
                    except: continue
            await asyncio.sleep(1)
        return result
    async def fetch_shopkz(self, session: AsyncSession) -> List[Dict[str, Any]]:
        r = await safe_request(session, "GET", "https://shop.kz/bitrix/catalog_export/yandex.php")
        if not r: return []
        try:
            root = ET.fromstring(r.content)
            res = []
            for off in root.findall(".//offer"):
                p = off.find("price")
                op = off.find("oldprice")
                if p is not None and op is not None:
                    new_p, old_p = float(p.text), float(op.text)
                    if old_p > new_p:
                        uid = off.get("id")
                        title = (off.find("model") or off.find("name")).text
                        res.append({
                            "id": f"bw_{uid}", "title": title, "old_price": fmt_price(int(old_p)),
                            "new_price": fmt_price(int(new_p)), "discount": calc_discount(old_p, new_p),
                            "link": off.find("url").text, "shop": "Белый Ветер 🌪", "category": "tech",
                        })
            return res
        except: return []
    def _meloman_extract_price(self, html: str) -> Optional[int]:
        if not html or html == "-": return None
        m = re.search(r'data-price-amount=["\']?(\d+)["\']?', html)
        if m: return int(m.group(1))
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find(attrs={"data-price-amount": True})
        try: return int(float(el["data-price-amount"])) if el else None
        except: return None
    def _meloman_extract_old_price(self, html: str) -> Optional[int]:
        if not html: return None
        m = re.search(r'data-price-type=["\']oldPrice["\'][^>]*data-price-amount=["\']?(\d+)["\']?', html, re.S)
        if m: return int(m.group(1))
        m = re.search(r'data-price-amount=["\']?(\d+)["\']?[^>]*data-price-type=["\']oldPrice["\']', html, re.S)
        if m: return int(m.group(1))
        soup = BeautifulSoup(html, "html.parser")
        el = soup.select_one(".old-price [data-price-amount], .price-old [data-price-amount], [data-price-type='oldPrice']")
        try: return int(float(el["data-price-amount"])) if el and el.get("data-price-amount") else None
        except: return None
    async def _meloman_fetch_prices(self, session, ids, heads):
        api = "https://www.meloman.kz/loyalty/products/prices/"
        prices = {}
        for chunk in [ids[i:i + 100] for i in range(0, len(ids), 100)]:
            qs = "&".join(f"ids%5B%5D={i}" for i in chunk)
            ts = int(time.time() * 1000)
            url = f"{api}?{qs}&_={ts}"
            r = await safe_request(session, "GET", url, headers=heads)
            if r:
                try:
                    data = r.json()
                    if isinstance(data, dict):
                        prices.update(data.get("prices", {}))
                except: continue
        return prices
    async def fetch_meloman(self, session: AsyncSession) -> List[Dict[str, Any]]:
        cats = ["catalogsearch/result/?q=sale", "books", "videogames", "toys-and-entertainment", "shkola-kancelyariya-19236"]
        res = []
        seen = set()
        heads = {
            **self.base_headers,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "https://www.meloman.kz/",
        }
        for cat in cats:
            max_pages = 10 if "q=" in cat else 8
            for pg in range(1, max_pages + 1):
                sep = "&" if "?" in cat else "?"
                url = f"https://www.meloman.kz/{cat}" + (f"{sep}p={pg}" if pg > 1 or sep == "&" else "")
                if pg == 1 and sep == "?": url = f"https://www.meloman.kz/{cat}/"
                r = await safe_request(session, "GET", url, headers=heads)
                if not r: break
                try:
                    data = r.json()
                    html = data.get("categoryProducts") or data.get("products") or r.text
                except:
                    html = r.text
                soup = BeautifulSoup(html, "html.parser")
                p_ids, info = [], {}
                cards = soup.select(".product-item")
                if not cards: break
                for card in cards:
                    pid = card.get("data-product-id") or card.get("data-id-product")
                    if not pid:
                        el = card.select_one("[data-product-id], [data-id-product]")
                        if el:
                            pid = el.get("data-product-id") or el.get("data-id-product")
                    if not pid: continue
                    link_el = card.select_one("a.product-item-link")
                    if link_el:
                        info[pid] = {"t": link_el.get_text(strip=True), "l": link_el["href"]}
                        p_ids.append(pid)
                if not p_ids: continue
                prices = await self._meloman_fetch_prices(session, p_ids, heads)
                for pid, html in prices.items():
                    np, op = self._meloman_extract_price(html), self._meloman_extract_old_price(html)
                    if np and op and op > np and pid not in seen:
                        seen.add(pid)
                        res.append({
                            "id": f"ml_{pid}", "title": info[pid]["t"], "old_price": fmt_price(op),
                            "new_price": fmt_price(np), "discount": calc_discount(op, np),
                            "link": info[pid]["l"], "shop": "Meloman 📚", "category": "other",
                        })
                if len(p_ids) < 15: break
                await asyncio.sleep(0.3)
        return res
    async def fetch_freedom(self, session: AsyncSession) -> List[Dict[str, Any]]:
        url = "https://api.fmobile.kz/catalog/api/v2/catalog/listing"
        heads = {**self.base_headers, "Referer": "https://fmobile.kz/", "Origin": "https://fmobile.kz"}
        res = []
        seen = set()
        for page in range(1, 500):
            params = {"channel": "ONLINE", "city_slug": CITY_SLUG_MECHTA, "page": page, "size": 50}
            r = await safe_request(session, "GET", url, headers=heads, params=params)
            if not r: break
            try:
                items = r.json().get("result", {}).get("items") or []
                if not items: break
                for i in items:
                    sku, title, np, op = i.get("sku"), i.get("model_stock_name"), i.get("price"), i.get("old_price")
                    if sku and title and np and op and op > np:
                        if sku in seen: continue
                        seen.add(sku)
                        res.append({
                            "id": f"fm_{sku}", "title": title, "old_price": fmt_price(op),
                            "new_price": fmt_price(np), "discount": calc_discount(op, np),
                            "link": f"https://fmobile.kz/category/{i.get('category_slug')}/{i.get('model_stock_slug')}",
                            "shop": "Freedom Mobile 🟢", "category": "tech",
                        })
            except: break
            await asyncio.sleep(0.2)
        return res
    async def fetch_adidas(self, session: AsyncSession) -> List[Dict[str, Any]]:
        cats = {
            "muzhchiny/obuv": "Мужская обувь",
            "zhenshhiny/obuv": "Женская обувь",
            "deti/obuv": "Детская обувь",
            "muzhchiny/odezhda": "Мужская одежда",
            "zhenshhiny/odezhda": "Женская одежда"
        }
        res = []
        seen = set()
        for slug, label in cats.items():
            for pg in range(1, 7):
                start = (pg - 1) * 48
                url = f"https://adidas.kz/{slug}/" + (f"?start={start}" if pg > 1 else "")
                r = await safe_request(session, "GET", url, headers=self.base_headers)
                if not r: break
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select("div.product.list__item")
                for item in items:
                    try:
                        title_el = item.select_one(".product__title")
                        if not title_el: continue
                        title = title_el.get_text(strip=True)
                        link_el = item.select_one("a.product__image--block") or item.select_one("a.product__info")
                        if not link_el: continue
                        href = link_el.get("href", "")
                        link = f"https://adidas.kz{href}" if href.startswith("/") else href
                        if link in seen: continue
                        price_sale = item.select_one(".price__sale")
                        price_old = item.select_one(".price__first.old")
                        if price_sale and price_old:
                            np = self._parse_price_val(price_sale.get_text())
                            op = self._parse_price_val(price_old.get_text())
                            if np and op and op > np:
                                slug_id = link.rstrip("/").split("/")[-1]
                                seen.add(link)
                                res.append({
                                    "id": f"ad_{slug_id}",
                                    "title": f"[{label}] {title}",
                                    "old_price": fmt_price(op),
                                    "new_price": fmt_price(np),
                                    "discount": calc_discount(op, np),
                                    "link": link,
                                    "shop": "Adidas KZ 👟",
                                    "category": "fashion",
                                })
                    except: continue
        return res
    async def fetch_intertop(self, session: AsyncSession) -> List[Dict[str, Any]]:
        cats = {
            "men/shoes": "Мужская обувь",
            "women/shoes": "Женская обувь",
            "men/clothing": "Мужская одежда",
            "women/clothing": "Женская одежда"
        }
        res = []
        seen = set()
        for slug, label in cats.items():
            url = f"https://intertop.kz/ru-kz/shopping/catalog/{slug}/"
            r = await safe_request(session, "GET", url, timeout=30)
            if not r: continue
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select(".in-product-tile")
            for card in items:
                try:
                    brand_el = card.select_one(".in-product-tile__product-brand")
                    name_el = card.select_one(".in-product-tile__product-name")
                    if not (brand_el and name_el): continue
                    pid = card.get("data-product-id") or card.get("data-product-sku")
                    l_el = card.select_one("a[href*='/product/']")
                    if not l_el: continue
                    link = f"https://intertop.kz{l_el['href']}" if l_el['href'].startswith("/") else l_el['href']
                    import hashlib
                    u_id = pid if pid else hashlib.md5(link.encode()).hexdigest()[:12]
                    if u_id in seen: continue
                    seen.add(u_id)
                    price_regular = card.select_one(".in-price__regular")
                    price_actual = card.select_one(".in-price__actual")
                    if price_regular and price_actual:
                        op = self._parse_price_val(price_regular.get_text())
                        np = self._parse_price_val(price_actual.get_text())
                        if np and op and op > np:
                            res.append({
                                "id": f"it_{u_id}",
                                "title": f"[{label}] {brand_el.get_text(strip=True)} {name_el.get_text(strip=True)}",
                                "old_price": fmt_price(op),
                                "new_price": fmt_price(np),
                                "discount": calc_discount(op, np),
                                "link": link,
                                "shop": "Intertop 👟",
                                "category": "fashion",
                            })
                except: continue
        return res
    async def _dns_discover_categories(self, session: AsyncSession) -> List[tuple]:
        """
        Динамически получает реальные категории с dns-shop.kz/catalog/
        Возвращает список (cat_id, slug) из href-ов на странице каталога.
        """
        target_slugs = {
            "noutbuki", "smartfony", "televizory", "planshety",
            "naushniki-i-garnitury", "igrovye-pristavki", "monitory",
            "holodilniki", "stiralnye-mashiny", "kondicionery",
            "umnye-chasy-i-braslety", "aksessuary-dlya-pk",
            "videokamery-i-fotoapparaty", "planshetyi",
        }
        found = []
        seen_ids = set()
        dns_headers = {
            **self.base_headers,
            "Referer": "https://www.dns-shop.kz/",
            "Origin": "https://www.dns-shop.kz",
            "DNT": "1",
            "Connection": "keep-alive",
        }
        r = await safe_request(session, "GET", "https://www.dns-shop.kz/catalog/", headers=dns_headers)
        if not r:
            logger.warning("[DNS] Не удалось получить страницу каталога для поиска категорий")
            return found
        matches = re.findall(
            r'/catalog/([0-9a-f]{16,})/([a-z0-9][a-z0-9\-]*[a-z0-9])/',
            r.text
        )
        for cat_id, slug in matches:
            if cat_id in seen_ids:
                continue
            if not target_slugs or slug in target_slugs:
                seen_ids.add(cat_id)
                found.append((cat_id, slug))
        if not found:
            seen_ids.clear()
            for cat_id, slug in matches:
                if cat_id not in seen_ids:
                    seen_ids.add(cat_id)
                    found.append((cat_id, slug))
                if len(found) >= 15:
                    break
        logger.info(f"[DNS] Найдено {len(found)} категорий на странице каталога")
        return found
    async def fetch_dns(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """
        Парсит скидочные товары с dns-shop.kz используя API.
        Алгоритм:
        1. Получает ID товаров со страницы категории
        2. Отправляет POST запрос на /ajax-state/product-buy/ с ID товаров
        3. Получает JSON с ценами для каждого товара
        """
        result = []
        seen = set()
        categories = await self._dns_discover_categories(session)
        if not categories:
            logger.warning("[DNS] Список категорий пустой, пропускаем DNS")
            return result
        get_headers = {
            **self.base_headers,
            "Referer": "https://www.dns-shop.kz/",
        }
        for cat_id, cat_slug in categories:
            cat_url = f"https://www.dns-shop.kz/catalog/{cat_id}/{cat_slug}/"
            for page in range(1, 4):
                params = {"order": 6}
                if page > 1:
                    params["p"] = page
                r = await safe_request(session, "GET", cat_url, headers=get_headers, params=params)
                if not r:
                    break
                soup = BeautifulSoup(r.text, "html.parser")
                product_ids = []
                for card in soup.select("[data-product]"):
                    entity_id = card.get("data-product", "").strip()
                    if entity_id and entity_id not in seen:
                        product_ids.append(entity_id)
                if not product_ids:
                    break
                for batch_start in range(0, len(product_ids), 18):
                    batch = product_ids[batch_start:batch_start + 18]
                    payload = {
                        "type": "product-buy",
                        "ids": batch
                    }
                    api_headers = {
                        "Host": "www.dns-shop.kz",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                        "Accept": "application/json, text/plain, */*",
                        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Content-Type": "application/json",
                        "X-Requested-With": "XMLHttpRequest",
                        "Origin": "https://www.dns-shop.kz",
                        "Referer": f"https://www.dns-shop.kz/catalog/{cat_id}/{cat_slug}/",
                        "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors",
                        "Sec-Fetch-Site": "same-origin",
                        "Cache-Control": "no-cache",
                        "Pragma": "no-cache",
                    }
                    api_url = "https://www.dns-shop.kz/ajax-state/product-buy/"
                    json_str = json.dumps(payload, ensure_ascii=False)
                    api_r = await safe_request(
                        session, "POST", api_url,
                        headers=api_headers,
                        content=json_str
                    )
                    if not api_r:
                        logger.warning(f"[DNS] API запрос вернул пусто: {api_r}")
                        continue
                    try:
                        if api_r.status_code != 200:
                            logger.warning(f"[DNS] API возвращает статус {api_r.status_code}, текст: {api_r.text[:200]}")
                            continue
                        response_text = api_r.text.strip()
                        if not response_text:
                            logger.warning(f"[DNS] API вернул пустой ответ")
                            continue
                        api_data = api_r.json()
                        if not api_data:
                            logger.warning(f"[DNS] JSON парсинг вернул пусто: {response_text[:200]}")
                            continue
                        items = await self._parse_dns_api_response(api_data, batch, seen)
                        result.extend(items)
                    except Exception as e:
                        logger.warning(f"[DNS] Ошибка парсинга API ответа: {e}\n  Ответ был: {api_r.text[:200] if api_r else 'None'}")
                    await asyncio.sleep(0.5)
        logger.info(f"[DNS] Собрано: {len(result)} товаров со скидками")
        return result
    async def _parse_dns_api_response(self, api_data: dict, product_ids: list, seen: set) -> List[Dict[str, Any]]:
        """Парсит ответ от /ajax-state/product-buy/ API"""
        items = []
        try:
            containers = api_data.get("containers", [])
            for container in containers:
                try:
                    container_id = container.get("id", "")
                    html = container.get("html", "")
                    if not html:
                        continue
                    soup = BeautifulSoup(html, "html.parser")
                    current_price_el = soup.select_one("[class*='current'], [class*='price']:not([class*='old'])")
                    old_price_el = soup.select_one("[class*='old'], [class*='crossed']")
                    if not (current_price_el and old_price_el):
                        continue
                    current_price_text = current_price_el.get_text(strip=True)
                    old_price_text = old_price_el.get_text(strip=True)
                    current = self._parse_price_val(current_price_text)
                    old = self._parse_price_val(old_price_text)
                    if not (current and old) or old <= current:
                        continue
                    link_el = soup.select_one("a[href*='/product/']")
                    if not link_el:
                        continue
                    href = link_el.get("href", "")
                    title = link_el.get_text(strip=True)[:100]
                    if not title:
                        continue
                    uid_m = re.search(r'/product/([0-9a-f\-]{30,})/', href)
                    uid = uid_m.group(1) if uid_m else href.split("/product/")[-1].split("/")[0]
                    if uid in seen:
                        continue
                    seen.add(uid)
                    link = f"https://www.dns-shop.kz{href}" if href.startswith("/") else href
                    items.append({
                        "id": f"dns_{uid}",
                        "title": title,
                        "old_price": fmt_price(old),
                        "new_price": fmt_price(current),
                        "discount": calc_discount(old, current),
                        "link": link,
                        "shop": "DNS 🔴",
                        "category": "tech",
                    })
                except Exception as e:
                    logger.debug(f"[DNS] Ошибка парсинга контейнера: {e}")
                    continue
        except Exception as e:
            logger.warning(f"[DNS] Ошибка при обработке API ответа: {e}")
        return items
    def _dns_parse_html(self, html: str, seen: set) -> List[Dict[str, Any]]:
        """
        Парсит HTML страницы каталога DNS shop KZ.
        Пробует несколько способов получить данные о товарах и ценах.
        """
        items = []
        soup = BeautifulSoup(html, "html.parser")
        for card in soup.select("[data-product]"):
            try:
                data = json.loads(card.get("data-product", "{}"))
                price     = data.get("price") or data.get("current_price")
                old_price = data.get("old_price") or data.get("base_price")
                title     = data.get("name") or data.get("title")
                uid       = str(data.get("id") or data.get("sku") or "")
                href      = data.get("url") or data.get("link") or ""
                if not (title and price and old_price and uid):
                    continue
                price     = float(re.sub(r"[^\d.]", "", str(price)))
                old_price = float(re.sub(r"[^\d.]", "", str(old_price)))
                if old_price <= price or uid in seen:
                    continue
                seen.add(uid)
                link = f"https://www.dns-shop.kz{href}" if href.startswith("/") else href
                items.append({
                    "id": f"dns_{uid}",
                    "title": title,
                    "old_price": fmt_price(int(old_price)),
                    "new_price": fmt_price(int(price)),
                    "discount": calc_discount(old_price, price),
                    "link": link,
                    "shop": "DNS 🔴",
                    "category": "tech",
                })
            except:
                continue
        if items:
            return items
        scripts = soup.find_all("script")
        for script in scripts:
            try:
                text = script.string or ""
                if "__NUXT_DATA__" not in text and "products" not in text:
                    continue
                if "window.__NUXT_DATA__" in text:
                    start = text.find("window.__NUXT_DATA__=") + len("window.__NUXT_DATA__=")
                    end = text.find("</script>", start)
                    if start > 0 and end > 0:
                        json_str = text[start:end].rstrip(";")
                        try:
                            data = json.loads(json_str)
                            products = self._extract_products_from_json(data, seen)
                            items.extend(products)
                            if items:
                                return items
                        except:
                            pass
            except:
                continue
        card_selectors = ["div.catalog-product", "div.product-card", "li.catalog-product"]
        cards = []
        for sel in card_selectors:
            cards = soup.select(sel)
            if cards:
                break
        for card in cards:
            try:
                title_el = card.select_one(
                    "a.catalog-product__name, "
                    ".catalog-product__name a, "
                    "a[class*='ProductName'], "
                    "a[href*='/product/']"
                )
                if not title_el:
                    continue
                href = title_el.get("href", "")
                title = title_el.get_text(strip=True)
                if not title or not href:
                    continue
                uid_m = re.search(r'/product/([0-9a-f\-]{30,})/', href)
                uid = uid_m.group(1) if uid_m else href.rstrip("/").split("/")[-1]
                if not uid or uid in seen:
                    continue
                link = f"https://www.dns-shop.kz{href}" if href.startswith("/") else href
                old_el = card.select_one("[class*='price-old'], [class*='old-price']")
                if not old_el:
                    continue
                seen.add(uid)
                items.append({
                    "id": f"dns_{uid}",
                    "title": title,
                    "old_price": "—",
                    "new_price": "—",
                    "discount": 0,
                    "link": link,
                    "shop": "DNS 🔴",
                    "category": "tech",
                })
            except:
                continue
        return items
    def _extract_products_from_json(self, data, seen: set) -> List[Dict[str, Any]]:
        """Рекурсивно ищет товары в JSON структуре"""
        items = []
        try:
            if isinstance(data, dict):
                for key in ["products", "items", "data", "content"]:
                    if key in data and isinstance(data[key], list):
                        for item in data[key]:
                            if isinstance(item, dict) and item.get("title"):
                                product = self._parse_json_product(item, seen)
                                if product:
                                    items.append(product)
                for v in data.values():
                    items.extend(self._extract_products_from_json(v, seen))
            elif isinstance(data, list):
                for item in data:
                    items.extend(self._extract_products_from_json(item, seen))
        except:
            pass
        return items
    def _parse_json_product(self, item: dict, seen: set) -> Optional[Dict[str, Any]]:
        """Парсит товар из JSON объекта"""
        try:
            title = item.get("title") or item.get("name") or ""
            if not title:
                return None
            uid = str(item.get("id") or item.get("sku") or "")
            if not uid or uid in seen:
                return None
            price = item.get("price") or item.get("current_price")
            old_price = item.get("old_price") or item.get("base_price")
            if not (price and old_price):
                return None
            try:
                price = float(re.sub(r"[^\d.]", "", str(price)))
                old_price = float(re.sub(r"[^\d.]", "", str(old_price)))
            except:
                return None
            if old_price <= price:
                return None
            seen.add(uid)
            href = item.get("url") or item.get("link") or ""
            link = f"https://www.dns-shop.kz{href}" if href.startswith("/") else (f"https://www.dns-shop.kz/product/{uid}/" if href == "" else href)
            return {
                "id": f"dns_{uid}",
                "title": title[:100],
                "old_price": fmt_price(int(old_price)),
                "new_price": fmt_price(int(price)),
                "discount": calc_discount(old_price, price),
                "link": link,
                "shop": "DNS 🔴",
                "category": "tech",
            }
        except:
            return None
    def _parse_price_val(self, raw) -> Optional[int]:
        if raw is None: return None
        digits = re.sub(r"[^\d]", "", str(raw))
        val = int(digits) if digits else None
        if val and (val < 500 or val > 2000000): return None
        return val
    async def fetch_discounts(self) -> List[Dict[str, Any]]:
        async with AsyncSession(impersonate=self.impersonate) as session:
            tasks = [
                self.fetch_technodom(session),
                self.fetch_sulpak(session),
                self.fetch_kaspi(session),
                self.fetch_alser(session),
                self.fetch_mechta(session),
                self.fetch_shopkz(session),
                self.fetch_meloman(session),
                self.fetch_freedom(session),
                self.fetch_adidas(session),
                self.fetch_dns(session),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            try: it = await self.fetch_intertop(session); results.append(it)
            except: results.append([])
        all_items = []
        seen = set()
        for r in results:
            if isinstance(r, list):
                for item in r:
                    if item["id"] not in seen:
                        seen.add(item["id"])
                        all_items.append(item)
        all_items.sort(key=lambda x: x.get("discount", 0), reverse=True)
        logger.info(f"=== ИТОГО: бот видит {len(all_items)} активных акций со всех работающих магазинов ===")
        return all_items
    async def get_single_product_price(self, url: str, shop: str) -> Optional[int]:
        async with AsyncSession(impersonate=self.impersonate) as session:
            try:
                r = await session.get(url, headers=self.base_headers, timeout=20)
                if r.status_code != 200: return None
                soup = BeautifulSoup(r.text, "html.parser")
                if shop == "Kaspi":
                    m = soup.find("meta", property="product:price:amount")
                    return int(float(m["content"])) if m else None
                elif shop == "Sulpak":
                    el = soup.select_one(".product__price, [data-price]")
                    return self._parse_price_val(el.get("data-price") or el.get_text()) if el else None
                elif shop == "Mechta":
                    from services.scraper import scraper_service
                    return await scraper_service.fetch_price(url)
            except: pass
        return None
parser = DiscountParser()