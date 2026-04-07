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
        queries = ["смартфон", "ноутбук", "телевизор", "iphone", "samsung", "наушники"]
        seen: set = set()

        for q in queries:
            for page in range(1, 3):
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
                if r is None: break
                try:
                    data = r.json()
                    products = data.get("products") or data.get("items") or []
                    if not products: break
                    for p in products:
                        pid = str(p.get("id") or p.get("sku") or "").strip()
                        if not pid or pid in seen: continue
                        seen.add(pid)
                        title = (p.get("name") or "").strip()
                        slug = (p.get("slug") or "").strip()
                        old_p = p.get("old_price")
                        new_p = p.get("price")
                        if not (title and slug and old_p and new_p and old_p > new_p): continue
                        result.append({
                            "id": f"td_{pid}",
                            "title": title,
                            "old_price": fmt_price(old_p),
                            "new_price": fmt_price(new_p),
                            "discount": calc_discount(old_p, new_p),
                            "link": f"https://www.technodom.kz/p/{slug}",
                            "shop": "Technodom",
                            "category": "tech",
                        })
                    if len(products) < PAGE_SIZE: break
                except: break
                await asyncio.sleep(0.3)
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  SULPAK
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_category_sulpak(self, session: AsyncSession, category: str, seen_ids: set) -> List[Dict[str, Any]]:
        result = []
        for page in range(1, 4):
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
        for page in range(1, 4):
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
        for cat in ["smartfoniy", "noutbuki", "televizoriy", "holodilnikiy", "stiralniye_mashiniy", "planshetiy"]:
            result.extend(await self.fetch_category_sulpak(session, cat, seen_ids))
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  MECHTA
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_mechta(self, session: AsyncSession) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        seen_ids = set()
        categories = ["smartfony", "noutbuki", "televizory"]
        for cat in categories:
            for page in range(1, 3):
                url = f"https://www.mechta.kz/category/{cat}/_payload.js" + (f"?page={page}" if page > 1 else "")
                r = await safe_request(session, "GET", url, headers=self.base_headers)
                if not r: break
                blocks = re.split(r'(?="?id"?\s*:)', r.text)
                for b in blocks:
                    if "oldPrice" not in b: continue
                    try:
                        title = re.search(r'"?(?:name|title)"?\s*:\s*"(.*?)"', b).group(1).encode().decode("unicode_escape", errors="ignore")
                        price = float(re.search(r'"?(?:finalPrice|price)"?\s*:\s*(\d+)', b).group(1))
                        old = float(re.search(r'"?(?:basePrice|oldPrice)"?\s*:\s*(\d+)', b).group(1))
                        slug = re.search(r'"?slug"?\s*:\s*"(.*?)"', b).group(1)
                        if old > price:
                            uid = f"mc_{slug}"
                            if uid not in seen_ids:
                                seen_ids.add(uid)
                                result.append({
                                    "id": uid, "title": title, "old_price": fmt_price(int(old)),
                                    "new_price": fmt_price(int(price)), "discount": calc_discount(old, price),
                                    "link": f"https://www.mechta.kz/product/{slug}", "shop": "Mechta 🔵", "category": "tech",
                                })
                    except: continue
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  KASPI
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_kaspi(self, session: AsyncSession) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        headers = {
            **self.base_headers,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://kaspi.kz/",
            "Origin": "https://kaspi.kz",
            "X-KS-City": CITY_ID_KASPI,
            "X-Requested-With": "XMLHttpRequest",
        }
        for page in range(0, 3):
            params = {":availableInZones": CITY_ID_KASPI, "page": page, "pageSize": 48}
            r = await safe_request(session, "GET", "https://kaspi.kz/yml/product-view/pl/filters", headers=headers, params=params)
            if not r: break
            try:
                cards = r.json().get("data", {}).get("cards") or []
                for o in cards:
                    pid = str(o.get("id", ""))
                    title = o.get("title", "")
                    old_p = o.get("unitPrice")
                    new_p = o.get("unitSalePrice")
                    if pid and title and old_p and new_p and old_p > new_p:
                        result.append({
                            "id": f"kp_{pid}", "title": title, "old_price": fmt_price(old_p),
                            "new_price": fmt_price(new_p), "discount": calc_discount(old_p, new_p),
                            "link": f"https://kaspi.kz/shop/p/{pid}/", "shop": "Kaspi", "category": "tech",
                        })
            except: break
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  ALSER
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_alser(self, session: AsyncSession) -> List[Dict[str, Any]]:
        result = []
        seen_ids = set()
        for cat in ["smartfony-i-planshety", "noutbuki-i-kompyutery", "televizory"]:
            for page in range(1, 3):
                url = f"https://alser.kz/c/{cat}/_payload.js" + (f"?page={page}" if page > 1 else "")
                r = await safe_request(session, "GET", url, headers=self.base_headers)
                if not r: break
                blocks = re.split(r'\{\s*"?id"?\s*:', r.text)
                for b in blocks:
                    if "oldPrice" not in b: continue
                    try:
                        title = re.search(r'title\s*:\s*"(.*?)"', b).group(1).replace("\\\"", "\"")
                        sku = re.search(r'sku\s*:\s*"(.*?)"', b).group(1)
                        price = float(re.search(r'(?<!"old)[Pp]rice\s*:\s*(\d+)', b).group(1))
                        old = float(re.search(r'oldPrice\s*:\s*(\d+)', b).group(1))
                        link = re.search(r'link_url\s*:\s*"(.*?)"', b).group(1).replace("\\u002f", "/")
                        if old > price and sku not in seen_ids:
                            seen_ids.add(sku)
                            result.append({
                                "id": f"al_{sku}", "title": title, "old_price": fmt_price(int(old)),
                                "new_price": fmt_price(int(price)), "discount": calc_discount(old, price),
                                "link": f"https://alser.kz{link}", "shop": "Alser", "category": "tech",
                            })
                    except: continue
        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  SHOP.KZ
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_shopkz(self, session: AsyncSession) -> List[Dict[str, Any]]:
        r = await safe_request(session, "GET", "https://shop.kz/bitrix/catalog_export/yandex.php")
        if not r: return []
        try:
            root = ET.fromstring(r.content)
            res = []
            for off in root.findall(".//offer")[:500]:
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

    # ─────────────────────────────────────────────────────────────────────────
    #  MELOMAN (Loyalty API)
    # ─────────────────────────────────────────────────────────────────────────
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
            max_pages = 10 if "q=" in cat else 3
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

    # ─────────────────────────────────────────────────────────────────────────
    #  FREEDOM MOBILE
    # ─────────────────────────────────────────────────────────────────────────
    async def fetch_freedom(self, session: AsyncSession) -> List[Dict[str, Any]]:
        url = "https://api.fmobile.kz/catalog/api/v2/catalog/listing"
        heads = {**self.base_headers, "Referer": "https://fmobile.kz/", "Origin": "https://fmobile.kz"}
        res = []
        for page in range(1, 5):
            params = {"channel": "ONLINE", "city_slug": CITY_SLUG_MECHTA, "page": page, "size": 50}
            r = await safe_request(session, "GET", url, headers=heads, params=params)
            if not r: break
            try:
                items = r.json().get("result", {}).get("items") or []
                for i in items:
                    sku, title, np, op = i.get("sku"), i.get("model_stock_name"), i.get("price"), i.get("old_price")
                    if sku and title and np and op and op > np:
                        res.append({
                            "id": f"fm_{sku}", "title": title, "old_price": fmt_price(op),
                            "new_price": fmt_price(np), "discount": calc_discount(op, np),
                            "link": f"https://fmobile.kz/category/{i.get('category_slug')}/{i.get('model_stock_slug')}",
                            "shop": "Freedom Mobile 🟢", "category": "tech",
                        })
            except: break
        return res

    # ─────────────────────────────────────────────────────────────────────────
    #  ADIDAS KZ
    # ─────────────────────────────────────────────────────────────────────────
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
            for pg in range(1, 3):
                start = (pg - 1) * 48
                url = f"https://adidas.kz/{slug}/" + (f"?start={start}" if pg > 1 else "")
                r = await safe_request(session, "GET", url, headers=self.base_headers)
                if not r: break
                soup = BeautifulSoup(r.text, "html.parser")
                
                # HTML Card Parsing (More reliable for old prices)
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

    # ─────────────────────────────────────────────────────────────────────────
    #  INTERTOP
    # ─────────────────────────────────────────────────────────────────────────
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
                    brand = card.select_one(".in-product-tile__product-brand")
                    name = card.select_one(".in-product-tile__product-name")
                    if not (brand and name): continue
                    
                    title = f"[{label}] {brand.get_text(strip=True)} {name.get_text(strip=True)}"
                    
                    l_el = card.select_one("a[href*='/product/']")
                    if not l_el: continue
                    link = f"https://intertop.kz{l_el['href']}" if l_el['href'].startswith("/") else l_el['href']
                    
                    if link in seen: continue
                    
                    price_regular = card.select_one(".in-price__regular")
                    price_actual = card.select_one(".in-price__actual")
                    
                    if price_regular and price_actual:
                        op = self._parse_price_val(price_regular.get_text())
                        np = self._parse_price_val(price_actual.get_text())
                        
                        if np and op and op > np:
                            seen.add(link)
                            res.append({
                                "id": f"it_{hash(link)}",
                                "title": title,
                                "old_price": fmt_price(op),
                                "new_price": fmt_price(np),
                                "discount": calc_discount(op, np),
                                "link": link,
                                "shop": "Intertop 👟",
                                "category": "fashion",
                            })
                except: continue
        return res

    def _parse_price_val(self, raw) -> Optional[int]:
        if raw is None: return None
        digits = re.sub(r"[^\d]", "", str(raw))
        val = int(digits) if digits else None
        if val and (val < 500 or val > 2000000): return None
        return val

    async def fetch_discounts(self) -> List[Dict[str, Any]]:
        async with AsyncSession(impersonate=self.impersonate) as session:
            tasks = [self.fetch_technodom(session), self.fetch_sulpak(session), self.fetch_kaspi(session), 
                     self.fetch_alser(session), self.fetch_shopkz(session), self.fetch_meloman(session), 
                     self.fetch_freedom(session), self.fetch_adidas(session)]
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
            except: pass
        return None

parser = DiscountParser()