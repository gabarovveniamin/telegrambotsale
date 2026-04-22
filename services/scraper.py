import asyncio
import logging
import random
import hashlib
import re
from typing import Optional, List, Dict
from playwright.async_api import async_playwright, Page, BrowserContext
from playwright_stealth import Stealth
from curl_cffi.requests import AsyncSession
logger = logging.getLogger(__name__)
class ScraperService:
    def __init__(self):
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        self.targets = [
            {"slug": "smartphones", "label": "tech"},
            {"slug": "home-appliances", "label": "tech"},
            {"slug": "tv", "label": "tech"},
            {"slug": "computers", "label": "tech"},
            {"slug": "furniture", "label": "other"},
            {"slug": "beauty-care", "label": "other"},
            {"slug": "car-goods", "label": "other"},
            {"slug": "sport-goods", "label": "other"}
        ]
        self.city_id = "750000000"
        self.mechta_targets = [
            {"slug": "smartfony-i-gadjety", "label": "Смартфоны"},
            {"slug": "tekhnika-dlya-doma", "label": "Техника для дома"},
            {"slug": "bytovaya-tekhnika", "label": "Бытовая техника"},
            {"slug": "tv-audio-video", "label": "ТВ и Аудио"},
            {"slug": "pk-noutbuky-periferiya", "label": "Компьютеры"},
            {"slug": "krasota-i-zdorove", "label": "Красота и здоровье"}
        ]
    async def fetch_kaspi_discounts(self) -> list:
        kaspi_categories = [
            {"code": "Smartphones", "label": "kaspi"},
            {"code": "Notebooks", "label": "kaspi"},
            {"code": "Tablets", "label": "kaspi"},
            {"code": "Headphones", "label": "kaspi"},
            {"code": "tv_audio", "label": "kaspi"},
            {"code": "Refrigerators", "label": "kaspi"},
            {"code": "Vacuum Cleaners", "label": "kaspi"},
            {"code": "Monitors", "label": "kaspi"},
            {"code": "Computers", "label": "kaspi"},
            {"code": "Desktop Computers", "label": "kaspi"},
            {"code": "Game consoles", "label": "kaspi"},
            {"code": "home equipment", "label": "kaspi"},
            {"code": "Furniture", "label": "kaspi"},
            {"code": "beauty care", "label": "kaspi"},
            {"code": "Car Goods", "label": "kaspi"},
            {"code": "sports and outdoors", "label": "kaspi"},
            {"code": "child goods", "label": "kaspi"},
            {"code": "pharmacy", "label": "kaspi"},
            {"code": "construction and repair", "label": "kaspi"},
            {"code": "fashion", "label": "kaspi"},
            {"code": "shoes", "label": "kaspi"},
            {"code": "fashion accessories", "label": "kaspi"},
            {"code": "jewelry and bijouterie", "label": "kaspi"},
            {"code": "home", "label": "kaspi"},
            {"code": "pet goods", "label": "kaspi"},
            {"code": "leisure", "label": "kaspi"},
        ]
        all_items = []
        async with AsyncSession(impersonate="chrome124") as session:
            for cat in kaspi_categories:
                try:
                    items = await self._fetch_kaspi_category_api(session, cat["code"], cat["label"])
                    all_items.extend(items)
                    logger.info(f"[Kaspi API] {cat['code']} -> {len(items)} товаров")
                except Exception as e:
                    logger.error(f"[Kaspi API] Ошибка категории {cat['code']}: {e}")
                await asyncio.sleep(random.uniform(1, 3))
        seen = set()
        final = []
        for i in all_items:
            if i["id"] not in seen:
                seen.add(i["id"])
                final.append(i)
        logger.info(f"[Kaspi API] Итого собрано уникальных товаров: {len(final)}")
        return final
    async def _fetch_kaspi_category_api(self, session: AsyncSession, category_code: str, label: str) -> list:
        items = []
        import urllib.parse
        cat_encoded = urllib.parse.quote(category_code)
        headers = {
            "Accept": "application/json, text/*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "User-Agent": self.user_agent,
            "Referer": "https://kaspi.kz/shop/",
            "Origin": "https://kaspi.kz",
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Ks-City": self.city_id,
        }
        max_pages = 10
        limit = 12
        for page in range(max_pages):
            try:
                url = (
                    f"https://kaspi.kz/yml/product-view/pl/results"
                    f"?q=%3AavailableInZones%3A{self.city_id}%3Acategory%3A{cat_encoded}"
                    f"&sort=relevance&sc=&ui=d&i=-1&c={self.city_id}"
                    f"&page={page}&limit={limit}"
                )
                r = await session.get(url, headers=headers, timeout=20)
                if r.status_code != 200:
                    logger.warning(f"[Kaspi API] {category_code} page {page} -> HTTP {r.status_code}")
                    break
                data = r.json()
                products = data.get("data", [])
                if not products:
                    break
                for p in products:
                    product_id = str(p.get("id", ""))
                    if not product_id:
                        continue
                    title = p.get("title", "")
                    price = p.get("unitSalePrice") or p.get("unitPrice")
                    if not price:
                        continue
                    price = int(price)
                    shop_link = p.get("shopLink", "")
                    link = f"https://kaspi.kz{shop_link}" if shop_link else ""
                    images = p.get("previewImages", [])
                    img_url = ""
                    if images:
                        img_url = images[0].get("medium", "") or images[0].get("small", "")
                    items.append({
                        "id": f"kp_{product_id}",
                        "title": title,
                        "new_price": price,
                        "old_price": 0,
                        "link": link,
                        "image": img_url,
                        "shop": "Kaspi",
                        "category": label
                    })
                if len(products) < limit:
                    break
                await asyncio.sleep(random.uniform(0.5, 1.5))
            except Exception as e:
                logger.error(f"[Kaspi API] Ошибка при загрузке {category_code} page {page}: {e}")
                break
        return items
    async def fetch_price(self, url: str) -> Optional[int]:
        m = re.search(r'(\d{5,})', url)
        if not m:
            logger.warning(f"[ScraperService] fetch_price: не удалось извлечь ID из URL: {url}")
            return None
        product_id = m.group(1)
        try:
            async with AsyncSession(impersonate="chrome124") as session:
                api_url = (
                    f"https://kaspi.kz/yml/product-view/pl/results"
                    f"?q={product_id}%3AavailableInZones%3A{self.city_id}"
                    f"&sort=relevance&sc=&ui=d&i=-1&c={self.city_id}"
                    f"&page=0&limit=5"
                )
                headers = {
                    "Accept": "application/json, text/*",
                    "Accept-Language": "ru-RU,ru;q=0.9",
                    "User-Agent": self.user_agent,
                    "Referer": "https://kaspi.kz/shop/",
                    "X-Ks-City": self.city_id,
                }
                r = await session.get(api_url, headers=headers, timeout=20)
                if r.status_code != 200:
                    logger.warning(f"[ScraperService] fetch_price API HTTP {r.status_code}")
                    return None
                data = r.json()
                products = data.get("data", [])
                for p in products:
                    if str(p.get("id", "")) == product_id:
                        return int(p.get("unitSalePrice") or p.get("unitPrice", 0))
                if products:
                    return int(products[0].get("unitSalePrice") or products[0].get("unitPrice", 0))
        except Exception as e:
            logger.error(f"[ScraperService] fetch_price API error: {e}")
        return None
    def _extract_price(self, text: str) -> Optional[int]:
        if not text or "мес" in text.lower(): return None
        digits = re.sub(r"[^\d]", "", str(text))
        return int(digits) if digits else None
    async def fetch_mechta_discounts(self) -> list:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            all_items = []
            try:
                logger.info("[ScraperService] Mechta: обход Cloudflare...")
                await page.goto("https://www.mechta.kz/", wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(8)
            except: pass
            for target in self.mechta_targets:
                try:
                    logger.info(f"[ScraperService] Mechta: раздел {target['label']}")
                    await page.goto(f"https://www.mechta.kz/section/{target['slug']}/", wait_until="domcontentloaded", timeout=60000)
                    links_found = 0
                    for _ in range(15):
                        links_found = await page.evaluate('() => document.querySelectorAll("a[href*=\\"/product/\\"]").length')
                        if links_found > 0: break
                        await asyncio.sleep(1)
                    logger.info(f"[ScraperService] Mechta: {target['label']} -> {links_found} ссылок")
                    await page.mouse.wheel(0, 1000)
                    await asyncio.sleep(4)
                    items = await page.evaluate(r"""() => {
                        const res = [];
                        const links = Array.from(document.querySelectorAll('a[href*="/product/"]'));
                        const seen = new Set();
                        links.forEach(l => {
                            const href = l.href;
                            if (seen.has(href)) return;
                            seen.add(href);
                            // Очистка названия: убираем бонусы, отзывы и цену из текста
                            let title = l.innerText.replace(/Нет отзывов/g, '')
                                                 .replace(/\+\s?\d[\d\s]*/g, '')
                                                 .split('\n')[0].trim();
                            if (title.length < 5) return;
                            let p = l.parentElement;
                            // Ограничиваем поиск родителя (обычно карточка не глубже 4-5 уровней)
                            for(let i=0; i<5; i++) {
                                if (!p) break;
                                const prices = p.innerText.match(/(\d[\d\s]*[₸TТ])/g);
                                // Ищем именно две разные цены внутри одного блока
                                if (prices && prices.length >= 2) {
                                    const p1 = parseInt(prices[0].replace(/[^\d]/g, ''));
                                    const p2 = parseInt(prices[1].replace(/[^\d]/g, ''));
                                    // Скидка должна быть логичной (не более 60%)
                                    const high = Math.max(p1, p2);
                                    const low = Math.min(p1, p2);
                                    const discountPercent = (high - low) / high;
                                    if (p1 > 0 && p2 > 0 && p1 !== p2 && discountPercent < 0.6) {
                                        res.push({
                                            title: title,
                                            old: high,
                                            new: low,
                                            link: href,
                                            img: p.querySelector('img')?.src || ''
                                        });
                                        break;
                                    }
                                }
                                p = p.parentElement;
                            }
                        });
                        return res;
                    }""")
                    if items:
                        logger.info(f"[ScraperService] Mechta: {target['label']} -> {len(items)} акций")
                        for it in items:
                            all_items.append({
                                "id": f"mc_{hash(it['link'])}", "title": it['title'], "new_price": it['new'],
                                "old_price": it['old'], "link": it['link'], "image": it['img'],
                                "shop": "Mechta", "category": target['label']
                            })
                except Exception as e:
                    logger.warning(f"Ошибка Mechta {target['label']}: {e}")
            await browser.close()
        return list({it['link']: it for it in all_items}.values())
scraper_service = ScraperService()
