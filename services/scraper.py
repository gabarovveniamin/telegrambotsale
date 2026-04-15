import asyncio
import logging
import re
import hashlib
from typing import Optional, List
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

class ScraperService:
    def __init__(self):
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        self.targets = [
            {"slug": "smartphones%20and%20gadgets", "label": "tech"},
            {"slug": "home%20equipment", "label": "tech"},
            {"slug": "tv_audio", "label": "tech"},
            {"slug": "computers", "label": "tech"},
            {"slug": "furniture", "label": "other"},
            {"slug": "beauty%20care", "label": "other"},
            {"slug": "car%20goods", "label": "other"}
        ]

    async def fetch_kaspi_discounts(self) -> list:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 1600}, locale="ru-RU")
            
            all_items = []
            for target in self.targets:
                url = f"https://kaspi.kz/shop/c/{target['slug']}/?q=%3AavailableInZones%3A750000000"
                items_from_cat = await self._parse_category_smart(context, url, target['label'])
                all_items.extend(items_from_cat)
                await asyncio.sleep(2)
            
            await browser.close()
            
        seen_ids = set()
        final_list = []
        for i in all_items:
            if i["id"] not in seen_ids:
                seen_ids.add(i["id"])
                final_list.append(i)
                
        logger.info(f"[ScraperService] Глобальный сбор завершен. ИТОГО УНИКАЛЬНЫХ: {len(final_list)}")
        return final_list

    async def _parse_category_smart(self, context, url: str, category_label: str) -> list:
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        captured_data = {}
        
        try:
            category_name = url.split('/')[-2].replace('%20', ' ')
            logger.info(f"[ScraperService] Раздел: {category_name}")
            
            await page.goto(url, wait_until="networkidle", timeout=50000)
            await asyncio.sleep(4)
            
            # --- УНИКАЛЬНЫЙ ФИЛЬТР ПОДКАТЕГОРИЙ (БЕЗ МУСОРА) ---
            subcat_links = await page.evaluate("""() => {
                const links = Array.from(document.querySelectorAll('a'));
                const blacklist = ['gos', 'announcements', 'actions', 'mybank', 'travel', 'mobile', 'pay', 'news', 'about', 'recommendations', 'lifestyle'];
                return links
                    .map(a => a.href)
                    .filter(href => {
                        const isNode = href.includes('/shop/c/');
                        const notBad = !blacklist.some(b => href.includes(b));
                        const isClean = !href.includes('?') && !href.includes('social');
                        return isNode && notBad && isClean;
                    })
                    .slice(0, 12);
            }""")
            
            links_to_scan = [l for l in list(set(subcat_links)) if l != url]
            
            # 1. Парсим главную
            await self._deep_scroll_and_capture(page, captured_data, category_label)
            
            # 2. Парсим топ-5 подразделов
            for scan_url in links_to_scan[:5]:
                try:
                    logger.info(f"[ScraperService] -> Ныряем в подраздел: {scan_url.split('/')[-2]}")
                    await page.goto(scan_url, wait_until="networkidle", timeout=35000)
                    await asyncio.sleep(2)
                    await self._deep_scroll_and_capture(page, captured_data, category_label)
                except: continue

            logger.info(f"[ScraperService] {category_name} готов. Уникальных товаров: {len(captured_data)}")
        except Exception as e:
            logger.error(f"[ScraperService] Ошибка в разделе {url}: {e}")
        finally:
            await page.close()
            
        return list(captured_data.values())

    async def _deep_scroll_and_capture(self, page, captured_data, category_label):
        """Анонимный сбор через поиск паттернов (не зависит от классов)"""
        for _ in range(8): 
            await page.evaluate("window.scrollBy(0, 1200)")
            await asyncio.sleep(1.4)
            
            # Этот скрипт ищет ссылки на товары и цены рядом с ними
            products = await page.evaluate("""() => {
                const results = [];
                // Ищем все ссылки на товары
                const productLinks = Array.from(document.querySelectorAll('a[href*="/shop/p/"]'));
                
                productLinks.forEach(link => {
                    const href = link.getAttribute('href');
                    const title = link.innerText.trim();
                    
                    if (title.length < 5 || href.includes('reviews')) return;

                    // Пытаемся найти цену в родительском контейнере этой ссылки
                    let parent = link.parentElement;
                    let priceText = "";
                    
                    // Поднимаемся на 4 уровня вверх и ищем текст с символом тенге
                    for(let i=0; i<4; i++) {
                        if (!parent) break;
                        const text = parent.innerText;
                        if (text.includes('₸')) {
                            const priceMatch = text.match(/(\\d[\\d\\s]+\\s*₸)/);
                            if (priceMatch) {
                                priceText = priceMatch[0];
                                break;
                            }
                        }
                        parent = parent.parentElement;
                    }

                    if (priceText) {
                        results.push({ title, href, priceText });
                    }
                });
                return results;
            }""")
            
            for data in products:
                href = data['href'].split('?')[0].rstrip('/')
                # ID - это уникальный цифровой код в конце ссылки
                id_match = re.search(r'(\d+)$', href)
                p_id = id_match.group(1) if id_match else hashlib.md5(href.encode()).hexdigest()[:10]
                
                full_id = f"kp_all_{p_id}"
                
                if full_id not in captured_data:
                    price_val = self._extract_price(data['priceText'])
                    if price_val and price_val > 50:
                        captured_data[full_id] = {
                            "id": full_id, "title": data['title'],
                            "new_price": price_val, "old_price": 0,
                            "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                            "shop": "Kaspi", "category": category_label
                        }

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    async def fetch_price(self, url: str) -> Optional[int]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                await page.goto(url, wait_until="load", timeout=30000)
                await asyncio.sleep(2)
                # Ищем любой текст со значком тенге
                content = await page.content()
                price_match = re.search(r'(\d[\d\s]+\s*₸)', content)
                return self._extract_price(price_match.group(1)) if price_match else None
            except: return None
            finally: await browser.close()

scraper_service = ScraperService()
