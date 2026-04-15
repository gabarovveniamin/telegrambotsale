import asyncio
import logging
import re
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
            
            await page.goto(url, wait_until="networkidle", timeout=45000)
            await asyncio.sleep(3)
            
            # --- УНИВЕРСАЛЬНЫЙ ПОИСК ПОДКАТЕГОРИЙ ---
            subcat_links = await page.evaluate("""() => {
                const links = Array.from(document.querySelectorAll('a'));
                return links
                    .map(a => a.href)
                    .filter(href => {
                        return href.includes('/shop/c/') && 
                               !href.includes('?') && 
                               !['partners', 'clients', 'guide'].some(w => href.includes(w));
                    })
                    .slice(0, 15); // Берем побольше
            }""")
            
            # Фильтруем оригинальный URL
            links_to_scan = [l for l in list(set(subcat_links)) if l != url]
            
            # Парсим главную раздела
            await self._deep_scroll_and_capture(page, captured_data, category_label)
            
            # Парсим подразделы (ограничим до 5 самых длинных ссылок для глубины)
            links_to_scan.sort(key=len, reverse=True)
            for scan_url in links_to_scan[:6]:
                try:
                    logger.info(f"[ScraperService] -> Ныряем в: {scan_url.split('/')[-2]}")
                    await page.goto(scan_url, wait_until="networkidle", timeout=30000)
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
        for _ in range(8): 
            await page.evaluate("window.scrollBy(0, 1100)")
            await asyncio.sleep(1.5)
            
            products = await page.evaluate("""() => {
                const results = [];
                // Берем все элементы, которые похожи на карточки
                const cards = document.querySelectorAll("div[class*='item-card'], div[class*='product-card'], ._9S-5Xl");
                cards.forEach(node => {
                    const titleEl = node.querySelector('a[class*="name"], a[class*="title"], .item-card__name-link');
                    const priceEl = node.querySelector("[class*='price'], [class*='prices-price']");
                    const linkEl = node.querySelector('a[href*="/shop/p/"]');
                    
                    if (titleEl && priceEl && linkEl) {
                        results.push({
                            title: titleEl.innerText.trim(),
                            href: linkEl.getAttribute('href'),
                            priceText: priceEl.innerText
                        });
                    }
                });
                return results;
            }""")
            
            for data in products:
                href = data['href']
                if not href: continue
                
                # Ищем ID товара (числа в ссылке)
                # Пример: /shop/p/apple-iphone-13-128gb-chernyi-102298404/
                nums = re.findall(r'\d+', href)
                p_id = nums[-1] if nums else None
                
                if not p_id:
                    p_id = href.strip('/').split('/')[-1]
                
                full_id = f"kp_all_{p_id}"
                
                if full_id not in captured_data:
                    price_val = self._extract_price(data['priceText'])
                    if price_val:
                        captured_data[full_id] = {
                            "id": full_id, "title": data['title'],
                            "new_price": price_val, "old_price": 0,
                            "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                            "shop": "Kaspi", "category": category_label
                        }

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        # Очистка максимально жесткая
        clean_text = text.replace(' ', '').replace('\xa0', '')
        # Отрезаем всё что после основного числа (мес, тенге и т.д.)
        # Каспи цена обычно идет первой: "120 000 ₸ х12 мес"
        match = re.search(r'(\d+)', clean_text)
        return int(match.group(1)) if match else None

    async def fetch_price(self, url: str) -> Optional[int]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                await page.goto(url, wait_until="load", timeout=30000)
                await asyncio.sleep(2)
                el = await page.query_selector("[class*='price'], .item-card__prices-price")
                return self._extract_price(await el.inner_text()) if el else None
            except: return None
            finally: await browser.close()

scraper_service = ScraperService()
