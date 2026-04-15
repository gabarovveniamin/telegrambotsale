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
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 1000}, locale="ru-RU")
            
            all_items = []
            for target in self.targets:
                url = f"https://kaspi.kz/shop/c/{target['slug']}/?q=%3AavailableInZones%3A750000000"
                items_from_cat = await self._parse_category_deep(context, url, target['label'])
                all_items.extend(items_from_cat)
                await asyncio.sleep(1)
            
            await browser.close()
            
        seen = set()
        unique_list = []
        for i in all_items:
            if i["id"] not in seen:
                seen.add(i["id"])
                unique_list.append(i)
                
        logger.info(f"[ScraperService] Глубокий сбор завершен. Итого уникальных товаров: {len(unique_list)}")
        return unique_list

    async def _parse_category_deep(self, context, url: str, category_label: str) -> list:
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        category_items = []
        try:
            category_name = url.split('/')[-2].replace('%20', ' ')
            logger.info(f"[ScraperService] Раздел: {category_name}")
            
            await page.goto(url, wait_until="load", timeout=45000)
            
            # Скроллим
            for _ in range(8):
                await page.evaluate("window.scrollBy(0, 1000)")
                await asyncio.sleep(1)
            
            await asyncio.sleep(3)
            
            # Собираем ВСЕ карточки
            cards = await page.query_selector_all("[class*='card'], .item-card, .p-card")
            logger.info(f"[ScraperService] Найдено {len(cards)} карточек в {category_name}")
            
            for card in cards:
                try:
                    # Извлекаем данные через evaluate, это надежнее чем query_selector
                    data = await card.evaluate("""(node) => {
                        const titleEl = node.querySelector('a[class*="name"], a[class*="title"], .item-card__name-link');
                        const priceEl = node.querySelector('[class*="price-once"], [class*="prices-price"], .item-card__prices-price');
                        
                        if (!titleEl || !priceEl) return null;
                        
                        return {
                            title: titleEl.innerText,
                            href: titleEl.getAttribute('href'),
                            priceText: priceEl.innerText
                        };
                    }""")
                    
                    if data:
                        price_val = self._extract_price(data['priceText'])
                        if price_val and data['href']:
                            product_id = data['href'].split('/')[-2] if '/' in data['href'] else data['href']
                            category_items.append({
                                "id": f"kp_all_{product_id}",
                                "title": data['title'].strip(),
                                "new_price": price_val,
                                "old_price": 0,
                                "link": f"https://kaspi.kz{data['href']}" if data['href'].startswith("/") else data['href'],
                                "shop": "Kaspi", "category": category_label
                            })
                except: continue
                
            logger.info(f"[ScraperService] Успешно извлечено: {len(category_items)}")
        except Exception as e:
            logger.error(f"[ScraperService] Ошибка в {url}: {e}")
        finally:
            await page.close()
            
        return category_items

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        # Если в строке есть 'x' и 'рассрочку' — это блок рассрочки, пытаемся найти основную цену
        # Обычно основная цена идет ПЕРВОЙ или она КРУПНЕЕ.
        # Просто убираем всё кроме цифр. Если там "120 000 ₸ 5 000 ₸ x 24", 
        # то забираем первые цифры до пробела или знака рассрочки.
        
        # Очищаем от мусора
        text = text.split('x')[0].split('х')[0] # отсекаем всё после знака рассрочки
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    async def fetch_price(self, url: str) -> Optional[int]:
        # (без изменений)
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                await page.goto(url, wait_until="load", timeout=30000)
                await asyncio.sleep(2)
                el = await page.query_selector("[class*='price-once'], [class*='prices-price']")
                return self._extract_price(await el.inner_text()) if el else None
            except: return None
            finally: await browser.close()

scraper_service = ScraperService()
