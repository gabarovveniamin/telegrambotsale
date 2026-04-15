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
        # Основные корневые категории по запросу пользователя
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
        """
        Глубокий парсинг основных разделов Kaspi (7 главных категорий).
        """
        all_items = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            
            for target in self.targets:
                url = f"https://kaspi.kz/shop/c/{target['slug']}/?q=%3AavailableInZones%3A750000000"
                items_from_cat = await self._parse_category_deep(context, url, target['label'])
                all_items.extend(items_from_cat)
                await asyncio.sleep(2) # Пауза для стабильности
            
            await browser.close()
            
        logger.info(f"[ScraperService] Глубокий сбор завершен. Всего товаров: {len(all_items)}")
        return all_items

    async def _parse_category_deep(self, context, url: str, category_label: str) -> list:
        """Метод для глубокого парсинга (10 скроллов) одной категории."""
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        category_items = []
        try:
            category_name = url.split('/')[-2].replace('%20', ' ')
            logger.info(f"[ScraperService] Сафари в раздел: {category_name}...")
            
            await page.goto(url, wait_until="load", timeout=45000)
            
            # РЕЖИМ ГЛУБОКОГО ПРОСМОТРА: 10 скроллов
            for _ in range(10):
                await page.evaluate("window.scrollBy(0, 1000)")
                await asyncio.sleep(1.2)
            
            await asyncio.sleep(2) # Ждем финальную прогрузку
            
            cards = await page.query_selector_all("[class*='card'], .item-card, .p-card")
            logger.info(f"[ScraperService] В разделе '{category_name}' захвачено {len(cards)} карточек")
            
            for card in cards:
                try:
                    title_el = await card.query_selector("a[class*='name'], a[class*='title']")
                    price_el = await card.query_selector("[class*='price-once'], [class*='prices-price']")
                    
                    if title_el and price_el:
                        title = (await title_el.inner_text()).strip()
                        href = await title_el.get_attribute("href")
                        price_val = self._extract_price(await price_el.inner_text())
                        
                        if price_val and href:
                            product_id = href.split('/')[-2] if '/' in href else href
                            category_items.append({
                                "id": f"kp_all_{product_id}",
                                "title": title,
                                "new_price": price_val,
                                "old_price": 0,
                                "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                                "shop": "Kaspi", "category": category_label
                            })
                except: continue
        except Exception as e:
            logger.error(f"[ScraperService] Ошибка в разделе {url}: {e}")
        finally:
            await page.close()
            
        return category_items

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    async def fetch_price(self, url: str) -> Optional[int]:
        """Для персональной следилки."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                await page.goto(url, wait_until="load", timeout=30000)
                await asyncio.sleep(2)
                el = await page.query_selector("[class*='price-once'], [class*='prices-price'], .price")
                return self._extract_price(await el.inner_text()) if el else None
            except: return None
            finally: await browser.close()

scraper_service = ScraperService()
