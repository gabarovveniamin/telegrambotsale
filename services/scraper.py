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
        # Список самых популярных категорий на Каспи
        self.categories = {
            "tech": [
                "smartphones", "laptops", "televisions", "tablets", 
                "refrigerators", "washing-machines", "headphones",
                "vacuum-cleaners", "multicookers", "air-conditioners"
            ],
            "other": [
                "car-electronics", "tires", "perfumes", "clocks"
            ]
        }

    async def fetch_kaspi_discounts(self) -> list:
        """
        Парсит последовательно главные категории Kaspi.
        """
        all_items = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            
            # Проходим по категориям
            for cat_group, slugs in self.categories.items():
                for slug in slugs:
                    url = f"https://kaspi.kz/shop/c/{slug}/?q=%3AavailableInZones%3A750000000"
                    items_from_cat = await self._parse_category_page(context, url, cat_group)
                    all_items.extend(items_from_cat)
                    # Небольшая пауза между категориями, чтобы не триггерить защиту
                    await asyncio.sleep(1)
            
            await browser.close()
            
        logger.info(f"[ScraperService] Всего по всем категориям Kaspi собрано: {len(all_items)} товаров")
        return all_items

    async def _parse_category_page(self, context, url: str, category_label: str) -> list:
        """Вспомогательный метод для парсинга одной страницы категории."""
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        category_items = []
        try:
            logger.info(f"[ScraperService] Парсинг категории: {url.split('/')[-2]}")
            await page.goto(url, wait_until="load", timeout=40000)
            
            # Быстрый скролл для активации подгрузки
            await page.evaluate("window.scrollTo(0, 1000)")
            await asyncio.sleep(2.5)
            
            cards = await page.query_selector_all("[class*='card'], .item-card, .p-card")
            
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
            logger.error(f"[ScraperService] Ошибка в категории {url}: {e}")
        finally:
            await page.close()
            
        return category_items

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    async def fetch_price(self, url: str) -> Optional[int]:
        """Функция для одиночной проверки цены (персональные трекеры)."""
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
