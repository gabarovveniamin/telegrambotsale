import asyncio
import logging
import re
from typing import Optional, List
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

class ScraperService:
    def __init__(self):
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

    async def fetch_price(self, url: str) -> Optional[int]:
        """Получение цены товара по прямой ссылке."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1920, "height": 1080}, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(2)
                for selector in [".item-card__prices-price", ".product-price__current", ".price"]:
                    el = await page.query_selector(selector)
                    if el: return self._extract_price(await el.inner_text())
                return None
            except: return None
            finally: await browser.close()

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    async def fetch_kaspi_discounts(self) -> list:
        """Парсит Kaspi и вручную ищет товары со старой ценой."""
        # Базовый URL смартфонов в Алматы
        url = "https://kaspi.kz/shop/c/smartphones/?q=%3AavailableInZones%3A750000000"
        logger.info(f"[ScraperService] Сбор всех смартфонов Kaspi для поиска скидок: {url}")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(
                user_agent=self.user_agent, 
                viewport={"width": 1920, "height": 1080}, 
                locale="ru-RU"
            )
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            items = []
            try:
                await page.goto(url, wait_until="load", timeout=60000)
                
                # Делаем серию скроллов, чтобы подгрузить больше товаров (Infinite Scroll)
                for _ in range(5):
                    await page.evaluate("window.scrollBy(0, 1000)")
                    await asyncio.sleep(1.5)
                
                # Ждем финальную отрисовку
                await asyncio.sleep(3)
                
                title = await page.title()
                logger.info(f"[ScraperService] Title: {title}")
                
                cards = await page.query_selector_all(".item-card, .p-card")
                logger.info(f"[ScraperService] Проверяю {len(cards)} карточек на наличие акций...")
                
                discount_count = 0
                for card in cards:
                    try:
                        title_el = await card.query_selector("a[class*='name'], a[class*='title']")
                        # Новая цена
                        price_new_el = await card.query_selector("[class*='price-once'], [class*='prices-price']")
                        # Старая (зачеркнутая) цена — ключевой признак скидки
                        price_old_el = await card.query_selector("[class*='old'], [class*='base'], .item-card__prices-old")
                        
                        if title_el and price_new_el and price_old_el:
                            title_text = (await title_el.inner_text()).strip()
                            href = await title_el.get_attribute("href")
                            new_val = self._extract_price(await price_new_el.inner_text())
                            old_val = self._extract_price(await price_old_el.inner_text())
                            
                            # Добавляем только если есть реальная разница в цене
                            if new_val and old_val and old_val > new_val:
                                discount_count += 1
                                items.append({
                                    "id": f"kp_br_{href.split('/')[-2]}",
                                    "title": title_text,
                                    "new_price": new_val,
                                    "old_price": old_val,
                                    "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                                    "shop": "Kaspi", "category": "tech"
                                })
                    except: continue
                
                logger.info(f"[ScraperService] Найдено реальных скидок: {discount_count}")
                        
            except Exception as e:
                logger.error(f"[ScraperService] Ошибка Kaspi: {e}")
            finally:
                await browser.close()
            return items

scraper_service = ScraperService()
