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

    async def fetch_price(self, url: str) -> Optional[int]:
        """Универсальное получение цены (для точечной следилки)."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(2)
                for s in [".prices-price", ".price", ".current-price", "span[class*='price']"]:
                    el = await page.query_selector(s)
                    if el:
                        val = self._extract_price(await el.inner_text())
                        if val: return val
                return None
            except: return None
            finally: await browser.close()

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    async def fetch_kaspi_discounts(self) -> list:
        """Мега-парсер: ищет всё, что выглядит как скидка на смартфонах."""
        url = "https://kaspi.kz/shop/c/smartphones/?q=%3AavailableInZones%3A750000000"
        logger.info(f"[ScraperService] Глубокий поиск скидок Kaspi: {url}")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            # Используем 1280x800 — этот режим ранее выдал нам 242 карточки
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            items = []
            try:
                await page.goto(url, wait_until="load", timeout=60000)
                
                # Делаем активный скроллинг
                for i in range(4):
                    await page.evaluate(f"window.scrollTo(0, {800 * (i+1)})")
                    await asyncio.sleep(2)
                
                # Ждем прогрузку
                await asyncio.sleep(4)
                
                # Ищем ВСЕ элементы, которые могут быть карточками
                cards = await page.query_selector_all("[class*='card'], .item-card, .p-card")
                logger.info(f"[ScraperService] Найдено потенциальных карточек: {len(cards)}")
                
                for card in cards:
                    try:
                        # Внутри карточки ищем заголовок, новую цену и старую (зачеркнутую)
                        title_el = await card.query_selector("a[class*='name'], a[class*='title'], .item-card__name-link")
                        price_new_el = await card.query_selector("[class*='prices-price'], [class*='price-once']")
                        # Старая цена (у Каспи она обычно зачеркнута)
                        price_old_el = await card.query_selector("[class*='old'], [class*='base'], [style*='text-decoration: line-through']")
                        
                        if title_el and price_new_el and price_old_el:
                            title = (await title_el.inner_text()).strip()
                            href = await title_el.get_attribute("href")
                            new_val = self._extract_price(await price_new_el.inner_text())
                            old_val = self._extract_price(await price_old_el.inner_text())
                            
                            if new_val and old_val and old_val > new_val:
                                # Проверяем на дубликаты по ID (последняя часть ссылки)
                                product_id = href.split('/')[-2] if '/' in href else href
                                items.append({
                                    "id": f"kp_br_{product_id}",
                                    "title": title,
                                    "new_price": new_val,
                                    "old_price": old_val,
                                    "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                                    "shop": "Kaspi", "category": "tech"
                                })
                    except: continue
                
                # Удаляем дубликаты (если один товар попал в 2 селектора)
                seen = set()
                unique_items = []
                for item in items:
                    if item["id"] not in seen:
                        seen.add(item["id"])
                        unique_items.append(item)
                
                logger.info(f"[ScraperService] Итого уникальных скидок найдено: {len(unique_items)}")
                return unique_items
                        
            except Exception as e:
                logger.error(f"[ScraperService] Ошибка Kaspi: {e}")
            finally:
                await browser.close()
            return []

scraper_service = ScraperService()
