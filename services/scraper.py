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
        """Универсальное получение цены."""
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

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    async def fetch_kaspi_discounts(self) -> list:
        """Диагностический парсер Kaspi."""
        url = "https://kaspi.kz/shop/c/smartphones/?q=%3AavailableInZones%3A750000000"
        logger.info(f"[ScraperService] Глубокий анализ верстки Kaspi: {url}")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            items = []
            try:
                await page.goto(url, wait_until="load", timeout=60000)
                await page.evaluate("window.scrollTo(0, 800)")
                await asyncio.sleep(5)
                
                cards = await page.query_selector_all("[class*='card'], .item-card, .p-card")
                logger.info(f"[ScraperService] Вижу карточек: {len(cards)}")
                
                if cards:
                    # ДИАГНОСТИКА: берем первую карточку и смотрим её ТЕКСТ
                    sample_text = await cards[0].inner_text()
                    logger.info(f"[ScraperService] ТЕКСТ КАРТОЧКИ ДЛЯ АНАЛИЗА: {sample_text.replace('\n', ' | ')}")

                for card in cards:
                    try:
                        title_el = await card.query_selector("a[class*='name'], a[class*='title']")
                        price_new_el = await card.query_selector("[class*='price-once'], [class*='prices-price']")
                        # Пробуем все варианты старой цены
                        price_old_el = await card.query_selector("[class*='old'], [class*='base'], [class*='crossed']")
                        
                        if title_el and price_new_el:
                            title = (await title_el.inner_text()).strip()
                            href = await title_el.get_attribute("href")
                            new_v = self._extract_price(await price_new_el.inner_text())
                            
                            # Если нашли старую цену ЧЕРЕЗ СЕЛЕКТОР или ОНА ЕСТЬ В ТЕКСТЕ (если там 2 числа)
                            old_v = None
                            if price_old_el:
                                old_v = self._extract_price(await price_old_el.inner_text())
                            
                            # Если старую цену не нашли селектором, попробуем вытащить её из текста (обычно старая больше новой)
                            if not old_v:
                                card_text = await card.inner_text()
                                prices = [self._extract_price(p) for p in re.findall(r"[\d\s]{5,}₸", card_text)]
                                prices = [p for p in prices if p and p > 1000]
                                if len(prices) >= 2:
                                    old_v = max(prices)
                                    new_v = min(prices)

                            if new_v and old_v and old_v > new_v:
                                items.append({
                                    "id": f"kp_br_{href.split('/')[-2]}",
                                    "title": title,
                                    "new_price": new_v,
                                    "old_price": old_v,
                                    "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                                    "shop": "Kaspi", "category": "tech"
                                })
                    except: continue
                
                # Уникализация
                seen = set()
                res = []
                for i in items:
                    if i["id"] not in seen:
                        seen.add(i["id"]); res.append(i)
                
                logger.info(f"[ScraperService] Найдено скидок после анализа текста: {len(res)}")
                return res
                        
            except Exception as e:
                logger.error(f"[ScraperService] Ошибка: {e}")
            finally:
                await browser.close()
            return []

scraper_service = ScraperService()
