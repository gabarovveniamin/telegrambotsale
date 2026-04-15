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
        """
        Универсальная функция для получения цены товара по прямой ссылке.
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 1920, "height": 1080},
                locale="ru-RU"
            )
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(2)

                priority_selectors = [
                    ".item-card__prices-price",
                    ".item-card__price-value",
                    ".product-price__current",
                    ".price",
                    ".current-price",
                ]
                
                for selector in priority_selectors:
                    elements = await page.query_selector_all(selector)
                    for el in elements:
                        text = await el.inner_text()
                        price = self._extract_price(text)
                        if price:
                            return price

                all_text_elements = await page.query_selector_all("span, div, b, strong")
                potential_prices = []
                for el in all_text_elements:
                    text = await el.inner_text()
                    if "₸" in text or "тг" in text.lower():
                        price = self._extract_price(text)
                        if price and 500 < price < 2000000:
                             potential_prices.append(price)

                return min(potential_prices) if potential_prices else None
            except:
                return None
            finally:
                await browser.close()

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        clean_text = re.sub(r"[^\d]", "", text)
        return int(clean_text) if clean_text else None

    async def fetch_kaspi_discounts(self) -> list:
        """
        Парсит список товаров со скидкой из Kaspi Shop.
        """
        # Используем проверенный рабочий URL
        url = "https://kaspi.kz/shop/c/smartphones/?q=%3AavailableInZones%3A750000000"
        logger.info(f"[ScraperService] Сбор скидок Kaspi: {url}")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 1280, "height": 800},
                locale="ru-RU"
            )
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            items = []
            try:
                await page.goto(url, wait_until="load", timeout=60000)
                
                # Имитируем скролл, чтобы прогрузить скидки
                await page.evaluate("window.scrollTo(0, 1000)")
                await asyncio.sleep(4)
                
                # Ищем карточки
                cards = await page.query_selector_all(".item-card, .p-card")
                logger.info(f"[ScraperService] Обработка {len(cards)} карточек...")
                
                for card in cards:
                    try:
                        title_el = await card.query_selector("a[class*='name'], a[class*='title']")
                        # У Каспи новая цена — .item-card__prices-price, старая (зачеркнутая) — .item-card__prices-old или base
                        price_new_el = await card.query_selector("[class*='price-once'], [class*='prices-price']")
                        price_old_el = await card.query_selector("[class*='prices-old'], [class*='price-old'], [class*='prices-base']")
                        
                        if title_el and price_new_el:
                            title = (await title_el.inner_text()).strip()
                            href = await title_el.get_attribute("href")
                            new_price = self._extract_price(await price_new_el.inner_text())
                            
                            # Если нашли старую цену — это 100% скидка
                            old_price = None
                            if price_old_el:
                                old_price = self._extract_price(await price_old_el.inner_text())
                            
                            # Только если есть и старая, и новая цена, и старая выше новой
                            if new_price and old_price and old_price > new_price:
                                items.append({
                                    "id": f"kp_br_{href.split('/')[-2]}",
                                    "title": title,
                                    "new_price": new_price,
                                    "old_price": old_price,
                                    "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                                    "shop": "Kaspi", "category": "tech"
                                })
                    except:
                        continue
                        
            except Exception as e:
                logger.error(f"[ScraperService] Ошибка Kaspi: {e}")
            finally:
                await browser.close()
            
            logger.info(f"[ScraperService] Итого реальных скидок Kaspi найдено: {len(items)}")
            return items

scraper_service = ScraperService()
