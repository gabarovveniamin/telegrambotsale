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
        Универсальная функция для получения цены с любого сайта через Playwright.
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

                if not potential_prices:
                    return None
                return min(potential_prices)

            except Exception as e:
                logger.error(f"[ScraperService] Ошибка fetch_price {url}: {e}")
                return None
            finally:
                await browser.close()

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        clean_text = re.sub(r"[^\d]", "", text)
        return int(clean_text) if clean_text else None

    async def fetch_kaspi_discounts(self) -> list:
        """
        Парсит список товаров со скидкой из Kaspi Shop через браузер.
        """
        url = "https://kaspi.kz/shop/c/smartphones/?q=%3AavailableInZones%3A750000000%3AallMerchants%3AisDiscount%3Atrue"
        logger.info(f"[ScraperService] Запуск парсинга Kaspi. URL: {url}")
        
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
                
                # Скроллинг для подгрузки контента
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                await asyncio.sleep(2)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(4)
                
                title = await page.title()
                logger.info(f"[ScraperService] Title: {title}")
                
                # Поиск карточек
                cards = await page.query_selector_all(".item-card, .p-card")
                logger.info(f"[ScraperService] Найдено карточек: {len(cards)}")
                
                if not cards:
                    # Если карточек нет, ищем любые товарные ссылки
                    links = await page.query_selector_all("a[href*='/shop/p/']")
                    logger.info(f"[ScraperService] Найдено ссылок: {len(links)}")
                    if links:
                        for link in links[:12]:
                            t_text = await link.inner_text()
                            href = await link.get_attribute("href")
                            if href and t_text:
                                items.append({
                                    "id": f"kp_br_{href.split('/')[-2]}",
                                    "title": t_text.strip(),
                                    "new_price": 0, "old_price": 0,
                                    "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                                    "shop": "Kaspi", "category": "tech"
                                })
                
                for card in cards:
                    try:
                        title_el = await card.query_selector("a[class*='name'], a[class*='title']")
                        price_el = await card.query_selector("[class*='price-once'], [class*='prices-price']")
                        
                        if title_el and price_el:
                            title_text = await title_el.inner_text()
                            href = await title_el.get_attribute("href")
                            price_val = "".join([c for c in await price_el.inner_text() if c.isdigit()])
                            
                            if price_val and href:
                                items.append({
                                    "id": f"kp_br_{href.split('/')[-2]}",
                                    "title": title_text.strip(),
                                    "new_price": int(price_val),
                                    "old_price": int(int(price_val) * 1.1),
                                    "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                                    "shop": "Kaspi",
                                    "category": "tech"
                                })
                    except:
                        continue
                        
            except Exception as e:
                logger.error(f"[ScraperService] Ошибка в Kaspi: {e}")
            finally:
                await browser.close()
            return items

scraper_service = ScraperService()
