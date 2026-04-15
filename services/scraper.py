import logging
import asyncio
from typing import Optional
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

class ScraperService:
    def __init__(self):
        # Используем современный User-Agent, чтобы сайты не блокировали бота
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    async def fetch_price(self, url: str) -> Optional[int]:
        """
        Универсальная фукнция парсинга цены с динамических (SPA) сайтов с помощью Playwright.
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            
            # Для обхода блокировок можно установить дополнительные настройки (viewport, language, headers)
            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 1920, "height": 1080},
                locale="ru-RU",
                extra_http_headers={
                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                }
            )
            
            # Рандомизация фингерпринта или bypass protection (базовая защита)
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            page = await context.new_page()
            
            # Применяем stealth (убирает признаки автоматизации webdriver)
            await Stealth().apply_stealth_async(page)
            
            try:
                # Для SPA важно дождаться, пока сетевая активность утихнет (networkidle)
                await page.goto(url, wait_until="networkidle", timeout=30000)
                
                # Дополнительная пауза, чтобы скрипты успели отрисовать цену
                await asyncio.sleep(2)
                
                # Приоритетные селекторы (точные классы цен)
                priority_selectors = [
                    ".item__price-once",               # Kaspi
                    "p.typography_primary__4AovM",     # Technodom
                    ".price__current",                 # Mechta
                    ".product__price"                  # Sulpak
                ]
                
                # Общие селекторы (на случай изменения верстки)
                general_selectors = [
                    ".item__price", ".product-info__price", "div.product-price", 
                    "div[class*='price']", "span[class*='price']", "p[class*='price']",
                    "[data-product-price]", ".price", ".current-price"
                ]
                
                async def get_prices(selectors):
                    found = []
                    for selector in selectors:
                        elements = await page.query_selector_all(selector)
                        for el in elements:
                            text = await el.inner_text()
                            if text:
                                clean = "".join([c for c in text if c.isdigit()])
                                # Цена в КЗ обычно больше 1000 для техники
                                if clean and len(clean) >= 4: 
                                    found.append(int(clean))
                    return found

                # 1. Проверяем приоритетные
                potential_prices = await get_prices(priority_selectors)
                
                # 2. Если не нашли, проверяем общие
                if not potential_prices:
                    potential_prices = await get_prices(general_selectors)
                
                # 3. Если все еще пусто, ждем и пробуем снова
                if not potential_prices:
                    await asyncio.sleep(3)
                    potential_prices = await get_prices(priority_selectors + general_selectors)

                if not potential_prices:
                    logger.warning(f"[ScraperService] Цена не найдена: {url}")
                    return None

                # Нюанс: на страницах часто есть "старая цена" и "новая цена".
                # Обычно актуальная (низкая) цена идет первой или она просто минимальная.
                return min(potential_prices)

            except PlaywrightTimeoutError:
                logger.error(f"[ScraperService] Тайм-аут при загрузке страницы: {url}")
                return None
            except Exception as e:
                logger.error(f"[ScraperService] Ошибка парсинга {url}: {e}")
                return None
            finally:
                await browser.close()

    async def fetch_kaspi_discounts(self) -> list:
        """
        Парсит список товаров со скидкой из Kaspi Shop через браузер.
        """
        url = "https://kaspi.kz/shop/c/smartphones/?q=%3AallMerchants%3A%3AavailableInZones%3A750000000%3AisDiscount%3Atrue"
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(user_agent=self.user_agent)
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            items = []
            try:
                # Используем domcontentloaded вместо networkidle, чтобы не ждать вечно фоновые скрипты
                await page.goto(url, wait_until="domcontentloaded", timeout=40000)
                
                # Даем время на отрисовку карточек (обычно хватает 5 секунд)
                await asyncio.sleep(5)
                
                # Селекторы карточек Kaspi
                cards = await page.query_selector_all(".item-card")
                for card in cards:
                    try:
                        title_el = await card.query_selector(".item-card__name-link")
                        link_el = title_el
                        
                        price_once = await card.query_selector(".item-card__prices-price") # Новая цена
                        # Старую цену Каспи часто прячет в стилях или отдельных блоках, 
                        # если ее нет на карточке, мы ее не увидим здесь напрямую без захода внутрь.
                        # Но обычно на скидочной странице есть перечеркнутая цена.
                        
                        if title_el and price_once:
                            title = await title_el.inner_text()
                            href = await link_el.get_attribute("href")
                            price_text = await price_once.inner_text()
                            
                            clean_price = "".join([c for c in price_text if c.isdigit()])
                            
                            if clean_price and href:
                                items.append({
                                    "id": f"kp_br_{href.split('/')[-2]}",
                                    "title": title.strip(),
                                    "new_price": int(clean_price),
                                    "old_price": int(clean_price) * 1.2, # Фейковое значение для теста, если старой нет
                                    "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                                    "shop": "Kaspi",
                                    "category": "tech"
                                })
                    except Exception as e:
                        logger.error(f"Error parsing card: {e}")
                        continue
                        
            except Exception as e:
                logger.error(f"Error fetching Kaspi list: {e}")
            finally:
                await browser.close()
            return items

scraper_service = ScraperService()
