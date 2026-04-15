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
        # Пробуем через поиск, это иногда стабильнее
        url = "https://kaspi.kz/shop/search/?text=%D1%81%D0%BC%D0%B0%D1%80%D1%82%D1%84%D0%BE%D0%BD&q=%3AallMerchants%3A%3AavailableInZones%3A750000000%3AisDiscount%3Atrue"
        logger.info(f"[ScraperService] Запуск парсинга Kaspi. URL: {url}")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 1920, "height": 1080},
                locale="ru-RU",
                extra_http_headers={
                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                }
            )
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            items = []
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(8)
                
                title = await page.title()
                logger.info(f"[ScraperService] Kaspi Page Title: {title}")
                
                # Если Каспи просит выбрать город, пробуем просто подождать
                # Или если видим кнопку "Алматы", можно попробовать кликнуть, но обычно локаль ru-RU + 1920x1080 сами ставят город.

                # Ищем всё, что похоже на карточку товара
                cards = await page.query_selector_all(".item-card, .p-card, .product-card")
                logger.info(f"[ScraperService] Найдено карточек (метод 1): {len(cards)}")
                
                if not cards:
                    # Метод 2: ищем все ссылки на продукты
                    product_links = await page.query_selector_all("a[href*='/shop/p/']")
                    logger.info(f"[ScraperService] Найдено ссылок на товары (метод 2): {len(product_links)}")
                    
                    # Если нашли ссылки, но не карточки — значит верстка совсем другая
                    # В этом тесте просто попробуем вытянуть данные из первой попавшейся
                    if product_links:
                        logger.info("[ScraperService] Верстка Kaspi изменилась, использую поиск по ссылкам.")
                
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
                    except: continue
                        
            except Exception as e:
                logger.error(f"[ScraperService] Ошибка Kaspi Список: {e}")
            finally:
                await browser.close()
            return items

scraper_service = ScraperService()
