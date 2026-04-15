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
            
        seen_ids = set()
        final_list = []
        for i in all_items:
            if i["id"] not in seen_ids:
                seen_ids.add(i["id"])
                final_list.append(i)
                
        logger.info(f"[ScraperService] Глобальный сбор завершен. ИТОГО УНИКАЛЬНЫХ: {len(final_list)}")
        return final_list

    async def _parse_category_deep(self, context, url: str, category_label: str) -> list:
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        captured_data = {} # Используем словарь для дедупликации по ID прямо в процессе
        try:
            category_name = url.split('/')[-2].replace('%20', ' ')
            logger.info(f"[ScraperService] Запуск конвейера в разделе: {category_name}")
            
            await page.goto(url, wait_until="load", timeout=45000)
            
            # Цикл: скролл + моментальный сбор
            for step in range(12):
                # 1. Скроллим
                await page.evaluate("window.scrollBy(0, 1000)")
                await asyncio.sleep(1.5) # Ждем прогрузку
                
                # 2. Собираем всё, что сейчас видно в DOM
                cards = await page.query_selector_all("[class*='card'], .item-card, .p-card")
                for card in cards:
                    try:
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
                        
                        if data and data['href']:
                            p_id = data['href'].split('/')[-2] if '/' in data['href'] else data['href']
                            full_id = f"kp_all_{p_id}"
                            
                            if full_id not in captured_data:
                                price_val = self._extract_price(data['priceText'])
                                if price_val:
                                    captured_data[full_id] = {
                                        "id": full_id,
                                        "title": data['title'].strip(),
                                        "new_price": price_val,
                                        "old_price": 0,
                                        "link": f"https://kaspi.kz{data['href']}" if data['href'].startswith("/") else data['href'],
                                        "shop": "Kaspi", "category": category_label
                                    }
                    except: continue
                
                if step % 4 == 0:
                    logger.info(f"[ScraperService] Шаг {step}/12: уже собрано {len(captured_data)} товаров...")

            logger.info(f"[ScraperService] Раздел {category_name} завершен. Собрано: {len(captured_data)}")
        except Exception as e:
            logger.error(f"[ScraperService] Ошибка в {url}: {e}")
        finally:
            await page.close()
            
        return list(captured_data.values())

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        # Убираем хвост рассрочки перед извлечением цифр
        text = text.split('x')[0].split('х')[0]
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
