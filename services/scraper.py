import asyncio
import logging
import re
import hashlib
import random
from typing import Optional, List, Dict
from playwright.async_api import async_playwright, Page, BrowserContext
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

class ScraperService:
    def __init__(self):
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        self.targets = [
            {"slug": "smartphones%20and%20gadgets", "label": "tech"},
            {"slug": "home%20equipment", "label": "tech"},
            {"slug": "tv_audio", "label": "tech"},
            {"slug": "computers", "label": "tech"},
            {"slug": "furniture", "label": "other"},
            {"slug": "beauty%20care", "label": "other"},
            {"slug": "car%20goods", "label": "other"},
            {"slug": "sport-goods", "label": "other"}
        ]
        self.city_id = "750000000"  # Almaty

    async def fetch_kaspi_discounts(self) -> list:
        async with async_playwright() as p:
            # Launch with more stealth-oriented args
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-infobars",
                    "--window-position=0,0",
                    "--ignore-certificate-errors",
                    "--ignore-certificate-errors-spki-list"
                ]
            )
            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 1920, "height": 1080},
                locale="ru-RU",
                timezone_id="Asia/Almaty"
            )
            
            all_items = []
            for target in self.targets:
                url = f"https://kaspi.kz/shop/c/{target['slug']}/?q=%3AavailableInZones%3A{self.city_id}"
                items_from_cat = await self._parse_category_smart(context, url, target['label'])
                all_items.extend(items_from_cat)
                
                # Рандомная задержка между разделами
                await asyncio.sleep(random.uniform(2, 5))
            
            await browser.close()
            
        seen_ids = set()
        final_list = []
        for i in all_items:
            if i["id"] not in seen_ids:
                seen_ids.add(i["id"])
                final_list.append(i)
                
        logger.info(f"[ScraperService] Сбор завершен. Уникальных товаров: {len(final_list)}")
        return final_list

    async def _parse_category_smart(self, context: BrowserContext, url: str, category_label: str) -> list:
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        captured_data = {}
        
        try:
            category_name = re.sub(r'%20', ' ', url.split('/')[-2])
            logger.info(f"[ScraperService] Работаем с разделом: {category_name}")
            
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(3)
            
            # --- ПЫТАЕМСЯ ЗАКРЫТЬ ПОПАПЫ ВЫБОРА ГОРОДА ИЛИ КУКИ ---
            try:
                city_confirm = await page.query_selector("text='Да, верно'")
                if city_confirm:
                    await city_confirm.click()
                    await asyncio.sleep(1)
            except: pass

            # --- УНИКАЛЬНЫЙ ФИЛЬТР ПОДКАТЕГОРИЙ ---
            subcat_links = await page.evaluate("""() => {
                const links = Array.from(document.querySelectorAll('a'));
                const blacklist = ['gos', 'announcements', 'actions', 'mybank', 'travel', 'mobile', 'pay', 'news', 'about', 'recommendations', 'lifestyle', 'social'];
                return links
                    .map(a => a.href)
                    .filter(href => {
                        if (!href) return false;
                        const isNode = href.includes('/shop/c/');
                        const notBad = !blacklist.some(b => href.includes(b));
                        const isNotMain = !href.endsWith('/shop/c/');
                        const isClean = !href.includes('&') || href.includes('availableInZones');
                        return isNode && notBad && isNotMain && isClean;
                    })
                    .slice(0, 15);
            }""")
            
            links_to_scan = list(set([l for l in subcat_links if l != url]))
            
            # 1. Парсим текущую категорию основательно
            await self._deep_scroll_and_capture(page, captured_data, category_label)
            
            # 2. Ныряем в подразделы
            random.shuffle(links_to_scan)
            for scan_url in links_to_scan[:4]:
                try:
                    final_scan_url = scan_url
                    if "availableInZones" not in final_scan_url:
                        sep = "&" if "?" in final_scan_url else "?"
                        final_scan_url += f"{sep}q=%3AavailableInZones%3A{self.city_id}"

                    logger.info(f"[ScraperService] -> Сканируем подраздел: {final_scan_url}")
                    await page.goto(final_scan_url, wait_until="networkidle", timeout=40000)
                    await asyncio.sleep(2)
                    await self._deep_scroll_and_capture(page, captured_data, category_label)
                except Exception as e: 
                    logger.debug(f"Ошибка при сканировании подраздела {scan_url}: {e}")
                    continue

        except Exception as e:
            logger.error(f"[ScraperService] Критическая ошибка в {url}: {e}")
        finally:
            await page.close()
            
        return list(captured_data.values())

    async def _deep_scroll_and_capture(self, page: Page, captured_data: dict, category_label: str):
        """Интеллектуальный сбор с обработкой 'Показать еще' и извлечением фото"""
        max_scrolls = 6
        for i in range(max_scrolls):
            try:
                show_more = await page.query_selector(".pagination__load-more, .show-more-button, text='Показать ещё'")
                if show_more and await show_more.is_visible():
                    await show_more.click()
                    await asyncio.sleep(2)
            except: pass

            await page.evaluate("window.scrollBy(0, 1500)")
            await asyncio.sleep(random.uniform(1.2, 2.0))
            
            products = await page.evaluate("""() => {
                const results = [];
                const cards = Array.from(document.querySelectorAll('div[data-product-id], .product-card, .item_card'));
                const links = Array.from(document.querySelectorAll('a[href*="/shop/p/"]'));
                
                const processLink = (link) => {
                    const href = link.getAttribute('href');
                    const title = (link.innerText || "").trim();
                    if (!title || title.length < 5 || href.includes('reviews')) return;

                    let container = link;
                    for(let depth=0; depth<6; depth++) {
                        if (!container.parentElement) break;
                        if (container.innerText.includes('₸') && container.offsetHeight > 100) break;
                        container = container.parentElement;
                    }

                    let priceText = "";
                    const priceMatch = container.innerText.match(/(\\d[\\d\\s]+\\s*₸)/);
                    if (priceMatch) priceText = priceMatch[0];

                    let imgUrl = "";
                    const img = container.querySelector('img');
                    if (img) {
                        imgUrl = img.getAttribute('src') || img.getAttribute('data-src') || "";
                    }

                    if (priceText && !results.some(r => r.href === href)) {
                        results.push({ title, href, priceText, imgUrl });
                    }
                };

                if (cards.length > 0) {
                    cards.forEach(card => {
                        const link = card.querySelector('a[href*="/shop/p/"]');
                        if (link) processLink(link);
                    });
                } else {
                    links.forEach(processLink);
                }
                return results;
            }""")
            
            for data in products:
                href = data['href'].split('?')[0].rstrip('/')
                id_match = re.search(r'(\d+)$', href)
                p_id = id_match.group(1) if id_match else hashlib.md5(href.encode()).hexdigest()[:10]
                
                full_id = f"kp_{p_id}"
                
                if full_id not in captured_data:
                    price_val = self._extract_price(data['priceText'])
                    if price_val and price_val > 500:
                        captured_data[full_id] = {
                            "id": full_id,
                            "title": data['title'],
                            "new_price": price_val,
                            "old_price": 0,
                            "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                            "image": data['imgUrl'] if data['imgUrl'] and data['imgUrl'].startswith('http') else None,
                            "shop": "Kaspi",
                            "category": category_label
                        }

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    async def fetch_price(self, url: str) -> Optional[int]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                await page.goto(url, wait_until="load", timeout=30000)
                await asyncio.sleep(2)
                body_text = await page.inner_text("body")
                price_match = re.search(r'(\d[\d\s]+\s*₸)', body_text)
                if price_match:
                    return self._extract_price(price_match.group(1))
                price_eval = await page.evaluate("""() => {
                    const el = document.querySelector('.item__price-once, [class*="price"], .price');
                    return el ? el.innerText : null;
                }""")
                return self._extract_price(price_eval) if price_eval else None
            except: return None
            finally: await browser.close()

scraper_service = ScraperService()
scraper_service = ScraperService()

