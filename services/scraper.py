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
            
            # Используем 'domcontentloaded' + короткую паузу вместо 'networkidle', 
            # так как 'networkidle' часто вызывает таймауты из-за трекеров.
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(5) # Даем время JS отрендерить товары
            except Exception as e:
                logger.warning(f"[ScraperService] Таймаут при загрузке {url}, пробуем продолжить: {e}")
            
            # --- ПЫТАЕМСЯ ЗАКРЫТЬ ПОПАПЫ (Город, Куки, Подписки) ---
            for _ in range(2):
                try:
                    # 'Да, верно' - подтверждение города Almaty
                    city_btn = await page.get_by_role("button", name="Да, верно").or_(page.locator(".dialog__close"))
                    if await city_btn.is_visible():
                        await city_btn.click()
                        await asyncio.sleep(1)
                except: break

            # Проверка на отсутствие товаров ("Ничего не найдено")
            empty_check = await page.locator("text='Ничего не найдено', .search-result__no-results").is_visible()
            if empty_check:
                logger.info(f"[ScraperService] Раздел {category_name} пуст.")
                return []

            # --- Умный поиск подразделов (только в дереве категорий) ---
            subcat_links = await page.evaluate("""() => {
                const tree = document.querySelector('.tree, .category-filter, .nav-sidebar');
                if (!tree) return [];
                
                const links = Array.from(tree.querySelectorAll('a[href*="/shop/c/"]'));
                const blacklist = ['gos', 'announcements', 'actions', 'mybank', 'travel', 'news', 'lifestyle', 'social', 'reviews'];
                
                return links
                    .map(a => a.href)
                    .filter(href => {
                        if (!href) return false;
                        const notBad = !blacklist.some(b => href.includes(b));
                        // Мы хотим ссылки, которые длиннее текущей (вложенные)
                        return notBad && href.length > window.location.href.length - 10;
                    })
                    .slice(0, 8);
            }""")

            
            links_to_scan = list(set([l for l in subcat_links if l != url]))
            
            # 1. Парсим текущую категорию основательно
            await self._deep_scroll_and_capture(page, captured_data, category_label)
            
            # 2. Ныряем в подразделы (топ-3 самых релевантных)
            for scan_url in links_to_scan[:3]:
                try:
                    final_scan_url = scan_url
                    if "availableInZones" not in final_scan_url:
                        sep = "&" if "?" in final_scan_url else "?"
                        final_scan_url += f"{sep}q=%3AavailableInZones%3A{self.city_id}"

                    logger.info(f"[ScraperService] -> Ныряем в подраздел: {final_scan_url}")
                    await page.goto(final_scan_url, wait_until="domcontentloaded", timeout=35000)
                    await asyncio.sleep(4)
                    await self._deep_scroll_and_capture(page, captured_data, category_label)
                except: continue

        except Exception as e:
            logger.error(f"[ScraperService] Критическая ошибка в {url}: {e}")
        finally:
            await page.close()
            
        return list(captured_data.values())

    async def _deep_scroll_and_capture(self, page: Page, captured_data: dict, category_label: str):
        """Интеллектуальный сбор с обработкой 'Показать еще' и извлечением фото"""
        
        # Лимит прокруток для одного подраздела
        for i in range(5):
            # Проверка на кнопку "Показать еще"
            try:
                for btn_sel in [".pagination__load-more", "text='Показать ещё'", ".show-more"]:
                    show_more = await page.query_selector(btn_sel)
                    if show_more and await show_more.is_visible():
                        await show_more.click()
                        await asyncio.sleep(2)
            except: pass

            await page.evaluate("window.scrollBy(0, 1200)")
            await asyncio.sleep(random.uniform(1.0, 1.8))
            
            # Извлекаем товары
            products = await page.evaluate("""() => {
                const results = [];
                // Ищем карточки товаров. Kaspi часто меняет классы, поэтому используем несколько селекторов.
                const cards = Array.from(document.querySelectorAll('.item-card, .product-card, [data-product-id], .search-result-item'));
                
                cards.forEach(card => {
                    const linkEl = card.querySelector('a[href*="/shop/p/"]');
                    if (!linkEl) return;

                    const href = linkEl.getAttribute('href');
                    const titleEl = card.querySelector('.item-card__name-link, .product-card__name, [class*="name"]');
                    const title = (titleEl ? titleEl.innerText : linkEl.innerText).trim();
                    if (!title || title.length < 3) return;

                    // Поиск цены
                    let priceText = "";
                    const priceEl = card.querySelector('.item-card__prices-price, .product-card__price, [class*="price"]');
                    if (priceEl) {
                        priceText = priceEl.innerText;
                    } else {
                        const allText = card.innerText;
                        const matched = allText.match(/(\\d[\\d\\s]+\\s*₸)/);
                        if (matched) priceText = matched[0];
                    }

                    // Поиск изображения (img или data-src)
                    let imgUrl = "";
                    const img = card.querySelector('img');
                    if (img) {
                        imgUrl = img.getAttribute('src') || img.getAttribute('data-src') || img.currentSrc || "";
                    }

                    if (priceText && !results.some(r => r.href === href)) {
                        results.push({ title, href, priceText, imgUrl });
                    }
                });

                
                // Если карточки не найдены специф. классами, пробуем старый fuzzy метод
                if (results.length === 0) {
                    const links = Array.from(document.querySelectorAll('a[href*="/shop/p/"]'));
                    links.forEach(link => {
                        const href = link.getAttribute('href');
                        const title = (link.innerText || "").trim();
                        if (!title || title.length < 5) return;
                        
                        let parent = link.parentElement;
                        for(let d=0; d<5; d++) {
                            if (!parent) break;
                            if (parent.innerText.includes('₸')) {
                                const priceMatch = parent.innerText.match(/(\\d[\\d\\s]+\\s*₸)/);
                                if (priceMatch) {
                                    results.push({ title, href, priceText: priceMatch[0], imgUrl: "" });
                                    break;
                                }
                            }
                            parent = parent.parentElement;
                        }
                    });
                }

                return results;
            }""")
            
            for d in products:
                href = d['href'].split('?')[0].rstrip('/')
                id_match = re.search(r'(\d+)$', href)
                p_id = id_match.group(1) if id_match else hashlib.md5(href.encode()).hexdigest()[:10]
                
                full_id = f"kp_{p_id}"
                
                if full_id not in captured_data:
                    price_val = self._extract_price(d['priceText'])
                    if price_val and price_val > 500:
                        captured_data[full_id] = {
                            "id": full_id,
                            "title": d['title'],
                            "new_price": price_val,
                            "old_price": 0,
                            "link": f"https://kaspi.kz{href}" if href.startswith("/") else href,
                            "image": d['imgUrl'] if d['imgUrl'] and d['imgUrl'].startswith('http') else None,
                            "shop": "Kaspi",
                            "category": category_label
                        }

    def _extract_price(self, text: str) -> Optional[int]:
        if not text: return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    async def fetch_price(self, url: str) -> Optional[int]:
        """Забор цены для одного товара с улучшенной стабильностью"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1280, "height": 800}, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(4)
                
                # Поиск цены
                eval_res = await page.evaluate("""() => {
                    const selectors = ['.item__price-once', '.product-card__price', '.item-card__prices-price', '.price'];
                    for (const s of selectors) {
                        const el = document.querySelector(s);
                        if (el && el.innerText.includes('₸')) return el.innerText;
                    }
                    return document.body.innerText.match(/(\\d[\\d\\s]+\\s*₸)/)?.[0] || null;
                }""")
                return self._extract_price(eval_res) if eval_res else None
            except: return None
            finally: await browser.close()

scraper_service = ScraperService()


scraper_service = ScraperService()


