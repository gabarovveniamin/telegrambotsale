import asyncio
import logging
import random
import hashlib
import re
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
        self.mechta_targets = [
            {"slug": "smartfony-i-gadjety", "label": "Смартфоны"},
            {"slug": "tekhnika-dlya-doma", "label": "Техника для дома"},
            {"slug": "bytovaya-tekhnika", "label": "Бытовая техника"},
            {"slug": "tv-audio-video", "label": "ТВ и Аудио"},
            {"slug": "pk-noutbuky-periferiya", "label": "Компьютеры"},
            {"slug": "krasota-i-zdorove", "label": "Красота и здоровье"}
        ]

    async def fetch_kaspi_discounts(self) -> list:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, viewport={"width": 1920, "height": 1080}, locale="ru-RU", timezone_id="Asia/Almaty")
            all_items = []
            for target in self.targets:
                url = f"https://kaspi.kz/shop/c/{target['slug']}/?q=%3AavailableInZones%3A{self.city_id}"
                items_from_cat = await self._parse_category_smart(context, url, target['label'])
                all_items.extend(items_from_cat)
                await asyncio.sleep(random.uniform(2, 5))
            await browser.close()
        seen = set()
        final = []
        for i in all_items:
            if i["id"] not in seen:
                seen.add(i["id"]); final.append(i)
        return final

    async def _parse_category_smart(self, context: BrowserContext, url: str, category_label: str) -> list:
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        captured = {}
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(5)
            await self._deep_scroll_and_capture(page, captured, category_label)
        except Exception as e:
            logger.error(f"[ScraperService] Kaspi Error: {e}")
        finally:
            await page.close()
        return list(captured.values())

    async def _deep_scroll_and_capture(self, page: Page, captured: dict, label: str):
        for _ in range(3):
            await page.evaluate("window.scrollBy(0, 1000)")
            await asyncio.sleep(2)
            products = await page.evaluate(r"""() => {
                const results = [];
                const cards = Array.from(document.querySelectorAll('.item-card, .product-card, [data-product-id]'));
                cards.forEach(card => {
                    const linkEl = card.querySelector('a[href*="/shop/p/"]');
                    if (!linkEl) return;
                    const priceEl = card.querySelector('.item-card__prices-price, .product-card__price');
                    if (priceEl && !priceEl.innerText.includes('мес')) {
                        results.push({ 
                            title: linkEl.innerText.trim(), 
                            href: linkEl.getAttribute('href'), 
                            priceText: priceEl.innerText,
                            imgUrl: card.querySelector('img')?.src || ""
                        });
                    }
                });
                return results;
            }""")
            for d in products:
                p_id = re.search(r'(\d+)$', d['href']).group(1) if re.search(r'(\d+)$', d['href']) else "0"
                full_id = f"kp_{p_id}"
                if full_id not in captured:
                    val = self._extract_price(d['priceText'])
                    if val:
                        captured[full_id] = {
                            "id": full_id, "title": d['title'], "new_price": val, "old_price": 0,
                            "link": f"https://kaspi.kz{d['href']}", "image": d['imgUrl'], "shop": "Kaspi", "category": label
                        }

    def _extract_price(self, text: str) -> Optional[int]:
        if not text or "мес" in text.lower(): return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    async def fetch_mechta_discounts(self) -> list:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent=self.user_agent, locale="ru-RU")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            all_items = []
            try:
                logger.info("[ScraperService] Mechta: обход Cloudflare...")
                await page.goto("https://www.mechta.kz/", wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(8)
            except: pass

            for target in self.mechta_targets:
                try:
                    logger.info(f"[ScraperService] Mechta: раздел {target['label']}")
                    await page.goto(f"https://www.mechta.kz/section/{target['slug']}/", wait_until="domcontentloaded", timeout=60000)
                    
                    # Умное ожидание ссылок
                    links_found = 0
                    for _ in range(15):
                        links_found = await page.evaluate('() => document.querySelectorAll("a[href*=\\"/product/\\"]").length')
                        if links_found > 0: break
                        await asyncio.sleep(1)
                    
                    logger.info(f"[ScraperService] Mechta: {target['label']} -> {links_found} ссылок")
                    await page.mouse.wheel(0, 1000)
                    await asyncio.sleep(4)

                    items = await page.evaluate(r"""() => {
                        const res = [];
                        const links = Array.from(document.querySelectorAll('a[href*="/product/"]'));
                        const seen = new Set();
                        links.forEach(l => {
                            const href = l.href;
                            if (seen.has(href)) return;
                            seen.add(href);
                            
                            // Очистка названия: убираем бонусы, отзывы и цену из текста
                            let title = l.innerText.replace(/Нет отзывов/g, '')
                                                 .replace(/\+\s?\d[\d\s]*/g, '')
                                                 .split('\n')[0].trim();
                            if (title.length < 5) return;

                            let p = l.parentElement;
                            // Ограничиваем поиск родителя (обычно карточка не глубже 4-5 уровней)
                            for(let i=0; i<5; i++) {
                                if (!p) break;
                                const prices = p.innerText.match(/(\d[\d\s]*[₸TТ])/g);
                                
                                // Ищем именно две разные цены внутри одного блока
                                if (prices && prices.length >= 2) {
                                    const p1 = parseInt(prices[0].replace(/[^\d]/g, ''));
                                    const p2 = parseInt(prices[1].replace(/[^\d]/g, ''));
                                    
                                    // Скидка должна быть логичной (не более 60%)
                                    const high = Math.max(p1, p2);
                                    const low = Math.min(p1, p2);
                                    const discountPercent = (high - low) / high;

                                    if (p1 > 0 && p2 > 0 && p1 !== p2 && discountPercent < 0.6) {
                                        res.push({
                                            title: title,
                                            old: high,
                                            new: low,
                                            link: href,
                                            img: p.querySelector('img')?.src || ''
                                        });
                                        break;
                                    }
                                }
                                p = p.parentElement;
                            }
                        });
                        return res;
                    }""")
                    
                    if items:
                        logger.info(f"[ScraperService] Mechta: {target['label']} -> {len(items)} акций")
                        for it in items:
                            all_items.append({
                                "id": f"mc_{hash(it['link'])}", "title": it['title'], "new_price": it['new'],
                                "old_price": it['old'], "link": it['link'], "image": it['img'], 
                                "shop": "Mechta", "category": target['label']
                            })
                except Exception as e:
                    logger.warning(f"Ошибка Mechta {target['label']}: {e}")
            await browser.close()
        return list({it['link']: it for it in all_items}.values())

scraper_service = ScraperService()
