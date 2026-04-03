import asyncio
import json
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

async def main():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://www.mechta.kz/",
    }
    url = "https://www.mechta.kz/section/smartfony/"
    
    print(f"🔥 Принимаю вызов! Парсим Next.js __NEXT_DATA__ с {url}")

    async with AsyncSession(impersonate="chrome") as session:
        r = await session.get(url, headers=headers)
        if r.status_code != 200:
            print(f"Ошибка {r.status_code}: {r.text[:100]}")
            return
            
        soup = BeautifulSoup(r.text, "html.parser")
        script_tag = soup.find("script", id="__NEXT_DATA__")
        
        if not script_tag:
            print("Бро, тега __NEXT_DATA__ нет на странице. Может они перешли на Nuxt или отключили SSR?")
            return
            
        print("✅ __NEXT_DATA__ найден! Извлекаем JSON...")
        data = json.loads(script_tag.string)
        
        try:
            # Навигируем по структуре Next.js
            items = data["props"]["pageProps"]["initialState"]["catalog"]["products"]["items"]
            print(f"✅ Найдено товаров на странице: {len(items)}")
            
            discounts = []
            for item in items:
                name = item.get("name") or item.get("title")
                pid = item.get("id") or item.get("code")
                prices = item.get("prices", {})
                
                base = prices.get("basePrice")
                final = prices.get("finalPrice")
                
                if base and final and base > final:
                    discount = round((1 - final / base) * 100)
                    discounts.append({
                        "name": name,
                        "old": base,
                        "new": final,
                        "discount": discount
                    })
            
            print(f"\n🎉 Найдено скидок: {len(discounts)}")
            for d in discounts[:5]:
                print(f"- {d['name']}: {d['old']} -> {d['new']} (-{d['discount']}%)")
                
        except KeyError as e:
            print(f"Структура JSON изменилась! Не могу найти ключ: {e}")
            # print("Вот ключи начального уровня:", data.keys())

if __name__ == "__main__":
    asyncio.run(main())
