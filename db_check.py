import asyncio
from database import db
async def check():
    await db.connect()
    rows = await db.pool.fetch("SELECT * FROM product_prices ORDER BY updated_at DESC LIMIT 30")
    print(f"{'ID товара':<25} | {'Цена (₸)':<12} | {'Обновлено'}")
    print("-" * 60)
    for r in rows:
        print(f"{r['product_id']:<25} | {r['price']:<12,} | {r['updated_at'].strftime('%H:%M:%S')}")
    await db.disconnect()
if __name__ == "__main__":
    asyncio.run(check())
