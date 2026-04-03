import asyncio
import asyncpg
from config import config

async def check():
    conn = await asyncpg.connect(dsn=config.DATABASE_URL)
    count = await conn.fetchval("SELECT count(*) FROM seen_items")
    recent = await conn.fetch("SELECT * FROM seen_items LIMIT 10")
    await conn.close()
    
    print(f"Total seen items (already parsed): {count}")
    if recent:
        print("\nLast parsed items (samples):")
        for r in recent:
            print(f"- {r['shop']}: {r['id']}")

if __name__ == '__main__':
    asyncio.run(check())
