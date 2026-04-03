import asyncio
import asyncpg
from config import config

async def check():
    if not config.DATABASE_URL:
        print("⚠️ DATABASE_URL не задан в .env файле.")
        return
        
    conn = await asyncpg.connect(config.DATABASE_URL)

    # Все созданные таблицы
    tables = await conn.fetch("""
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
    """)

    print("📦 Таблицы в базе данных:")
    for t in tables:
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {t['tablename']}")
        print(f"  ✅ {t['tablename']} — {count} записей")

    if not tables:
        print("  ⚠️ Таблицы ещё не созданы. Запусти main.py сначала.")

    await conn.close()

asyncio.run(check())
