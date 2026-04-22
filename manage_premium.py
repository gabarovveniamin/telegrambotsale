"""
manage_premium.py — Ручное управление Premium-подписками
=========================================================
Запуск:  python manage_premium.py
Работает независимо от бота, обращается к БД напрямую.
"""
import asyncio
import asyncpg
from config import config
async def get_conn():
    return await asyncpg.connect(dsn=config.DATABASE_URL)
async def give_permanent(user_id: int):
    """Дать бессрочный Premium."""
    conn = await get_conn()
    await conn.execute(
        "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
        user_id
    )
    await conn.execute(
        "INSERT INTO subscriptions (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
        user_id
    )
    await conn.execute(
        """
        UPDATE subscriptions
        SET is_active = TRUE, expires_at = NULL,
            activated_at = COALESCE(activated_at, NOW()), updated_at = NOW()
        WHERE user_id = $1
        """,
        user_id
    )
    await conn.close()
    print(f"✅ Premium выдан навсегда → user_id: {user_id}")
async def give_days(user_id: int, days: int):
    """Дать Premium на N дней."""
    conn = await get_conn()
    await conn.execute(
        "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
        user_id
    )
    await conn.execute(
        "INSERT INTO subscriptions (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
        user_id
    )
    await conn.execute(
        f"""
        UPDATE subscriptions
        SET is_active = TRUE,
            expires_at = GREATEST(COALESCE(expires_at, NOW()), NOW()) + INTERVAL '{days} days',
            activated_at = COALESCE(activated_at, NOW()),
            updated_at = NOW()
        WHERE user_id = $1
        """,
        user_id
    )
    await conn.close()
    print(f"✅ Premium выдан на {days} дней → user_id: {user_id}")
async def revoke(user_id: int):
    """Забрать Premium."""
    conn = await get_conn()
    await conn.execute(
        "UPDATE subscriptions SET is_active = FALSE, expires_at = NULL, updated_at = NOW() WHERE user_id = $1",
        user_id
    )
    await conn.close()
    print(f"🚫 Premium отозван → user_id: {user_id}")
async def list_premium():
    """Показать всех активных Premium-пользователей."""
    conn = await get_conn()
    rows = await conn.fetch(
        """
        SELECT s.user_id, u.username, s.expires_at, s.stars_paid, s.activated_at
        FROM subscriptions s
        LEFT JOIN users u ON u.user_id = s.user_id
        WHERE s.is_active = TRUE
        ORDER BY s.activated_at DESC
        """
    )
    await conn.close()
    if not rows:
        print("📋 Нет активных Premium-пользователей.")
        return
    print(f"\n👑 Активные Premium ({len(rows)} чел.):\n")
    print(f"{'user_id':<15} {'username':<20} {'expires_at':<20} {'stars_paid'}")
    print("─" * 70)
    for r in rows:
        exp = "♾️ навсегда" if r["expires_at"] is None else r["expires_at"].strftime("%d.%m.%Y %H:%M")
        uname = f"@{r['username']}" if r["username"] else "—"
        print(f"{r['user_id']:<15} {uname:<20} {exp:<20} {r['stars_paid'] or 0}")
async def check_user(user_id: int):
    """Проверить статус конкретного пользователя."""
    conn = await get_conn()
    row = await conn.fetchrow(
        """
        SELECT s.*, u.username FROM subscriptions s
        LEFT JOIN users u ON u.user_id = s.user_id
        WHERE s.user_id = $1
        """,
        user_id
    )
    await conn.close()
    if not row:
        print(f"❌ Пользователь {user_id} не найден в БД.")
        return
    exp = "♾️ навсегда" if row["expires_at"] is None else row["expires_at"].strftime("%d.%m.%Y %H:%M UTC")
    status = "👑 PREMIUM" if row["is_active"] else "🆓 Бесплатный"
    print(f"\n📋 Пользователь {user_id}")
    print(f"  Username   : @{row['username'] or '—'}")
    print(f"  Статус     : {status}")
    print(f"  Истекает   : {exp}")
    print(f"  Stars paid : {row['stars_paid'] or 0}")
async def main():
    print("\n🔧 Управление Premium-подписками")
    print("─" * 40)
    print("1. Дать Premium навсегда")
    print("2. Дать Premium на N дней")
    print("3. Забрать Premium")
    print("4. Показать всех Premium-пользователей")
    print("5. Проверить пользователя по ID")
    print("0. Выход")
    print("─" * 40)
    choice = input("Выбери действие: ").strip()
    if choice == "1":
        uid = int(input("Введи user_id: ").strip())
        await give_permanent(uid)
    elif choice == "2":
        uid = int(input("Введи user_id: ").strip())
        days = int(input("Количество дней: ").strip())
        await give_days(uid, days)
    elif choice == "3":
        uid = int(input("Введи user_id: ").strip())
        await revoke(uid)
    elif choice == "4":
        await list_premium()
    elif choice == "5":
        uid = int(input("Введи user_id: ").strip())
        await check_user(uid)
    elif choice == "0":
        print("👋 Выход.")
    else:
        print("❌ Неверный выбор.")
if __name__ == "__main__":
    asyncio.run(main())
