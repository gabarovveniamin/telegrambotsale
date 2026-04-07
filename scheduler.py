import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from parser import parser
from database import db
from bot import broadcast_message
from config import config
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

async def run_monitoring_cycle():
    """
    Основной цикл мониторинга:
    1. Получение данных (Парсинг)
    2. Проверка на новизну (Дедупликация)
    3. Рассылка уведомлений пользователям с процентом скидки
    """
    logger.info("Starting monitoring cycle...")
    items = await parser.fetch_discounts()
    
    new_items_to_send = []
    
    for item in items:
        # Пытаемся сохранить в БД. Если вернуло True — товар новый.
        is_new = await db.is_new_item(item['id'])
        if is_new:
            new_items_to_send.append(item)
            
    logger.info(f"Найдено {len(new_items_to_send)} абсолютно новых товаров.")

    # Защита от спама при первом запуске или добавлении нового магазина
    if len(new_items_to_send) > 50:
        logger.warning(f"Товаров слишком много ({len(new_items_to_send)} > 50). Это означает первый запуск бота или подключение нового магазина. Все товары сохранены в кэш без отправки уведомлений, чтобы не заспамить пользователей!")
    else:
        sent_count = 0
        for item in new_items_to_send:
            # Считаем процент скидки
            try:
                old = float(item["old_price"].replace(" ₸", "").replace("₸", "").replace(" ", "").strip())
                new = float(item["new_price"].replace(" ₸", "").replace("₸", "").replace(" ", "").strip())
                percent = round((old - new) / old * 100)
            except Exception:
                percent = "?"

            # Отправляем только если удалось посчитать скидку и она не ничтожная (напр. > 5%)
            if not isinstance(percent, int) or percent < 5:
                continue

            # Формируем сообщение
            text = (
                f"🆕 Новая скидка в {item['shop']}!\n\n"
                f"🏷 <b>{item['title']}</b>\n"
                f"📉 -{percent}%\n"
                f"💰 <s>{item['old_price']}</s> → <b>{item['new_price']}</b>\n"
                f"🔗 <a href='{item['link']}'>Купить сейчас</a>"
            )
            
            # Рассылка пользователям (Premium-only), чей порог скидки ниже или равен текущей
            await broadcast_message(
                text, 
                premium_only=True, 
                min_discount=percent, 
                category=item.get("category", "tech")
            )
            sent_count += 1
            
        logger.info(f"Monitoring cycle finished. Sent {sent_count} notifications to Premium users.")

async def run_personal_tracker_cycle():
    """
    Цикл проверки цен на товары из 'точечной следилки'.
    """
    logger.info("Starting personal tracker cycle...")
    tracked_items = await db.get_all_tracked_items()
    
    for item in tracked_items:
        current_price = await parser.get_single_product_price(item['url'], item['shop'])
        if current_price is None: continue
            
        if current_price < item['last_price']:
            diff = item['last_price'] - current_price
            text = (
                f"🎯 <b>Цена снизилась на ваш товар!</b>\n\n"
                f"🏪 Магазин: {item['shop']}\n"
                f"💰 Старая цена: {item['last_price']:,} ₸\n"
                f"🔥 Новая цена: {current_price:,} ₸\n"
                f"📉 Выгода: {diff:,} ₸\n\n"
                f"🔗 <a href='{item['url']}'>Посмотреть на сайте</a>"
            )
            from bot import bot
            try:
                await bot.send_message(item['user_id'], text, parse_mode="HTML")
                await db.update_tracked_price(item['id'], current_price)
            except Exception as e:
                logger.error(f"Failed to send notice: {e}")

    logger.info("Personal tracker cycle finished.")


async def run_subscription_check_cycle():
    """
    Проверка подписок:
    - За 3 дня до истечения — предупреждение
    - После истечения — уведомление + деактивация
    Запускается раз в час.
    """
    from bot import bot

    # ── 1. Скоро истекают (через 1–3 дня) ────────────────────────────────────
    expiring = await db.get_expiring_subscriptions(days_before=3)
    for sub in expiring:
        user_id = sub["user_id"]
        expires_at = sub["expires_at"]
        days_left = (expires_at - datetime.now(timezone.utc)).days + 1

        try:
            await bot.send_message(
                user_id,
                f"⚠️ <b>Ваша Premium-подписка скоро закончится!</b>\n\n"
                f"⏳ Осталось: <b>{days_left} дн.</b>\n"
                f"📅 Дата истечения: {expires_at.strftime('%d.%m.%Y')}\n\n"
                f"Продлите подписку за <b>50 ⭐️ Stars</b> через /premium, чтобы не потерять уведомления о скидках!",
                parse_mode="HTML"
            )
            await db.update_last_notified(user_id)
            logger.info(f"Expiry warning sent to {user_id} ({days_left} days left)")
        except Exception as e:
            logger.warning(f"Could not notify {user_id} about expiry: {e}")

    # ── 2. Уже истекли — деактивируем и уведомляем ───────────────────────────
    expired = await db.get_expired_subscriptions()
    for sub in expired:
        user_id = sub["user_id"]
        await db.deactivate_subscription(user_id)
        try:
            await bot.send_message(
                user_id,
                f"😔 <b>Ваша Premium-подписка закончилась.</b>\n\n"
                f"Вы больше не будете получать уведомления о скидках.\n\n"
                f"👉 Продлите подписку за <b>50 ⭐️ Stars/месяц</b> → /premium",
                parse_mode="HTML"
            )
            logger.info(f"Expiry notification sent to {user_id}")
        except Exception as e:
            logger.warning(f"Could not notify {user_id} about expired sub: {e}")

    if expiring or expired:
        logger.info(
            f"Subscription check: {len(expiring)} expiring soon, {len(expired)} just expired."
        )


def setup_scheduler():
    """Настройка планировщика (без запуска — запуск делается в main.py)."""
    from datetime import datetime
    scheduler = AsyncIOScheduler()

    # Глобальный мониторинг
    scheduler.add_job(
        run_monitoring_cycle,
        "interval",
        minutes=config.FETCH_INTERVAL_MINUTES,
        id="monitoring_job",
        next_run_time=datetime.now()
    )

    # Точечная следилка (каждые 5 минут)
    scheduler.add_job(
        run_personal_tracker_cycle,
        "interval",
        minutes=5,
        id="personal_tracker_job",
        next_run_time=datetime.now()
    )

    # Проверка подписок: предупреждения и деактивация (каждый час)
    scheduler.add_job(
        run_subscription_check_cycle,
        "interval",
        hours=1,
        id="subscription_check_job",
        next_run_time=datetime.now()  # Запускаем сразу при старте
    )

    return scheduler