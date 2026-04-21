import asyncio
import logging
from bot import dp, bot
from database import db
from scheduler import setup_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    """Entry point: init DB + scheduler once, then start polling with auto-reconnect."""

    # ── One-time initialization ────────────────────────────────────────────────
    try:
        await db.init()
        logger.info("Database initialized.")
    except Exception as e:
        logger.critical(f"Failed to initialize database: {e}")
        return

    try:
        scheduler = setup_scheduler()
        scheduler.start()
        logger.info("Scheduler started.")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")

    # ── Start TON Payment Poller (Background) ──────────────────────────────────
    try:
        from services.ton_poller import ton_poller_task
        asyncio.create_task(ton_poller_task())
        logger.info("TON Payment Poller started.")
    except Exception as e:
        logger.error(f"TON Poller error: {e}")

    # ── Polling with auto-reconnect ────────────────────────────────────────────
    retry_delay = 5

    try:
        while True:
            try:
                logger.info("Bot is starting polling...")
                await dp.start_polling(bot, handle_signals=False)
                break  # clean stop
            except (KeyboardInterrupt, SystemExit):
                break
            except Exception as e:
                logger.error(f"Polling crashed: {e}. Reconnecting in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
    finally:
        from services.cryptopay_service import cryptopay_service
        await cryptopay_service.close()
        await db.disconnect()
        logger.info("Bot stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass