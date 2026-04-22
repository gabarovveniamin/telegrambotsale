import asyncio
import logging
import os
import aiohttp
from database import db
from bot import bot
from config import config
logger = logging.getLogger(__name__)
async def ton_poller_task():
    """
    Background task to poll TON blockchain for payments.
    Uses TonAPI.io (or Toncenter if needed).
    """
    logger.info("TON Poller started.")
    while True:
        try:
            api_key = os.getenv("TONAPI_KEY")
            wallet = os.getenv("MY_TON_WALLET")
            if not api_key or not wallet:
                await asyncio.sleep(60)
                continue
            headers = {"Authorization": f"Bearer {api_key}"}
            async with aiohttp.ClientSession() as session:
                url = f"https://tonapi.io/v2/blockchain/accounts/{wallet}/transactions?limit=20"
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        transactions = data.get("transactions", [])
                        for tx in transactions:
                            if not tx.get("success"): continue
                            tx_hash = tx.get("hash")
                            if await db.is_transaction_used(tx_hash):
                                continue
                            in_msg = tx.get("in_msg", {})
                            decoded_body = in_msg.get("decoded_body", {})
                            msg_text = decoded_body.get("text", "")
                            if msg_text and msg_text.startswith("premium_"):
                                try:
                                    user_id = int(msg_text.replace("premium_", ""))
                                    await db.activate_subscription(user_id, days=30, stars_paid=0)
                                    await db.save_used_transaction(tx_hash, user_id)
                                    try:
                                        await bot.send_message(
                                            user_id,
                                            "✅ <b>Оплата через TON получена!</b>\n\nПодписка Premium активирована на 30 дней. Спасибо!",
                                            parse_mode="HTML"
                                        )
                                        logger.info(f"Successfully processed TON payment for user {user_id}")
                                    except Exception as e:
                                        logger.error(f"Failed to notify user {user_id}: {e}")
                                except ValueError:
                                    continue
                    else:
                        logger.error(f"TonAPI error in poller: {resp.status}")
        except Exception as e:
            logger.error(f"Error in TON Poller loop: {e}")
        await asyncio.sleep(30)
