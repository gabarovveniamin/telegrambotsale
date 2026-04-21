from aiocryptopay import AioCryptoPay, Networks
from config import config
import logging

logger = logging.getLogger(__name__)

class CryptoPayService:
    def __init__(self):
        self.crypto = AioCryptoPay(token=config.CRYPTOPAY_TOKEN, network=Networks.MAIN_NET)

    async def get_ton_price_for_stars(self, stars_amount: int) -> float:
        """
        Calculate TON amount for a given number of Stars.
        Telegram Stars: 1 Star approx $0.02 USD.
        """
        try:
            # 1. Calculate USD equivalent
            usd_price = stars_amount * 0.02
            
            # 2. Get TON price in USD from CryptoPay
            # get_exchange_rates returns a list of rates
            rates = await self.crypto.get_exchange_rates()
            
            # Find TON to USD rate
            ton_to_usd = 0
            for rate in rates:
                if rate.source == 'TON' and rate.target == 'USD':
                    ton_to_usd = rate.rate
                    break
            
            if ton_to_usd == 0:
                # Fallback or error
                logger.warning("Could not fetch TON/USD rate, using fallback 5.0")
                ton_to_usd = 5.0
            
            # 3. Calculate TON amount
            ton_amount = usd_price / ton_to_usd
            
            # Round up slightly for safety or just return
            return round(ton_amount, 4)
        except Exception as e:
            logger.error(f"Error calculating TON price: {e}")
            # Fallback: if 100 stars = 5 TON, then 150 stars = 7.5 TON
            return 7.5

    async def create_invoice(self, user_id: int, ton_amount: float, days: int):
        """Create a CryptoPay invoice."""
        invoice = await self.crypto.create_invoice(
            asset='TON',
            amount=ton_amount,
            description=f"Premium subscription for {days} days",
            payload=f"premium_ton_{days}_{user_id}"
        )
        return invoice

    async def check_invoice(self, invoice_id: int):
        """Check invoice status."""
        invoices = await self.crypto.get_invoices(invoice_ids=invoice_id)
        if invoices:
            return invoices[0]
        return None

    async def close(self):
        await self.crypto.close()

cryptopay_service = CryptoPayService()
