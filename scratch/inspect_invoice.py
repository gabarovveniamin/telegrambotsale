import asyncio
from aiocryptopay import AioCryptoPay, Networks

async def main():
    # We can use a fake token or just inspect the properties of the class
    # But better to just check what's available in the library
    from aiocryptopay.models.invoice import Invoice
    print(dir(Invoice))

if __name__ == "__main__":
    asyncio.run(main())
