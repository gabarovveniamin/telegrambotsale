import os
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from database import db
from config import config
from services.cryptopay_service import cryptopay_service
import logging
import asyncio
import aiohttp

logger = logging.getLogger(__name__)

app = FastAPI()

# Serving static files
app.mount("/static", StaticFiles(directory="tma"), name="static")

class VerifyRequest(BaseModel):
    user_id: int
    boc: str

@app.get("/pay", response_class=HTMLResponse)
async def read_item(request: Request):
    with open("tma/index.html", "r", encoding="utf-8") as f:
        content = f.read()
    # Dynamic replace of wallet address from env
    wallet = os.getenv("MY_TON_WALLET", "UQA_PLACEHOLDER")
    return content.replace("YOUR_WALLET_ADDRESS_HERE", wallet)

@app.get("/api/get-price")
async def get_price():
    # Use existing service to get dynamic price
    price = await cryptopay_service.get_ton_price_for_stars(150) # 150 stars as baseline
    return {"price": price}

@app.post("/api/verify-payment")
async def verify_payment(data: VerifyRequest):
    logger.info(f"Verifying payment for user {data.user_id}")
    
    api_key = os.getenv("TONAPI_KEY") # We'll use TONAPI_KEY now
    wallet = os.getenv("MY_TON_WALLET")
    
    if not api_key or not wallet:
        return {"success": False, "error": "Server configuration missing"}

    memo = f"premium_{data.user_id}"
    
    try:
        headers = {"Authorization": f"Bearer {api_key}"}
        async with aiohttp.ClientSession() as session:
            # TonAPI endpoint to get account events (transactions)
            url = f"https://tonapi.io/v2/blockchain/accounts/{wallet}/transactions?limit=15"
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    return {"success": False, "error": f"TonAPI error: {resp.status} - {text}"}
                
                result = await resp.json()
                transactions = result.get("transactions", [])
                
                for tx in transactions:
                    # In TonAPI, we look at out_msgs or in_msgs
                    # For incoming payment, we check in_msg
                    in_msg = tx.get("in_msg", {})
                    if not in_msg: continue
                    
                    # Check for comment (decoded_body)
                    decoded_body = in_msg.get("decoded_body", {})
                    msg_text = decoded_body.get("text", "")
                    
                    if msg_text == memo:
                        tx_hash = tx.get("hash")
                        if await db.is_transaction_used(tx_hash):
                            continue
                            
                        # Double check if it's successful
                        if not tx.get("success"): continue

                        await db.activate_subscription(data.user_id, days=30, stars_paid=0)
                        await db.save_used_transaction(tx_hash, data.user_id)
                        
                        return {"success": True}
        
        return {"success": False, "error": "Транзакция пока не найдена. Если вы уже отправили — подождите 1-2 минуты, пока блокчейн обновится."}
        
    except Exception as e:
        logger.error(f"Error verifying payment: {e}")
        return {"success": False, "error": str(e)}

async def run_server():
    config_uvicorn = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config_uvicorn)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(run_server())
