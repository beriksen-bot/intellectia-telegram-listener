import requests
import os
import re
from fastapi import FastAPI, Request

app = FastAPI()

TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK")

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):

    data = await request.json()

    text = data.get("message", {}).get("text", "")

    print("Received:", text)

    # Extract ticker
    match = re.search(r"Symbol (\w+)", text)

    if not match:
        return {"status": "no symbol"}

    symbol = match.group(1)

    print("Parsed symbol:", symbol)

    payload = {
        "ticker": symbol,
        "action": "buy"
    }

    r = requests.post(TRADERSPOST_WEBHOOK, json=payload)

    print("TradersPost response:", r.text)

    return {"status": "sent", "symbol": symbol}
