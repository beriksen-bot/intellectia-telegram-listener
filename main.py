import os
import re
import requests
from fastapi import FastAPI, Request

app = FastAPI()

TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK")

def extract_message(data: dict) -> dict:
    # Telegram can deliver several message-like fields
    return (
        data.get("message")
        or data.get("edited_message")
        or data.get("channel_post")
        or data.get("edited_channel_post")
        or {}
    )

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    msg = extract_message(data)

    # Text can be in "text" OR "caption" (photos, videos, etc.)
    text = msg.get("text") or msg.get("caption") or ""

    print("Update keys:", list(data.keys()))
    print("Received text/caption:", text[:300])

    # More forgiving match (handles emojis/newlines)
    m = re.search(r"\bSymbol\s+([A-Za-z]{1,10})\b", text)
    if not m:
        return {"status": "no symbol", "sample": text[:120]}

    symbol = m.group(1).upper()
    print("Parsed symbol:", symbol)

    if not TRADERSPOST_WEBHOOK:
        print("ERROR: TRADERSPOST_WEBHOOK env var is not set")
        return {"status": "missing_webhook"}

    payload = {"ticker": symbol, "action": "buy"}
    try:
        r = requests.post(TRADERSPOST_WEBHOOK, json=payload, timeout=10)
        print("TradersPost status:", r.status_code)
        print("TradersPost response:", r.text[:500])
        return {"status": "sent", "symbol": symbol, "tp_status": r.status_code}
    except Exception as e:
        print("ERROR posting to TradersPost:", str(e))
        return {"status": "tp_error", "error": str(e)}
