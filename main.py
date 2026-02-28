import re
from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    update = await request.json()

    msg = update.get("message") or update.get("channel_post") or {}
    text = msg.get("text", "") or ""

    m = re.search(r"\bSymbol\s+([A-Z\.]{1,10})\s+has a daytrading signal\b", text, re.IGNORECASE)
    symbol = m.group(1).upper() if m else None

    print({"parsed_symbol": symbol, "text": text})

    return {"ok": True, "symbol": symbol}
