import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, Request

app = FastAPI()

TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK", "").strip()
TZ_NAME = os.getenv("TZ_NAME", "America/Denver")
MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "5"))

TZ = ZoneInfo(TZ_NAME)

# In-memory daily state (resets if Railway restarts)
_state = {
    "date": None,           # YYYY-MM-DD in TZ
    "buy_count": 0,         # number of BUYs sent today
    "bought_symbols": set() # symbols already bought today (to prevent duplicates)
}


def _today_str() -> str:
    return datetime.now(TZ).date().isoformat()


def _reset_if_new_day() -> None:
    today = _today_str()
    if _state["date"] != today:
        _state["date"] = today
        _state["buy_count"] = 0
        _state["bought_symbols"] = set()
        print(f"[DAILY RESET] New day in {TZ_NAME}: {today}. Counters cleared.")


def _post_to_traderspost(symbol: str, action: str) -> str:
    if not TRADERSPOST_WEBHOOK:
        raise RuntimeError("TRADERSPOST_WEBHOOK env var is not set")

    payload = {"ticker": symbol, "action": action}
    r = requests.post(TRADERSPOST_WEBHOOK, json=payload, timeout=15)
    return f"{r.status_code} {r.text}"


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    _reset_if_new_day()

    data = await request.json()

    msg = data.get("message", {}) or {}
    text = msg.get("text") or msg.get("caption") or ""
    print(f"Received text/caption: {text[:200]}")

    # Your Intellectia format: "Symbol CIEN has a daytrading signal!"
    m = re.search(r"Symbol\s+([A-Z]{1,10})\b", text, re.IGNORECASE)
    if not m:
        return {"status": "ignored", "reason": "no Symbol <TICKER> found"}

    symbol = m.group(1).upper()

    # If you later add SELL support, you can detect it here.
    # For now, treat all signals as BUY (current behavior).
    action = "buy"

    # Enforce max buys per day
    if action == "buy":
        if symbol in _state["bought_symbols"]:
            print(f"[SKIP] Duplicate BUY for {symbol} on {_state['date']}.")
            return {"status": "skipped", "reason": "duplicate buy today", "symbol": symbol}

        if _state["buy_count"] >= MAX_BUYS_PER_DAY:
            print(f"[SKIP] Daily BUY limit reached ({MAX_BUYS_PER_DAY}). Ignoring {symbol}.")
            return {
                "status": "skipped",
                "reason": "daily buy limit reached",
                "symbol": symbol,
                "buy_count": _state["buy_count"],
                "max_buys_per_day": MAX_BUYS_PER_DAY,
            }

    # Send to TradersPost
    try:
        tp_resp = _post_to_traderspost(symbol, action)
    except Exception as e:
        print(f"[ERROR] TradersPost post failed: {e}")
        return {"status": "error", "symbol": symbol, "error": str(e)}

    print(f"[SENT] {action.upper()} {symbol} | TradersPost response: {tp_resp}")

    if action == "buy":
        _state["buy_count"] += 1
        _state["bought_symbols"].add(symbol)
        print(f"[COUNT] buy_count={_state['buy_count']} / {MAX_BUYS_PER_DAY}")

    return {
        "status": "sent",
        "symbol": symbol,
        "action": action,
        "date_tz": _state["date"],
        "buy_count": _state["buy_count"],
        "max_buys_per_day": MAX_BUYS_PER_DAY,
        "traderspost_response": tp_resp,
    }
