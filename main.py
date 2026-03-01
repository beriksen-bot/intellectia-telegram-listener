import os
import re
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, Request, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = FastAPI()

TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK")  # e.g. https://webhooks.traderspost.io/trading/webhook/...
if not TRADERSPOST_WEBHOOK:
    raise RuntimeError("Missing env var TRADERSPOST_WEBHOOK")

# Timezone for Utah
MT = ZoneInfo("America/Denver")

# Keep a simple in-memory set of symbols we've bought and not yet flattened.
# (This resets if Railway restarts — good enough for a daily flatten.)
open_symbols = set()
lock = threading.Lock()

# Regex that matches your Intellectia message format:
# "Symbol SOUN has a daytrading signal!"
SYMBOL_RE = re.compile(r"\bSymbol\s+([A-Z]{1,10})\b", re.IGNORECASE)


def _extract_text(update: dict) -> str:
    """
    Telegram updates can deliver content as:
      - update["message"]["text"]
      - update["message"]["caption"] (media messages)
      - update["channel_post"]["text"] / ["caption"] (channels)
    """
    msg = update.get("message") or update.get("channel_post") or {}
    return (msg.get("text") or msg.get("caption") or "").strip()


def _post_to_traderspost(ticker: str, action: str) -> dict:
    payload = {"ticker": ticker, "action": action}
    r = requests.post(TRADERSPOST_WEBHOOK, json=payload, timeout=15)
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}

    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail={"status": r.status_code, "body": body})
    return body


@app.get("/health")
def health():
    return {"ok": True, "open_symbols": sorted(list(open_symbols))}


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    update = await request.json()

    text = _extract_text(update)
    print("Received text/caption:", text)

    m = SYMBOL_RE.search(text)
    if not m:
        return {"status": "no symbol found"}

    symbol = m.group(1).upper()
    print("Parsed symbol:", symbol)

    # Send BUY signal to TradersPost
    resp = _post_to_traderspost(symbol, "buy")
    print("TradersPost response:", resp)

    # Track as open so we can flatten later
    with lock:
        open_symbols.add(symbol)

    return {"status": "sent", "symbol": symbol, "traderspost": resp}


def flatten_all_open_positions():
    """
    Sells every symbol we've seen a BUY for (since last restart),
    then clears the list.
    """
    with lock:
        symbols = sorted(list(open_symbols))

    if not symbols:
        print("Flatten: nothing to do")
        return

    print(f"Flatten: selling {symbols} @ {datetime.now(MT).isoformat()}")

    failures = []
    for sym in symbols:
        try:
            _post_to_traderspost(sym, "sell")
        except Exception as e:
            failures.append((sym, str(e)))

    if failures:
        print("Flatten failures:", failures)
        # Keep failures in the set so we try again next day
        with lock:
            for sym, _ in failures:
                open_symbols.add(sym)
            for sym in symbols:
                if sym not in dict(failures):
                    open_symbols.discard(sym)
        return

    # All succeeded
    with lock:
        for sym in symbols:
            open_symbols.discard(sym)

    print("Flatten: complete")


@app.post("/flatten-now")
def flatten_now():
    flatten_all_open_positions()
    return {"ok": True}


# Schedule flatten at 1:50pm Utah time, Mon-Fri
scheduler = BackgroundScheduler(timezone=MT)
scheduler.add_job(
    flatten_all_open_positions,
    CronTrigger(day_of_week="mon-fri", hour=13, minute=50),
    id="flatten_1350_mt",
    replace_existing=True,
)
scheduler.start()
