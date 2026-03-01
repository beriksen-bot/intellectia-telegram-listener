import os
import re
import threading
from datetime import datetime, date
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, Request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = FastAPI()

# ====== CONFIG ======
TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK")  # required
if not TRADERSPOST_WEBHOOK:
    raise RuntimeError("Missing env var TRADERSPOST_WEBHOOK")

# Utah time (Mountain). America/Denver handles DST correctly.
MT = ZoneInfo("America/Denver")

# Failsafe: set to "true" to flatten all tracked positions at 1:50pm MT Mon-Fri.
ENABLE_FLATTEN_FAILSAFE = os.getenv("ENABLE_FLATTEN_FAILSAFE", "true").lower() in ("1", "true", "yes", "y")

# Flatten time (defaults to 13:50 MT)
FLATTEN_HOUR = int(os.getenv("FLATTEN_HOUR", "13"))
FLATTEN_MINUTE = int(os.getenv("FLATTEN_MINUTE", "50"))

# ====== STATE (in-memory) ======
# Tracks today's "signal count" per symbol so we can do: 1st=buy, 2nd=sell
state_lock = threading.Lock()
today_key = None  # will hold date() in MT
signal_count_by_symbol = {}  # { "AAPL": 1 } etc
open_symbols = set()  # symbols we have bought and not yet sold (best effort)


# ====== HELPERS ======
SYMBOL_RE = re.compile(r"\bSymbol\s+([A-Z]{1,10})\b", re.IGNORECASE)

def _extract_text(update: dict) -> str:
    """Pull Telegram text from message/channel_post, and from text/caption."""
    msg = (
        update.get("message")
        or update.get("edited_message")
        or update.get("channel_post")
        or update.get("edited_channel_post")
        or {}
    )
    return (msg.get("text") or msg.get("caption") or "").strip()

def _roll_day_if_needed():
    """Reset state when the day changes in Mountain Time."""
    global today_key, signal_count_by_symbol, open_symbols
    now_mt = datetime.now(MT)
    d = now_mt.date()
    with state_lock:
        if today_key != d:
            today_key = d
            signal_count_by_symbol = {}
            open_symbols = set()
            print(f"[STATE] New day detected ({today_key}). State reset.")

def _post_to_traderspost(ticker: str, action: str) -> str:
    """Send buy/sell to TradersPost webhook."""
    payload = {"ticker": ticker, "action": action}
    r = requests.post(TRADERSPOST_WEBHOOK, json=payload, timeout=15)
    return f"{r.status_code} {r.text[:300]}"

def _decide_action(symbol: str) -> str:
    """
    Your rule:
      - 1st signal today => BUY
      - 2nd signal today => SELL
      - 3rd => BUY (if it happens again), 4th => SELL, etc.
    """
    _roll_day_if_needed()

    with state_lock:
        c = signal_count_by_symbol.get(symbol, 0) + 1
        signal_count_by_symbol[symbol] = c

        # Odd = buy, even = sell
        action = "buy" if (c % 2 == 1) else "sell"

        # Track opens best-effort
        if action == "buy":
            open_symbols.add(symbol)
        else:
            open_symbols.discard(symbol)

    return action

def flatten_all_open_positions():
    """Failsafe sell: closes everything we think is still open."""
    _roll_day_if_needed()

    with state_lock:
        symbols = sorted(list(open_symbols))

    if not symbols:
        print(f"[FLATTEN] Nothing to flatten @ {datetime.now(MT).isoformat()}")
        return

    print(f"[FLATTEN] Selling all open symbols {symbols} @ {datetime.now(MT).isoformat()}")

    for sym in symbols:
        try:
            resp = _post_to_traderspost(sym, "sell")
            print(f"[FLATTEN] SELL {sym} -> {resp}")
        except Exception as e:
            print(f"[FLATTEN] ERROR selling {sym}: {e}")

    # Clear after attempting
    with state_lock:
        for sym in symbols:
            open_symbols.discard(sym)

    print("[FLATTEN] Done.")


# ====== ROUTES ======
@app.get("/health")
def health():
    _roll_day_if_needed()
    with state_lock:
        return {
            "ok": True,
            "date_mt": str(today_key),
            "open_symbols": sorted(list(open_symbols)),
            "signal_counts": dict(sorted(signal_count_by_symbol.items()))
        }

@app.post("/flatten-now")
def flatten_now():
    """Manual test endpoint: hit this to force a flatten immediately."""
    flatten_all_open_positions()
    return {"ok": True}

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    update = await request.json()
    text = _extract_text(update)

    # Log what we received (trimmed)
    print("[TELEGRAM] Received:", text[:300])

    # Find Symbol <TICKER>
    m = SYMBOL_RE.search(text)
    if not m:
        return {"status": "ignored", "reason": "no Symbol <TICKER> found"}

    symbol = m.group(1).upper()
    action = _decide_action(symbol)

    print(f"[LOGIC] {symbol} -> {action.upper()} (paired mode)")

    try:
        resp = _post_to_traderspost(symbol, action)
        print(f"[TRADERSPOST] {action.upper()} {symbol} -> {resp}")
        return {"status": "sent", "symbol": symbol, "action": action}
    except Exception as e:
        print(f"[ERROR] Posting to TradersPost failed: {e}")
        return {"status": "error", "error": str(e), "symbol": symbol, "action": action}


# ====== SCHEDULER (failsafe flatten) ======
scheduler = BackgroundScheduler(timezone=MT)

if ENABLE_FLATTEN_FAILSAFE:
    scheduler.add_job(
        flatten_all_open_positions,
        CronTrigger(day_of_week="mon-fri", hour=FLATTEN_HOUR, minute=FLATTEN_MINUTE),
        id="flatten_failsafe",
        replace_existing=True,
    )
    print(f"[SCHEDULER] Flatten failsafe ENABLED at {FLATTEN_HOUR:02d}:{FLATTEN_MINUTE:02d} MT Mon-Fri")
else:
    print("[SCHEDULER] Flatten failsafe DISABLED")

scheduler.start()
