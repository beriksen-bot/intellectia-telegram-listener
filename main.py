import os
import re
import threading
from typing import Dict, Set, Optional

import requests
from fastapi import FastAPI, Request

# Optional scheduler (failsafe flatten)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = FastAPI()

# ====== ENV VARS ======
TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK", "").strip()

# Max number of OPEN positions allowed at any moment
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "5"))

# Failsafe flatten: 1:55pm Utah time (America/Denver)
ENABLE_FLATTEN_FAILSAFE = os.getenv("ENABLE_FLATTEN_FAILSAFE", "true").lower() in ("1", "true", "yes", "y")
FLATTEN_TZ = os.getenv("FLATTEN_TZ", "America/Denver")
FLATTEN_HOUR = int(os.getenv("FLATTEN_HOUR", "13"))     # 13 = 1pm
FLATTEN_MINUTE = int(os.getenv("FLATTEN_MINUTE", "55")) # 55

# ====== STATE ======
# Track symbols currently "open" according to our webhook logic
_open_positions: Set[str] = set()
_lock = threading.Lock()

# ====== HELPERS ======
SYMBOL_RE = re.compile(r"Symbol\s+([A-Z]{1,10})\b")

def _extract_text(update: dict) -> str:
    """
    Telegram updates may store content in:
      - message.text (normal)
      - message.caption (forwarded media / link previews)
      - edited_message.text/caption
    """
    msg = update.get("message") or update.get("edited_message") or {}
    return (msg.get("text") or msg.get("caption") or "").strip()

def _parse_symbol(text: str) -> Optional[str]:
    m = SYMBOL_RE.search(text)
    if not m:
        return None
    return m.group(1).upper()

def _send_to_traderspost(symbol: str, action: str) -> None:
    """
    action: 'buy' or 'sell'
    NOTE: In TradersPost, configure the strategy so a SELL signal closes the position (not short).
    """
    if not TRADERSPOST_WEBHOOK:
        print("[ERROR] TRADERSPOST_WEBHOOK not set")
        return

    payload = {
        "ticker": symbol,
        "action": action.lower(),
    }

    try:
        r = requests.post(TRADERSPOST_WEBHOOK, json=payload, timeout=15)
        print(f"[TP] Sent {action.upper()} {symbol} -> {r.status_code} {r.text}")
    except Exception as e:
        print(f"[TP][ERROR] Failed sending {action.upper()} {symbol}: {e}")

def _flatten_all() -> None:
    """
    Failsafe: sell everything we *think* is open, then clear the set.
    """
    with _lock:
        to_close = sorted(_open_positions)

    if not to_close:
        print("[FLATTEN] No open positions to close.")
        return

    print(f"[FLATTEN] Closing {len(to_close)} positions: {to_close}")
    for sym in to_close:
        _send_to_traderspost(sym, "sell")

    with _lock:
        _open_positions.clear()

# ====== TELEGRAM WEBHOOK ======
@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    update = await request.json()
    text = _extract_text(update)

    print("[RX] Raw text/caption:", text)

    symbol = _parse_symbol(text)
    if not symbol:
        return {"status": "no_symbol_found"}

    # Toggle logic:
    # - If symbol is already open -> SELL (close)
    # - Else -> BUY, but only if we have capacity under MAX_OPEN_POSITIONS
    with _lock:
        is_open = symbol in _open_positions
        open_count = len(_open_positions)

        if is_open:
            # Close it
            _open_positions.remove(symbol)
            action = "sell"
            reason = "toggle_close"
        else:
            # Open it only if capacity allows
            if open_count >= MAX_OPEN_POSITIONS:
                print(f"[LIMIT] Max open positions reached ({open_count}/{MAX_OPEN_POSITIONS}). Ignoring BUY for {symbol}.")
                return {"status": "blocked_max_open_positions", "symbol": symbol, "open_positions": open_count}

            _open_positions.add(symbol)
            action = "buy"
            reason = "toggle_open"

    print(f"[DECISION] {action.upper()} {symbol} ({reason}). Open now: {len(_open_positions)}/{MAX_OPEN_POSITIONS}")
    _send_to_traderspost(symbol, action)

    return {
        "status": "sent",
        "symbol": symbol,
        "action": action,
        "open_positions": len(_open_positions),
        "max_open_positions": MAX_OPEN_POSITIONS,
    }

# ====== SCHEDULER FAILSAFE ======
scheduler = BackgroundScheduler(timezone=FLATTEN_TZ)
if ENABLE_FLATTEN_FAILSAFE:
    scheduler.add_job(
        _flatten_all,
        CronTrigger(day_of_week="mon-fri", hour=FLATTEN_HOUR, minute=FLATTEN_MINUTE),
        id="flatten_failsafe",
        replace_existing=True,
    )
    print(f"[SCHEDULER] Failsafe FLATTEN enabled at {FLATTEN_HOUR:02d}:{FLATTEN_MINUTE:02d} ({FLATTEN_TZ}) Mon–Fri")
else:
    print("[SCHEDULER] Failsafe FLATTEN disabled")

scheduler.start()
