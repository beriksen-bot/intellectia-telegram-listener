import os
import re
import threading
from datetime import datetime, time
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, Request

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = FastAPI()

# =========================
# CONFIG
# =========================
TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK", "").strip()
if not TRADERSPOST_WEBHOOK:
    raise RuntimeError("Missing env var TRADERSPOST_WEBHOOK")

MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "5"))

# Utah time (handles DST correctly)
MT = ZoneInfo("America/Denver")

# --- BUY WINDOW (defaults to ET) ---
BUY_WINDOW_ENABLED = os.getenv("BUY_WINDOW_ENABLED", "true").lower() in ("1", "true", "yes", "y")
BUY_WINDOW_TZ = ZoneInfo(os.getenv("BUY_WINDOW_TZ", "America/New_York"))
BUY_WINDOW_START = os.getenv("BUY_WINDOW_START", "09:00")
BUY_WINDOW_END = os.getenv("BUY_WINDOW_END", "11:00")

# --- BLACKLIST ---
BLACKLIST_RAW = os.getenv("BLACKLIST", "")
BLACKLIST = {s.strip().upper() for s in BLACKLIST_RAW.split(",") if s.strip()}

# Failsafe flatten at 1:55pm Utah time (13:55)
ENABLE_FLATTEN_FAILSAFE = os.getenv("ENABLE_FLATTEN_FAILSAFE", "true").lower() in ("1", "true", "yes", "y")
FLATTEN_HOUR = int(os.getenv("FLATTEN_HOUR", "13"))
FLATTEN_MINUTE = int(os.getenv("FLATTEN_MINUTE", "55"))

# Match Intellectia format: "Symbol CIEN has a daytrading signal!"
SYMBOL_RE = re.compile(r"\bSymbol\s+([A-Z]{1,10})\b", re.IGNORECASE)

# =========================
# STATE (in-memory)
# =========================
lock = threading.Lock()
today_date = None  # date in MT
signal_count_by_symbol = {}  # {"AAPL": 1, ...} counts signals today per ticker
buy_count_today = 0          # number of BUYs sent today
open_positions = set()       # best-effort tracking for failsafe flatten


# =========================
# HELPERS
# =========================
def parse_hhmm(s: str) -> time:
    # expects "HH:MM"
    hh, mm = s.strip().split(":")
    return time(int(hh), int(mm))


BUY_START_T = parse_hhmm(BUY_WINDOW_START)
BUY_END_T = parse_hhmm(BUY_WINDOW_END)


def in_time_window(now_t: time, start_t: time, end_t: time) -> bool:
    """
    True if now_t is within [start_t, end_t).
    Supports windows crossing midnight.
    """
    if start_t == end_t:
        return True
    if start_t < end_t:
        return start_t <= now_t < end_t
    return now_t >= start_t or now_t < end_t


def buy_window_open_now() -> bool:
    """
    Window check in BUY_WINDOW_TZ (defaults ET).
    """
    if not BUY_WINDOW_ENABLED:
        return True
    now = datetime.now(BUY_WINDOW_TZ)
    now_t = now.time().replace(second=0, microsecond=0)
    return in_time_window(now_t, BUY_START_T, BUY_END_T)


def reset_if_new_day() -> None:
    """Reset counters when the day changes in Utah time."""
    global today_date, signal_count_by_symbol, buy_count_today, open_positions

    d = datetime.now(MT).date()
    with lock:
        if today_date != d:
            today_date = d
            signal_count_by_symbol = {}
            buy_count_today = 0
            open_positions = set()

            print("=================================")
            print(f"[RESET] New trading day (MT): {today_date}")
            print(f"[BUY COUNT] {buy_count_today} / {MAX_BUYS_PER_DAY}")
            if BLACKLIST:
                print(f"[BLACKLIST] {sorted(BLACKLIST)}")
            print(f"[BUY WINDOW] enabled={BUY_WINDOW_ENABLED} tz={BUY_WINDOW_TZ.key} {BUY_WINDOW_START}->{BUY_WINDOW_END}")
            print("=================================")


def extract_text(update: dict) -> str:
    """Telegram updates can store content in multiple places; handle all common ones."""
    msg = (
        update.get("message")
        or update.get("edited_message")
        or update.get("channel_post")
        or update.get("edited_channel_post")
        or {}
    )
    return (msg.get("text") or msg.get("caption") or "").strip()


def parse_symbol(text: str):
    m = SYMBOL_RE.search(text or "")
    return m.group(1).upper() if m else None


def post_to_traderspost(symbol: str, action: str) -> None:
    payload = {"ticker": symbol, "action": action}
    r = requests.post(TRADERSPOST_WEBHOOK, json=payload, timeout=15)
    print(f"[TRADERSPOST] {action.upper()} {symbol} -> {r.status_code} {r.text[:250]}")


def decide_action(symbol: str):
    """
    Pairing logic (per ticker, per day):
      - 1st signal -> BUY (if under daily buy limit AND within buy window)
      - 2nd signal -> SELL (only if we believe we opened it)
      - 3rd -> BUY ...
    """
    global buy_count_today

    reset_if_new_day()

    # Hard ignore blacklisted symbols (no counting, no trades)
    if symbol in BLACKLIST:
        print(f"[BLOCKED] {symbol} is blacklisted — ignoring signal.")
        return None, "blacklisted"

    # We decide based on what the *next* count would be, but only commit it if allowed.
    with lock:
        current = signal_count_by_symbol.get(symbol, 0)
        next_count = current + 1

        # Odd signals => BUY attempt
        if next_count % 2 == 1:
            if buy_count_today >= MAX_BUYS_PER_DAY:
                print(f"[LIMIT] Max buys reached: {buy_count_today}/{MAX_BUYS_PER_DAY} — BLOCK BUY {symbol}")
                return None, "max_buys_reached"

            if not buy_window_open_now():
                now_et = datetime.now(BUY_WINDOW_TZ).strftime("%H:%M")
                print(f"[WINDOW] Outside buy window ({BUY_WINDOW_START}-{BUY_WINDOW_END} {BUY_WINDOW_TZ.key}). Now={now_et}. BLOCK BUY {symbol}")
                # IMPORTANT: do NOT increment signal count (prevents afternoon sell turning into buy)
                return None, "outside_buy_window"

            # Allowed BUY: commit count + state
            signal_count_by_symbol[symbol] = next_count
            buy_count_today += 1
            open_positions.add(symbol)

            print(f"[BUY] {symbol}")
            print(f"[BUY COUNT] {buy_count_today} / {MAX_BUYS_PER_DAY}")
            return "buy", None

        # Even signals => SELL attempt
        # Only SELL if we think we actually opened it
        if symbol not in open_positions:
            print(f"[SELL-BLOCKED] {symbol} has no tracked open position — ignoring SELL signal.")
            # We also do NOT increment the counter in this case to avoid drifting state.
            return None, "no_open_position"

        # Allowed SELL: commit count + state
        signal_count_by_symbol[symbol] = next_count
        open_positions.discard(symbol)

        print(f"[SELL] {symbol}")
        print(f"[BUY COUNT] still {buy_count_today} / {MAX_BUYS_PER_DAY}")
        return "sell", None


def flatten_all_open_positions() -> None:
    """Failsafe: sell everything we think is still open (best-effort)."""
    reset_if_new_day()

    with lock:
        symbols = sorted(open_positions)

    if not symbols:
        print(f"[FAILSAFE] Flatten @ {datetime.now(MT).isoformat()} — nothing open.")
        return

    print(f"[FAILSAFE] Flatten @ {datetime.now(MT).isoformat()} — closing: {symbols}")
    for sym in symbols:
        try:
            post_to_traderspost(sym, "sell")
        except Exception as e:
            print(f"[FAILSAFE][ERROR] SELL {sym} failed: {e}")

    with lock:
        open_positions.clear()
    print("[FAILSAFE] Flatten complete.")


# =========================
# ROUTES
# =========================
@app.get("/health")
def health():
    reset_if_new_day()
    with lock:
        return {
            "ok": True,
            "date_mt": str(today_date),
            "buy_count_today": buy_count_today,
            "max_buys_per_day": MAX_BUYS_PER_DAY,
            "open_positions": sorted(open_positions),
            "blacklist": sorted(BLACKLIST),
            "buy_window": {
                "enabled": BUY_WINDOW_ENABLED,
                "tz": BUY_WINDOW_TZ.key,
                "start": BUY_WINDOW_START,
                "end": BUY_WINDOW_END,
            },
        }


@app.post("/flatten-now")
def flatten_now():
    """Manual test endpoint."""
    flatten_all_open_positions()
    return {"ok": True}


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    update = await request.json()
    reset_if_new_day()

    text = extract_text(update)
    print(f"[RX] {text[:300]}")

    symbol = parse_symbol(text)
    if not symbol:
        return {"status": "ignored", "reason": "no Symbol <TICKER> found"}

    action, reason = decide_action(symbol)
    if action is None:
        return {
            "status": "blocked",
            "symbol": symbol,
            "reason": reason,
            "buy_count_today": buy_count_today,
            "max_buys_per_day": MAX_BUYS_PER_DAY,
        }

    try:
        post_to_traderspost(symbol, action)
    except Exception as e:
        print(f"[ERROR] TradersPost post failed for {symbol} {action}: {e}")
        return {"status": "error", "symbol": symbol, "action": action, "error": str(e)}

    return {"status": "sent", "symbol": symbol, "action": action}


# =========================
# SCHEDULER (Failsafe 1:55pm MT)
# =========================
scheduler = BackgroundScheduler(timezone=MT)

if ENABLE_FLATTEN_FAILSAFE:
    scheduler.add_job(
        flatten_all_open_positions,
        CronTrigger(day_of_week="mon-fri", hour=FLATTEN_HOUR, minute=FLATTEN_MINUTE),
        id="flatten_failsafe",
        replace_existing=True,
    )
    print(f"[SCHEDULER] Failsafe flatten ENABLED at {FLATTEN_HOUR:02d}:{FLATTEN_MINUTE:02d} MT (Mon–Fri)")
else:
    print("[SCHEDULER] Failsafe flatten DISABLED")

scheduler.start()
