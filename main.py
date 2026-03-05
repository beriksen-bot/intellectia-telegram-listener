import os
import re
import asyncio
import threading
from datetime import datetime, time
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI
from telethon import TelegramClient, events
from telethon.sessions import StringSession

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

# Timezone used for "day" reset + failsafe (you were using Utah time)
MT = ZoneInfo(os.getenv("TZ_NAME", "America/Denver"))

# --- BUY WINDOW (defaults to ET) ---
BUY_WINDOW_ENABLED = os.getenv("BUY_WINDOW_ENABLED", "true").lower() in ("1", "true", "yes", "y")
BUY_WINDOW_TZ = ZoneInfo(os.getenv("BUY_WINDOW_TZ", "America/New_York"))
BUY_WINDOW_START = os.getenv("BUY_WINDOW_START", "09:00")
BUY_WINDOW_END = os.getenv("BUY_WINDOW_END", "11:00")

# --- BLACKLIST ---
BLACKLIST_RAW = os.getenv("BLACKLIST", "")
BLACKLIST = {s.strip().upper() for s in BLACKLIST_RAW.split(",") if s.strip()}

# Failsafe flatten (Mon–Fri)
ENABLE_FLATTEN_FAILSAFE = os.getenv("ENABLE_FLATTEN_FAILSAFE", "true").lower() in ("1", "true", "yes", "y")
FLATTEN_HOUR = int(os.getenv("FLATTEN_HOUR", "13"))
FLATTEN_MINUTE = int(os.getenv("FLATTEN_MINUTE", "55"))

# Match Intellectia format: "Symbol CIEN has a daytrading signal!"
SYMBOL_RE = re.compile(r"\bSymbol\s+([A-Z]{1,10})\b", re.IGNORECASE)

# --- TELETHON (listen to DT Relay) ---
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()

# This should be your DT Relay chat id (e.g. -1003724596299)
SOURCE_CHAT = os.getenv("SOURCE_CHAT", "").strip()
if not SOURCE_CHAT:
    raise RuntimeError("Missing env var SOURCE_CHAT (set to DT Relay chat id, e.g. -100...)")

if not API_ID or not API_HASH or not SESSION_STRING:
    raise RuntimeError("Missing Telethon env vars: API_ID, API_HASH, SESSION_STRING")

try:
    SOURCE_CHAT_ID = int(SOURCE_CHAT)
except ValueError:
    # Allow username too (e.g. @dt_relay_channel). Telethon can resolve it.
    SOURCE_CHAT_ID = SOURCE_CHAT

# =========================
# STATE (in-memory)
# =========================
lock = threading.Lock()
today_date = None  # date in MT
signal_count_by_symbol = {}  # {"AAPL": 1, ...}
buy_count_today = 0
open_positions = set()

# =========================
# HELPERS
# =========================
def parse_hhmm(s: str) -> time:
    hh, mm = s.strip().split(":")
    return time(int(hh), int(mm))

BUY_START_T = parse_hhmm(BUY_WINDOW_START)
BUY_END_T = parse_hhmm(BUY_WINDOW_END)

def in_time_window(now_t: time, start_t: time, end_t: time) -> bool:
    """True if now_t is within [start_t, end_t). Supports windows crossing midnight."""
    if start_t == end_t:
        return True
    if start_t < end_t:
        return start_t <= now_t < end_t
    return now_t >= start_t or now_t < end_t

def buy_window_open_now() -> bool:
    if not BUY_WINDOW_ENABLED:
        return True
    now = datetime.now(BUY_WINDOW_TZ)
    now_t = now.time().replace(second=0, microsecond=0)
    return in_time_window(now_t, BUY_START_T, BUY_END_T)

def reset_if_new_day() -> None:
    """Reset counters when the day changes in MT."""
    global today_date, signal_count_by_symbol, buy_count_today, open_positions
    now_mt = datetime.now(MT).date()
    with lock:
        if today_date != now_mt:
            today_date = now_mt
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

    if symbol in BLACKLIST:
        print(f"[BLOCKED] {symbol} is blacklisted — ignoring signal.")
        return None, "blacklisted"

    with lock:
        current = signal_count_by_symbol.get(symbol, 0)
        next_count = current + 1

        # Odd => BUY attempt
        if next_count % 2 == 1:
            if buy_count_today >= MAX_BUYS_PER_DAY:
                print(f"[LIMIT] Max buys reached: {buy_count_today}/{MAX_BUYS_PER_DAY} — BLOCK BUY {symbol}")
                return None, "max_buys_reached"

            if not buy_window_open_now():
                now_et = datetime.now(BUY_WINDOW_TZ).strftime("%H:%M")
                print(f"[WINDOW] Outside buy window ({BUY_WINDOW_START}-{BUY_WINDOW_END} {BUY_WINDOW_TZ.key}). Now={now_et}. BLOCK BUY {symbol}")
                return None, "outside_buy_window"

            signal_count_by_symbol[symbol] = next_count
            buy_count_today += 1
            open_positions.add(symbol)

            print(f"[BUY] {symbol}  ({buy_count_today}/{MAX_BUYS_PER_DAY})")
            return "buy", None

        # Even => SELL attempt (only if tracked open)
        if symbol not in open_positions:
            print(f"[SELL-BLOCKED] {symbol} has no tracked open position — ignoring SELL signal.")
            return None, "no_open_position"

        signal_count_by_symbol[symbol] = next_count
        open_positions.discard(symbol)

        print(f"[SELL] {symbol}")
        print(f"[BUY COUNT] still {buy_count_today} / {MAX_BUYS_PER_DAY}")
        return "sell", None

def flatten_all_open_positions() -> None:
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
# FASTAPI ROUTES
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
            "source_chat": str(SOURCE_CHAT),
        }

@app.post("/flatten-now")
def flatten_now():
    flatten_all_open_positions()
    return {"ok": True}

# =========================
# SCHEDULER (Failsafe)
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

# =========================
# TELETHON LISTENER
# =========================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

@client.on(events.NewMessage(chats=SOURCE_CHAT_ID))
async def on_new_message(event):
    reset_if_new_day()

    text = (event.raw_text or "").strip()
    if not text:
        return

    # Log what we received (first 300 chars)
    print(f"[RX] {text[:300]}")

    symbol = parse_symbol(text)
    if not symbol:
        print("[IGNORED] No 'Symbol <TICKER>' pattern found.")
        return

    action, reason = decide_action(symbol)
    if action is None:
        print(f"[BLOCKED] {symbol} reason={reason}")
        return

    try:
        post_to_traderspost(symbol, action)
    except Exception as e:
        print(f"[ERROR] TradersPost post failed for {symbol} {action}: {e}")

@app.on_event("startup")
async def startup_event():
    print("[BOOT] Starting Telethon listener…")
    await client.start()
    me = await client.get_me()
    print(f"[BOOT] Logged in as: {getattr(me, 'first_name', '')} (id={me.id})")
    print(f"[BOOT] Listening for messages from: {SOURCE_CHAT}")
