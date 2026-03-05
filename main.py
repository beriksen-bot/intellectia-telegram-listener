import os
import re
import asyncio
import threading
from datetime import datetime, time
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI
import uvicorn

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from telethon import TelegramClient, events
from telethon.sessions import StringSession


app = FastAPI()

# =========================
# REQUIRED ENV VARS
# =========================
TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK", "").strip()
if not TRADERSPOST_WEBHOOK:
    raise RuntimeError("Missing env var TRADERSPOST_WEBHOOK")

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()
if not API_ID or not API_HASH or not SESSION_STRING:
    raise RuntimeError("Missing API_ID / API_HASH / SESSION_STRING")

# DT Relay chat id (recommended) or username (optional)
# Example: -1003724596299
SOURCE_CHAT = os.getenv("SOURCE_CHAT", "").strip()
if not SOURCE_CHAT:
    raise RuntimeError("Missing env var SOURCE_CHAT (set to your DT Relay chat id like -100...)")

MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "5"))

# Utah time (handles DST correctly)
MT = ZoneInfo("America/Denver")

# --- BUY WINDOW (defaults ET) ---
BUY_WINDOW_ENABLED = os.getenv("BUY_WINDOW_ENABLED", "true").lower() in ("1", "true", "yes", "y")
BUY_WINDOW_TZ = ZoneInfo(os.getenv("BUY_WINDOW_TZ", "America/New_York"))
BUY_WINDOW_START = os.getenv("BUY_WINDOW_START", "09:00")
BUY_WINDOW_END = os.getenv("BUY_WINDOW_END", "11:00")

# --- BLACKLIST ---
BLACKLIST_RAW = os.getenv("BLACKLIST", "")
BLACKLIST = {s.strip().upper() for s in BLACKLIST_RAW.split(",") if s.strip()}

# Failsafe flatten
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
signal_count_by_symbol = {}
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
    # True if now_t in [start_t, end_t), supports windows crossing midnight
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

def parse_symbol(text: str):
    m = SYMBOL_RE.search(text or "")
    return m.group(1).upper() if m else None

def post_to_traderspost(symbol: str, action: str) -> None:
    payload = {"ticker": symbol, "action": action}
    r = requests.post(TRADERSPOST_WEBHOOK, json=payload, timeout=15)
    print(f"[TRADERSPOST] {action.upper()} {symbol} -> {r.status_code} {r.text[:250]}")

def decide_action(symbol: str):
    global buy_count_today
    reset_if_new_day()

    if symbol in BLACKLIST:
        print(f"[BLOCKED] {symbol} is blacklisted — ignoring signal.")
        return None, "blacklisted"

    with lock:
        current = signal_count_by_symbol.get(symbol, 0)
        next_count = current + 1

        # Odd = BUY attempt
        if next_count % 2 == 1:
            if buy_count_today >= MAX_BUYS_PER_DAY:
                print(f"[LIMIT] Max buys reached {buy_count_today}/{MAX_BUYS_PER_DAY} — BLOCK BUY {symbol}")
                return None, "max_buys_reached"

            if not buy_window_open_now():
                now_et = datetime.now(BUY_WINDOW_TZ).strftime("%H:%M")
                print(f"[WINDOW] Outside buy window ({BUY_WINDOW_START}-{BUY_WINDOW_END} {BUY_WINDOW_TZ.key}). Now={now_et}. BLOCK BUY {symbol}")
                return None, "outside_buy_window"

            # commit BUY
            signal_count_by_symbol[symbol] = next_count
            buy_count_today += 1
            open_positions.add(symbol)
            print(f"[BUY] {symbol} | buy_count_today={buy_count_today}/{MAX_BUYS_PER_DAY}")
            return "buy", None

        # Even = SELL attempt (only if open)
        if symbol not in open_positions:
            print(f"[SELL-BLOCKED] {symbol} has no tracked open position — ignoring SELL signal.")
            return None, "no_open_position"

        # commit SELL
        signal_count_by_symbol[symbol] = next_count
        open_positions.discard(symbol)
        print(f"[SELL] {symbol} | buy_count_today still {buy_count_today}/{MAX_BUYS_PER_DAY}")
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
            "listening_to": SOURCE_CHAT,
        }

@app.post("/flatten-now")
def flatten_now():
    flatten_all_open_positions()
    return {"ok": True}

# =========================
# SCHEDULER
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
def parse_source_chat(value: str):
    # allow -100... id or @username
    v = value.strip()
    if not v:
        return None
    if v.startswith("@"):
        return v
    try:
        return int(v)
    except ValueError:
        return v  # fallback (username without @)

async def run_telethon_and_api():
    print("[BOOT] Starting Telethon listener...")
    src = parse_source_chat(SOURCE_CHAT)
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

    await client.start()
    me = await client.get_me()
    print(f"[BOOT] Logged in as: {getattr(me, 'first_name', '')} (id={me.id})")

    # Resolve source entity once
    try:
        src_entity = await client.get_entity(src)
        print(f"[RESOLVE] SOURCE_CHAT resolved: {getattr(src_entity, 'title', None) or getattr(src_entity, 'username', None) or src} (id={getattr(src_entity, 'id', 'n/a')})")
    except Exception as e:
        print(f"[ERROR] Could not resolve SOURCE_CHAT={SOURCE_CHAT}: {e}")
        raise

    @client.on(events.NewMessage(chats=src_entity))
    async def handler(event):
        text = (event.raw_text or "").strip()
        if not text:
            return

        print(f"[RX] {text[:300]}")
        symbol = parse_symbol(text)
        if not symbol:
            return

        action, reason = decide_action(symbol)
        if action is None:
            print(f"[BLOCKED] {symbol} — {reason}")
            return

        try:
            post_to_traderspost(symbol, action)
        except Exception as e:
            print(f"[ERROR] TradersPost post failed for {symbol} {action}: {e}")

    port = int(os.getenv("PORT", "8080"))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)

    print(f"[OK] Listening to Telegram chat: {SOURCE_CHAT}")
    print(f"[OK] Health endpoint on :{port}/health")

    await asyncio.gather(
        server.serve(),
        client.run_until_disconnected(),
    )

if __name__ == "__main__":
    asyncio.run(run_telethon_and_api())
