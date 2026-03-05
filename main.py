import os
import re
import threading
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from typing import Optional, Tuple

import requests
from fastapi import FastAPI, Query
from telethon import TelegramClient, events
from telethon.sessions import StringSession

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ============================================================
# CONFIG (env)
# ============================================================
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()

TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK", "").strip()
if not TRADERSPOST_WEBHOOK:
    raise RuntimeError("Missing env var TRADERSPOST_WEBHOOK")

SOURCE_CHAT = os.getenv("SOURCE_CHAT", "").strip()
if not SOURCE_CHAT:
    raise RuntimeError("Missing env var SOURCE_CHAT (chat id like -100..., or @username)")

MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "5"))

# Trading timezones
MT = ZoneInfo(os.getenv("TZ_NAME", "America/Denver"))

BUY_WINDOW_ENABLED = os.getenv("BUY_WINDOW_ENABLED", "true").lower() in ("1", "true", "yes", "y")
BUY_WINDOW_TZ = ZoneInfo(os.getenv("BUY_WINDOW_TZ", "America/New_York"))
BUY_WINDOW_START = os.getenv("BUY_WINDOW_START", "09:00")
BUY_WINDOW_END = os.getenv("BUY_WINDOW_END", "11:00")

# Blacklist: comma-separated tickers
BLACKLIST_RAW = os.getenv("BLACKLIST", "")
BLACKLIST = {s.strip().upper() for s in BLACKLIST_RAW.split(",") if s.strip()}

# Failsafe flatten
ENABLE_FLATTEN_FAILSAFE = os.getenv("ENABLE_FLATTEN_FAILSAFE", "true").lower() in ("1", "true", "yes", "y")
FLATTEN_HOUR = int(os.getenv("FLATTEN_HOUR", "13"))
FLATTEN_MINUTE = int(os.getenv("FLATTEN_MINUTE", "55"))

# TradersPost payload keys (keep defaults unless you know TradersPost expects different keys)
TP_TICKER_KEY = os.getenv("TP_TICKER_KEY", "ticker")
TP_ACTION_KEY = os.getenv("TP_ACTION_KEY", "action")

# Intellectia message formats you showed:
# "Symbol FIX has a daytrading signal!"
SYMBOL_RE = re.compile(r"\bSymbol\s+([A-Z]{1,10})\b", re.IGNORECASE)

# ============================================================
# STATE (in-memory)
# ============================================================
lock = threading.Lock()
today_date = None  # MT date
signal_count_by_symbol = {}   # per symbol counter for the day
buy_count_today = 0           # number of BUYs sent today
open_positions = set()        # best-effort tracking (for SELL and failsafe)

last_event = None
last_error = None

# ============================================================
# HELPERS
# ============================================================
def parse_hhmm(s: str) -> dtime:
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))

BUY_START_T = parse_hhmm(BUY_WINDOW_START)
BUY_END_T = parse_hhmm(BUY_WINDOW_END)

def in_time_window(now_t: dtime, start_t: dtime, end_t: dtime) -> bool:
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
            print(f"[RESET] New trading day ({MT.key}): {today_date}")
            print(f"[BUY COUNT] {buy_count_today} / {MAX_BUYS_PER_DAY}")
            print(f"[BUY WINDOW] enabled={BUY_WINDOW_ENABLED} tz={BUY_WINDOW_TZ.key} {BUY_WINDOW_START}->{BUY_WINDOW_END}")
            if BLACKLIST:
                print(f"[BLACKLIST] {sorted(BLACKLIST)}")
            print("=================================")

def parse_symbol(text: str) -> Optional[str]:
    m = SYMBOL_RE.search(text or "")
    return m.group(1).upper() if m else None

def post_to_traderspost(symbol: str, action: str) -> None:
    payload = {TP_TICKER_KEY: symbol, TP_ACTION_KEY: action}
    r = requests.post(TRADERSPOST_WEBHOOK, json=payload, timeout=20)
    print(f"[TRADERSPOST] {action.upper()} {symbol} -> {r.status_code} {r.text[:250]}")

def decide_action(symbol: str) -> Tuple[Optional[str], Optional[str]]:
    """
    - Ignore blacklisted symbols entirely.
    - Odd signal count => BUY (subject to buy-window + max buys)
    - Even signal count => SELL (only if we tracked it open)
    """
    global buy_count_today

    reset_if_new_day()

    if symbol in BLACKLIST:
        print(f"[BLOCKED] {symbol} is blacklisted — ignoring.")
        return None, "blacklisted"

    with lock:
        current = signal_count_by_symbol.get(symbol, 0)
        next_count = current + 1

        # BUY attempt on odd counts
        if next_count % 2 == 1:
            if buy_count_today >= MAX_BUYS_PER_DAY:
                print(f"[LIMIT] Max buys reached {buy_count_today}/{MAX_BUYS_PER_DAY} — BLOCK BUY {symbol}")
                return None, "max_buys_reached"

            if not buy_window_open_now():
                now_et = datetime.now(BUY_WINDOW_TZ).strftime("%H:%M")
                print(f"[WINDOW] Outside BUY window ({BUY_WINDOW_START}-{BUY_WINDOW_END} {BUY_WINDOW_TZ.key}). Now={now_et}. BLOCK BUY {symbol}")
                return None, "outside_buy_window"

            # commit BUY
            signal_count_by_symbol[symbol] = next_count
            buy_count_today += 1
            open_positions.add(symbol)
            print(f"[BUY] {symbol} | buy_count_today={buy_count_today}/{MAX_BUYS_PER_DAY}")
            return "buy", None

        # SELL attempt on even counts
        if symbol not in open_positions:
            print(f"[SELL-BLOCKED] {symbol} not in open_positions — ignoring SELL signal to avoid drift.")
            return None, "no_open_position"

        # commit SELL
        signal_count_by_symbol[symbol] = next_count
        open_positions.discard(symbol)
        print(f"[SELL] {symbol} | buy_count_today={buy_count_today}/{MAX_BUYS_PER_DAY}")
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

# ============================================================
# FASTAPI
# ============================================================
app = FastAPI()

@app.get("/")
def root():
    return {"ok": True}

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
            "last_event": last_event,
            "last_error": last_error,
        }

@app.post("/flatten-now")
def flatten_now():
    flatten_all_open_positions()
    return {"ok": True}

@app.post("/test-buy")
def test_buy(ticker: str = Query(..., min_length=1, max_length=10)):
    sym = ticker.upper()
    post_to_traderspost(sym, "buy")
    return {"ok": True, "sent": {"ticker": sym, "action": "buy"}}

@app.post("/test-sell")
def test_sell(ticker: str = Query(..., min_length=1, max_length=10)):
    sym = ticker.upper()
    post_to_traderspost(sym, "sell")
    return {"ok": True, "sent": {"ticker": sym, "action": "sell"}}

# ============================================================
# SCHEDULER
# ============================================================
scheduler = BackgroundScheduler(timezone=MT)
if ENABLE_FLATTEN_FAILSAFE:
    scheduler.add_job(
        flatten_all_open_positions,
        CronTrigger(day_of_week="mon-fri", hour=FLATTEN_HOUR, minute=FLATTEN_MINUTE),
        id="flatten_failsafe",
        replace_existing=True,
    )
    print(f"[SCHEDULER] Failsafe flatten ENABLED at {FLATTEN_HOUR:02d}:{FLATTEN_MINUTE:02d} {MT.key} (Mon–Fri)")
else:
    print("[SCHEDULER] Failsafe flatten DISABLED")
scheduler.start()

# ============================================================
# TELETHON RUNNER
# ============================================================
client: Optional[TelegramClient] = None
resolved_source_id: Optional[int] = None

async def start_telethon():
    global client, resolved_source_id, last_error

    if not (API_ID and API_HASH and SESSION_STRING):
        raise RuntimeError("Missing API_ID/API_HASH/SESSION_STRING")

    print("[BOOT] Starting Telethon userbot...")
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

    await client.start()
    me = await client.get_me()
    print(f"[BOOT] Logged in as: {getattr(me, 'first_name', '')} (id={me.id})")
    print(f"[BOOT] SOURCE_CHAT={SOURCE_CHAT}")

    # Resolve source chat
    try:
        entity = await client.get_entity(SOURCE_CHAT)
        resolved_source_id = getattr(entity, "id", None)
        print(f"[RESOLVE] SOURCE_CHAT resolved: name={getattr(entity, 'title', getattr(entity, 'username', ''))!s} id={resolved_source_id}")
    except Exception as e:
        last_error = f"resolve_source_failed: {e}"
        print(f"[ERROR] Could not resolve SOURCE_CHAT={SOURCE_CHAT}: {e}")
        raise

    @client.on(events.NewMessage(chats=entity))
    async def handler(event):
        global last_event, last_error

        try:
            reset_if_new_day()

            text = (event.raw_text or "").strip()
            last_event = text[:300]
            print(f"[RX] {text[:400]}")

            symbol = parse_symbol(text)
            if not symbol:
                print("[IGNORE] No 'Symbol <TICKER>' found.")
                return

            action, reason = decide_action(symbol)
            if action is None:
                print(f"[BLOCKED] {symbol}: {reason}")
                return

            post_to_traderspost(symbol, action)

        except Exception as e:
            last_error = str(e)
            print(f"[ERROR] handler failed: {e}")

    print("[OK] Telethon listening…")

@app.on_event("startup")
async def on_startup():
    # Start Telethon in the same event loop FastAPI/Uvicorn uses.
    await start_telethon()
