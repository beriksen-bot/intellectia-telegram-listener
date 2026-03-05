import os
import re
import json
import asyncio
import threading
from datetime import datetime, time as dtime
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
from urllib import request as urlrequest
from urllib.error import URLError, HTTPError

from fastapi import FastAPI
import uvicorn

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


# ============================================================
# ENV / CONFIG
# ============================================================
def env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")


API_ID = int(os.getenv("API_ID", "0") or "0")
API_HASH = (os.getenv("API_HASH", "") or "").strip()
SESSION_STRING = (os.getenv("SESSION_STRING", "") or "").strip()

SOURCE_CHAT = (os.getenv("SOURCE_CHAT", "") or "").strip()  # e.g. @intellectia_1_bot_bot
TRADERSPOST_WEBHOOK = (os.getenv("TRADERSPOST_WEBHOOK", "") or "").strip()

MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "5"))

# Buy window (ET by default)
BUY_WINDOW_ENABLED = env_bool("BUY_WINDOW_ENABLED", "true")
BUY_WINDOW_TZ = ZoneInfo(os.getenv("BUY_WINDOW_TZ", "America/New_York"))
BUY_WINDOW_START = os.getenv("BUY_WINDOW_START", "12:00")  # HH:MM
BUY_WINDOW_END = os.getenv("BUY_WINDOW_END", "16:00")      # HH:MM

# Option B toggle (the new feature)
DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL = env_bool("DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL", "false")

# Blacklist
BLACKLIST_RAW = os.getenv("BLACKLIST", "")
BLACKLIST = {s.strip().upper() for s in BLACKLIST_RAW.split(",") if s.strip()}

# Failsafe flatten (Utah time)
TZ_NAME = os.getenv("TZ_NAME", "America/Denver")
MT = ZoneInfo(TZ_NAME)
ENABLE_FLATTEN_FAILSAFE = env_bool("ENABLE_FLATTEN_FAILSAFE", "true")
FLATTEN_HOUR = int(os.getenv("FLATTEN_HOUR", "13"))
FLATTEN_MINUTE = int(os.getenv("FLATTEN_MINUTE", "55"))

# TradersPost payload keys (normally ticker/action)
TP_TICKER_KEY = os.getenv("TP_TICKER_KEY", "ticker")
TP_ACTION_KEY = os.getenv("TP_ACTION_KEY", "action")

# Intellectia format: "Symbol XYZ has a daytrading signal!"
SYMBOL_RE = re.compile(r"\bSymbol\s+([A-Z]{1,10})\b", re.IGNORECASE)

# Basic required checks
if not (API_ID and API_HASH and SESSION_STRING):
    raise RuntimeError("Missing API_ID / API_HASH / SESSION_STRING")
if not SOURCE_CHAT:
    raise RuntimeError("Missing SOURCE_CHAT (e.g. @intellectia_1_bot_bot)")
if not TRADERSPOST_WEBHOOK:
    raise RuntimeError("Missing TRADERSPOST_WEBHOOK")


# ============================================================
# STATE (in-memory)
# ============================================================
lock = threading.Lock()

today_date_mt = None  # MT date
signal_count_by_symbol = {}  # per-day per-symbol counter (for BUY/SELL toggling)
buy_count_today = 0
open_positions = set()       # best-effort: track symbols we bought

# Option B state
disqualified_symbols = set()  # symbols ignored for the rest of the day

# Debug state
last_event = None
last_error = None
connected = False
resolved_source = None


# ============================================================
# HELPERS
# ============================================================
def parse_hhmm(s: str) -> dtime:
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))


BUY_START_T = parse_hhmm(BUY_WINDOW_START)
BUY_END_T = parse_hhmm(BUY_WINDOW_END)


def in_time_window(now_t: dtime, start_t: dtime, end_t: dtime) -> bool:
    """True if now_t in [start_t, end_t). Supports windows crossing midnight."""
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
    global today_date_mt, signal_count_by_symbol, buy_count_today, open_positions, disqualified_symbols

    d = datetime.now(MT).date()
    with lock:
        if today_date_mt != d:
            today_date_mt = d
            signal_count_by_symbol = {}
            buy_count_today = 0
            open_positions = set()
            disqualified_symbols = set()

            print("=================================")
            print(f"[RESET] New trading day ({TZ_NAME}): {today_date_mt}")
            print(f"[BUY COUNT] {buy_count_today} / {MAX_BUYS_PER_DAY}")
            print(f"[BUY WINDOW] enabled={BUY_WINDOW_ENABLED} tz={BUY_WINDOW_TZ.key} {BUY_WINDOW_START}->{BUY_WINDOW_END}")
            print(f"[DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL] {DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL}")
            if BLACKLIST:
                print(f"[BLACKLIST] {sorted(BLACKLIST)}")
            print("=================================")


def parse_symbol(text: str) -> Optional[str]:
    m = SYMBOL_RE.search(text or "")
    return m.group(1).upper() if m else None


def post_to_traderspost(symbol: str, action: str) -> Tuple[Optional[int], str]:
    payload = {TP_TICKER_KEY: symbol, TP_ACTION_KEY: action}
    data = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        TRADERSPOST_WEBHOOK,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlrequest.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            print(f"[TRADERSPOST] {action.upper()} {symbol} -> {resp.status} {body[:250]}")
            return resp.status, body
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore") if hasattr(e, "read") else str(e)
        print(f"[TRADERSPOST][HTTPError] {action.upper()} {symbol} -> {e.code} {body[:250]}")
        return e.code, body
    except URLError as e:
        msg = str(e)
        print(f"[TRADERSPOST][URLError] {action.upper()} {symbol} -> {msg}")
        return None, msg


def decide_action(symbol: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Logic:
      - Ignore blacklist always
      - Option B: If first sighting of symbol is outside buy window -> disqualify symbol all day
      - Odd count = BUY attempt (subject to window + max buys/day)
      - Even count = SELL attempt (only if we tracked open)
    """
    global buy_count_today

    reset_if_new_day()

    if symbol in BLACKLIST:
        return None, "blacklisted"

    with lock:
        # Option B guard (new)
        if DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL and symbol in disqualified_symbols:
            return None, "disqualified_for_day"

        current = signal_count_by_symbol.get(symbol, 0)

        # Option B: first ever message for this symbol arrives outside buy window → disqualify for day
        if DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL and current == 0 and not buy_window_open_now():
            disqualified_symbols.add(symbol)
            return None, "first_signal_outside_buy_window"

        next_count = current + 1

        # BUY attempt on odd counts
        if next_count % 2 == 1:
            if buy_count_today >= MAX_BUYS_PER_DAY:
                return None, "max_buys_reached"

            if not buy_window_open_now():
                return None, "outside_buy_window"

            # commit BUY
            signal_count_by_symbol[symbol] = next_count
            buy_count_today += 1
            open_positions.add(symbol)
            return "buy", None

        # SELL attempt on even counts
        if symbol not in open_positions:
            return None, "no_open_position"

        # commit SELL
        signal_count_by_symbol[symbol] = next_count
        open_positions.discard(symbol)
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
        post_to_traderspost(sym, "sell")

    with lock:
        open_positions.clear()


# ============================================================
# FastAPI endpoints
# ============================================================
app = FastAPI()


@app.get("/health")
def health():
    reset_if_new_day()
    with lock:
        return {
            "ok": True,
            "connected": connected,
            "resolved_source": resolved_source,
            "date_mt": str(today_date_mt),
            "buy_count_today": buy_count_today,
            "max_buys_per_day": MAX_BUYS_PER_DAY,
            "open_positions": sorted(list(open_positions)),
            "disqualified_symbols": sorted(list(disqualified_symbols)),
            "blacklist": sorted(list(BLACKLIST)),
            "buy_window": {
                "enabled": BUY_WINDOW_ENABLED,
                "tz": BUY_WINDOW_TZ.key,
                "start": BUY_WINDOW_START,
                "end": BUY_WINDOW_END,
            },
            "disqualify_outside_window_first_signal": DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL,
            "last_event": last_event,
            "last_error": last_error,
        }


@app.post("/flatten-now")
def flatten_now():
    flatten_all_open_positions()
    return {"ok": True}


# ============================================================
# Scheduler
# ============================================================
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


# ============================================================
# Telethon listener
# ============================================================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


async def telethon_main():
    global connected, resolved_source, last_error, last_event

    print("[BOOT] Starting Telethon listener…")
    await client.start()
    me = await client.get_me()
    connected = True
    print(f"[BOOT] Logged in as: {getattr(me, 'first_name', '')} (id={me.id})")
    print(f"[BOOT] Listening for messages from: {SOURCE_CHAT}")

    try:
        entity = await client.get_entity(SOURCE_CHAT)
        resolved_source = f"id={getattr(entity, 'id', None)} name={getattr(entity, 'title', None) or getattr(entity, 'username', None) or SOURCE_CHAT}"
        print(f"[RESOLVE] SOURCE_CHAT resolved: {resolved_source}")
    except Exception as e:
        last_error = f"resolve_failed: {e}"
        print(f"[ERROR] Could not resolve SOURCE_CHAT={SOURCE_CHAT}: {e}")
        raise

    @client.on(events.NewMessage(chats=entity))
    async def on_message(event):
        nonlocal entity
        global last_event, last_error

        try:
            reset_if_new_day()

            text = (event.raw_text or "").strip()
            if not text:
                return

            # Store a short version for /health
            last_event = text[:300]

            # Print receive log
            print(f"[RX] {text[:400]}")

            symbol = parse_symbol(text)
            if not symbol:
                return

            action, reason = decide_action(symbol)

            if action is None:
                if reason in ("outside_buy_window", "first_signal_outside_buy_window"):
                    now_et = datetime.now(BUY_WINDOW_TZ).strftime("%H:%M")
                    print(f"[WINDOW] Outside buy window ({BUY_WINDOW_START}-{BUY_WINDOW_END} {BUY_WINDOW_TZ.key}). Now={now_et}. BLOCK {symbol} ({reason})")
                else:
                    print(f"[BLOCKED] {symbol} reason={reason}")
                return

            print(f"[SIGNAL] {symbol} -> {action.upper()}")
            post_to_traderspost(symbol, action)

        except Exception as e:
            last_error = str(e)
            print(f"[ERROR] handler: {e}")

    await client.run_until_disconnected()


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(telethon_main())


# ============================================================
# Entrypoint
# ============================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
