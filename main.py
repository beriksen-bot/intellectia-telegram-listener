import os
import re
import json
import asyncio
from datetime import datetime, time
from zoneinfo import ZoneInfo
from urllib import request as urlrequest
from urllib.error import URLError, HTTPError

from fastapi import FastAPI
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ==========================================================
# ENV / CONFIG
# ==========================================================
API_ID = int(os.getenv("API_ID", "0") or "0")
API_HASH = (os.getenv("API_HASH", "") or "").strip()
SESSION_STRING = (os.getenv("SESSION_STRING", "") or "").strip()

SOURCE_CHAT = (os.getenv("SOURCE_CHAT", "") or "").strip()  # DT Relay id or @username
TRADERSPOST_WEBHOOK = (os.getenv("TRADERSPOST_WEBHOOK", "") or "").strip()

MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "5"))

BUY_WINDOW_ENABLED = (os.getenv("BUY_WINDOW_ENABLED", "true").lower() in ("1", "true", "yes", "y"))
BUY_WINDOW_TZ = ZoneInfo(os.getenv("BUY_WINDOW_TZ", "America/New_York"))
BUY_WINDOW_START = os.getenv("BUY_WINDOW_START", "09:30")
BUY_WINDOW_END = os.getenv("BUY_WINDOW_END", "16:00")

BLACKLIST_RAW = os.getenv("BLACKLIST", "")
BLACKLIST = {s.strip().upper() for s in BLACKLIST_RAW.split(",") if s.strip()}

ENABLE_FLATTEN_FAILSAFE = (os.getenv("ENABLE_FLATTEN_FAILSAFE", "true").lower() in ("1", "true", "yes", "y"))
FLATTEN_HOUR = int(os.getenv("FLATTEN_HOUR", "13"))
FLATTEN_MINUTE = int(os.getenv("FLATTEN_MINUTE", "55"))
TZ_NAME = os.getenv("TZ_NAME", "America/Denver")
MT = ZoneInfo(TZ_NAME)

# Intellectia pattern:
# "Symbol FIX has a daytrading signal!"
SYMBOL_RE = re.compile(r"\bSymbol\s+([A-Z]{1,10})\b", re.IGNORECASE)

if not (API_ID and API_HASH and SESSION_STRING):
    raise RuntimeError("Missing API_ID / API_HASH / SESSION_STRING")

if not SOURCE_CHAT:
    raise RuntimeError("Missing SOURCE_CHAT (should be your DT Relay channel id)")

if not TRADERSPOST_WEBHOOK:
    raise RuntimeError("Missing TRADERSPOST_WEBHOOK")

# ==========================================================
# STATE
# ==========================================================
state = {
    "today_date_mt": None,
    "signal_count_by_symbol": {},  # {"AAPL": 1, ...}
    "buy_count_today": 0,
    "open_positions": set(),       # best-effort tracking
    "last_event": None,
    "last_error": None,
    "connected": False,
    "source_chat_resolved": None,
}

def _parse_hhmm(s: str) -> time:
    hh, mm = s.strip().split(":")
    return time(int(hh), int(mm))

BUY_START_T = _parse_hhmm(BUY_WINDOW_START)
BUY_END_T = _parse_hhmm(BUY_WINDOW_END)

def _in_time_window(now_t: time, start_t: time, end_t: time) -> bool:
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
    return _in_time_window(now_t, BUY_START_T, BUY_END_T)

def reset_if_new_day():
    d = datetime.now(MT).date()
    if state["today_date_mt"] != d:
        state["today_date_mt"] = d
        state["signal_count_by_symbol"] = {}
        state["buy_count_today"] = 0
        state["open_positions"] = set()
        print("=================================")
        print(f"[RESET] New trading day ({TZ_NAME}): {d}")
        print(f"[BUY COUNT] {state['buy_count_today']} / {MAX_BUYS_PER_DAY}")
        print(f"[BUY WINDOW] enabled={BUY_WINDOW_ENABLED} tz={BUY_WINDOW_TZ.key} {BUY_WINDOW_START}->{BUY_WINDOW_END}")
        if BLACKLIST:
            print(f"[BLACKLIST] {sorted(BLACKLIST)}")
        print("=================================")

def parse_symbol(text: str):
    m = SYMBOL_RE.search(text or "")
    return m.group(1).upper() if m else None

def post_to_traderspost(symbol: str, action: str):
    payload = {"ticker": symbol, "action": action}
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
        print(f"[TRADERSPOST][URLError] {action.upper()} {symbol} -> {e}")
        return None, str(e)

def decide_action(symbol: str):
    reset_if_new_day()

    if symbol in BLACKLIST:
        return None, "blacklisted"

    current = state["signal_count_by_symbol"].get(symbol, 0)
    next_count = current + 1

    # Odd => BUY attempt
    if next_count % 2 == 1:
        if state["buy_count_today"] >= MAX_BUYS_PER_DAY:
            return None, "max_buys_reached"
        if not buy_window_open_now():
            return None, "outside_buy_window"

        state["signal_count_by_symbol"][symbol] = next_count
        state["buy_count_today"] += 1
        state["open_positions"].add(symbol)
        return "buy", None

    # Even => SELL attempt
    if symbol not in state["open_positions"]:
        return None, "no_open_position"

    state["signal_count_by_symbol"][symbol] = next_count
    state["open_positions"].discard(symbol)
    return "sell", None

def flatten_all_open_positions():
    reset_if_new_day()
    symbols = sorted(state["open_positions"])
    if not symbols:
        print("[FAILSAFE] No open positions to flatten.")
        return

    print(f"[FAILSAFE] Flattening {len(symbols)} positions: {symbols}")
    for sym in symbols:
        post_to_traderspost(sym, "sell")
    state["open_positions"] = set()

# ==========================================================
# FASTAPI (so Railway has an HTTP service)
# ==========================================================
app = FastAPI()

@app.get("/")
def root():
    return {
        "ok": True,
        "date_mt": str(state["today_date_mt"]),
        "buy_count_today": state["buy_count_today"],
        "max_buys_per_day": MAX_BUYS_PER_DAY,
        "open_positions": sorted(list(state["open_positions"])),
        "blacklist": sorted(list(BLACKLIST)),
        "buy_window": {
            "enabled": BUY_WINDOW_ENABLED,
            "tz": BUY_WINDOW_TZ.key,
            "start": BUY_WINDOW_START,
            "end": BUY_WINDOW_END
        },
        "connected": state["connected"],
        "source_chat_resolved": state["source_chat_resolved"],
        "last_event": state["last_event"],
        "last_error": state["last_error"],
    }

# ==========================================================
# TELETHON WORKER
# ==========================================================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

async def resolve_source_entity():
    # Preload dialogs so numeric -100... ids resolve reliably
    await client.get_dialogs()

    # If SOURCE_CHAT is numeric, use int
    ent = None
    if SOURCE_CHAT.lstrip("-").isdigit():
        target_id = int(SOURCE_CHAT)
        for d in await client.get_dialogs():
            if getattr(d, "id", None) == target_id:
                ent = d.entity
                state["source_chat_resolved"] = f"matched dialog cache: name={d.name} id={d.id}"
                break
        if ent is None:
            # Last try: get_entity on the int id
            ent = await client.get_entity(target_id)
            state["source_chat_resolved"] = f"get_entity(int): {target_id}"
    else:
        ent = await client.get_entity(SOURCE_CHAT)
        state["source_chat_resolved"] = f"get_entity(str): {SOURCE_CHAT}"
    return ent

@client.on(events.NewMessage)
async def handler(event):
    try:
        # Only accept messages from SOURCE_CHAT
        if event.chat_id != handler.source_chat_id:
            return

        text = (event.raw_text or "").strip()
        if not text:
            return

        sym = parse_symbol(text)
        if not sym:
            return

        action, reason = decide_action(sym)
        state["last_event"] = {
            "ts": datetime.now(MT).isoformat(),
            "symbol": sym,
            "action": action,
            "reason": reason,
            "sample": text[:120],
        }

        if not action:
            print(f"[SKIP] {sym} reason={reason}")
            return

        print(f"[SIGNAL] {sym} -> {action.upper()}")
        post_to_traderspost(sym, action)

    except Exception as e:
        state["last_error"] = f"{type(e).__name__}: {e}"
        print(f"[ERROR] handler: {type(e).__name__}: {e}")

async def telethon_main():
    await client.start()
    me = await client.get_me()
    state["connected"] = True
    print(f"[BOOT] Logged in as: {getattr(me, 'first_name', '')} (id={me.id})")

    # Resolve source chat entity and lock handler chat id
    ent = await resolve_source_entity()
    handler.source_chat_id = getattr(ent, "id", None)
    print(f"[OK] Listening for messages from SOURCE_CHAT={SOURCE_CHAT} (resolved chat_id={handler.source_chat_id})")

    # Failsafe scheduler (flatten at MT time)
    async def scheduler_loop():
        while True:
            try:
                reset_if_new_day()
                if ENABLE_FLATTEN_FAILSAFE:
                    now = datetime.now(MT)
                    if now.hour == FLATTEN_HOUR and now.minute == FLATTEN_MINUTE and now.second < 5:
                        flatten_all_open_positions()
                await asyncio.sleep(1)
            except Exception as e:
                state["last_error"] = f"{type(e).__name__}: {e}"
                await asyncio.sleep(2)

    asyncio.create_task(scheduler_loop())

    # Run forever
    await client.run_until_disconnected()

@app.on_event("startup")
async def startup():
    # Start Telethon in background
    asyncio.create_task(telethon_main())
