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
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()

SOURCE_CHAT = os.getenv("SOURCE_CHAT", "").strip()
TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK", "").strip()

MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "5"))

BUY_WINDOW_ENABLED = env_bool("BUY_WINDOW_ENABLED", "true")
BUY_WINDOW_TZ = ZoneInfo(os.getenv("BUY_WINDOW_TZ", "America/New_York"))
BUY_WINDOW_START = os.getenv("BUY_WINDOW_START", "12:00")
BUY_WINDOW_END = os.getenv("BUY_WINDOW_END", "16:00")

DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL = env_bool(
    "DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL", "false"
)

BLACKLIST_RAW = os.getenv("BLACKLIST", "")
BLACKLIST = {s.strip().upper() for s in BLACKLIST_RAW.split(",") if s.strip()}

TZ_NAME = os.getenv("TZ_NAME", "America/Denver")
MT = ZoneInfo(TZ_NAME)

ENABLE_FLATTEN_FAILSAFE = env_bool("ENABLE_FLATTEN_FAILSAFE", "true")
FLATTEN_HOUR = int(os.getenv("FLATTEN_HOUR", "13"))
FLATTEN_MINUTE = int(os.getenv("FLATTEN_MINUTE", "55"))

TP_TICKER_KEY = os.getenv("TP_TICKER_KEY", "ticker")
TP_ACTION_KEY = os.getenv("TP_ACTION_KEY", "action")

# Position sizing controls
ENABLE_TIMEFRAME_SIZING = env_bool("ENABLE_TIMEFRAME_SIZING", "true")
ENABLE_DAY_MULTIPLIER = env_bool("ENABLE_DAY_MULTIPLIER", "true")
DEFAULT_POSITION_SIZE = float(os.getenv("DEFAULT_POSITION_SIZE", "25"))
MAX_POSITION_SIZE = float(os.getenv("MAX_POSITION_SIZE", "100"))

# Optional persistent state
PERSIST_STATE = env_bool("PERSIST_STATE", "false")
STATE_FILE = os.getenv("STATE_FILE", "/data/trading_state.json").strip()

SYMBOL_RE = re.compile(r"\bSymbol\s+([A-Z]{1,10})\b", re.IGNORECASE)

if not (API_ID and API_HASH and SESSION_STRING):
    raise RuntimeError("Missing API_ID / API_HASH / SESSION_STRING")
if not SOURCE_CHAT:
    raise RuntimeError("Missing SOURCE_CHAT")
if not TRADERSPOST_WEBHOOK:
    raise RuntimeError("Missing TRADERSPOST_WEBHOOK")


# ============================================================
# POSITION SIZING CONFIG
# ============================================================
WINDOW_SIZES = {
    "09:30": float(os.getenv("WINDOW_0930", "30")),
    "10:00": float(os.getenv("WINDOW_1000", "28")),
    "10:30": float(os.getenv("WINDOW_1030", "26")),
    "11:00": float(os.getenv("WINDOW_1100", "24")),
    "11:30": float(os.getenv("WINDOW_1130", "22")),
    "12:00": float(os.getenv("WINDOW_1200", "20")),
    "12:30": float(os.getenv("WINDOW_1230", "18")),
    "13:00": float(os.getenv("WINDOW_1300", "16")),
    "13:30": float(os.getenv("WINDOW_1330", "14")),
    "14:00": float(os.getenv("WINDOW_1400", "12")),
}

DAY_MULTIPLIERS = {
    0: float(os.getenv("DAY_MON", "1.0")),
    1: float(os.getenv("DAY_TUE", "1.0")),
    2: float(os.getenv("DAY_WED", "1.0")),
    3: float(os.getenv("DAY_THU", "1.0")),
    4: float(os.getenv("DAY_FRI", "1.0")),
}


def get_window_size(now_time: dtime) -> float:
    if dtime(9, 30) <= now_time < dtime(10, 0):
        return WINDOW_SIZES["09:30"]
    elif dtime(10, 0) <= now_time < dtime(10, 30):
        return WINDOW_SIZES["10:00"]
    elif dtime(10, 30) <= now_time < dtime(11, 0):
        return WINDOW_SIZES["10:30"]
    elif dtime(11, 0) <= now_time < dtime(11, 30):
        return WINDOW_SIZES["11:00"]
    elif dtime(11, 30) <= now_time < dtime(12, 0):
        return WINDOW_SIZES["11:30"]
    elif dtime(12, 0) <= now_time < dtime(12, 30):
        return WINDOW_SIZES["12:00"]
    elif dtime(12, 30) <= now_time < dtime(13, 0):
        return WINDOW_SIZES["12:30"]
    elif dtime(13, 0) <= now_time < dtime(13, 30):
        return WINDOW_SIZES["13:00"]
    elif dtime(13, 30) <= now_time < dtime(14, 0):
        return WINDOW_SIZES["13:30"]
    else:
        return WINDOW_SIZES["14:00"]


def get_position_size() -> float:
    now = datetime.now(BUY_WINDOW_TZ)
    weekday = now.weekday()
    now_time = now.time()

    # If both are disabled, use default
    if not ENABLE_TIMEFRAME_SIZING and not ENABLE_DAY_MULTIPLIER:
        return min(DEFAULT_POSITION_SIZE, MAX_POSITION_SIZE)

    base_size = DEFAULT_POSITION_SIZE
    if ENABLE_TIMEFRAME_SIZING:
        base_size = get_window_size(now_time)

    multiplier = 1.0
    if ENABLE_DAY_MULTIPLIER:
        multiplier = DAY_MULTIPLIERS.get(weekday, 1.0)

    size = round(base_size * multiplier, 2)
    return min(size, MAX_POSITION_SIZE)


# ============================================================
# STATE
# ============================================================
lock = threading.Lock()

today_date_mt = None
signal_count_by_symbol = {}
buy_count_today = 0
open_positions = set()

disqualified_symbols = set()

last_event = None
last_error = None
connected = False
resolved_source = None


# ============================================================
# PERSISTENCE
# ============================================================
def _load_state() -> dict:
    if not PERSIST_STATE:
        return {"open_positions": []}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {"open_positions": []}
    except FileNotFoundError:
        return {"open_positions": []}
    except Exception as e:
        print(f"[STATE][ERROR] load failed: {e}")
        return {"open_positions": []}


def _save_state(open_positions_set: set) -> None:
    if not PERSIST_STATE:
        return

    try:
        folder = os.path.dirname(STATE_FILE) or "."
        os.makedirs(folder, exist_ok=True)

        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"open_positions": sorted(list(open_positions_set))}, f)
        os.replace(tmp, STATE_FILE)

        print(f"[STATE] Saved open_positions ({len(open_positions_set)}): {sorted(list(open_positions_set))}")
    except Exception as e:
        print(f"[STATE][ERROR] save failed: {e}")


def _sync_open_positions_from_disk() -> None:
    global open_positions
    st = _load_state()
    raw = st.get("open_positions", [])
    if not isinstance(raw, list):
        raw = []
    open_positions = {str(s).upper() for s in raw if str(s).strip()}
    print(f"[STATE] Loaded open_positions from disk ({len(open_positions)}): {sorted(list(open_positions))}")


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
    global today_date_mt, signal_count_by_symbol, buy_count_today, disqualified_symbols

    d = datetime.now(MT).date()
    with lock:
        if today_date_mt != d:
            today_date_mt = d
            signal_count_by_symbol = {}
            buy_count_today = 0
            disqualified_symbols = set()

            print("=================================")
            print(f"[RESET] New trading day ({TZ_NAME}): {today_date_mt}")
            print(f"[BUY COUNT] {buy_count_today}/{MAX_BUYS_PER_DAY}")
            print(f"[BUY WINDOW] enabled={BUY_WINDOW_ENABLED} tz={BUY_WINDOW_TZ.key} {BUY_WINDOW_START}->{BUY_WINDOW_END}")
            print(f"[DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL] {DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL}")
            print(f"[ENABLE_TIMEFRAME_SIZING] {ENABLE_TIMEFRAME_SIZING}")
            print(f"[ENABLE_DAY_MULTIPLIER] {ENABLE_DAY_MULTIPLIER}")
            print(f"[DEFAULT_POSITION_SIZE] {DEFAULT_POSITION_SIZE}")
            print(f"[MAX_POSITION_SIZE] {MAX_POSITION_SIZE}")
            if BLACKLIST:
                print(f"[BLACKLIST] {sorted(BLACKLIST)}")
            print("=================================")


def parse_symbol(text: str) -> Optional[str]:
    m = SYMBOL_RE.search(text or "")
    return m.group(1).upper() if m else None


# ============================================================
# WEBHOOK
# ============================================================
def post_to_traderspost(symbol: str, action: str) -> Tuple[Optional[int], str]:
    payload = {
        TP_TICKER_KEY: symbol,
        TP_ACTION_KEY: action,
    }

    if action == "buy":
        size = get_position_size()
        payload["size"] = size
        print(f"[POSITION SIZE] {size}%")

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


# ============================================================
# SIGNAL LOGIC
# ============================================================
def decide_action(symbol: str) -> Tuple[Optional[str], Optional[str]]:
    global buy_count_today

    reset_if_new_day()

    if symbol in BLACKLIST:
        return None, "blacklisted"

    with lock:
        if DISQUALIFY_OUTSIDE_WINDOW_FIRST_SIGNAL and symbol in disqualified_symbols:
            return None, "disqualified_for_day"

        current = signal_count_by_symbol.get(symbol, 0)

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

            signal_count_by_symbol[symbol] = next_count
            buy_count_today += 1
            open_positions.add(symbol)
            _save_state(open_positions)

            print(f"[BUY COUNT] {buy_count_today}/{MAX_BUYS_PER_DAY}")

            return "buy", None

        # SELL attempt on even counts
        if symbol not in open_positions:
            return None, "no_open_position"

        signal_count_by_symbol[symbol] = next_count
        open_positions.discard(symbol)
        _save_state(open_positions)

        print(f"[BUY COUNT] still {buy_count_today}/{MAX_BUYS_PER_DAY}")

        return "sell", None


# ============================================================
# FAILSAFE
# ============================================================
def flatten_all_open_positions() -> None:
    reset_if_new_day()

    with lock:
        symbols = sorted(open_positions)

    print(f"[FAILSAFE] Trigger @ {datetime.now(MT).isoformat()} open_positions={symbols}")

    if not symbols:
        print("[FAILSAFE] nothing open")
        return

    print(f"[FAILSAFE] closing {symbols}")

    for sym in symbols:
        post_to_traderspost(sym, "sell")

    with lock:
        open_positions.clear()
    _save_state(open_positions)

    print("[FAILSAFE] Flatten complete; state cleared and saved.")


# ============================================================
# FASTAPI
# ============================================================
app = FastAPI()


@app.get("/health")
def health():
    reset_if_new_day()
    return {
        "buy_count_today": buy_count_today,
        "open_positions": list(open_positions),
        "timeframe_sizing_enabled": ENABLE_TIMEFRAME_SIZING,
        "day_multiplier_enabled": ENABLE_DAY_MULTIPLIER,
        "default_position_size": DEFAULT_POSITION_SIZE,
        "max_position_size": MAX_POSITION_SIZE,
    }


@app.post("/flatten-now")
def flatten_now():
    flatten_all_open_positions()
    return {"ok": True}


# ============================================================
# SCHEDULER
# ============================================================
scheduler = BackgroundScheduler(timezone=MT)

if ENABLE_FLATTEN_FAILSAFE:
    scheduler.add_job(
        flatten_all_open_positions,
        CronTrigger(day_of_week="mon-fri", hour=FLATTEN_HOUR, minute=FLATTEN_MINUTE),
    )

scheduler.start()


# ============================================================
# TELEGRAM LISTENER
# ============================================================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


async def telethon_main():
    global connected, resolved_source, last_error, last_event

    print("[BOOT] starting listener")

    await client.start()
    me = await client.get_me()
    connected = True

    print(f"[BOOT] Logged in as: {getattr(me, 'first_name', '')} (id={me.id})")
    print(f"[BOOT] Listening for messages from: {SOURCE_CHAT}")

    # Load persistent state if enabled
    _sync_open_positions_from_disk()

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
        global last_event, last_error

        try:
            reset_if_new_day()

            text = (event.raw_text or "").strip()
            if not text:
                return

            last_event = text[:300]
            print(f"[RX] {text}")

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
async def startup():
    asyncio.create_task(telethon_main())


# ============================================================
# ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
