import os
import re
import json
import asyncio
from datetime import datetime
from typing import Optional, Set

import aiohttp
from fastapi import FastAPI
import uvicorn

from telethon import TelegramClient, events
from telethon.sessions import StringSession

# -----------------------
# Config helpers
# -----------------------
def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")

def parse_csv_set(value: str) -> Set[str]:
    return {x.strip().upper() for x in (value or "").split(",") if x.strip()}

# -----------------------
# Environment
# -----------------------
API_ID = int(env("API_ID"))
API_HASH = env("API_HASH")
SESSION_STRING = env("SESSION_STRING")

# DT Relay can be -100... or @username; we support both.
SOURCE_CHAT_RAW = env("SOURCE_CHAT")

TRADERSPOST_WEBHOOK = env("TRADERSPOST_WEBHOOK")

BLACKLIST = parse_csv_set(os.getenv("BLACKLIST", ""))

BUY_WINDOW_ENABLED = env_bool("BUY_WINDOW_ENABLED", "false")
BUY_WINDOW_START = os.getenv("BUY_WINDOW_START", "09:30")   # HH:MM
BUY_WINDOW_END = os.getenv("BUY_WINDOW_END", "16:00")       # HH:MM
BUY_WINDOW_TZ = os.getenv("BUY_WINDOW_TZ", "America/New_York")

MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "999999"))

# Regex for the Intellectia message format
SYMBOL_RE = re.compile(r"\bSymbol\s+([A-Z.\-]{1,10})\b", re.IGNORECASE)

# -----------------------
# State
# -----------------------
app = FastAPI()
client: Optional[TelegramClient] = None

buy_count_today = 0
buy_count_date = None  # YYYY-MM-DD in BUY_WINDOW_TZ

# -----------------------
# Time window
# -----------------------
def now_in_tz(tz_name: str) -> datetime:
    # Python 3.9+ zoneinfo is standard
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo(tz_name))

def within_buy_window() -> bool:
    if not BUY_WINDOW_ENABLED:
        return True
    n = now_in_tz(BUY_WINDOW_TZ)
    hhmm = f"{n.hour:02d}:{n.minute:02d}"
    return BUY_WINDOW_START <= hhmm <= BUY_WINDOW_END

def reset_daily_counter_if_needed():
    global buy_count_today, buy_count_date
    n = now_in_tz(BUY_WINDOW_TZ)
    d = n.date().isoformat()
    if buy_count_date != d:
        buy_count_date = d
        buy_count_today = 0

# -----------------------
# TradersPost call
# -----------------------
async def post_to_traderspost(symbol: str, raw_text: str) -> bool:
    """
    Sends a signal payload to TradersPost.
    Adjust payload keys here if your TradersPost endpoint expects something different.
    """
    payload = {
        "symbol": symbol,
        "action": "BUY",
        "source": "DT Relay (Telethon)",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "raw_text": raw_text,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(TRADERSPOST_WEBHOOK, json=payload, timeout=20) as resp:
            text = await resp.text()
            ok = 200 <= resp.status < 300
            print(f"[TRADERSPOST] status={resp.status} ok={ok} response={text[:500]}")
            return ok

# -----------------------
# Chat resolution
# -----------------------
async def resolve_chat(chat_raw: str):
    """
    Supports:
      - numeric ids (including -100... form)
      - @usernames
      - plain names (best-effort)
    """
    chat_raw = chat_raw.strip()

    # Numeric ID?
    if re.fullmatch(r"-?\d+", chat_raw):
        return int(chat_raw)

    # Username like @DT_Relay (not typical for private channels)
    if chat_raw.startswith("@"):
        return chat_raw

    # Otherwise treat as a name; Telethon can often resolve by entity
    return chat_raw

# -----------------------
# Telegram handler
# -----------------------
async def start_telethon_listener():
    global client

    print("[BOOT] Starting listener userbot...")
    print(f"[BOOT] SOURCE_CHAT={SOURCE_CHAT_RAW}")
    print(f"[BOOT] BUY_WINDOW_ENABLED={BUY_WINDOW_ENABLED} {BUY_WINDOW_START}-{BUY_WINDOW_END} {BUY_WINDOW_TZ}")
    print(f"[BOOT] BLACKLIST={sorted(list(BLACKLIST))}")
    print(f"[BOOT] MAX_BUYS_PER_DAY={MAX_BUYS_PER_DAY}")

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    me = await client.get_me()
    print(f"[BOOT] Logged in as: {getattr(me, 'first_name', '')} (id={me.id})")

    source_entity = await resolve_chat(SOURCE_CHAT_RAW)
    try:
        # Warm up / validate resolution
        resolved = await client.get_entity(source_entity)
        print(f"[RESOLVE] SOURCE_CHAT resolved: name={getattr(resolved, 'title', getattr(resolved, 'username', ''))} id={resolved.id}")
    except Exception as e:
        print(f"[ERROR] Could not resolve SOURCE_CHAT={SOURCE_CHAT_RAW}: {e}")
        raise

    @client.on(events.NewMessage(chats=source_entity))
    async def on_new_message(event):
        global buy_count_today

        try:
            text = event.raw_text or ""
            if not text.strip():
                return

            # Extract symbol
            m = SYMBOL_RE.search(text)
            if not m:
                # If DT Relay forwards the whole message, it usually contains "Symbol XXX"
                # If not, ignore silently.
                print("[SKIP] No symbol found in message.")
                return

            symbol = m.group(1).upper()

            reset_daily_counter_if_needed()

            if symbol in BLACKLIST:
                print(f"[SKIP] {symbol} is blacklisted")
                return

            if not within_buy_window():
                print(f"[SKIP] Outside buy window ({BUY_WINDOW_TZ} {BUY_WINDOW_START}-{BUY_WINDOW_END}) symbol={symbol}")
                return

            if buy_count_today >= MAX_BUYS_PER_DAY:
                print(f"[SKIP] Max buys per day reached ({buy_count_today}/{MAX_BUYS_PER_DAY})")
                return

            print(f"[SIGNAL] symbol={symbol} (buy_count_today={buy_count_today})")

            ok = await post_to_traderspost(symbol, text)
            if ok:
                buy_count_today += 1
                print(f"[OK] Sent to TradersPost. buy_count_today={buy_count_today}")
            else:
                print("[WARN] TradersPost call failed (non-2xx).")

        except Exception as e:
            print(f"[ERROR] Handler exception: {e}")

    print("[OK] Listening for messages from DT Relay...")
    # Keep running forever
    await client.run_until_disconnected()

# -----------------------
# FastAPI endpoints (health)
# -----------------------
@app.get("/health")
def health():
    reset_daily_counter_if_needed()
    return {
        "ok": True,
        "date_tz": BUY_WINDOW_TZ,
        "date": buy_count_date,
        "buy_count_today": buy_count_today,
        "max_buys_per_day": MAX_BUYS_PER_DAY,
        "buy_window": {
            "enabled": BUY_WINDOW_ENABLED,
            "start": BUY_WINDOW_START,
            "end": BUY_WINDOW_END,
            "tz": BUY_WINDOW_TZ,
        },
        "blacklist": sorted(list(BLACKLIST)),
        "source_chat": SOURCE_CHAT_RAW,
    }

@app.on_event("startup")
async def _startup():
    # Run listener in background so uvicorn can serve /health
    asyncio.create_task(start_telethon_listener())

# -----------------------
# Entrypoint
# -----------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
