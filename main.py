import os
import re
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, Request

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = FastAPI()

# ===== SETTINGS =====

TRADERSPOST_WEBHOOK = os.getenv("TRADERSPOST_WEBHOOK")

MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "5"))

MT = ZoneInfo("America/Denver")

FLATTEN_HOUR = 13
FLATTEN_MINUTE = 55


# ===== STATE =====

lock = threading.Lock()

today_date = None

signal_count = {}      # ticker → number of signals today
buy_count = 0          # buys executed today
open_positions = set() # best estimate


# ===== HELPERS =====

SYMBOL_RE = re.compile(r"Symbol\s+([A-Z]{1,10})\b")


def reset_if_new_day():

    global today_date, signal_count, buy_count, open_positions

    d = datetime.now(MT).date()

    with lock:
        if today_date != d:

            today_date = d
            signal_count = {}
            buy_count = 0
            open_positions = set()

            print("[RESET] New trading day")


def send_to_traderspost(symbol, action):

    payload = {
        "ticker": symbol,
        "action": action
    }

    r = requests.post(TRADERSPOST_WEBHOOK, json=payload)

    print("[TRADERSPOST]", symbol, action, r.status_code)



def decide_action(symbol):

    global buy_count

    reset_if_new_day()

    with lock:

        count = signal_count.get(symbol, 0) + 1
        signal_count[symbol] = count

        # BUY SIGNAL
        if count % 2 == 1:

            if buy_count >= MAX_BUYS_PER_DAY:

                print("[LIMIT] Max buys reached:", MAX_BUYS_PER_DAY)

                return None

            buy_count += 1
            open_positions.add(symbol)

            return "buy"

        # SELL SIGNAL
        else:

            open_positions.discard(symbol)

            return "sell"



# ===== TELEGRAM WEBHOOK =====

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):

    update = await request.json()

    msg = update.get("message") or update.get("channel_post") or {}

    text = msg.get("text") or msg.get("caption") or ""

    print("[RX]", text)

    m = SYMBOL_RE.search(text)

    if not m:
        return {"ignored": True}

    symbol = m.group(1).upper()

    action = decide_action(symbol)

    if action is None:

        return {
            "blocked": True,
            "reason": "max buys reached"
        }

    send_to_traderspost(symbol, action)

    return {
        "symbol": symbol,
        "action": action,
        "buy_count": buy_count,
        "max_buys": MAX_BUYS_PER_DAY
    }


# ===== FAILSAFE FLATTEN =====

def flatten_all():

    reset_if_new_day()

    print("[FAILSAFE] Flatten running")

    with lock:
        symbols = list(open_positions)

    for s in symbols:
        send_to_traderspost(s, "sell")

    with lock:
        open_positions.clear()


scheduler = BackgroundScheduler(timezone=MT)

scheduler.add_job(

    flatten_all,

    CronTrigger(
        day_of_week="mon-fri",
        hour=FLATTEN_HOUR,
        minute=FLATTEN_MINUTE
    ),

    id="flatten"

)

scheduler.start()

print(f"[SCHEDULER] Flatten scheduled {FLATTEN_HOUR}:{FLATTEN_MINUTE} Utah")
