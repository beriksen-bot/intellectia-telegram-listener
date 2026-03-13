import os
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd


SIGNAL_LOG_DIR = os.getenv("SIGNAL_LOG_DIR", "/data/signals")
OUTPUT_XLSX = os.getenv("SIGNAL_XLSX_FILE", "/data/intellectia_signal_history.xlsx")


def read_jsonl_files(signal_dir: str) -> list[dict]:
    rows: list[dict] = []
    base = Path(signal_dir)

    if not base.exists():
        print(f"No signal directory found: {signal_dir}")
        return rows

    for path in sorted(base.glob("*.jsonl")):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    row["_source_file"] = path.name
                    rows.append(row)
                except Exception as e:
                    print(f"Skipping bad line in {path.name}: {e}")

    return rows


def normalize_rows(rows: list[dict]) -> pd.DataFrame:
    normalized = []

    for r in rows:
        wp = r.get("webhook_payload") or {}
        normalized.append(
            {
                "source_file": r.get("_source_file"),
                "kind": r.get("kind"),
                "message_id": r.get("message_id"),
                "telegram_ts": r.get("telegram_ts"),
                "logged_ts": r.get("logged_ts"),
                "source_chat": r.get("source_chat"),
                "incoming": r.get("incoming"),
                "outgoing": r.get("outgoing"),
                "symbol": r.get("symbol"),
                "raw_text": r.get("raw_text"),
                "decision": r.get("decision"),
                "reason": r.get("reason"),
                "buy_count_today": r.get("buy_count_today"),
                "open_positions": ", ".join(r.get("open_positions", [])) if isinstance(r.get("open_positions"), list) else r.get("open_positions"),
                "webhook_action": wp.get("action"),
                "webhook_ticker": wp.get("ticker"),
                "webhook_quantity": wp.get("quantity"),
                "webhook_quantity_type": wp.get("quantityType"),
                "webhook_status": r.get("webhook_status"),
                "webhook_body": r.get("webhook_body"),
            }
        )

    df = pd.DataFrame(normalized)

    if not df.empty:
        df["telegram_ts"] = pd.to_datetime(df["telegram_ts"], errors="coerce")
        df["logged_ts"] = pd.to_datetime(df["logged_ts"], errors="coerce")
        df["date"] = df["telegram_ts"].dt.date
        df["time"] = df["telegram_ts"].dt.time

    return df


def build_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = []
    for day, group in df.groupby("date", dropna=True):
        out.append(
            {
                "date": day,
                "total_messages": len(group),
                "symbols_seen": group["symbol"].dropna().nunique(),
                "buy_signals": (group["decision"] == "buy").sum(),
                "sell_signals": (group["decision"] == "sell").sum(),
                "blocked_signals": group["reason"].notna().sum(),
                "blacklisted": (group["reason"] == "blacklisted").sum(),
                "outside_window": (group["reason"] == "outside_buy_window").sum(),
                "first_signal_outside_window": (group["reason"] == "first_signal_outside_buy_window").sum(),
                "max_buys_reached": (group["reason"] == "max_buys_reached").sum(),
                "no_open_position": (group["reason"] == "no_open_position").sum(),
                "disqualified_for_day": (group["reason"] == "disqualified_for_day").sum(),
            }
        )

    return pd.DataFrame(out).sort_values("date")


def build_symbol_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = []
    for symbol, group in df.groupby("symbol", dropna=True):
        out.append(
            {
                "symbol": symbol,
                "total_messages": len(group),
                "buy_signals": (group["decision"] == "buy").sum(),
                "sell_signals": (group["decision"] == "sell").sum(),
                "blocked_signals": group["reason"].notna().sum(),
                "first_seen": group["telegram_ts"].min(),
                "last_seen": group["telegram_ts"].max(),
            }
        )

    return pd.DataFrame(out).sort_values(["total_messages", "symbol"], ascending=[False, True])


def export_excel(signal_dir: str, output_xlsx: str) -> None:
    rows = read_jsonl_files(signal_dir)
    df = normalize_rows(rows)
    daily_df = build_daily_summary(df)
    symbol_df = build_symbol_summary(df)

    Path(output_xlsx).parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Signals", index=False)
        daily_df.to_excel(writer, sheet_name="Daily Summary", index=False)
        symbol_df.to_excel(writer, sheet_name="Symbol Summary", index=False)

        for sheet_name, frame in {
            "Signals": df,
            "Daily Summary": daily_df,
            "Symbol Summary": symbol_df,
        }.items():
            ws = writer.book[sheet_name]
            ws.freeze_panes = "A2"

            for col_cells in ws.columns:
                max_len = 0
                col_letter = col_cells[0].column_letter
                for cell in col_cells:
                    try:
                        val = "" if cell.value is None else str(cell.value)
                        max_len = max(max_len, len(val))
                    except Exception:
                        pass
                ws.column_dimensions[col_letter].width = min(max(max_len + 2, 12), 40)

    print(f"Excel exported to: {output_xlsx}")


if __name__ == "__main__":
    export_excel(SIGNAL_LOG_DIR, OUTPUT_XLSX)
