import os
import json
import glob
import pandas as pd


SIGNAL_LOG_DIR = os.getenv("SIGNAL_LOG_DIR", "/data/signals").strip()
SIGNAL_XLSX_FILE = os.getenv(
    "SIGNAL_XLSX_FILE", "/data/intellectia_signal_history.xlsx"
).strip()

FALLBACK_FILES = [
    "/data/intellectia_signal_log.jsonl",
    "/data/signals.jsonl",
]


def load_jsonl_file(path: str) -> list[dict]:
    rows = []
    if not os.path.exists(path):
        return rows

    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception as e:
                print(f"[WARN] Skipping bad JSON in {path} line {line_num}: {e}")
    return rows


def discover_signal_files() -> list[str]:
    files = []

    if os.path.isdir(SIGNAL_LOG_DIR):
        files.extend(sorted(glob.glob(os.path.join(SIGNAL_LOG_DIR, "*.jsonl"))))

    for fallback in FALLBACK_FILES:
        if os.path.exists(fallback):
            files.append(fallback)

    seen = set()
    unique = []
    for f in files:
        if f not in seen:
            seen.add(f)
            unique.append(f)

    return unique


def maybe_datetime_to_excel_string(series: pd.Series) -> pd.Series:
    """
    Convert any datetime-like column to timezone-free string format so Excel
    cannot choke on tz-aware values.
    """
    parsed = pd.to_datetime(series, errors="coerce", utc=True)

    # if nothing parsed, leave column alone
    if not parsed.notna().any():
        return series

    # convert to naive datetime, then to string
    parsed = parsed.dt.tz_localize(None)
    out = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")

    # preserve blanks where parsing failed
    out = out.where(parsed.notna(), None)
    return out


def normalize_rows(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.json_normalize(rows)

    # aggressively convert every column that looks datetime-like into strings
    for col in df.columns:
        try:
            df[col] = maybe_datetime_to_excel_string(df[col])
        except Exception as e:
            print(f"[WARN] Could not normalize column {col}: {e}")

    preferred_cols = [
        "kind",
        "message_id",
        "telegram_ts",
        "logged_ts",
        "source_chat",
        "incoming",
        "outgoing",
        "symbol",
        "decision",
        "reason",
        "buy_count_today",
        "raw_text",
        "webhook_status",
        "webhook_body",
    ]

    existing_preferred = [c for c in preferred_cols if c in df.columns]
    remaining = [c for c in df.columns if c not in existing_preferred]
    df = df[existing_preferred + remaining]

    sort_cols = [c for c in ["telegram_ts", "logged_ts", "message_id"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, kind="stable")

    return df


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = {
        "total_rows": [len(df)],
        "unique_symbols": [df["symbol"].nunique() if "symbol" in df.columns else 0],
        "date_min": [df["telegram_ts"].min() if "telegram_ts" in df.columns else None],
        "date_max": [df["telegram_ts"].max() if "telegram_ts" in df.columns else None],
    }
    return pd.DataFrame(summary)


def main():
    files = discover_signal_files()

    print(f"[EXPORT] SIGNAL_LOG_DIR={SIGNAL_LOG_DIR}")
    print(f"[EXPORT] Found {len(files)} candidate files")
    for f in files:
        print(f"[EXPORT] file: {f}")

    all_rows = []
    for path in files:
        rows = load_jsonl_file(path)
        print(f"[EXPORT] loaded {len(rows)} rows from {path}")
        all_rows.extend(rows)

    print(f"[EXPORT] Loaded {len(all_rows)} total rows")

    if not all_rows:
        raise SystemExit("No signal rows found to export")

    df = normalize_rows(all_rows)
    summary_df = build_summary(df)

    out_dir = os.path.dirname(SIGNAL_XLSX_FILE) or "."
    os.makedirs(out_dir, exist_ok=True)

    with pd.ExcelWriter(SIGNAL_XLSX_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="signals", index=False)
        summary_df.to_excel(writer, sheet_name="summary", index=False)

    print(f"[EXPORT] Wrote Excel file: {SIGNAL_XLSX_FILE}")


if __name__ == "__main__":
    main()
