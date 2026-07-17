"""
vn_stock_price_emailer.py

Fetches prices for a watchlist of Vietnam-listed stocks (HOSE/HNX/UPCOM),
plus the major indices (VN-Index, HNX-Index, UPCOM-Index), from two
independent free/no-key public sources, and emails a daily summary.
Designed to run on GitHub Actions (see .github/workflows/send-stock-price.yml)
or locally via cron. No local computer needs to stay on.

Modeled on the same pattern as currency-rate-emailer / gold-price-emailer /
tech-price-mailer: multiple independent sources, each rendered as its own
section, each degrades gracefully if it fails on a given run.

Data sources:

1. TCBS public price-history feed (apipubaws.tcbs.com.vn) - used for the
   latest daily close, previous close, volume, and index levels. This is
   the same public feed the `vnstock` Python package wraps; it is not an
   official/documented API, so treat it as best-effort and expect it may
   change shape or rate-limit without notice.
2. VNDirect public quotes feed (finfo-api.vndirect.com.vn) - independent
   second source for latest close + daily change, used for a cross-check.

Both are undocumented public JSON endpoints that happen to back TCBS's and
VNDirect's own web/mobile apps, not stable published APIs - the same
caveat the Vietcombank source in currency-rate-emailer carries. If either
one goes down or changes shape, the run should still complete and email
using whichever source(s) succeeded; this script is written defensively
for that. Swap in a proper vendor (SSI FastConnect, a paid VN data API,
etc.) if you need contractual reliability.

Extra features (matching the sibling emailers):

- Daily % change per stock, with an UP/DOWN/FLAT arrow
- Top gainers / top losers within the watchlist
- Cross-source discrepancy alert: flags tickers where TCBS and VNDirect
  closes disagree by more than a threshold
- Market index snapshot: VN-Index, HNX-Index, UPCOM-Index
- Historical tracking + weekly trend: logs every run to price_history.csv
  and emails a 7-day % change summary once a week
- Move-threshold alerting: only send if some stock moved >= X% since the
  last run (optional)

Usage:
    python vn_stock_price_emailer.py generate   # fetch prices, build email body -> email_body.txt
    python vn_stock_price_emailer.py send       # send email_body.txt via SMTP

Required environment variables (set as GitHub Actions secrets, or export locally):
    GMAIL_ADDRESS       - sender gmail address
    GMAIL_APP_PASSWORD  - Gmail App Password (not your normal password)
    STOCK_RECIPIENT     - recipient email address

Optional environment variables:
    WATCHLIST                      - comma-separated tickers, default below
    ALERT_THRESHOLD_PERCENT        - only send if some stock moved >= this % since last run
                                      (leave unset to always send)
    DISCREPANCY_THRESHOLD_PERCENT  - flag a ticker if sources disagree by >= this % (default 1.0)
"""

import os
import sys
import csv
import json
import time
import smtplib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from email.mime.text import MIMEText

import requests

# --- Config -------------------------------------------------------------

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def now_vn():
    """Current time in Vietnam (UTC+7), regardless of the runner's local timezone."""
    return datetime.now(VN_TZ)


DEFAULT_WATCHLIST = [
    "VNM", "VIC", "VHM", "HPG", "FPT", "MWG", "VCB", "TCB", "MBB", "SSI",
]


def _env(name, default=None):
    """os.environ.get, but treats an unset-but-present GitHub Actions
    variable (which comes through as an empty string, not a missing key)
    the same as truly unset.
    """
    val = os.environ.get(name)
    if val is None or val.strip() == "":
        return default
    return val


WATCHLIST = _env("WATCHLIST", ",".join(DEFAULT_WATCHLIST)).split(",")
WATCHLIST = [t.strip().upper() for t in WATCHLIST if t.strip()]

INDICES = [
    ("VNINDEX", "VN-Index"),
    ("HNXINDEX", "HNX-Index"),
    ("UPCOMINDEX", "UPCOM-Index"),
]

TCBS_BARS_URL = "https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term"
VNDIRECT_QUOTES_URL = "https://finfo-api.vndirect.com.vn/v4/stock_prices"

SOURCES = [
    ("TCBS", "https://tcinvest.tcbs.com.vn/"),
    ("VNDirect", "https://dstock.vndirect.com.vn/"),
]

EMAIL_BODY_FILE = "email_body.txt"
STATE_FILE = "last_prices.json"
HISTORY_FILE = "price_history.csv"

ALERT_THRESHOLD_PERCENT = _env("ALERT_THRESHOLD_PERCENT")
ALERT_THRESHOLD_PERCENT = float(ALERT_THRESHOLD_PERCENT) if ALERT_THRESHOLD_PERCENT else None

DISCREPANCY_THRESHOLD_PERCENT = float(_env("DISCREPANCY_THRESHOLD_PERCENT", "1.0"))

GMAIL_ADDRESS = _env("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = _env("GMAIL_APP_PASSWORD")
STOCK_RECIPIENT = _env("STOCK_RECIPIENT")

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

HEADERS = {
    # Several of these hosts block the bare default "python-requests/x.y"
    # User-Agent, so we look like an ordinary browser instead.
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# --- Fetch: TCBS ----------------------------------------------------------


def _fetch_tcbs_bars(ticker, asset_type, days_back=14):
    """Returns a list of daily bars (oldest -> newest) for one ticker/index
    from TCBS's public bars feed: [{tradingDate, close, volume}, ...]
    """
    to_ts = int(time.time())
    from_ts = to_ts - days_back * 86400
    params = {
        "ticker": ticker,
        "type": asset_type,  # "stock" or "index"
        "resolution": "D",
        "from": from_ts,
        "to": to_ts,
    }
    resp = requests.get(TCBS_BARS_URL, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    bars = data.get("data") or []
    bars.sort(key=lambda b: b.get("tradingDate", ""))
    return bars


def fetch_tcbs_stock_prices():
    """Returns {ticker: {"close": VND, "prev_close": VND, "volume": int}}.
    TCBS bars come back in *thousand VND* for stock prices (e.g. 85.5 == 85,500 VND),
    so we scale by 1000 to get plain VND like the other emailers use for currency.
    """
    prices = {}
    for ticker in WATCHLIST:
        try:
            bars = _fetch_tcbs_bars(ticker, "stock")
            if len(bars) < 1:
                continue
            latest = bars[-1]
            prev = bars[-2] if len(bars) >= 2 else None
            prices[ticker] = {
                "close": float(latest["close"]) * 1000,
                "prev_close": float(prev["close"]) * 1000 if prev else None,
                "volume": int(latest.get("volume") or 0),
            }
        except Exception as e:
            print(f"TCBS fetch failed for {ticker}: {e}")
            continue
    return prices


def fetch_tcbs_indices():
    """Returns {index_name: {"close": pts, "prev_close": pts}} for VN-Index etc."""
    indices = {}
    for code, label in INDICES:
        try:
            bars = _fetch_tcbs_bars(code, "index")
            if len(bars) < 1:
                continue
            latest = bars[-1]
            prev = bars[-2] if len(bars) >= 2 else None
            indices[label] = {
                "close": float(latest["close"]),
                "prev_close": float(prev["close"]) if prev else None,
            }
        except Exception as e:
            print(f"TCBS index fetch failed for {label}: {e}")
            continue
    return indices


# --- Fetch: VNDirect --------------------------------------------------------


def fetch_vndirect_prices():
    """Returns {ticker: {"close": VND, "prev_close": VND}} from VNDirect's
    public quotes feed, used as an independent cross-check on TCBS.
    VNDirect closes are already in plain VND.
    """
    prices = {}
    codes = ",".join(WATCHLIST)
    params = {
        "sort": "date",
        "q": f"code:{codes}",
        "size": len(WATCHLIST) * 2,
    }
    try:
        resp = requests.get(VNDIRECT_QUOTES_URL, headers=HEADERS, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data") or []
        by_ticker = {}
        for row in rows:
            code = row.get("code")
            if code not in WATCHLIST:
                continue
            by_ticker.setdefault(code, []).append(row)
        for code, rows_for_code in by_ticker.items():
            rows_for_code.sort(key=lambda r: r.get("date", ""), reverse=True)
            latest = rows_for_code[0]
            prev = rows_for_code[1] if len(rows_for_code) >= 2 else None
            close = latest.get("close") or latest.get("adClose")
            if close is None:
                continue
            prices[code] = {
                "close": float(close),
                "prev_close": float(prev["close"]) if prev and prev.get("close") else None,
            }
    except Exception as e:
        print(f"VNDirect fetch failed: {e}")
        raise
    return prices


# --- State (for % change + threshold) --------------------------------------


def load_previous_prices():
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE) as f:
        return json.load(f)


def save_prices(prices):
    flat = {ticker: vals["close"] for ticker, vals in prices.items()}
    with open(STATE_FILE, "w") as f:
        json.dump(flat, f)


def should_send(prices, previous_prices):
    if ALERT_THRESHOLD_PERCENT is None or previous_prices is None:
        return True
    for ticker, vals in prices.items():
        if ticker in previous_prices and previous_prices[ticker]:
            pct = abs((vals["close"] - previous_prices[ticker]) / previous_prices[ticker] * 100)
            if pct >= ALERT_THRESHOLD_PERCENT:
                return True
    return False


# --- Historical tracking + weekly trend -------------------------------------


def append_history(prices):
    """Appends this run's TCBS closes to a CSV: timestamp,ticker,close"""
    is_new_file = not os.path.exists(HISTORY_FILE)
    with open(HISTORY_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if is_new_file:
            writer.writerow(["timestamp", "ticker", "close"])
        ts = now_vn().strftime("%Y-%m-%d %H:%M")
        for ticker, vals in prices.items():
            writer.writerow([ts, ticker, vals["close"]])


def weekly_trend_section():
    """Once a week (first run after midnight Monday, Vietnam time), compares
    today's close to the close from ~7 days ago and returns a summary section,
    or None if it's not time yet / there's not enough history.
    """
    vn_now = now_vn()
    is_weekly_slot = vn_now.weekday() == 0 and vn_now.hour == 0  # Monday, 00:xx
    if not is_weekly_slot or not os.path.exists(HISTORY_FILE):
        return None

    cutoff = vn_now - timedelta(days=7)
    oldest_near_cutoff = {}  # ticker -> (timestamp, close) closest to 7 days ago
    latest = {}  # ticker -> (timestamp, close) most recent

    with open(HISTORY_FILE) as f:
        for row in csv.DictReader(f):
            try:
                ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M").replace(tzinfo=VN_TZ)
                close = float(row["close"])
            except (ValueError, KeyError):
                continue
            ticker = row["ticker"]

            if ticker not in latest or ts > latest[ticker][0]:
                latest[ticker] = (ts, close)

            if ts <= cutoff and (ticker not in oldest_near_cutoff or ts > oldest_near_cutoff[ticker][0]):
                oldest_near_cutoff[ticker] = (ts, close)

    lines = []
    for ticker in WATCHLIST:
        if ticker in latest and ticker in oldest_near_cutoff:
            _, old_close = oldest_near_cutoff[ticker]
            _, new_close = latest[ticker]
            pct = (new_close - old_close) / old_close * 100
            arrow = "UP" if pct > 0 else ("DOWN" if pct < 0 else "FLAT")
            lines.append(f"{ticker:<8}{arrow} {pct:+.2f}% over the past week")

    if not lines:
        return None  # not enough history yet (less than a week of data)

    return ["Weekly trend (7-day change)"] + ["-" * 42] + lines


# --- Gainers / losers + discrepancy analysis ---------------------------------


def gainers_losers_section(prices):
    """Top 3 gainers and top 3 losers in the watchlist by daily % change."""
    changes = []
    for ticker, vals in prices.items():
        if vals.get("prev_close"):
            pct = (vals["close"] - vals["prev_close"]) / vals["prev_close"] * 100
            changes.append((ticker, pct))
    if not changes:
        return None

    changes.sort(key=lambda t: t[1], reverse=True)
    gainers = [c for c in changes if c[1] > 0][:3]
    losers = [c for c in changes if c[1] < 0][-3:]

    lines = ["Top movers"]
    lines.append("-" * 42)
    if gainers:
        lines.append("Gainers: " + ", ".join(f"{t} {p:+.2f}%" for t, p in gainers))
    if losers:
        lines.append("Losers:  " + ", ".join(f"{t} {p:+.2f}%" for t, p in sorted(losers, key=lambda t: t[1])))
    return lines if (gainers or losers) else None


def discrepancy_section(tcbs_prices, vndirect_prices):
    """Flags tickers where TCBS and VNDirect closes disagree by >= threshold."""
    lines = []
    for ticker in WATCHLIST:
        a = tcbs_prices.get(ticker, {}).get("close")
        b = vndirect_prices.get(ticker, {}).get("close")
        if not a or not b:
            continue
        spread_pct = abs(a - b) / min(a, b) * 100
        if spread_pct >= DISCREPANCY_THRESHOLD_PERCENT:
            lines.append(f"{ticker:<8} TCBS {a:,.0f} vs VNDirect {b:,.0f} VND (diff {spread_pct:.2f}%)")

    if not lines:
        return None
    return [f"Source discrepancy alert (>= {DISCREPANCY_THRESHOLD_PERCENT:.1f}% spread)"] + ["-" * 42] + lines


# --- Formatting -------------------------------------------------------------


def format_email_body(prices, indices, vndirect_prices, previous_prices, vndirect_error=None):
    lines = [f"Vietnam stock watchlist - {now_vn().strftime('%Y-%m-%d %H:%M')} (Asia/Ho_Chi_Minh)\n"]

    if indices:
        lines.append("Market indices")
        lines.append(f"{'Index':<14}{'Points':<14}{'Change'}")
        lines.append("-" * 42)
        for label, vals in indices.items():
            change_str = ""
            if vals.get("prev_close"):
                pct = (vals["close"] - vals["prev_close"]) / vals["prev_close"] * 100
                arrow = "UP" if pct > 0 else ("DOWN" if pct < 0 else "FLAT")
                change_str = f"{arrow} {pct:+.2f}%"
            lines.append(f"{label:<14}{vals['close']:,.2f}{'':<6}{change_str}")
        lines.append("")

    movers = gainers_losers_section(prices)
    if movers:
        lines += movers + [""]

    discrepancy = discrepancy_section(prices, vndirect_prices)
    if discrepancy:
        lines += discrepancy + [""]

    lines.append("TCBS closing prices")
    lines.append(f"{'Ticker':<8}{'Close (VND)':<16}{'Change':<14}{'Volume'}")
    lines.append("-" * 42)
    for ticker in WATCHLIST:
        vals = prices.get(ticker)
        if not vals:
            lines.append(f"{ticker:<8}unavailable this run")
            continue
        change_str = ""
        if vals.get("prev_close"):
            pct = (vals["close"] - vals["prev_close"]) / vals["prev_close"] * 100
            arrow = "UP" if pct > 0 else ("DOWN" if pct < 0 else "FLAT")
            change_str = f"{arrow} {pct:+.2f}%"
        lines.append(f"{ticker:<8}{vals['close']:,.0f}{'':<6}{change_str:<14}{vals.get('volume', 0):,.0f}")

    used_sources = [SOURCES[0]]

    if vndirect_prices:
        lines.append("")
        lines.append("VNDirect closing prices (cross-check)")
        lines.append(f"{'Ticker':<8}{'Close (VND)'}")
        lines.append("-" * 42)
        for ticker in WATCHLIST:
            if ticker in vndirect_prices:
                lines.append(f"{ticker:<8}{vndirect_prices[ticker]['close']:,.0f}")
        used_sources.append(SOURCES[1])
    elif vndirect_error:
        lines.append("")
        lines.append(f"VNDirect: unavailable this run ({vndirect_error})")

    trend = weekly_trend_section()
    if trend:
        lines.append("")
        lines += trend

    lines.append("")
    lines.append("Sources:")
    for name, url in used_sources:
        lines.append(f"  {name}: {url}")
    lines.append("")
    lines.append(
        "Note: these are public feeds behind TCBS's and VNDirect's own apps, not "
        "documented/guaranteed APIs. Verify against your broker before trading on them."
    )

    return "\n".join(lines)


# --- Email --------------------------------------------------------------------


def send_email(body):
    msg = MIMEText(body)
    msg["Subject"] = f"VN Stock Watchlist - {now_vn().strftime('%Y-%m-%d %H:%M')}"
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = STOCK_RECIPIENT

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_ADDRESS, [STOCK_RECIPIENT], msg.as_string())


# --- Commands -----------------------------------------------------------------


def cmd_generate():
    prices = fetch_tcbs_stock_prices()
    if not prices:
        print("No prices fetched from TCBS, aborting this run.")
        open(EMAIL_BODY_FILE, "w").close()
        return

    previous_prices = load_previous_prices()

    if not should_send(prices, previous_prices):
        print("No significant change, skipping email.")
        open(EMAIL_BODY_FILE, "w").close()
        return

    try:
        indices = fetch_tcbs_indices()
    except Exception as e:
        print(f"Index fetch failed ({e}), continuing without it.")
        indices = {}

    try:
        vndirect_prices = fetch_vndirect_prices()
        vndirect_error = None
    except Exception as e:
        print(f"VNDirect source failed ({e}), continuing without it.")
        vndirect_prices = {}
        vndirect_error = str(e)

    body = format_email_body(prices, indices, vndirect_prices, previous_prices, vndirect_error)
    with open(EMAIL_BODY_FILE, "w") as f:
        f.write(body)

    print(body)
    save_prices(prices)
    append_history(prices)


def cmd_send():
    if not os.path.exists(EMAIL_BODY_FILE):
        print("No email body found, run 'generate' first.")
        return

    with open(EMAIL_BODY_FILE) as f:
        body = f.read()

    if not body.strip():
        print("Email body empty, nothing to send.")
        return

    if not (GMAIL_ADDRESS and GMAIL_APP_PASSWORD and STOCK_RECIPIENT):
        print("GMAIL_ADDRESS / GMAIL_APP_PASSWORD / STOCK_RECIPIENT not set, skipping send.")
        return

    send_email(body)
    print("Email sent.")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "generate"
    if command == "generate":
        cmd_generate()
    elif command == "send":
        cmd_send()
    else:
        print(f"Unknown command: {command}. Use 'generate' or 'send'.")
