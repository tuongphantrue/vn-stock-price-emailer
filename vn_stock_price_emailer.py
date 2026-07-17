"""
vn_stock_price_emailer.py

Fetches prices for a watchlist of Vietnam-listed stocks (HOSE/HNX/UPCOM),
plus the major indices (VN-Index, HNX-Index, UPCOM-Index), and emails a
daily summary. Designed to run on GitHub Actions (see
.github/workflows/send-stock-price.yml) or locally via cron. No local
computer needs to stay on.

Modeled on the same pattern as currency-rate-emailer / gold-price-emailer /
tech-price-mailer: pulls from public sources, degrades gracefully if one
fails, tracks state/history, and emails a summary.

Data sources, tried in this order per ticker (first success wins for that
ticker; sources are independent hosts, so a block on one doesn't take out
the others):

1. CafeF (s.cafef.vn) - public AJAX endpoint behind cafef.vn's own price
   history pages. Field names verified against several independent
   scrapers using this exact endpoint over multiple years. Requires an
   X-Requested-With: XMLHttpRequest header - without it, the endpoint
   returns HTTP 200 with an empty/error body instead of real data.
2. VNDirect (finfo-api.vndirect.com.vn) - confirmed reachable and
   correctly-shaped in earlier testing, but times out entirely from
   GitHub Actions runners (Azure IP ranges appear to be blocked). Kept as
   a fallback since it may work fine from a non-cloud IP (e.g. your own
   machine, or a self-hosted runner), and costs nothing to try last.

(FireAnt was tried as a third source but dropped: its documented-looking
endpoint 404s outright, and other people's write-ups of FireAnt's API note
it may require authentication that isn't publicly available - not worth
guessing at further without real docs.)

None of these are documented/versioned APIs - they're the public JSON
endpoints behind each site's own web app, the same category of caveat the
Vietcombank source carries in currency-rate-emailer. Any of them can
change shape, rate-limit, or block a given IP range without notice.

Extra features (matching the sibling emailers):

- Daily % change per stock, with an UP/DOWN/FLAT arrow
- Top gainers / top losers within the watchlist
- Market index snapshot: VN-Index, HNX-Index, UPCOM-Index
- Historical tracking + weekly trend: logs every run to price_history.csv
  and emails a 7-day % change summary once a week
- Move-threshold alerting: only send if some stock moved >= X% since the
  last run (optional)
- Per-run footer noting which source(s) actually supplied data, so you
  can tell at a glance if one of the three has gone dark

Usage:
    python vn_stock_price_emailer.py generate   # fetch prices, build email body -> email_body.txt
    python vn_stock_price_emailer.py send       # send email_body.txt via SMTP

Required environment variables (set as GitHub Actions secrets, or export locally):
    GMAIL_ADDRESS       - sender gmail address
    GMAIL_APP_PASSWORD  - Gmail App Password (not your normal password)
    STOCK_RECIPIENT     - recipient email address

Optional environment variables:
    WATCHLIST                - comma-separated tickers, default below
    ALERT_THRESHOLD_PERCENT  - only send if some stock moved >= this % since last run
                                (leave unset to always send)
"""

import os
import sys
import csv
import json
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

CAFEF_HISTORY_URL = "https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx"
VNDIRECT_QUOTES_URL = "https://finfo-api.vndirect.com.vn/v4/stock_prices"

EMAIL_BODY_FILE = "email_body.txt"
STATE_FILE = "last_prices.json"
HISTORY_FILE = "price_history.csv"

ALERT_THRESHOLD_PERCENT = _env("ALERT_THRESHOLD_PERCENT")
ALERT_THRESHOLD_PERCENT = float(ALERT_THRESHOLD_PERCENT) if ALERT_THRESHOLD_PERCENT else None

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
# CafeF's AJAX endpoint is called from cafef.vn pages via XHR in the browser;
# it may check Referer/Origin (common hotlink-protection) and silently return
# an empty result set to requests that don't look like they came from the
# site itself, rather than an HTTP error. Send matching headers to look like
# a real page load.
CAFEF_HEADERS = dict(
    HEADERS,
    Referer="https://cafef.vn/",
    Origin="https://cafef.vn",
    # CafeF's .ashx handler returned HTTP 200 with {"Message":"symbol is null
    # or empty"} for every ticker until this header was added - it appears
    # to require this to treat the request as a genuine in-page AJAX call
    # before it will read the query string at all.
    **{"X-Requested-With": "XMLHttpRequest"},
)
REQUEST_TIMEOUT = 15
DEBUG_EMPTY_RESPONSES = _env("DEBUG_EMPTY_RESPONSES") is not None


def _debug_snippet(resp):
    """Best-effort short debug string for a response that came back with no
    usable rows: status code + first ~150 chars of body. Helps tell apart
    'blocked and served an HTML/captcha page' from 'valid JSON, just empty'.
    """
    try:
        return f"status={resp.status_code} body[:150]={resp.text[:150]!r}"
    except Exception:
        return "status=? body=?"


def _first_present(d, keys):
    """Returns the first non-None value found in dict d for any key in keys."""
    if not d:
        return None
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return None


# --- Source 1: CafeF ----------------------------------------------------------


def _fetch_cafef_history(symbol, page_size=5):
    """Returns rows (newest first) from CafeF's price-history AJAX feed for
    one symbol. Works for both stock tickers and index codes (VNINDEX etc.)
    """
    params = {
        "Symbol": symbol,
        "StartDate": "",
        "EndDate": "",
        "PageIndex": 1,
        "PageSize": page_size,
    }
    resp = requests.get(
        CAFEF_HISTORY_URL, headers=CAFEF_HEADERS, params=params, timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    data = resp.json()
    rows = ((data.get("Data") or {}).get("Data")) or []

    if not rows and DEBUG_EMPTY_RESPONSES:
        print(f"CafeF empty response for {symbol}: {_debug_snippet(resp)}")

    def _row_date(row):
        try:
            return datetime.strptime(row.get("Ngay", ""), "%d/%m/%Y")
        except ValueError:
            return datetime.min

    rows.sort(key=_row_date, reverse=True)
    return rows


def fetch_cafef_stock_prices(tickers):
    """Returns {ticker: {"close": VND, "prev_close": VND|None, "volume": int}}.
    CafeF quotes stock prices in thousand VND (e.g. 85.5 == 85,500 VND), so
    we scale by 1000 to get plain VND, matching the rest of the script.
    """
    result = {}
    for ticker in tickers:
        try:
            rows = _fetch_cafef_history(ticker)
            if not rows:
                continue
            latest = rows[0]
            prev = rows[1] if len(rows) >= 2 else None
            close = latest.get("GiaDongCua")
            if close is None:
                continue
            result[ticker] = {
                "close": float(close) * 1000,
                "prev_close": float(prev["GiaDongCua"]) * 1000 if prev and prev.get("GiaDongCua") else None,
                "volume": int(latest.get("KhoiLuongKhopLenh") or 0),
            }
        except Exception as e:
            print(f"CafeF fetch failed for {ticker}: {e}")
            continue
    return result


def fetch_cafef_indices():
    """Returns {index_label: {"close": pts, "prev_close": pts|None}}.
    Index points are used as-is (no thousand-VND scaling).
    """
    result = {}
    for code, label in INDICES:
        try:
            rows = _fetch_cafef_history(code)
            if not rows:
                continue
            latest = rows[0]
            prev = rows[1] if len(rows) >= 2 else None
            close = latest.get("GiaDongCua")
            if close is None:
                continue
            result[label] = {
                "close": float(close),
                "prev_close": float(prev["GiaDongCua"]) if prev and prev.get("GiaDongCua") else None,
            }
        except Exception as e:
            print(f"CafeF index fetch failed for {label}: {e}")
            continue
    return result


# --- Source 2: VNDirect --------------------------------------------------------


def _fetch_vndirect_rows(codes, days_back=15):
    """Returns raw rows from VNDirect's stock_prices feed for the given
    codes (list of tickers/index codes) over the last `days_back` days.
    One HTTP call covers the whole list of codes.
    """
    today = now_vn().date()
    from_date = (today - timedelta(days=days_back)).isoformat()
    to_date = today.isoformat()
    query = f"code:{','.join(codes)}~date:gte:{from_date}~date:lte:{to_date}"
    params = {
        "sort": "date:desc",
        "q": query,
        "size": days_back * max(len(codes), 1),
        "page": 1,
    }
    resp = requests.get(
        VNDIRECT_QUOTES_URL, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("data") or []


def _vndirect_rows_to_latest_prev(rows, codes):
    by_code = {}
    for row in rows:
        code = row.get("code")
        if code not in codes:
            continue
        by_code.setdefault(code, []).append(row)

    result = {}
    for code, code_rows in by_code.items():
        code_rows.sort(key=lambda r: r.get("date", ""), reverse=True)
        latest = code_rows[0]
        prev = code_rows[1] if len(code_rows) >= 2 else None
        close = latest.get("close") or latest.get("adClose")
        if close is None:
            continue
        result[code] = {
            "close": float(close),
            "prev_close": float(prev["close"]) if prev and prev.get("close") else None,
            "volume": int(latest.get("nmVolume") or latest.get("volume") or 0),
        }
    return result


def fetch_vndirect_stock_prices(tickers):
    """Returns {ticker: {"close": VND, "prev_close": VND|None, "volume": int}}
    for the given tickers in one batched HTTP call."""
    rows = _fetch_vndirect_rows(tickers)
    return _vndirect_rows_to_latest_prev(rows, set(tickers))


def fetch_vndirect_indices():
    codes = [code for code, _label in INDICES]
    rows = _fetch_vndirect_rows(codes)
    by_code = _vndirect_rows_to_latest_prev(rows, set(codes))
    result = {}
    for code, label in INDICES:
        if code in by_code:
            result[label] = {
                "close": by_code[code]["close"],
                "prev_close": by_code[code]["prev_close"],
            }
    return result


# --- Cascade across sources ----------------------------------------------------

STOCK_SOURCES = [
    ("CafeF", fetch_cafef_stock_prices),
    ("VNDirect", fetch_vndirect_stock_prices),
]

INDEX_SOURCES = [
    ("CafeF", fetch_cafef_indices),
    ("VNDirect", fetch_vndirect_indices),
]


def fetch_all_stock_prices():
    """Tries each source in order, only asking for tickers still missing
    after the previous source. Returns (prices_dict, {ticker: source_name}).
    A block or outage on one source just means the next one fills the gaps.
    """
    prices = {}
    used_source = {}
    for name, fetch_fn in STOCK_SOURCES:
        missing = [t for t in WATCHLIST if t not in prices]
        if not missing:
            break
        try:
            partial = fetch_fn(missing)
        except Exception as e:
            print(f"{name} source failed entirely: {e}")
            continue
        for ticker, vals in partial.items():
            if ticker not in prices:
                prices[ticker] = vals
                used_source[ticker] = name
    return prices, used_source


def fetch_all_indices():
    indices = {}
    for name, fetch_fn in INDEX_SOURCES:
        missing = [label for _code, label in INDICES if label not in indices]
        if not missing:
            break
        try:
            partial = fetch_fn()
        except Exception as e:
            print(f"{name} index source failed entirely: {e}")
            continue
        for label, vals in partial.items():
            if label not in indices:
                indices[label] = vals
    return indices


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
    """Appends this run's closes to a CSV: timestamp,ticker,close"""
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


# --- Gainers / losers ---------------------------------------------------------


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


# --- Formatting -------------------------------------------------------------


def format_email_body(prices, indices, used_source, previous_prices):
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

    lines.append("Closing prices")
    lines.append(f"{'Ticker':<8}{'Close (VND)':<16}{'Change':<14}{'Volume':<14}{'Source'}")
    lines.append("-" * 60)
    for ticker in WATCHLIST:
        vals = prices.get(ticker)
        if not vals:
            lines.append(f"{ticker:<8}unavailable this run (all sources failed)")
            continue
        change_str = ""
        if vals.get("prev_close"):
            pct = (vals["close"] - vals["prev_close"]) / vals["prev_close"] * 100
            arrow = "UP" if pct > 0 else ("DOWN" if pct < 0 else "FLAT")
            change_str = f"{arrow} {pct:+.2f}%"
        lines.append(
            f"{ticker:<8}{vals['close']:,.0f}{'':<6}{change_str:<14}"
            f"{vals.get('volume', 0):,.0f}{'':<8}{used_source.get(ticker, '?')}"
        )

    trend = weekly_trend_section()
    if trend:
        lines.append("")
        lines += trend

    sources_used = sorted(set(used_source.values()))
    lines.append("")
    if sources_used:
        lines.append(f"Sources that supplied data this run: {', '.join(sources_used)}")
    lines.append(
        "Note: these are public feeds behind each provider's own app, not documented/"
        "guaranteed APIs. Verify against your broker before trading on them."
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
    prices, used_source = fetch_all_stock_prices()

    if not prices:
        print("No prices fetched from any source, aborting this run.")
        open(EMAIL_BODY_FILE, "w").close()
        return

    previous_prices = load_previous_prices()

    if not should_send(prices, previous_prices):
        print("No significant change, skipping email.")
        open(EMAIL_BODY_FILE, "w").close()
        return

    try:
        indices = fetch_all_indices()
    except Exception as e:
        print(f"Index fetch failed ({e}), continuing without it.")
        indices = {}

    body = format_email_body(prices, indices, used_source, previous_prices)
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
