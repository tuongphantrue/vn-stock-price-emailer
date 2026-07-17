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

1. Yahoo Finance (query1.finance.yahoo.com) - Vietnamese tickers are
   available there under a .VN suffix (e.g. VNM.VN, VCB.VN) and VN-Index
   under ^VNINDEX.VN. This is US-hosted infrastructure entirely separate
   from Vietnam's domestic anti-scraping layers, and its chart API is
   used from every kind of environment (including cloud CI) without the
   IP-based blocking seen on the Vietnamese sources below. Prices are
   already in plain VND, no thousand-scaling needed. HNX-Index and
   UPCOM-Index don't have a confirmed Yahoo ticker, so those two fall
   through to the sources below.
2. CafeF (s.cafef.vn) - public AJAX endpoint behind cafef.vn's own price
   history pages. Field names verified against several independent
   scrapers using this exact endpoint over multiple years. Requires an
   X-Requested-With: XMLHttpRequest header - without it, the endpoint
   returns HTTP 200 with an empty/error body instead of real data. Also
   appears to reject requests from cloud/datacenter IP ranges (returns
   the same empty/error body regardless of headers sent from GitHub
   Actions), so treat this as a fallback rather than reliable from CI.
3. VNDirect (finfo-api.vndirect.com.vn) - confirmed reachable and
   correctly-shaped in earlier testing, but times out entirely from
   GitHub Actions runners (Azure IP ranges appear to be blocked). Kept as
   a last-resort fallback since it may work fine from a non-cloud IP
   (e.g. your own machine, or a self-hosted runner).

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
from email.mime.multipart import MIMEMultipart

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

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
CAFEF_HISTORY_URL = "https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx"
VNDIRECT_QUOTES_URL = "https://finfo-api.vndirect.com.vn/v4/stock_prices"

EMAIL_BODY_FILE = "email_body.txt"
EMAIL_HTML_FILE = "email_body.html"
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


# --- Source 1: Yahoo Finance ---------------------------------------------------


def _fetch_yahoo_chart(symbol, days_back=10):
    """Returns a list of (close, volume) pairs, oldest -> newest, for the
    given Yahoo symbol (already including the .VN or ^...VN suffix),
    skipping any entries where close is null (non-trading days Yahoo still
    includes a slot for).
    """
    url = YAHOO_CHART_URL.format(symbol=symbol)
    params = {"range": f"{days_back}d", "interval": "1d"}
    resp = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    chart = data.get("chart") or {}
    results = chart.get("result") or []
    if not results:
        if DEBUG_EMPTY_RESPONSES:
            print(f"Yahoo empty result for {symbol}: error={chart.get('error')}")
        return []
    result = results[0]
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    pairs = [(c, volumes[i] if i < len(volumes) else None) for i, c in enumerate(closes) if c is not None]
    return pairs


def fetch_yahoo_stock_prices(tickers):
    """Returns {ticker: {"close": VND, "prev_close": VND|None, "volume": int}}.
    Yahoo already reports Vietnamese stock prices in plain VND (not
    thousands), so no scaling is needed here, unlike CafeF/TCBS.
    """
    result = {}
    for ticker in tickers:
        try:
            pairs = _fetch_yahoo_chart(f"{ticker}.VN")
            if not pairs:
                continue
            latest_close, latest_vol = pairs[-1]
            prev_close = pairs[-2][0] if len(pairs) >= 2 else None
            result[ticker] = {
                "close": float(latest_close),
                "prev_close": float(prev_close) if prev_close is not None else None,
                "volume": int(latest_vol or 0),
            }
        except Exception as e:
            print(f"Yahoo Finance fetch failed for {ticker}: {e}")
            continue
    return result


def fetch_yahoo_indices():
    """Only VN-Index is confirmed available on Yahoo Finance under a stable
    symbol (^VNINDEX.VN) - HNX-Index / UPCOM-Index don't have a confirmed
    Yahoo ticker, so they're left for the CafeF/VNDirect fallback (the
    merge-by-label logic in fetch_all_indices() already handles a source
    covering only part of the index list).
    """
    result = {}
    try:
        pairs = _fetch_yahoo_chart("^VNINDEX.VN")
        if pairs:
            latest_close, _vol = pairs[-1]
            prev_close = pairs[-2][0] if len(pairs) >= 2 else None
            result["VN-Index"] = {
                "close": float(latest_close),
                "prev_close": float(prev_close) if prev_close is not None else None,
            }
    except Exception as e:
        print(f"Yahoo Finance index fetch failed: {e}")
    return result


# --- Source 2: CafeF ----------------------------------------------------------


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


# --- Source 3: VNDirect --------------------------------------------------------


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
    ("Yahoo Finance", fetch_yahoo_stock_prices),
    ("CafeF", fetch_cafef_stock_prices),
    ("VNDirect", fetch_vndirect_stock_prices),
]

INDEX_SOURCES = [
    ("Yahoo Finance", fetch_yahoo_indices),
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


# --- Formatting: plain text (fallback) --------------------------------------


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


# --- Formatting: HTML -----------------------------------------------------------

_GREEN = "#16a34a"
_GREEN_BG = "#ecfdf3"
_RED = "#dc2626"
_RED_BG = "#fef2f2"
_GRAY = "#6b7280"
_GRAY_BG = "#f3f4f6"
_NAVY = "#0f172a"
_BORDER = "#e5e7eb"


def _pct_change(vals):
    """Returns % change vs prev_close, or None if there's no prev_close."""
    if vals.get("prev_close"):
        return (vals["close"] - vals["prev_close"]) / vals["prev_close"] * 100
    return None


def _pct_style(pct):
    """Returns (text_color, bg_color, arrow_char) for a % change value."""
    if pct is None:
        return (_GRAY, _GRAY_BG, "\u2013")  # en dash
    if pct > 0:
        return (_GREEN, _GREEN_BG, "\u25b2")  # ▲
    if pct < 0:
        return (_RED, _RED_BG, "\u25bc")  # ▼
    return (_GRAY, _GRAY_BG, "\u25ac")  # ▬


def _change_badge(pct):
    """A small colored pill showing arrow + signed percentage."""
    color, bg, arrow = _pct_style(pct)
    text = f"{arrow} {pct:+.2f}%" if pct is not None else "\u2013"
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:999px;'
        f'font-size:12px;font-weight:600;color:{color};background:{bg};">{text}</span>'
    )


def _html_escape(s):
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def format_email_html(prices, indices, used_source, previous_prices):
    parts = []
    parts.append(f"""\
<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:24px 0;">
<tr><td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.08);">

<tr><td style="background:{_NAVY};padding:24px 28px;">
  <div style="color:#ffffff;font-size:20px;font-weight:700;">VN Stock Watchlist</div>
  <div style="color:#94a3b8;font-size:13px;margin-top:4px;">{now_vn().strftime('%A, %d %B %Y - %H:%M')} (Asia/Ho_Chi_Minh)</div>
</td></tr>
""")

    # --- Market indices ---
    if indices:
        cells = []
        for label, vals in indices.items():
            pct = _pct_change(vals)
            color, _bg, arrow = _pct_style(pct)
            change_text = f"{arrow} {pct:+.2f}%" if pct is not None else "\u2013"
            cells.append(f"""\
<td width="33%" style="padding:14px 10px;text-align:center;border-right:1px solid {_BORDER};">
  <div style="font-size:12px;color:{_GRAY};font-weight:600;text-transform:uppercase;letter-spacing:0.04em;">{_html_escape(label)}</div>
  <div style="font-size:19px;font-weight:700;color:{_NAVY};margin-top:4px;">{vals['close']:,.2f}</div>
  <div style="font-size:13px;font-weight:600;color:{color};margin-top:2px;">{change_text}</div>
</td>""")
        # strip trailing border on last cell
        if cells:
            cells[-1] = cells[-1].replace(f"border-right:1px solid {_BORDER};", "")
        parts.append(f"""\
<tr><td style="padding:20px 28px 4px 28px;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid {_BORDER};border-radius:8px;">
    <tr>{''.join(cells)}</tr>
  </table>
</td></tr>
""")

    # --- Top movers ---
    movers = gainers_losers_section(prices)
    if movers:
        changes = []
        for ticker, vals in prices.items():
            pct = _pct_change(vals)
            if pct is not None:
                changes.append((ticker, pct))
        changes.sort(key=lambda t: t[1], reverse=True)
        gainers = [c for c in changes if c[1] > 0][:3]
        losers = sorted([c for c in changes if c[1] < 0][-3:], key=lambda t: t[1])

        def _chip(ticker, pct):
            color, bg, arrow = _pct_style(pct)
            return (
                f'<span style="display:inline-block;margin:3px 6px 3px 0;padding:4px 10px;'
                f'border-radius:999px;background:{bg};color:{color};font-size:13px;font-weight:600;">'
                f"{_html_escape(ticker)} {arrow} {pct:+.2f}%</span>"
            )

        rows = []
        if gainers:
            rows.append(
                f'<div style="margin-bottom:6px;"><span style="font-size:12px;color:{_GRAY};font-weight:600;">GAINERS&nbsp;</span>'
                + "".join(_chip(t, p) for t, p in gainers) + "</div>"
            )
        if losers:
            rows.append(
                f'<div><span style="font-size:12px;color:{_GRAY};font-weight:600;">LOSERS&nbsp;</span>'
                + "".join(_chip(t, p) for t, p in losers) + "</div>"
            )
        parts.append(f"""\
<tr><td style="padding:16px 28px 4px 28px;">
  {''.join(rows)}
</td></tr>
""")

    # --- Price table ---
    row_html = []
    for i, ticker in enumerate(WATCHLIST):
        vals = prices.get(ticker)
        stripe = "#ffffff" if i % 2 == 0 else "#f8fafc"
        if not vals:
            row_html.append(f"""\
<tr style="background:{stripe};">
  <td style="padding:10px 12px;font-weight:700;color:{_NAVY};">{_html_escape(ticker)}</td>
  <td colspan="4" style="padding:10px 12px;color:{_GRAY};font-size:13px;">unavailable this run</td>
</tr>""")
            continue
        pct = _pct_change(vals)
        source = used_source.get(ticker, "?")
        row_html.append(f"""\
<tr style="background:{stripe};">
  <td style="padding:10px 12px;font-weight:700;color:{_NAVY};">{_html_escape(ticker)}</td>
  <td style="padding:10px 12px;text-align:right;font-variant-numeric:tabular-nums;color:{_NAVY};">{vals['close']:,.0f}</td>
  <td style="padding:10px 12px;text-align:center;">{_change_badge(pct)}</td>
  <td style="padding:10px 12px;text-align:right;color:{_GRAY};font-size:13px;font-variant-numeric:tabular-nums;">{vals.get('volume', 0):,.0f}</td>
  <td style="padding:10px 12px;text-align:right;color:{_GRAY};font-size:11px;">{_html_escape(source)}</td>
</tr>""")

    parts.append(f"""\
<tr><td style="padding:20px 28px 4px 28px;">
  <div style="font-size:13px;font-weight:700;color:{_NAVY};text-transform:uppercase;letter-spacing:0.04em;margin-bottom:8px;">Closing Prices</div>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid {_BORDER};border-radius:8px;overflow:hidden;font-size:14px;">
    <tr style="background:#f8fafc;">
      <th align="left" style="padding:8px 12px;font-size:11px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.04em;">Ticker</th>
      <th align="right" style="padding:8px 12px;font-size:11px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.04em;">Close (VND)</th>
      <th align="center" style="padding:8px 12px;font-size:11px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.04em;">Change</th>
      <th align="right" style="padding:8px 12px;font-size:11px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.04em;">Volume</th>
      <th align="right" style="padding:8px 12px;font-size:11px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.04em;">Source</th>
    </tr>
    {''.join(row_html)}
  </table>
</td></tr>
""")

    # --- Weekly trend ---
    trend = weekly_trend_section()
    if trend:
        # trend[0] is a title line, trend[1] is a divider, rest are "TICKER  UP +x.xx% ..." lines
        trend_rows = []
        for line in trend[2:]:
            ticker = line.split()[0]
            vals_line = line[len(ticker):].strip()
            is_up = "UP" in vals_line
            is_down = "DOWN" in vals_line
            color = _GREEN if is_up else (_RED if is_down else _GRAY)
            trend_rows.append(
                f'<div style="padding:4px 0;font-size:13px;">'
                f'<span style="font-weight:700;color:{_NAVY};display:inline-block;width:56px;">{_html_escape(ticker)}</span>'
                f'<span style="color:{color};font-weight:600;">{_html_escape(vals_line)}</span></div>'
            )
        parts.append(f"""\
<tr><td style="padding:20px 28px 4px 28px;">
  <div style="font-size:13px;font-weight:700;color:{_NAVY};text-transform:uppercase;letter-spacing:0.04em;margin-bottom:8px;">Weekly Trend (7-Day Change)</div>
  {''.join(trend_rows)}
</td></tr>
""")

    # --- Footer ---
    sources_used = sorted(set(used_source.values()))
    sources_line = f"Sources this run: {_html_escape(', '.join(sources_used))}" if sources_used else ""
    parts.append(f"""\
<tr><td style="padding:20px 28px 28px 28px;">
  <div style="border-top:1px solid {_BORDER};padding-top:14px;font-size:11px;color:#9ca3af;line-height:1.5;">
    {sources_line}<br>
    These are public feeds behind each provider's own app, not documented/guaranteed APIs.
    Verify against your broker before trading on them.
  </div>
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>
""")

    return "".join(parts)


# --- Email --------------------------------------------------------------------


def send_email(text_body, html_body=None):
    if html_body:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))
    else:
        msg = MIMEText(text_body)

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
        open(EMAIL_HTML_FILE, "w").close()
        return

    previous_prices = load_previous_prices()

    if not should_send(prices, previous_prices):
        print("No significant change, skipping email.")
        open(EMAIL_BODY_FILE, "w").close()
        open(EMAIL_HTML_FILE, "w").close()
        return

    try:
        indices = fetch_all_indices()
    except Exception as e:
        print(f"Index fetch failed ({e}), continuing without it.")
        indices = {}

    body = format_email_body(prices, indices, used_source, previous_prices)
    html = format_email_html(prices, indices, used_source, previous_prices)
    with open(EMAIL_BODY_FILE, "w") as f:
        f.write(body)
    with open(EMAIL_HTML_FILE, "w") as f:
        f.write(html)

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

    html = None
    if os.path.exists(EMAIL_HTML_FILE):
        with open(EMAIL_HTML_FILE) as f:
            html = f.read().strip() or None

    if not (GMAIL_ADDRESS and GMAIL_APP_PASSWORD and STOCK_RECIPIENT):
        print("GMAIL_ADDRESS / GMAIL_APP_PASSWORD / STOCK_RECIPIENT not set, skipping send.")
        return

    send_email(body, html)
    print("Email sent.")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "generate"
    if command == "generate":
        cmd_generate()
    elif command == "send":
        cmd_send()
    else:
        print(f"Unknown command: {command}. Use 'generate' or 'send'.")
