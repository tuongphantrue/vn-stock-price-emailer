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

Stock prices for the watchlist come from three sources, tried in this
order per ticker (first success wins for that ticker); indices (VN-Index
etc.) come from the first two only:

1. Yahoo Finance (query1.finance.yahoo.com) - Vietnamese tickers are
   available there under a .VN suffix (e.g. VNM.VN, VCB.VN) and VN-Index
   under ^VNINDEX.VN. Prices are already in plain VND, no thousand-scaling
   needed. HNX-Index and UPCOM-Index don't have a confirmed Yahoo ticker,
   so those two fall through to TradingView.
2. TradingView (scanner.tradingview.com) - TradingView's own scanner/
   screener API, confirmed via an open-source Python wrapper's docs and
   usage examples. Each ticker is qualified with its actual exchange
   (HOSE/HNX/UPCOM) via TICKER_EXCHANGE - a ticker this script doesn't
   have an exchange mapping for defaults to HOSE. Confirmed in production
   to give full HOSE/HNX/UPCOM coverage via a Vietnam-scoped scanner
   endpoint (TRADINGVIEW_VIETNAM_SCAN_URL).
3. MSN Finance (assets.msn.com / services.bingapis.com) - Microsoft-hosted,
   same globally-hosted category as the two above. A two-step lookup:
   Bing's finance autosuggest endpoint resolves a ticker to MSN's own
   opaque instrument ID (cached to MSN_ID_CACHE_FILE after first
   resolution, since that mapping never changes), then one batched call
   to MSN's Quotes endpoint fetches live prices for every resolved ID.
   Both endpoints and their exact response shapes - including a real
   double-JSON-encoding quirk in the autosuggest response that would have
   been very easy to get wrong - were confirmed via live requests during
   development (a browser DevTools capture for Quotes, a direct fetch for
   autosuggest), not guessed at.

All three are globally-hosted infrastructure, confirmed reliable from
GitHub Actions (Yahoo/TradingView in production; MSN not yet exercised in
a real scheduled run at time of writing - worth checking test-sources
output on the first one). Three domestic Vietnamese sources - CafeF,
VNDirect, and SSI's iBoard - were also tried earlier and each confirmed
blocked from GitHub Actions' cloud IPs in a different way (CafeF: a
fake-success empty response; VNDirect: connection timeout; SSI: 403
Forbidden) despite being correctly implemented against real, working
endpoints. They were removed rather than kept as permanently-inert code;
a proxy with residential/ISP IPs (not datacenter) would very likely be
needed for a domestic VN source to work from a cloud CI runner - worth
revisiting if that's ever set up.

Neither Yahoo Finance nor TradingView are documented/versioned APIs -
they're public JSON endpoints, the same category of caveat the
Vietcombank source carries in currency-rate-emailer. Either can change
shape, rate-limit, or block a given IP range without notice.

Extra features (matching the sibling emailers):

- Daily % change per stock, with an UP/DOWN/FLAT arrow
- Top gainers / top losers within the watchlist
- Market index snapshot: VN-Index, HNX-Index, UPCOM-Index
- Historical tracking + weekly trend: logs every run to price_history.csv
  and emails a 7-day % change summary once a week
- Move-threshold alerting: only send if some stock moved >= X% since the
  last run (optional)
- Per-run footer noting which source(s) actually supplied data

Usage:
    python vn_stock_price_emailer.py generate      # fetch prices, build email body -> email_body.txt
    python vn_stock_price_emailer.py send          # send email_body.txt via SMTP
    python vn_stock_price_emailer.py test-sources  # diagnostic: test each source independently,
                                                    # bypassing the "only call what's still missing"
                                                    # cascade logic so a redundant source still gets
                                                    # exercised and you can see if it still works

Required environment variables (set as GitHub Actions secrets, or export locally):
    GMAIL_ADDRESS       - sender gmail address
    GMAIL_APP_PASSWORD  - Gmail App Password (not your normal password)
    STOCK_RECIPIENT     - recipient email address

Optional environment variables:
    WATCHLIST                - comma-separated tickers, default below
    ALERT_THRESHOLD_PERCENT  - only send if some stock moved >= this % since last run
                                (leave unset to always send)
    DEBUG_EMPTY_RESPONSES    - set to any non-empty value to log the actual HTTP
                                status/body when a source returns no usable data,
                                instead of failing silently
"""

import os
import sys
import csv
import json
import smtplib
import uuid
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


_VN_WEEKDAYS = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
_VN_MONTHS = [
    "Tháng 1", "Tháng 2", "Tháng 3", "Tháng 4", "Tháng 5", "Tháng 6",
    "Tháng 7", "Tháng 8", "Tháng 9", "Tháng 10", "Tháng 11", "Tháng 12",
]


def format_vn_datetime(dt):
    """Vietnamese weekday/month names, formatted by hand rather than via
    strftime('%A, %B') - GitHub Actions runners don't have the vi_VN
    locale installed by default, so a locale-based approach would
    silently fall back to English instead of erroring.
    """
    weekday = _VN_WEEKDAYS[dt.weekday()]
    month = _VN_MONTHS[dt.month - 1]
    return f"{weekday}, {dt.day:02d} {month} {dt.year} - {dt.strftime('%H:%M')}"


DEFAULT_WATCHLIST_BY_EXCHANGE = {
    "HOSE": [
        # Banking
        "VCB", "TCB", "MBB", "BID", "CTG", "ACB", "VPB", "STB", "HDB", "TPB",
        # Real estate
        "VIC", "VHM", "NVL", "KDH", "DXG", "PDR",
        # Retail / consumer
        "MWG", "PNJ", "VNM", "SAB", "MSN",
        # Industrials / materials
        "HPG", "GVR", "DGC", "HSG",
        # Technology
        "FPT",
        # Securities
        "SSI", "VND", "VCI", "HCM",
        # Energy / utilities
        "GAS", "PLX", "POW",
        # Aviation
        "VJC", "HVN",
        # Media / entertainment
        "YEG",
    ],
    "HNX": [
        "SHS",  # Saigon-Hanoi Securities
        "PVS",  # PetroVietnam Technical Services
        "IDC",  # IDICO Corp
        "VCS",  # Vicostone
        "CEO",  # CEO Group
        "NTP",  # Tien Phong Plastic
        "PVI",  # PVI Holdings
        "TNG",  # TNG Investment and Trading
        "BAB",  # Bac A Commercial Bank
        "MBS",  # MB Securities
        "VC3",  # Vinaconex 3
    ],
    "UPCOM": [
        "BSR",  # Binh Son Refining
        "ACV",  # Airports Corporation of Vietnam
        "VEA",  # VEAM Corporation
        "MCH",  # Masan Consumer Holdings
        "QNS",  # Quang Ngai Sugar
        "VGI",  # Viettel Global Investment
        "FOX",  # FPT Telecom
        "VGT",  # Vietnam National Textile and Garment Group (Vinatex)
        "LTG",  # Loc Troi Group
    ],
}

# Exchange order used consistently for display grouping throughout the email.
EXCHANGE_ORDER = ["HOSE", "HNX", "UPCOM"]

DEFAULT_WATCHLIST = [
    t for exch in EXCHANGE_ORDER for t in DEFAULT_WATCHLIST_BY_EXCHANGE[exch]
]

# ticker -> exchange, built from the same source of truth as the default
# watchlist above. A custom ticker added via the WATCHLIST env var that
# isn't in this map falls back to "HOSE" (see ticker_exchange() below) -
# true for the large majority of actively-traded VN tickers, but worth
# double-checking if you add an HNX/UPCOM-only name yourself.
TICKER_EXCHANGE = {
    t: exch for exch, tickers in DEFAULT_WATCHLIST_BY_EXCHANGE.items() for t in tickers
}


def ticker_exchange(ticker):
    """Returns the exchange ("HOSE"/"HNX"/"UPCOM") for a ticker, defaulting
    to HOSE for anything not in TICKER_EXCHANGE (i.e. a custom addition via
    the WATCHLIST env var that this script doesn't already know about).
    """
    return TICKER_EXCHANGE.get(ticker, "HOSE")


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


def watchlist_by_exchange():
    """Groups WATCHLIST tickers by exchange, in EXCHANGE_ORDER, skipping
    exchanges with no tickers. Used to render the email's price table as
    separate per-exchange sections instead of one flat list.
    """
    groups = {exch: [] for exch in EXCHANGE_ORDER}
    for ticker in WATCHLIST:
        groups[ticker_exchange(ticker)].append(ticker)
    return {exch: tickers for exch, tickers in groups.items() if tickers}

INDICES = [
    ("VNINDEX", "VN-Index"),
    ("HNXINDEX", "HNX-Index"),
    ("UPCOMINDEX", "UPCOM-Index"),
]

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
TRADINGVIEW_SCAN_URL = "https://scanner.tradingview.com/scan"
# A country/market-scoped scanner endpoint also exists (confirmed via an
# open-source Python wrapper's docs, which reference market="vietnam" as a
# supported scope). The generic /scan endpoint above appeared to only
# return data for some HOSE tickers in practice and came back empty for
# every HNX/UPCOM ticker tried - this market-scoped endpoint is tried
# first now, on the theory that it indexes the full local instrument
# universe rather than a curated cross-market subset. Confirmed in
# production to give full HOSE/HNX/UPCOM coverage.
TRADINGVIEW_VIETNAM_SCAN_URL = "https://scanner.tradingview.com/vietnam/scan"
# MSN Money / Bing Finance - both endpoints and their exact response
# shapes were confirmed via live requests during development (a browser
# DevTools capture for Quotes, a direct fetch for the autosuggest one),
# not guessed at. See fetch_msn_stock_prices() for details.
MSN_AUTOSUGGEST_URL = "https://services.bingapis.com/contentservices-finance.csautosuggest/api/v1/Query"
MSN_QUOTES_URL = "https://assets.msn.com/service/Finance/Quotes"
# Public app key baked into MSN's own frontend JS (confirmed via browser
# DevTools network capture against a real MSN Money page) - not a secret,
# just an app identifier every visitor's browser already sends.
MSN_API_KEY = "0QfOX3Vn51YCzitbLaRkTTBadtWpgTN8NZLW0C1SEM"
MSN_ID_CACHE_FILE = "msn_id_cache.json"

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
    # Both of these hosts block the bare default "python-requests/x.y"
    # User-Agent, so we look like an ordinary browser instead.
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}
REQUEST_TIMEOUT = 15
DEBUG_EMPTY_RESPONSES = _env("DEBUG_EMPTY_RESPONSES") is not None


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
    thousands), so no scaling is needed here.
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
    Yahoo ticker, so they're left for TradingView (the merge-by-label
    logic in fetch_all_indices() already handles a source covering only
    part of the index list).
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


# --- Source 2: TradingView -------------------------------------------------------


def _tradingview_scan(qualified_tickers, columns, url=TRADINGVIEW_SCAN_URL):
    """POSTs a batch scan request for fully-qualified TradingView tickers
    (e.g. "HOSE:VNM") and returns {bare_ticker: {column_name: value}}. One
    HTTP call covers the whole batch, across mixed exchanges if needed.
    """
    resp = requests.post(
        url,
        headers=HEADERS,
        json={"symbols": {"tickers": qualified_tickers, "query": {"types": []}}, "columns": columns},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("data") or []
    if not rows and DEBUG_EMPTY_RESPONSES:
        print(f"TradingView empty scan response ({url}): {json.dumps(data)[:200]}")
    result = {}
    for row in rows:
        symbol = row.get("s", "")
        bare = symbol.split(":", 1)[-1]
        values = row.get("d") or []
        result[bare] = dict(zip(columns, values))
    return result


def _tradingview_prev_close(close, change_pct):
    """TradingView's scanner returns today's % change directly rather than
    yesterday's close, so prev_close is derived: close = prev * (1 + pct/100).
    """
    if close is None or change_pct is None or change_pct <= -100:
        return None
    return close / (1 + change_pct / 100)


def fetch_tradingview_stock_prices(tickers):
    """Returns {ticker: {"close": VND, "prev_close": VND|None, "volume": int}}.
    Each ticker is qualified with its actual exchange (HOSE/HNX/UPCOM) via
    ticker_exchange() rather than assuming HOSE for everything. Uses the
    Vietnam-scoped scanner endpoint - confirmed working in production
    (full HOSE/HNX/UPCOM coverage). The generic cross-market endpoint was
    tried here too originally but turned out to 404 outright (confirmed
    in a real run), so it's no longer called - it never contributed
    anything and just added noise/latency.
    """
    result = {}
    qualified = {t: f"{ticker_exchange(t)}:{t}" for t in tickers}

    try:
        rows = _tradingview_scan(
            [qualified[t] for t in tickers], ["close", "change", "volume"], url=TRADINGVIEW_VIETNAM_SCAN_URL
        )
    except Exception as e:
        print(f"TradingView fetch failed entirely ({TRADINGVIEW_VIETNAM_SCAN_URL}): {e}")
        return result

    for ticker in tickers:
        vals = rows.get(ticker)
        if not vals or vals.get("close") is None:
            continue
        close = vals["close"]
        prev_close = _tradingview_prev_close(close, vals.get("change"))
        result[ticker] = {
            "close": float(close),
            "prev_close": float(prev_close) if prev_close is not None else None,
            "volume": int(vals.get("volume") or 0),
        }
    return result


def fetch_tradingview_indices():
    """VN-Index (HOSE:VNINDEX) and HNX-Index (HNX:HNXINDEX) are both
    confirmed directly against TradingView's own site. UPCOM-Index
    (UPCOM:UPCOMINDEX) follows the same convention but wasn't
    independently confirmed, and in practice the Vietnam-scoped scanner
    hasn't returned a value for it - if Yahoo also misses it, the email
    shows an "unavailable" row for it rather than silently dropping it.

    Known minor gap, not yet root-caused: VN-Index's % change has come
    back empty in production even though its points value comes through
    fine - only close was populated in that response, change wasn't.
    Uses only the Vietnam-scoped scanner endpoint; the generic
    cross-market one was tried here too originally but confirmed to 404
    outright in a real run, so it's no longer called.
    """
    index_tickers = {
        "HOSE:VNINDEX": "VN-Index",
        "HNX:HNXINDEX": "HNX-Index",
        "UPCOM:UPCOMINDEX": "UPCOM-Index",
    }
    result = {}
    try:
        rows = _tradingview_scan(list(index_tickers.keys()), ["close", "change"], url=TRADINGVIEW_VIETNAM_SCAN_URL)
    except Exception as e:
        print(f"TradingView index fetch failed entirely ({TRADINGVIEW_VIETNAM_SCAN_URL}): {e}")
        return result

    for qualified, label in index_tickers.items():
        bare = qualified.split(":", 1)[-1]
        vals = rows.get(bare)
        if not vals or vals.get("close") is None:
            continue
        close = vals["close"]
        prev_close = _tradingview_prev_close(close, vals.get("change"))
        result[label] = {
            "close": float(close),
            "prev_close": float(prev_close) if prev_close is not None else None,
        }
    return result


# --- Source 3: MSN Finance -----------------------------------------------------


def _load_msn_id_cache():
    if not os.path.exists(MSN_ID_CACHE_FILE):
        return {}
    try:
        with open(MSN_ID_CACHE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_msn_id_cache(cache):
    with open(MSN_ID_CACHE_FILE, "w") as f:
        json.dump(cache, f)


def _msn_resolve_instrument_id(ticker):
    """Resolves a bare ticker (e.g. "VNM") to MSN's opaque internal
    instrument ID (e.g. "aqk1a2") via Bing's finance autosuggest endpoint.

    Confirmed response shape (via a live fetch during development, not a
    guess): {"count": N, "data": {"stocks": ["<json-encoded-string>", ...]}}
    - each element of "stocks" is itself a JSON *string*, not an object,
    and needs a second json.loads() to reach fields like "SecId". Easy to
    miss without seeing a real response - the API double-encodes.

    Cross-checks the resolved result's own ticker field (RT00S) against
    what was asked for, and returns None (skip, don't guess) if they
    don't match - autosuggest is a "did you mean" endpoint, and the top
    hit for an ambiguous query might be a company name match rather than
    the exact ticker.
    """
    params = {"query": ticker, "market": "en-us", "count": 1}
    resp = requests.get(MSN_AUTOSUGGEST_URL, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    stock_strings = ((data.get("data") or {}).get("stocks")) or []
    if not stock_strings:
        if DEBUG_EMPTY_RESPONSES:
            print(f"MSN autosuggest empty response for {ticker}: {json.dumps(data)[:200]}")
        return None
    try:
        stock = json.loads(stock_strings[0])
    except Exception as e:
        if DEBUG_EMPTY_RESPONSES:
            print(f"MSN autosuggest unparseable inner JSON for {ticker}: {e}")
        return None
    resolved_symbol = stock.get("RT00S")
    if resolved_symbol and resolved_symbol.upper() != ticker.upper():
        if DEBUG_EMPTY_RESPONSES:
            print(f"MSN autosuggest resolved {ticker} to a different symbol ({resolved_symbol}), skipping")
        return None
    return stock.get("SecId")


def _msn_resolve_all(tickers):
    """Resolves every ticker to an MSN instrument ID, using and updating a
    small on-disk cache (MSN_ID_CACHE_FILE, committed back to the repo by
    the workflow like last_prices.json/price_history.csv already are) so
    this lookup - one HTTP call per *new* ticker - effectively only
    happens once per ticker ever, not on every run. MSN's internal IDs
    are stable for a given company, so caching them indefinitely is safe.
    """
    cache = _load_msn_id_cache()
    changed = False
    ids = {}
    for ticker in tickers:
        if ticker in cache:
            ids[ticker] = cache[ticker]
            continue
        try:
            instrument_id = _msn_resolve_instrument_id(ticker)
        except Exception as e:
            print(f"MSN ID resolution failed for {ticker}: {e}")
            continue
        if instrument_id:
            ids[ticker] = instrument_id
            cache[ticker] = instrument_id
            changed = True
        else:
            print(f"MSN could not resolve an instrument ID for {ticker}")
    if changed:
        _save_msn_id_cache(cache)
    return ids


def _msn_fetch_quotes(instrument_ids):
    """Batched quote fetch for a list of MSN instrument IDs - one HTTP
    call covers all of them via a comma-separated ids= param. Confirmed
    response shape via a live browser DevTools capture against a real MSN
    Money page: a JSON array of quote objects, each including (among many
    other fields) symbol, price, pricePreviousClose, accumulatedVolume.
    """
    if not instrument_ids:
        return []
    params = {
        "apikey": MSN_API_KEY,
        "activityId": str(uuid.uuid4()),
        "ocid": "finance-utils-peregrine",
        "cm": "en-us",
        "it": "web",
        "scn": "ANON",
        "ids": ",".join(instrument_ids),
        "wrapodata": "false",
    }
    resp = requests.get(MSN_QUOTES_URL, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    rows = data if isinstance(data, list) else []
    if not rows and DEBUG_EMPTY_RESPONSES:
        print(f"MSN quotes empty response: {json.dumps(data)[:200]}")
    return rows


def fetch_msn_stock_prices(tickers):
    """Returns {ticker: {"close": VND, "prev_close": VND|None, "volume": int}}.

    Two-step, since MSN's Quotes endpoint takes its own opaque instrument
    IDs rather than ticker symbols: resolve each ticker to an ID (cached
    after the first successful resolution - see _msn_resolve_all), then
    one batched call to Quotes for every resolved ID. Confirmed against a
    real Vietnamese ticker (VNM) during development: price is already in
    plain VND (matches Yahoo/TradingView, not thousand-scaled like the
    old CafeF/TCBS sources), and pricePreviousClose is the exchange's own
    reference/previous-close price, used directly as prev_close.

    MSN Money is Microsoft-hosted infrastructure (Bing/Azure), the same
    globally-hosted category as Yahoo and TradingView - not a domestic VN
    site, so it shouldn't hit the cloud-IP blocking that ruled out CafeF/
    VNDirect/SSI.
    """
    result = {}
    ids_by_ticker = _msn_resolve_all(tickers)
    if not ids_by_ticker:
        return result

    id_to_ticker = {v: k for k, v in ids_by_ticker.items()}
    try:
        quotes = _msn_fetch_quotes(list(ids_by_ticker.values()))
    except Exception as e:
        print(f"MSN quotes fetch failed entirely: {e}")
        return result

    for q in quotes:
        instrument_id = q.get("instrumentId") or q.get("_p")
        ticker = id_to_ticker.get(instrument_id) or q.get("symbol")
        if ticker not in ids_by_ticker:
            continue
        close = q.get("price")
        if close is None:
            continue
        prev_close = q.get("pricePreviousClose")
        volume = q.get("accumulatedVolume") or 0
        result[ticker] = {
            "close": float(close),
            "prev_close": float(prev_close) if prev_close else None,
            "volume": int(volume),
        }
    return result


# --- Cascade across sources ----------------------------------------------------

STOCK_SOURCES = [
    ("Yahoo Finance", fetch_yahoo_stock_prices),
    ("TradingView", fetch_tradingview_stock_prices),
    ("MSN Finance", fetch_msn_stock_prices),
]

# MSN Finance isn't part of INDEX_SOURCES: indices (VN-Index etc.) would
# need their own confirmed MSN instrument IDs, which weren't looked into -
# the stock-side integration above only covers individual tickers.
INDEX_SOURCES = [
    ("Yahoo Finance", fetch_yahoo_indices),
    ("TradingView", fetch_tradingview_indices),
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


def weekly_trend_data():
    """Once a week (first run after midnight Monday, Vietnam time), compares
    today's close to the close from ~7 days ago. Returns a list of
    (ticker, pct_change) tuples, or None if it's not time yet / there's not
    enough history. Returning raw data (rather than pre-formatted strings)
    keeps this reusable for both the plain-text and HTML renderers without
    one having to parse the other's output.
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

    result = []
    for ticker in WATCHLIST:
        if ticker in latest and ticker in oldest_near_cutoff:
            _, old_close = oldest_near_cutoff[ticker]
            _, new_close = latest[ticker]
            pct = (new_close - old_close) / old_close * 100
            result.append((ticker, pct))

    return result if result else None  # None = not enough history yet


# --- Gainers / losers ---------------------------------------------------------


def top_movers(prices):
    """Returns (gainers, losers) as lists of (ticker, pct) tuples, top 3
    each by daily % change, or (None, None) if there's nothing to compare.
    """
    changes = []
    for ticker, vals in prices.items():
        pct = _pct_change(vals)
        if pct is not None:
            changes.append((ticker, pct))
    if not changes:
        return None, None

    changes.sort(key=lambda t: t[1], reverse=True)
    gainers = [c for c in changes if c[1] > 0][:3]
    losers = sorted([c for c in changes if c[1] < 0][-3:], key=lambda t: t[1])
    return gainers, losers


def _pct_change(vals):
    """Returns % change vs prev_close, or None if there's no prev_close."""
    if vals.get("prev_close"):
        return (vals["close"] - vals["prev_close"]) / vals["prev_close"] * 100
    return None


def _pct_arrow_word(pct):
    """Vietnamese text label for a % change, used in the plain-text email."""
    if pct is None:
        return "\u2013"
    if pct > 0:
        return "TĂNG"
    if pct < 0:
        return "GIẢM"
    return "ĐI NGANG"


# --- Formatting: plain text (fallback) --------------------------------------


def format_email_body(prices, indices, used_source, previous_prices):
    lines = [f"Danh mục cổ phiếu Việt Nam - {format_vn_datetime(now_vn())} (Giờ Việt Nam)\n"]

    all_index_labels = [label for _code, label in INDICES]
    if any(label in indices for label in all_index_labels):
        lines.append("Chỉ số thị trường")
        lines.append(f"{'Chỉ số':<14}{'Điểm':<14}{'Thay đổi'}")
        lines.append("-" * 42)
        for label in all_index_labels:
            vals = indices.get(label)
            if not vals:
                lines.append(f"{label:<14}không có dữ liệu lần này")
                continue
            pct = _pct_change(vals)
            change_str = f"{_pct_arrow_word(pct)} {pct:+.2f}%" if pct is not None else ""
            lines.append(f"{label:<14}{vals['close']:,.2f}{'':<6}{change_str}")
        lines.append("")

    gainers, losers = top_movers(prices)
    if gainers or losers:
        lines.append("Biến động nổi bật")
        lines.append("-" * 42)
        if gainers:
            lines.append("Tăng: " + ", ".join(f"{t} {p:+.2f}%" for t, p in gainers))
        if losers:
            lines.append("Giảm: " + ", ".join(f"{t} {p:+.2f}%" for t, p in losers))
        lines.append("")

    for exch, tickers in watchlist_by_exchange().items():
        lines.append(f"Giá đóng cửa - {exch}")
        lines.append(f"{'Mã CK':<8}{'Giá đóng cửa (VNĐ)':<22}{'Thay đổi':<14}{'Khối lượng':<16}{'Nguồn'}")
        lines.append("-" * 68)
        for ticker in tickers:
            vals = prices.get(ticker)
            if not vals:
                lines.append(f"{ticker:<8}không có dữ liệu lần này")
                continue
            pct = _pct_change(vals)
            change_str = f"{_pct_arrow_word(pct)} {pct:+.2f}%" if pct is not None else ""
            lines.append(
                f"{ticker:<8}{vals['close']:,.0f}{'':<12}{change_str:<14}"
                f"{vals.get('volume', 0):,.0f}{'':<10}{used_source.get(ticker, '?')}"
            )
        lines.append("")

    trend = weekly_trend_data()
    if trend:
        lines.append("")
        lines.append("Xu hướng tuần (thay đổi 7 ngày)")
        lines.append("-" * 42)
        for ticker, pct in trend:
            lines.append(f"{ticker:<8}{_pct_arrow_word(pct)} {pct:+.2f}% trong tuần qua")

    sources_used = sorted(set(used_source.values()))
    lines.append("")
    if sources_used:
        lines.append(f"Nguồn dữ liệu lần này: {', '.join(sources_used)}")
    lines.append(
        "Lưu ý: đây là các nguồn dữ liệu công khai từ ứng dụng của từng nhà cung cấp, "
        "không phải API chính thức/được đảm bảo. Vui lòng kiểm tra lại với công ty "
        "chứng khoán của bạn trước khi giao dịch dựa trên các số liệu này."
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
<html lang="vi">
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:24px 0;">
<tr><td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.08);">

<tr><td style="background:{_NAVY};padding:24px 28px;">
  <div style="color:#ffffff;font-size:20px;font-weight:700;">Danh Mục Cổ Phiếu Việt Nam</div>
  <div style="color:#94a3b8;font-size:13px;margin-top:4px;">{format_vn_datetime(now_vn())} (Giờ Việt Nam)</div>
</td></tr>
""")

    # --- Market indices ---
    all_index_labels = [label for _code, label in INDICES]
    if any(label in indices for label in all_index_labels):
        cells = []
        for label in all_index_labels:
            vals = indices.get(label)
            if not vals:
                cells.append(f"""\
<td width="33%" style="padding:14px 10px;text-align:center;border-right:1px solid {_BORDER};">
  <div style="font-size:12px;color:{_GRAY};font-weight:600;text-transform:uppercase;letter-spacing:0.04em;">{_html_escape(label)}</div>
  <div style="font-size:12px;color:{_GRAY};margin-top:8px;">không có dữ liệu lần này</div>
</td>""")
                continue
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
    gainers, losers = top_movers(prices)
    if gainers or losers:
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
                f'<div style="margin-bottom:6px;"><span style="font-size:12px;color:{_GRAY};font-weight:600;">TĂNG GIÁ&nbsp;</span>'
                + "".join(_chip(t, p) for t, p in gainers) + "</div>"
            )
        if losers:
            rows.append(
                f'<div><span style="font-size:12px;color:{_GRAY};font-weight:600;">GIẢM GIÁ&nbsp;</span>'
                + "".join(_chip(t, p) for t, p in losers) + "</div>"
            )
        parts.append(f"""\
<tr><td style="padding:16px 28px 4px 28px;">
  {''.join(rows)}
</td></tr>
""")

    # --- Price table, grouped by exchange ---
    exchange_sections = []
    for exch, tickers in watchlist_by_exchange().items():
        row_html = []
        for i, ticker in enumerate(tickers):
            vals = prices.get(ticker)
            stripe = "#ffffff" if i % 2 == 0 else "#f8fafc"
            if not vals:
                row_html.append(f"""\
<tr style="background:{stripe};">
  <td style="padding:10px 12px;font-weight:700;color:{_NAVY};">{_html_escape(ticker)}</td>
  <td colspan="4" style="padding:10px 12px;color:{_GRAY};font-size:13px;">không có dữ liệu lần này</td>
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

        exchange_sections.append(f"""\
  <div style="margin:16px 0 8px 0;">
    <span style="display:inline-block;padding:2px 9px;border-radius:5px;background:{_NAVY};color:#ffffff;font-size:11px;font-weight:700;letter-spacing:0.04em;">{exch}</span>
    <span style="font-size:12px;color:{_GRAY};margin-left:6px;">{len(tickers)} mã</span>
  </div>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid {_BORDER};border-radius:8px;overflow:hidden;font-size:14px;">
    <tr style="background:#f8fafc;">
      <th align="left" style="padding:8px 12px;font-size:11px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.04em;">Mã CK</th>
      <th align="right" style="padding:8px 12px;font-size:11px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.04em;">Giá đóng cửa (VNĐ)</th>
      <th align="center" style="padding:8px 12px;font-size:11px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.04em;">Thay đổi</th>
      <th align="right" style="padding:8px 12px;font-size:11px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.04em;">Khối lượng</th>
      <th align="right" style="padding:8px 12px;font-size:11px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.04em;">Nguồn</th>
    </tr>
    {''.join(row_html)}
  </table>""")

    parts.append(f"""\
<tr><td style="padding:20px 28px 4px 28px;">
  <div style="font-size:13px;font-weight:700;color:{_NAVY};text-transform:uppercase;letter-spacing:0.04em;">Giá Đóng Cửa</div>
  {''.join(exchange_sections)}
</td></tr>
""")

    # --- Weekly trend ---
    trend = weekly_trend_data()
    if trend:
        trend_rows = []
        for ticker, pct in trend:
            color, _bg, arrow = _pct_style(pct)
            trend_rows.append(
                f'<div style="padding:4px 0;font-size:13px;">'
                f'<span style="font-weight:700;color:{_NAVY};display:inline-block;width:56px;">{_html_escape(ticker)}</span>'
                f'<span style="color:{color};font-weight:600;">{arrow} {pct:+.2f}% trong tuần qua</span></div>'
            )
        parts.append(f"""\
<tr><td style="padding:20px 28px 4px 28px;">
  <div style="font-size:13px;font-weight:700;color:{_NAVY};text-transform:uppercase;letter-spacing:0.04em;margin-bottom:8px;">Xu Hướng Tuần (Thay Đổi 7 Ngày)</div>
  {''.join(trend_rows)}
</td></tr>
""")

    # --- Footer ---
    sources_used = sorted(set(used_source.values()))
    sources_line = f"Nguồn dữ liệu lần này: {_html_escape(', '.join(sources_used))}" if sources_used else ""
    parts.append(f"""\
<tr><td style="padding:20px 28px 28px 28px;">
  <div style="border-top:1px solid {_BORDER};padding-top:14px;font-size:11px;color:#9ca3af;line-height:1.5;">
    {sources_line}<br>
    Đây là các nguồn dữ liệu công khai từ ứng dụng của từng nhà cung cấp, không phải API
    chính thức/được đảm bảo. Vui lòng kiểm tra lại với công ty chứng khoán của bạn trước
    khi giao dịch dựa trên các số liệu này.
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
        msg.attach(MIMEText(text_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))
    else:
        msg = MIMEText(text_body, "plain", "utf-8")

    msg["Subject"] = f"Bảng Giá Cổ Phiếu Việt Nam - {now_vn().strftime('%Y-%m-%d %H:%M')}"
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


def cmd_test_sources():
    """Diagnostic mode: calls every stock/index source directly and
    independently for the full watchlist, regardless of whether an earlier
    source in the cascade already covered a given ticker. The normal
    'generate' run only calls a source for gaps the previous ones left, so
    a source that's fully redundant on a given day (e.g. Yahoo covering
    everything) never actually gets exercised - this bypasses that so you
    can confirm each source still works on its own.
    """
    print(f"Testing {len(STOCK_SOURCES)} stock source(s) against {len(WATCHLIST)} ticker(s): {', '.join(WATCHLIST)}\n")

    for name, fetch_fn in STOCK_SOURCES:
        try:
            result = fetch_fn(list(WATCHLIST))
        except Exception as e:
            print(f"{name}: FAILED ENTIRELY - {e}")
            continue
        hits = [t for t in WATCHLIST if t in result]
        misses = [t for t in WATCHLIST if t not in result]
        print(f"{name}: {len(hits)}/{len(WATCHLIST)} tickers returned")
        if hits:
            sample = hits[0]
            print(f"  sample: {sample} -> {result[sample]}")
        if misses:
            print(f"  missing: {', '.join(misses)}")
        print()

    print(f"Testing {len(INDEX_SOURCES)} index source(s) against {len(INDICES)} index(es)\n")
    index_labels = [label for _code, label in INDICES]
    for name, fetch_fn in INDEX_SOURCES:
        try:
            result = fetch_fn()
        except Exception as e:
            print(f"{name}: FAILED ENTIRELY - {e}")
            continue
        hits = [label for label in index_labels if label in result]
        misses = [label for label in index_labels if label not in result]
        print(f"{name}: {len(hits)}/{len(index_labels)} indices returned")
        for label in hits:
            print(f"  {label}: {result[label]}")
        if misses:
            print(f"  missing: {', '.join(misses)}")
        print()


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "generate"
    if command == "generate":
        cmd_generate()
    elif command == "send":
        cmd_send()
    elif command == "test-sources":
        cmd_test_sources()
    else:
        print(f"Unknown command: {command}. Use 'generate', 'send', or 'test-sources'.")
