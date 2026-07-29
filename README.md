# vn-stock-price-emailer

Emails a summary of Vietnam's top stocks by market cap (HOSE/HNX/UPCOM)
plus VN-Index / HNX-Index / UPCOM-Index, in Vietnamese, as a styled HTML
email. Same shape as its siblings
[currency-rate-emailer](https://github.com/tuongphantrue/currency-rate-emailer),
[gold-price-emailer](https://github.com/tuongphantrue/gold-price-emailer), and
[tech-price-mailer](https://github.com/tuongphantrue/tech-price-mailer): runs
on GitHub Actions on a schedule, no server to keep on, pulls from a few
independent free sources and degrades gracefully if one is down.

## What it does

- Tracks the **top 250 Vietnamese stocks by market cap** (configurable via
  `TOP_N_STOCKS`) across all three exchanges, resolved dynamically each
  day via TradingView's scanner - not a hand-typed static list, so it
  stays accurate as rankings shift and doesn't need manual updates when a
  company migrates exchanges (HNX's ~299 listed stocks are being
  transferred to HOSE by the end of 2026 under a market restructuring, for
  instance - a dynamic lookup just reflects wherever a stock currently is)
- Fetches closing prices from a cascade of three independent sources -
  **TradingView -> Yahoo Finance -> MSN Finance** - first source to
  answer for a given ticker wins; a block on one doesn't take out the
  rest. TradingView goes first since it can fetch the whole watchlist in
  one batched call; Yahoo (one request per ticker) and MSN only pick up
  whatever TradingView misses, keeping their request volume low even at
  250+ tickers
- Pulls VN-Index / HNX-Index / UPCOM-Index levels
- Groups the price table by exchange (HOSE / HNX / UPCOM), each with its
  own section
- Highlights top gainers/losers across the whole watchlist
- Tracks history to `price_history.csv` and emails a 7-day trend once a
  week (first run after midnight Monday, Vietnam time)
- Optional: only sends when something moved more than a threshold %
- A `test-sources` diagnostic command that exercises every source
  independently, bypassing the normal "only call what's still missing"
  cascade logic, so you can check a source's health even when it would
  otherwise never get called

## How the watchlist is determined

1. **`WATCHLIST` env var, if set** - a full manual override (comma-separated
   tickers), unchanged from earlier versions of this script.
2. **Otherwise, a live top-N-by-market-cap query** against TradingView,
   cached to `watchlist_cache.json` for 24 hours (market-cap rankings
   don't meaningfully shift run-to-run, so there's no reason to re-query
   every 30 minutes). A few specific tickers (currently just `YEG`) are
   always included regardless of the cutoff, since they were added by
   explicit request rather than a market-cap ranking.
3. **If that query fails and there's no usable cache**, falls back to a
   curated ~56-ticker list baked into the script, so a TradingView hiccup
   degrades to "the old watchlist" rather than "no watchlist at all."

## Important caveats

None of TradingView, Yahoo Finance, or MSN Finance are documented,
versioned, or guaranteed APIs - they're public JSON endpoints behind each
provider's own app, reverse-engineered rather than officially supported.
They can change shape, rate-limit, or block traffic without notice. Treat
this as a personal watchlist/notification tool, not a trading system -
always confirm prices with your broker before acting on them.

Three domestic Vietnamese sources - CafeF, VNDirect, and SSI's iBoard -
were also tried and removed. Each was confirmed blocked from GitHub
Actions' cloud IPs in a different way (CafeF: a fake-success empty
response; VNDirect: connection timeout; SSI: 403 Forbidden) despite being
correctly implemented against real, working endpoints - same likely root
cause (a WAF rejecting known datacenter IP ranges), three different
symptoms. A proxy service offering residential/ISP IPs (not datacenter)
would likely be needed for a domestic VN source to work from a cloud CI
runner; not something this version of the script attempts.

## Setup

1. Fork/clone this repo.
2. In **Settings -> Secrets and variables -> Actions -> Secrets**, add:
   - `GMAIL_ADDRESS` -- sender Gmail address
   - `GMAIL_APP_PASSWORD` -- a [Gmail App Password](https://myaccount.google.com/apppasswords) (not your normal password)
   - `STOCK_RECIPIENT` -- recipient email address
3. Optionally add repo **variables** (same page, Variables tab) to
   override defaults:
   - `WATCHLIST` -- comma-separated tickers, bypasses the dynamic top-N lookup entirely
   - `TOP_N_STOCKS` -- how many stocks to track when `WATCHLIST` isn't set (default `250`)
   - `ALERT_THRESHOLD_PERCENT` -- only email if some stock moved >= this % (leave unset to always send)
   - `DEBUG_EMPTY_RESPONSES` -- set to `1` to log the actual HTTP status/body when a source returns nothing, instead of failing silently
4. The workflow runs on the schedule in
   `.github/workflows/send-stock-price.yml`. You can also trigger it
   manually from the Actions tab (`workflow_dispatch`).

## Local usage

```bash
pip install -r requirements.txt

export GMAIL_ADDRESS=you@gmail.com
export GMAIL_APP_PASSWORD=xxxxxxxxxxxxxxxx
export STOCK_RECIPIENT=you@gmail.com

python vn_stock_price_emailer.py generate       # fetch prices, build email_body.txt / email_body.html
python vn_stock_price_emailer.py send           # send whatever generate produced
python vn_stock_price_emailer.py test-sources   # diagnostic: test every source independently
```

`generate` and `send` are split so you can inspect the body before
sending, and so a failed send doesn't lose the fetched data.
