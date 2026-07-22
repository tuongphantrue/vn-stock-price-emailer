# vn-stock-price-emailer

Emails a daily summary of a Vietnam stock watchlist (HOSE/HNX/UPCOM tickers)
plus VN-Index / HNX-Index / UPCOM-Index, in Vietnamese, as a styled HTML
email. Same shape as its siblings
[currency-rate-emailer](https://github.com/tuongphantrue/currency-rate-emailer),
[gold-price-emailer](https://github.com/tuongphantrue/gold-price-emailer), and
[tech-price-mailer](https://github.com/tuongphantrue/tech-price-mailer): runs
on GitHub Actions on a schedule, no server to keep on, pulls from several
independent free sources and degrades gracefully if one is down.

## What it does

- Fetches closing prices for a 56-ticker watchlist across all three
  Vietnamese exchanges (HOSE, HNX, UPCOM) from a cascade of sources -
  **Yahoo Finance -> TradingView -> CafeF -> VNDirect** - first source to
  answer for a given ticker wins; a block on one doesn't take out the rest
- Also queries **SSI** (Vietnam's largest brokerage) independently and
  unconditionally every run, shown as its own dedicated section for
  cross-checking against the cascade above, rather than folded into it
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
  otherwise never get called (e.g. Yahoo already covering everything)

## Important caveats

None of these are documented, versioned, or guaranteed APIs - they're the
public endpoints behind each provider's own app, reverse-engineered rather
than officially supported. They can change shape, rate-limit, or block
traffic without notice. Treat this as a personal watchlist/notification
tool, not a trading system - always confirm prices with your broker before
acting on them.

**Yahoo Finance and TradingView** are globally-hosted and have worked
reliably from GitHub Actions. **CafeF, VNDirect, and SSI** are Vietnamese
domestic sites and have each been confirmed blocked from GitHub Actions'
cloud IPs in three different ways (CafeF: a fake-success empty response;
VNDirect: the connection times out; SSI: an outright 403 Forbidden) - same
likely root cause (a WAF rejecting known datacenter IP ranges), three
different symptoms. They're kept in the code because they may still work
run from a non-cloud IP (your own machine, a self-hosted runner, or via a
proxy - see below), and cost nothing to try even when they fail.

## Setup

1. Fork/clone this repo.
2. In **Settings -> Secrets and variables -> Actions -> Secrets**, add:
   - `GMAIL_ADDRESS` -- sender Gmail address
   - `GMAIL_APP_PASSWORD` -- a [Gmail App Password](https://myaccount.google.com/apppasswords) (not your normal password)
   - `STOCK_RECIPIENT` -- recipient email address
   - `PROXY_URL` *(optional)* -- see "Proxy support" below
3. Optionally add repo **variables** (same page, Variables tab) to
   override defaults:
   - `WATCHLIST` -- comma-separated tickers, overrides the built-in 56-ticker default entirely
   - `ALERT_THRESHOLD_PERCENT` -- only email if some stock moved >= this % (leave unset to always send)
   - `DEBUG_EMPTY_RESPONSES` -- set to `1` to log the actual HTTP status/body when a source returns nothing, instead of failing silently
4. The workflow runs on the schedule in
   `.github/workflows/send-stock-price.yml`. You can also trigger it
   manually from the Actions tab (`workflow_dispatch`).

## Proxy support

CafeF, VNDirect, and SSI are blocked from GitHub Actions' cloud IPs (see
above). If you want them to actually work rather than sit as unused
fallbacks, route them through a proxy by setting the `PROXY_URL` secret:

```
http://user:pass@host:port
socks5://user:pass@host:port
```

**A datacenter proxy is unlikely to help** - the whole problem is
datacenter IP ranges getting blocked in the first place. What actually
helps is a proxy service offering **residential or "ISP" IPs**. Providers
commonly used for this (no endorsement, just commonly seen for this exact
kind of anti-bot bypass): Bright Data, Smartproxy, Webshare, IPRoyal,
ScraperAPI. Free tiers from these are usually datacenter-based and may
still get blocked the same way GitHub Actions' own IPs are - a paid
residential tier is what's actually likely to work.

Yahoo Finance and TradingView are never routed through the proxy, since
they don't need it and it would just spend proxy bandwidth for no benefit.

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
