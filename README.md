# vn-stock-price-emailer

Emails a daily summary of a Vietnam stock watchlist (HOSE/HNX/UPCOM tickers)
plus VN-Index / HNX-Index / UPCOM-Index. Same shape as its siblings
[currency-rate-emailer](https://github.com/tuongphantrue/currency-rate-emailer),
[gold-price-emailer](https://github.com/tuongphantrue/gold-price-emailer), and
[tech-price-mailer](https://github.com/tuongphantrue/tech-price-mailer): runs
on GitHub Actions on a schedule, no server to keep on, pulls from a couple of
independent free sources and degrades gracefully if one is down.

## What it does

- Fetches daily closing prices for your watchlist from **TCBS**'s public
  price feed (the same feed the `vnstock` Python package wraps)
- Cross-checks against **VNDirect**'s public quotes feed and flags any
  ticker where the two disagree by more than a threshold
- Pulls VN-Index / HNX-Index / UPCOM-Index levels
- Highlights top gainers/losers in your watchlist
- Tracks history to `price_history.csv` and emails a 7-day trend once a week
- Optional: only sends when something moved more than a threshold %

## Important caveat

TCBS's and VNDirect's feeds used here are the **public JSON endpoints behind
their own web/mobile apps** -- not documented, versioned, or guaranteed
APIs. They can change shape, rate-limit, or block traffic from cloud IPs
(like GitHub Actions runners) without notice, the same way Vietcombank's
feed sometimes does in `currency-rate-emailer`. Treat this as a personal
watchlist/notification tool, not a trading system -- always confirm prices
with your broker before acting on them. If you need contractual reliability,
swap in SSI FastConnect, a paid VN market data vendor, or vnstock's
maintained wrapper instead of hitting these endpoints directly.

## Setup

1. Fork/clone this repo.
2. In **Settings -> Secrets and variables -> Actions**, add secrets:
   - `GMAIL_ADDRESS` -- sender Gmail address
   - `GMAIL_APP_PASSWORD` -- a [Gmail App Password](https://myaccount.google.com/apppasswords) (not your normal password)
   - `STOCK_RECIPIENT` -- recipient email address
3. Optionally add repo **variables** (Settings -> Secrets and variables ->
   Actions -> Variables) to override defaults:
   - `WATCHLIST` -- comma-separated tickers, e.g. `VNM,VIC,VHM,HPG,FPT,MWG,VCB,TCB,MBB,SSI`
   - `ALERT_THRESHOLD_PERCENT` -- only email if some stock moved >= this % (leave unset to always send)
   - `DISCREPANCY_THRESHOLD_PERCENT` -- flag TCBS/VNDirect disagreement >= this % (default `1.0`)
4. The workflow runs weekdays at 09:15 Vietnam time by default (adjust the
   cron in `.github/workflows/send-stock-price.yml`). You can also trigger
   it manually from the Actions tab (`workflow_dispatch`).

## Local usage

```bash
pip install -r requirements.txt

export GMAIL_ADDRESS=you@gmail.com
export GMAIL_APP_PASSWORD=xxxxxxxxxxxxxxxx
export STOCK_RECIPIENT=you@gmail.com
export WATCHLIST=VNM,VIC,VHM,HPG,FPT

python vn_stock_price_emailer.py generate
python vn_stock_price_emailer.py send
```

`generate` fetches prices, updates `last_prices.json` and
`price_history.csv`, and writes `email_body.txt`. `send` mails whatever is
in `email_body.txt`. They're split so you can inspect the body before
sending, and so a failed send doesn't lose the fetched data.
