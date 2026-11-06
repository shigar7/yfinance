# IVV Prediction

An indicative IVV.AX open, and a backtest of how well the same conversion has
called the real open. All data comes from Yahoo through `yfinance`.

    open = US IVV close / AUDUSD / 15 x (1 + ES move since the US close)

The backtest leaves out the ES term: US close / AUDUSD / 15 against the real open.

- **Pairing:** each ASX open is matched by timestamp to the latest US close before
  it, so a holiday in either market can't shift rows. Opens that follow a US
  holiday use the older close and are flagged.
- **FX:** the backtest uses the hourly `AUDUSD=X` bar ending at 16:00 ET. Yahoo
  keeps about 730 days of hourly FX, which caps the backtest. US half-days closed
  at 13:00 ET but still use the 16:00 rate.
- **Prices:** `auto_adjust=False`, so these are traded prices, not adjusted for
  dividends. Neither fund's ex-dividend gap is corrected for.
- **15:1:** the median implied ratio is shown on the page, so a drift would be
  visible. Yahoo records no split and its history is split-adjusted, so the data
  supports the ratio but can't date a split.
- **Not known here:** ASX and US holiday calendars, for the "next open" and for
  sessions still to come.

## Run

    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    ./service.sh start     # 127.0.0.1:9003

There is no auth here: the gateway in `../..` handles login, so this must only
listen on loopback. The page works at any path, with or without a trailing slash,
because it builds its API URLs from `location.pathname`.
