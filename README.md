# Stonks Tracker

A local stock tracker: tabbed watchlists, quotes, and historical charts pulled
from Yahoo Finance via [`yfinance`](https://github.com/ranaroussi/yfinance).

**No Yahoo account or login required** — it reads the public quote endpoints.

## Run

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
./run.sh                 # http://localhost:7777
```

```bash
PORT=8080 ./run.sh       # different port
HOST=127.0.0.1 ./run.sh  # loopback only (default binds 0.0.0.0)
```

## ⚠️ There is no authentication

Every endpoint is unauthenticated, so anyone who can reach the port can read and
modify your lists. That is fine on `localhost` or over a private network
(Tailscale, WireGuard, an SSH tunnel). **Do not expose this to the internet
without putting auth in front of it.**

Note also that `yfinance` is an unofficial scraper. A publicly reachable
instance serving real traffic will get rate-limited or IP-blocked by Yahoo, and
Yahoo's terms don't permit redistributing their data. Personal use behind auth
avoids both problems.

## Tabs

Lists live in `watchlist.json` and are editable from the page: **+ New list** to
add, and `×` on the active tab to delete. Symbols reorder within a list with the
`▲▼` arrows on each row.

Renaming and reordering *tabs* are not in the UI, but the endpoints are still
there if you want them (`PATCH /api/lists/{id}` and `POST /api/lists/{id}/move`).

Use Yahoo's suffixes — `.AX` for ASX, `=F` for futures, `^` for indices. Some
bare tickers resolve to something you did not mean: `QAU` is a US fund, while
`QAU.AX` is Betashares Gold. Symbols are validated against Yahoo before they are
saved, but a symbol that resolves to the *wrong* instrument still resolves.

## Table columns

| Column | Source |
|---|---|
| Symbol | click to chart it |
| Close | `fast_info.lastPrice`, falling back to the last settled close |
| Day % | vs previous close |
| 1M % | vs the last close on or before 30 calendar days ago |
| 52-week range | meter: the last price's position between the 1y low and high |
| 1 year | inline area chart of the last year of closes |

## Three things worth knowing

**In-progress sessions come back as NaN.** Yahoo's daily bar for a session that
has not settled has NaN OHLC — notably on `.AX` — so `Close.iloc[-1]` is NaN
during ASX hours. `fetch_quote()` reads the live price from `fast_info` and
`dropna()`s the series.

**Exchanges keep different calendars.** ASX and CME holidays do not line up, so
series of different lengths would misalign if charted by array position. The
compare endpoint reindexes every series onto the union of dates and
forward-fills — `align()` in `app.py`.

**Quotes are fetched in parallel.** Each quote costs roughly three Yahoo round
trips, so a seven-symbol tab is twenty-one requests. They go through a thread
pool (`cached_many()`), which makes a cold tab load roughly flat with symbol
count rather than linear.

**Colour is pinned per symbol, not per row.** `ensure_slots()` assigns each
symbol a stable colour slot that survives reordering and deletion, so removing
one symbol never repaints the ones below it.

## Charts

Clicking a row charts that symbol at the finest interval Yahoo will serve for
the window, stepping down automatically when Yahoo refuses:

| Period | Interval | Yahoo's cap |
|---|---|---|
| 1D | 1m | 7 days |
| 5D | 5m | 60 days |
| 1M | 30m | 60 days |
| 3M–1Y | 1h | 730 days |
| 5Y / MAX | 1d / 1wk | — |

The relative-performance chart indexes every series to 100 at the period start,
so AUD and USD instruments compare on one axis. A dual-axis chart of raw prices
would invent correlations that are not in the data.

## API

| Endpoint | Purpose |
|---|---|
| `GET /api/lists` | all lists |
| `POST /api/lists` `{"name":"AUS"}` | create |
| `PATCH /api/lists/{id}` `{"name":"..."}` | rename |
| `DELETE /api/lists/{id}` | delete (the last list is protected) |
| `POST /api/lists/{id}/move` `{"delta":-1}` | reorder tabs |
| `POST /api/lists/{id}/symbols` `{"symbol":"GC=F"}` | add (validated first) |
| `DELETE /api/lists/{id}/symbols/{symbol}` | remove |
| `POST /api/lists/{id}/symbols/{symbol}/move` `{"delta":1}` | reorder symbols |
| `GET /api/quotes?list_id={id}` | rows for one list (60s cache) |
| `GET /api/history?symbols=A,B&period=1Y` | aligned closes for the compare chart |
| `GET /api/history?symbols=A&period=1Y&granular=true` | finest-interval series |
| `POST /api/refresh` | drop all caches |

Periods: `1D 5D 1M 3M 6M YTD 1Y 5Y MAX`.

## Note on `watchlist.json`

Your lists are committed to this repo, so a fresh clone comes up populated. The
app rewrites the file as you use it, so it will show up as modified in
`git status`. If that churn is annoying:

```bash
git rm --cached watchlist.json && echo watchlist.json >> .gitignore
```
