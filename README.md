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

### Running it as a daemon

`run.sh` stays in the foreground, which is right for a terminal and wrong for
anything that has to survive you closing it. `service.sh` backgrounds it with a
PID file:

```bash
./service.sh start       # $PORT, default 8000
./service.sh status
./service.sh restart
./service.sh stop
```

`start` is idempotent, so a cron entry doubles as a keep-alive — it brings the
app back within five minutes of a crash, and starts it after a reboot:

```cron
@reboot     /path/to/yfinance/service.sh start >> /path/to/yfinance/cron.log 2>&1
*/5 * * * * /path/to/yfinance/service.sh start >> /path/to/yfinance/cron.log 2>&1
```

If the port is already held by something else, `start` says so and exits rather
than letting uvicorn fail and cron retry forever with no explanation.

In a container there is usually no init, so `crond` is not running and neither
of those lines will ever fire. Start it from the entrypoint, not just by hand —
otherwise the schedule dies at the next restart and takes every other cron job
with it.

## ⚠️ There is no authentication

Every endpoint is unauthenticated, so anyone who can reach the port can read and
modify your lists. That is fine on `localhost` or over a private network
(Tailscale, WireGuard, an SSH tunnel). **Do not expose this to the internet
without putting auth in front of it.**

Note also that `yfinance` is an unofficial scraper. A publicly reachable
instance serving real traffic will get rate-limited or IP-blocked by Yahoo, and
Yahoo's terms don't permit redistributing their data. Personal use behind auth
avoids both problems.

## On a phone

The layout restacks below 620px: the table becomes one card per row, and the
charts drop their desktop margins. There is a manifest and an icon set, so
**Add to Home screen** gives a real icon and name rather than a thumbnail.

Whether you get a *dedicated window* or just a tab depends on the browser:

| | Result |
|---|---|
| Desktop Chrome | ⋮ → Create shortcut → **Open as window**. Works over plain http. |
| iOS Safari | Share → Add to Home Screen. Standalone, over plain http — iOS keys off `apple-mobile-web-app-capable`, not installability. |
| Android Chrome | A tab, unless the origin is https. No workaround. |

Chrome only offers **Install app** on a secure origin, so over http on a LAN
the install path is closed regardless of what the page contains. Everything
else it checks is already in place — manifest, 192 and 512 icons, `standalone`,
a service worker with a fetch handler — so putting a cert in front of it is the
only change needed. `sw.js` registers behind an `isSecureContext` guard and does
not cache: the app is live market data, and a stale cache would show yesterday's
prices under today's timestamp.

## Tabs

Lists live in `watchlist.json` and are editable from the page: **+ New list** to
add, and `×` on the active tab to delete. Symbols reorder within a list with the
`▲▼` arrows on each row.

Renaming and reordering *tabs* are not in the UI, but the endpoints are still
there if you want them (`PATCH /api/lists/{id}` and `POST /api/lists/{id}/move`).

The period row sits between the table and the charts, because it only ever
scoped the charts: the table is always a one-year window, and Day % and 1M %
are fixed columns.

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
| 52-week range | meter: the last price's position between the 1y low and high, as traded |
| 1 year | inline area chart of the last year of closes |

## A few things worth knowing

**In-progress sessions come back as NaN.** Yahoo's daily bar for a session that
has not settled has NaN OHLC — notably on `.AX` — so `Close.iloc[-1]` is NaN
during ASX hours. `fetch_quote()` reads the live price from `fast_info` and
`dropna()`s the series.

**Exchanges keep different calendars.** ASX and CME holidays do not line up, so
series of different lengths would misalign if charted by array position. The
compare endpoint reindexes every series onto the union of dates and
forward-fills — `align()` in `app.py`.

**Everything is cached for three hours, on disk.** Quotes, names and every
history series land in `cache.sqlite` next to the app, and a request only goes
to Yahoo when what is held is older than three hours — so switching tabs or
stocks you have already looked at is a local read, not a round trip, and a
restart comes back warm. The header's *as of* is when the oldest row on the tab
was fetched, not when it was served. **Refresh** drops the whole cache; a fetch
that failed (rate limit, bad symbol) is retried after a minute rather than
pinned for three hours. Delete the file if you want to start cold.

**Quotes are fetched in parallel.** Each quote costs roughly three Yahoo round
trips, so a seven-symbol tab is twenty-one requests. On a cold tab they go
through a thread pool (`cached_many()`), which makes the load roughly flat with
symbol count rather than linear.

**Only one chart includes distributions.** yfinance defaults to
`auto_adjust=True`, which back-adjusts historical closes for dividends, so a
series is total return unless you say otherwise. That is right for the compare
chart, where a 3.5% yielder next to a 1.0% one would otherwise read as a
laggard, and wrong everywhere else — a 52-week low nobody ever paid is not a
52-week low. So `fetch_quote()` and the detail chart pass `auto_adjust=False`
and the compare chart does not.

Yahoo never adjusts intraday bars, which is what made this worth chasing: the
detail chart was already on real prices up to 1Y (1m–1h bars) and silently
switched to total return at 3Y, where the interval steps down to daily. VHY's
MAX low read 19.74 against a floor of 43.21. Splits are handled either way —
Yahoo's raw OHLC is already split-adjusted — so `auto_adjust=False` does not
reintroduce NVDA's 10:1 cliff.

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
| 6M–1Y | 1h | 730 days |
| 3Y / 5Y | 1d | — |
| MAX | 1wk | — |

`3Y` is the one window Yahoo has no period string for — its vocabulary goes
`2y` then `5y` — so it is fetched by start date instead (`raw_history()`).

The detail chart carries a second y-axis on the right showing each price as a
percentage of the period high (`price / high - 1`). This is not a dual-axis
chart: it is the same scale relabelled, so the gridlines are shared and every
price maps to exactly one percentage. The high is taken from the series actually
plotted, which uses intraday bars for most periods — so it can differ by a few
cents from the 52-week high in the table, which uses daily closes.

The relative-performance chart indexes every series to 100 at the period start,
so AUD and USD instruments compare on one axis. A dual-axis chart of raw prices
would invent correlations that are not in the data.

The selected symbol — whichever row is driving the detail chart — draws solid
and paints last, so it crosses over the bundle; the rest drop to 0.28 alpha and
stay as context. Colours do not change, so the legend still reads. Emphasis is
resolved against the series actually drawn: hiding the selected symbol from the
legend leaves all of them solid rather than fading every line at once.

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
| `GET /api/quotes?list_id={id}` | rows for one list |
| `GET /api/history?symbols=A,B&period=1Y` | aligned closes for the compare chart |
| `GET /api/history?symbols=A&period=1Y&granular=true` | finest-interval series |
| `POST /api/refresh` | drop the cache, memory and disk |
| `GET /` | the page |
| `GET /sw.js` | service worker, served from the root so its scope covers the origin |
| `GET /static/…` | icons and the web manifest |

Periods: `1D 5D 1M 6M YTD 1Y 3Y 5Y MAX`.

## Note on `watchlist.json`

Your lists are committed to this repo, so a fresh clone comes up populated. The
app rewrites the file as you use it, so it will show up as modified in
`git status`. If that churn is annoying:

```bash
git rm --cached watchlist.json && echo watchlist.json >> .gitignore
```
