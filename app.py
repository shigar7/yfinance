"""Stonks Tracker — tabbed watchlists + historical charts, backed by yfinance.

Run:  ./run.sh          (or: .venv/bin/uvicorn app:app --port 7777)
Then: http://localhost:7777
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
from fastapi import FastAPI, HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).parent
STATIC = ROOT / "static"
WATCHLIST_FILE = ROOT / "watchlist.json"
SHORT_TERM_FILE = ROOT / "short_term.json"   # which short-term lines are hidden
ALERTS_FILE = ROOT / "alerts.json"
ALERT_TOPIC_FILE = ROOT / "alert_topic.txt"  # the name is the secret, so not in git
NTFY = "https://ntfy.sh"
ALERT_TZ = ZoneInfo("Australia/Sydney")
ALERT_HOUR = 17
MAX_LISTS = 24

DEFAULT_SYMBOLS = ["ES=F", "CL=F", "IVV.AX", "VAS.AX", "VGB.AX"]

# Compare chart: daily/weekly bars, aligned across exchanges.
PERIODS = {
    "1D":  ("5d", "1d"),
    "5D":  ("1mo", "1d"),
    "1M":  ("1mo", "1d"),
    "6M":  ("6mo", "1d"),
    "YTD": ("ytd", "1d"),
    "1Y":  ("1y", "1d"),
    "3Y":  ("3y", "1d"),
    "5Y":  ("5y", "1wk"),
    "MAX": ("max", "1mo"),
}

# Detail chart: the finest interval Yahoo will serve for each window.
# Yahoo's caps - 1m: 7d | 2m-90m: 60d | 1h: 730d | 1d+: unlimited.
DETAIL_PERIODS = {
    "1D":  ("1d", "1m"),
    "5D":  ("5d", "5m"),
    "1M":  ("1mo", "30m"),
    "6M":  ("6mo", "1h"),
    "YTD": ("ytd", "1h"),
    "1Y":  ("1y", "1h"),
    "3Y":  ("3y", "1d"),
    "5Y":  ("5y", "1d"),
    "MAX": ("max", "1wk"),
}
INTRADAY = {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"}

# Yahoo's period vocabulary jumps straight from 2y to 5y, so the windows it
# has no word for are fetched by start date instead.
WINDOW_DAYS = {"3y": 365 * 3}

# Coarser fallbacks to try when Yahoo returns nothing for a fine interval.
FALLBACK = {"1m": "5m", "5m": "30m", "30m": "1h", "1h": "1d", "1d": "1wk", "1wk": "1mo"}

# Each quote costs ~3 Yahoo round trips (history, fast_info, info), so a
# 7-symbol tab is 21 sequential requests if fetched in a loop. Fan them out.
POOL = ThreadPoolExecutor(max_workers=12, thread_name_prefix="yf")

# Everything Yahoo hands back is kept for three hours, on disk, so a tab or
# symbol you have already looked at comes back without a round trip — and so
# a restart does not start cold. Refresh drops the lot.
CACHE_TTL = 3 * 3600    # seconds
# A failed fetch (rate limit, bad symbol, network) must not be pinned for
# three hours: retry it after a minute.
ERROR_TTL = 60
CACHE_FILE = ROOT / "cache.sqlite"


@asynccontextmanager
async def lifespan(_: FastAPI):
    threading.Thread(target=alert_loop, daemon=True, name="alerts").start()
    yield


app = FastAPI(title="Stonks Tracker", lifespan=lifespan)
# These payloads are long arrays of numbers and near-identical date strings,
# which is close to the best case for gzip: a year of hourly bars goes from
# ~64KB to ~8KB. Worth far more than server-side speed when the client is a
# phone on the other end of a slow link.
app.add_middleware(GZipMiddleware, minimum_size=500)


# --------------------------------------------------------------------------
# TTL cache: memory in front, sqlite behind
# --------------------------------------------------------------------------
# Values are the JSON the endpoints return (dicts and strings), so they go to
# disk as-is. One row per key; the memory dict is only a copy of what is on
# disk, so a cold process reads through to it and warms itself up.
_cache: dict[str, tuple[float, object]] = {}
_lock = threading.Lock()
_db = sqlite3.connect(CACHE_FILE, check_same_thread=False)
_db.execute("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, at REAL, value TEXT)")
_db.commit()


def _disk_get(key: str) -> tuple[float, object] | None:
    row = _db.execute("SELECT at, value FROM cache WHERE key = ?", (key,)).fetchone()
    if not row:
        return None
    try:
        return row[0], json.loads(row[1])
    except (json.JSONDecodeError, TypeError):
        return None


def _disk_put(key: str, at: float, value) -> None:
    _db.execute("INSERT OR REPLACE INTO cache (key, at, value) VALUES (?, ?, ?)",
                (key, at, json.dumps(value)))
    _db.commit()


def _failed(value) -> bool:
    return isinstance(value, dict) and bool(value.get("error"))


def cached(key: str, ttl: int, produce):
    now = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit is None:
            hit = _disk_get(key)
            if hit is not None:
                _cache[key] = hit
        if hit and now - hit[0] < (ERROR_TTL if _failed(hit[1]) else ttl):
            return hit[1]
    value = produce()
    with _lock:
        _cache[key] = (now, value)
        _disk_put(key, now, value)
    return value


def cached_at(key: str) -> float | None:
    """When the value under `key` was fetched, if it is held."""
    with _lock:
        hit = _cache.get(key)
    return hit[0] if hit else None


def cached_many(items: list, key: str, ttl: int, produce) -> list:
    """cached() over a batch, resolved in parallel. `key` is a format string
    applied to each item; cache hits return without touching the pool."""
    if not items:
        return []
    work = lambda item: cached(key.format(item), ttl, lambda: produce(item))
    if len(items) == 1:
        return [work(items[0])]
    return list(POOL.map(work, items))


def invalidate(prefix: str = "") -> None:
    with _lock:
        for k in [k for k in _cache if k.startswith(prefix)]:
            del _cache[k]
        _db.execute("DELETE FROM cache WHERE substr(key, 1, ?) = ?", (len(prefix), prefix))
        _db.commit()


# --------------------------------------------------------------------------
# watchlist persistence — a file of named lists ("tabs")
# --------------------------------------------------------------------------


def new_id() -> str:
    return uuid4().hex[:8]


def default_lists() -> list[dict]:
    return [{"id": new_id(), "name": "Main", "symbols": list(DEFAULT_SYMBOLS), "slots": {}}]


def _coerce(raw) -> list[dict] | None:
    """Accept both the current shape and the original single-list file."""
    if not isinstance(raw, dict):
        return None
    if isinstance(raw.get("lists"), list):        # current shape
        out = []
        for entry in raw["lists"]:
            if not isinstance(entry, dict):
                continue
            symbols = [s for s in entry.get("symbols", []) if isinstance(s, str)]
            slots = entry.get("slots")
            out.append({"id": str(entry.get("id") or new_id()),
                        "name": str(entry.get("name") or "Untitled"),
                        "symbols": symbols,
                        "slots": slots if isinstance(slots, dict) else {}})
        return out or None
    if isinstance(raw.get("symbols"), list):      # migrate the pre-tabs file
        symbols = [s for s in raw["symbols"] if isinstance(s, str)]
        return [{"id": new_id(), "name": "Main", "symbols": symbols}]
    return None


def load_lists() -> list[dict]:
    """Always returns lists with stable ids. Anything repaired here is written
    back immediately — otherwise a migrated file would mint fresh ids on every
    request and the tab the browser has selected would never match."""
    if not WATCHLIST_FILE.exists():
        lists = default_lists()
        save_lists(lists)
        return lists

    raw = None
    try:
        raw = json.loads(WATCHLIST_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        pass

    lists = _coerce(raw)
    dirty = lists is None or not isinstance(raw, dict) or not isinstance(raw.get("lists"), list)
    if lists is None:
        lists = default_lists()

    # de-duplicate ids defensively; a collision would make tabs unaddressable
    seen: set[str] = set()
    for entry in lists:
        while entry["id"] in seen:
            entry["id"] = new_id()
            dirty = True
        seen.add(entry["id"])

    for entry in lists:
        if ensure_slots(entry):
            dirty = True

    if dirty:
        save_lists(lists)
    return lists


def save_lists(lists: list[dict]) -> None:
    for entry in lists:
        ensure_slots(entry)
    WATCHLIST_FILE.write_text(json.dumps({"lists": lists}, indent=2) + "\n")


def ensure_slots(entry: dict) -> bool:
    """Pin each symbol to a colour slot that survives reordering and removal.
    Without this, colour is assigned by row position, so moving or deleting one
    symbol repaints every series below it."""
    before = dict(entry.get("slots") or {})
    slots = {s: n for s, n in before.items()
             if s in entry["symbols"] and isinstance(n, int) and n >= 0}
    used = set(slots.values())
    for symbol in entry["symbols"]:
        if symbol not in slots:
            n = 0
            while n in used:
                n += 1
            slots[symbol] = n
            used.add(n)
    entry["slots"] = slots
    return slots != before


def reposition(items: list, index: int, delta: int, to: int | None) -> int:
    target = index + delta if to is None else to
    target = max(0, min(len(items) - 1, target))
    items.insert(target, items.pop(index))
    return target


def find_list(lists: list[dict], list_id: str) -> dict:
    for entry in lists:
        if entry["id"] == list_id:
            return entry
    raise HTTPException(404, "No such list")


# --------------------------------------------------------------------------
# yfinance helpers
# --------------------------------------------------------------------------
def clean(x) -> float | None:
    """Yahoo hands back NaN for in-progress sessions (notably .AX) — drop those."""
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def raw_history(symbol: str, period: str, interval: str,
                adjust: bool = True) -> pd.DataFrame:
    """history() by period, falling back to a start date for the windows that
    are not in Yahoo's period vocabulary.

    `adjust` is yfinance's auto_adjust: True back-adjusts historical closes for
    distributions, so the series is total return rather than price. Splits are
    handled either way — Yahoo's raw OHLC is already split-adjusted — so this
    flag only decides whether dividends are in the line. Note that Yahoo never
    adjusts intraday bars, so on those it is a no-op whichever way it is set.
    """
    tk = yf.Ticker(symbol)
    days = WINDOW_DAYS.get(period)
    if days is None:
        return tk.history(period=period, interval=interval, auto_adjust=adjust)
    start = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)).date()
    return tk.history(start=start.isoformat(), interval=interval, auto_adjust=adjust)


def close_series(hist: pd.DataFrame) -> pd.Series:
    if hist is None or hist.empty or "Close" not in hist:
        return pd.Series(dtype="float64")
    return hist["Close"].dropna()


def fetch_name(symbol: str) -> str:
    try:
        info = yf.Ticker(symbol).get_info()
        return info.get("shortName") or info.get("longName") or symbol
    except Exception:
        return symbol


def pct_change_since(closes: pd.Series, days: int) -> float | None:
    """% change vs the last close on or before `days` calendar days ago."""
    if len(closes) < 2:
        return None
    cutoff = closes.index[-1] - pd.Timedelta(days=days)
    prior = closes[closes.index <= cutoff]
    if not len(prior):
        return None
    base, now = clean(prior.iloc[-1]), clean(closes.iloc[-1])
    if base in (None, 0) or now is None:
        return None
    return (now - base) / base * 100.0


def trim(values: list) -> list:
    """Yahoo hands back full float64 repr — 71.58999633789062 is seventeen
    characters to say 71.59. Eight significant figures is more than any of
    these instruments quote to, and it is a third of the bytes on the wire.
    Significant figures rather than decimal places, so a sub-cent instrument
    does not get rounded to zero."""
    return [None if v is None else float(f"{v:.8g}") for v in values]


def downsample(values: list, target: int = 120) -> list:
    if len(values) <= target:
        return values
    stride = len(values) / target
    out = [values[int(i * stride)] for i in range(target)]
    out[-1] = values[-1]
    return out


def fetch_quote(symbol: str) -> dict:
    """One year of daily closes powers the quote, the 1M change, the 52-week
    range and the inline chart. fast_info carries the current session price
    that the daily OHLC bar is still NaN for (notably on .AX).

    Unadjusted, so the whole table is prices: the 52-week range matches what a
    broker shows, and 1M % sits beside a Day % that was always price-based.
    Total return lives on the compare chart, which is the one place it earns
    its keep — there a 3.5% yielder and a 1.0% yielder are on one axis."""
    out: dict = {"symbol": symbol, "name": symbol, "currency": None,
                 "price": None, "prevClose": None, "change": None,
                 "changePct": None, "monthPct": None,
                 "low52": None, "high52": None, "pos52": None,
                 "spark": [], "sparkStart": None, "error": None}
    try:
        tk = yf.Ticker(symbol)
        hist = tk.history(period="1y", interval="1d", auto_adjust=False)
        closes = close_series(hist)

        price = prev = None
        try:
            fi = tk.fast_info
            price = clean(fi.get("lastPrice"))
            prev = clean(fi.get("previousClose"))
            out["currency"] = fi.get("currency")
        except Exception:
            pass

        if price is None and len(closes):
            price = clean(closes.iloc[-1])
        if prev is None and len(closes) >= 2:
            prev = clean(closes.iloc[-2])
        # If fast_info's price IS the final settled close, compare to the one before.
        if price is not None and prev is not None and len(closes) >= 2:
            if abs(price - float(closes.iloc[-1])) < 1e-9:
                prev = clean(closes.iloc[-2])

        out["price"] = price
        out["prevClose"] = prev
        if price is not None and prev not in (None, 0):
            out["change"] = price - prev
            out["changePct"] = (price - prev) / prev * 100.0

        out["monthPct"] = pct_change_since(closes, 30)

        if len(closes):
            lo, hi = clean(closes.min()), clean(closes.max())
            # a live price can print through the settled 52-week extremes
            if price is not None:
                lo = min(lo, price) if lo is not None else price
                hi = max(hi, price) if hi is not None else price
            out["low52"], out["high52"] = lo, hi
            if lo is not None and hi is not None and price is not None and hi > lo:
                out["pos52"] = (price - lo) / (hi - lo) * 100.0
            elif price is not None:
                out["pos52"] = 50.0

        vals = [clean(v) for v in closes.tolist()]
        vals = [v for v in vals if v is not None]
        if price is not None and vals and vals[-1] != price:
            vals.append(price)
        out["spark"] = trim(downsample(vals))
        if len(closes):
            out["sparkStart"] = closes.index[0].strftime("%Y-%m-%d")

        out["name"] = cached(f"n:{symbol}", CACHE_TTL, lambda: fetch_name(symbol))
    except Exception as exc:  # one bad symbol must not blank the page
        out["error"] = str(exc)[:200]
    return out


def fetch_history(symbol: str, period: str, granular: bool = False) -> dict:
    table = DETAIL_PERIODS if granular else PERIODS
    yf_period, interval = table[period]
    out = {"symbol": symbol, "dates": [], "close": [],
           "interval": interval, "intraday": False, "error": None}
    tried = interval
    try:
        closes = pd.Series(dtype="float64")
        # Yahoo returns an empty frame when a fine interval exceeds its window;
        # step down until something comes back.
        for _ in range(4):
            # The detail chart is a record of what the thing traded at, so it
            # stays on price. Without this it would silently switch basis at
            # the 1Y/3Y boundary, where the interval steps down from 1h to 1d
            # and Yahoo starts adjusting: VHY's MAX low reads 19.74 adjusted
            # against a real floor of 43.21.
            hist = raw_history(symbol, yf_period, tried, adjust=not granular)
            closes = close_series(hist)
            if len(closes) > 1:
                break
            nxt = FALLBACK.get(tried)
            if not nxt:
                break
            tried = nxt

        out["interval"] = tried
        out["intraday"] = tried in INTRADAY
        fmt = "%Y-%m-%dT%H:%M" if out["intraday"] else "%Y-%m-%d"
        out["dates"] = [d.strftime(fmt) for d in closes.index]
        out["close"] = trim([clean(v) for v in closes.tolist()])
    except Exception as exc:
        out["error"] = str(exc)[:200]
    return out


# --------------------------------------------------------------------------
# short term — a fixed set of rate / index / energy futures, followed closely
# --------------------------------------------------------------------------
# Seven days of one-minute bars (the finest Yahoo serves, and as far back as
# it serves them), with the overnight session included — futures trade nearly 23
# hours and most of the move in ES and CL happens outside the cash session.
#
# "Live" is as live as Yahoo's free feed: CME futures arrive ~10 minutes
# delayed and ^TNX ~15, and Yahoo's websocket streams nothing at all for the
# futures. So the page polls instead, and these short TTLs mean however many
# browsers are watching, Yahoo sees one fetch per symbol per window.
SHORT_TERM = ["^TNX", "ES=F", "CL=F", "GC=F"]
SHORT_HISTORY_TTL = 60
SHORT_LIVE_TTL = 4


# The legend toggles are kept here rather than in the browser, so a line hidden
# on the laptop is hidden on the phone too. Anything not in SHORT_TERM is
# dropped on read, so retiring a symbol can't leave a ghost entry behind.
def load_short_hidden() -> list[str]:
    try:
        raw = json.loads(SHORT_TERM_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []
    hidden = raw.get("hidden") if isinstance(raw, dict) else None
    return [s for s in SHORT_TERM if isinstance(hidden, list) and s in hidden]


def save_short_hidden(hidden: list[str]) -> list[str]:
    hidden = [s for s in SHORT_TERM if s in hidden]
    SHORT_TERM_FILE.write_text(json.dumps({"hidden": hidden}, indent=2) + "\n")
    return hidden


def epoch_bars(hist: pd.DataFrame) -> tuple[list[int], list]:
    closes = close_series(hist)
    return ([int(ts.timestamp()) for ts in closes.index],
            trim([clean(v) for v in closes.tolist()]))


def fetch_short_history(symbol: str) -> dict:
    out = {"symbol": symbol, "t": [], "close": [], "error": None}
    try:
        hist = yf.Ticker(symbol).history(period="7d", interval="1m",
                                         prepost=True, auto_adjust=False)
        out["t"], out["close"] = epoch_bars(hist)
    except Exception as exc:
        out["error"] = str(exc)[:200]
    return out


def fetch_short_live(symbol: str) -> dict:
    """Today's 1m bars and the chart metadata in one round trip. The metadata
    carries the last trade and its time, which can sit inside a bar Yahoo has
    not closed yet, so the page gets the price before the bar."""
    out = {"symbol": symbol, "name": symbol, "currency": None, "price": None,
           "time": None, "prevClose": None, "t": [], "close": [], "error": None}
    try:
        tk = yf.Ticker(symbol)
        hist = tk.history(period="1d", interval="1m", prepost=True, auto_adjust=False)
        meta = tk.history_metadata or {}
        out["t"], out["close"] = epoch_bars(hist)
        out["name"] = meta.get("shortName") or symbol
        out["currency"] = meta.get("currency")
        out["price"] = clean(meta.get("regularMarketPrice"))
        at = meta.get("regularMarketTime")
        if at is not None:
            out["time"] = int(pd.Timestamp(at).timestamp()) if not isinstance(at, (int, float)) else int(at)
        out["prevClose"] = clean(meta.get("chartPreviousClose") or meta.get("previousClose"))
    except Exception as exc:
        out["error"] = str(exc)[:200]
    return out


def align(series: list[dict]) -> list[dict]:
    """ASX and CME keep different calendars, so position-indexed series would
    misalign. Reindex every series onto the union of dates and forward-fill."""
    usable = [s for s in series if s["dates"]]
    if not usable:
        return series
    index = sorted({d for s in usable for d in s["dates"]})
    for s in series:
        if not s["dates"]:
            s["dates"], s["close"] = list(index), [None] * len(index)
            continue
        by_date = dict(zip(s["dates"], s["close"]))
        filled, last = [], None
        for d in index:
            v = by_date.get(d)
            if v is not None:
                last = v
            filled.append(last)   # None until the series' first observation
        s["dates"], s["close"] = list(index), filled
    return series


# --------------------------------------------------------------------------
# API
# --------------------------------------------------------------------------
class SymbolIn(BaseModel):
    symbol: str


class ListIn(BaseModel):
    name: str


class HiddenIn(BaseModel):
    hidden: list[str]


class MoveIn(BaseModel):
    delta: int = 0          # -1 up / +1 down
    to: int | None = None   # or an explicit target index


def clean_name(raw: str) -> str:
    name = " ".join(raw.split())[:40]
    if not name:
        raise HTTPException(400, "A list needs a name")
    return name


@app.get("/api/lists")
def get_lists():
    return {"lists": load_lists()}


@app.post("/api/lists")
def create_list(body: ListIn):
    lists = load_lists()
    if len(lists) >= MAX_LISTS:
        raise HTTPException(400, f"That's the {MAX_LISTS}-list ceiling")
    name = clean_name(body.name)
    entry = {"id": new_id(), "name": name, "symbols": [], "slots": {}}
    lists.append(entry)
    save_lists(lists)
    return {"lists": lists, "created": entry}


@app.patch("/api/lists/{list_id}")
def rename_list(list_id: str, body: ListIn):
    lists = load_lists()
    find_list(lists, list_id)["name"] = clean_name(body.name)
    save_lists(lists)
    return {"lists": lists}


@app.delete("/api/lists/{list_id}")
def delete_list(list_id: str):
    lists = load_lists()
    if len(lists) <= 1:
        raise HTTPException(400, "The last list can't be deleted")
    entry = find_list(lists, list_id)
    lists.remove(entry)
    save_lists(lists)
    prune_alerts()
    return {"lists": lists}


@app.post("/api/lists/{list_id}/move")
def move_list(list_id: str, body: MoveIn):
    """Reorder the tabs themselves."""
    lists = load_lists()
    entry = find_list(lists, list_id)
    index = reposition(lists, lists.index(entry), body.delta, body.to)
    save_lists(lists)
    return {"lists": lists, "index": index}


@app.post("/api/lists/{list_id}/symbols/{symbol:path}/move")
def move_symbol(list_id: str, symbol: str, body: MoveIn):
    """Reorder a symbol within its list. Colour slots are pinned per symbol,
    so the rows move but nothing is recoloured."""
    lists = load_lists()
    entry = find_list(lists, list_id)
    symbol = symbol.upper()
    if symbol not in entry["symbols"]:
        raise HTTPException(404, f"{symbol} is not in {entry['name']}")
    index = reposition(entry["symbols"], entry["symbols"].index(symbol), body.delta, body.to)
    save_lists(lists)
    return {"lists": lists, "index": index}


@app.post("/api/lists/{list_id}/symbols")
def add_symbol(list_id: str, body: SymbolIn):
    symbol = body.symbol.strip().upper()
    if not symbol:
        raise HTTPException(400, "Empty symbol")
    lists = load_lists()
    entry = find_list(lists, list_id)
    if symbol in entry["symbols"]:
        raise HTTPException(409, f"{symbol} is already in {entry['name']}")

    probe = cached(f"q:{symbol}", CACHE_TTL, lambda: fetch_quote(symbol))
    if probe["price"] is None:
        raise HTTPException(404, f"No data for {symbol} — check the suffix (e.g. .AX for ASX)")

    entry["symbols"].append(symbol)
    save_lists(lists)
    return {"lists": lists, "added": probe}


@app.delete("/api/lists/{list_id}/symbols/{symbol:path}")
def remove_symbol(list_id: str, symbol: str):
    lists = load_lists()
    entry = find_list(lists, list_id)
    symbol = symbol.upper()
    if symbol not in entry["symbols"]:
        raise HTTPException(404, f"{symbol} is not in {entry['name']}")
    entry["symbols"].remove(symbol)
    save_lists(lists)
    prune_alerts()
    return {"lists": lists}


@app.get("/api/quotes")
def quotes(list_id: str | None = None):
    lists = load_lists()
    entry = find_list(lists, list_id) if list_id else lists[0]
    rows = cached_many(entry["symbols"], "q:{}", CACHE_TTL, fetch_quote)
    # "as of" is when the oldest row was fetched, not when it was served —
    # with a three-hour cache the two can be a long way apart.
    ages = [t for t in (cached_at(f"q:{s}") for s in entry["symbols"]) if t]
    as_of = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(min(ages))) if ages else None
    return {"listId": entry["id"], "name": entry["name"], "quotes": rows,
            "slots": entry.get("slots", {}), "asOf": as_of}


@app.get("/api/history")
def history(symbols: str, period: str = "1Y", granular: bool = False):
    period = period.upper()
    if period not in PERIODS:
        raise HTTPException(400, f"period must be one of {', '.join(PERIODS)}")
    wanted = [s.strip() for s in (symbols or "").split(",") if s.strip()]
    series = cached_many(wanted, "h:{}:" + f"{period}:{granular}", CACHE_TTL,
                         lambda s: fetch_history(s, period, granular))
    if not granular:
        series = align(series)
    return {"period": period, "series": series}


@app.get("/api/short_term/history")
def short_term_history():
    return {"series": cached_many(SHORT_TERM, "st:h:{}", SHORT_HISTORY_TTL, fetch_short_history)}


@app.get("/api/short_term/prefs")
def short_term_prefs():
    return {"hidden": load_short_hidden()}


@app.put("/api/short_term/prefs")
def set_short_term_prefs(body: HiddenIn):
    return {"hidden": save_short_hidden(body.hidden)}


@app.get("/api/short_term/live")
def short_term_live():
    return {"now": int(time.time()),
            "quotes": cached_many(SHORT_TERM, "st:l:{}", SHORT_LIVE_TTL, fetch_short_live)}


# --------------------------------------------------------------------------
# price alerts — checked once a day, sent to a phone through ntfy
# --------------------------------------------------------------------------
# The check lives in this process rather than in cron: crond has died silently
# on this box before, and this way a 5pm missed during a restart still runs as
# soon as the app is back. Prices bypass the cache, since a three-hour-old quote
# could fire or miss an alert on a price that has gone.
_alerts_lock = threading.Lock()


def load_alert_state() -> dict:
    # A missing file is an empty start, but a corrupt one should fail loudly:
    # treating it as empty would write over the alerts on the next save.
    if not ALERTS_FILE.exists():
        return {"last_poll": None, "alerts": []}
    return json.loads(ALERTS_FILE.read_text())


def save_alert_state(state: dict) -> None:
    tmp = ALERTS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2) + "\n")
    tmp.replace(ALERTS_FILE)


def prune_alerts() -> None:
    """An alert on a symbol that is on no list could not be seen or removed."""
    listed = {s for entry in load_lists() for s in entry["symbols"]}
    with _alerts_lock:
        state = load_alert_state()
        state["alerts"] = [a for a in state["alerts"] if a["symbol"] in listed]
        save_alert_state(state)


def latest_price(symbol: str) -> float | None:
    tk = yf.Ticker(symbol)
    try:
        price = clean(tk.fast_info.get("lastPrice"))
        if price is not None:
            return price
    except Exception:
        pass
    try:
        closes = close_series(tk.history(period="5d", interval="1d", auto_adjust=False))
        return clean(closes.iloc[-1]) if len(closes) else None
    except Exception:
        return None


def year_high_note(symbol: str, price: float) -> str:
    """Price against the 52-week high, measured as the table does: daily closes
    plus the live price. Empty if Yahoo fails, which should not cost the alert."""
    try:
        closes = close_series(yf.Ticker(symbol).history(period="1y", interval="1d", auto_adjust=False))
    except Exception:
        return ""
    if not len(closes):
        return ""
    return f" {price / max(float(closes.max()), price) - 1:+.3f} YearlyHigh"


def crossed(alert: dict, price: float) -> bool:
    return price >= alert["price"] if alert["direction"] == "above" else price <= alert["price"]


def send_ntfy(title: str, message: str, tag: str) -> None:
    topic = ALERT_TOPIC_FILE.read_text().strip()
    request = urllib.request.Request(f"{NTFY}/{topic}", data=message.encode(),
                                     headers={"Title": title, "Tags": tag})
    urllib.request.urlopen(request, timeout=15).close()


def poll_alerts() -> list[dict]:
    """Fire whatever has crossed. An alert is only removed once ntfy has taken
    the message, so a failed send or a failed price is retried at the next poll."""
    with _alerts_lock:
        alerts = load_alert_state()["alerts"]
    symbols = sorted({a["symbol"] for a in alerts})
    prices = dict(zip(symbols, POOL.map(latest_price, symbols)))
    fired = []
    for alert in alerts:
        symbol, price = alert["symbol"], prices[alert["symbol"]]
        if price is None:
            print(f"alerts: no price for {symbol}, keeping alert {alert['id']}", flush=True)
            continue
        if not crossed(alert, price):
            continue
        level = fmt_level(alert["price"])
        try:
            send_ntfy(f"{symbol} {alert['direction']} {level}",
                      f"{symbol} is {fmt_level(price)}, {alert['direction']} your {level} alert."
                      + year_high_note(symbol, price),
                      "chart_with_upwards_trend" if alert["direction"] == "above"
                      else "chart_with_downwards_trend")
        except Exception as error:
            print(f"alerts: could not send {alert['id']}: {error}", flush=True)
            continue
        with _alerts_lock:
            state = load_alert_state()
            state["alerts"] = [a for a in state["alerts"] if a["id"] != alert["id"]]
            save_alert_state(state)
        fired.append(alert)
    with _alerts_lock:
        state = load_alert_state()
        state["last_poll"] = datetime.now(ALERT_TZ).date().isoformat()
        save_alert_state(state)
    print(f"alerts: polled {len(alerts)}, fired {len(fired)}", flush=True)
    return fired


def fmt_level(value: float) -> str:
    return f"{value:,.2f}" if abs(value) >= 1 else f"{value:.4f}"


def poll_due(now: datetime, last_poll: str | None) -> bool:
    local = now.astimezone(ALERT_TZ)
    return local.hour >= ALERT_HOUR and last_poll != local.date().isoformat()


def alert_loop() -> None:
    while True:
        try:
            if poll_due(datetime.now(ALERT_TZ), load_alert_state()["last_poll"]):
                poll_alerts()
        except Exception as error:
            print(f"alerts: poll failed: {error}", flush=True)
        time.sleep(60)


class AlertIn(BaseModel):
    symbol: str
    price: float
    direction: Literal["above", "below"]


@app.get("/api/alerts")
def get_alerts():
    with _alerts_lock:
        return load_alert_state()


@app.post("/api/alerts")
def create_alert(body: AlertIn):
    symbol = body.symbol.strip().upper()
    if not math.isfinite(body.price) or body.price <= 0:
        raise HTTPException(400, "Price must be above zero")
    if not any(symbol in entry["symbols"] for entry in load_lists()):
        raise HTTPException(404, f"{symbol} is not on any list")
    with _alerts_lock:
        state = load_alert_state()
        state["alerts"].append({"id": new_id(), "symbol": symbol, "price": body.price,
                                "direction": body.direction, "created": time.time()})
        save_alert_state(state)
        return state


@app.delete("/api/alerts/{alert_id}")
def delete_alert(alert_id: str):
    with _alerts_lock:
        state = load_alert_state()
        kept = [a for a in state["alerts"] if a["id"] != alert_id]
        if len(kept) == len(state["alerts"]):
            raise HTTPException(404, "No such alert")
        state["alerts"] = kept
        save_alert_state(state)
        return state


@app.post("/api/refresh")
def refresh():
    invalidate()
    return {"ok": True}


@app.get("/healthz")
def healthz():
    """Unauthenticated on purpose: it says the process is up and nothing else.
    service.sh and cron can watch it without holding the token."""
    return {"ok": True}


# Pages only: browsers otherwise guess a freshness from the file's age and keep
# showing an old page after a deploy. no-cache still allows a cheap 304.
NO_CACHE = {"Cache-Control": "no-cache"}


@app.get("/")
def root():
    return RedirectResponse("stonks", status_code=302)


@app.get("/stonks")
def stonks_page():
    return FileResponse(STATIC / "index.html", headers=NO_CACHE)


@app.get("/today_stonks")
def today_stonks_page():
    return FileResponse(STATIC / "short_term.html", headers=NO_CACHE)


@app.get("/sw.js")
def service_worker():
    """Served above /static, so its scope covers both pages.
    Chrome requires a registered worker before it will offer to install the app."""
    return FileResponse(STATIC / "sw.js", media_type="text/javascript",
                        headers={"Cache-Control": "no-cache"})


# icons + manifest; mounted last so it cannot shadow an API route
app.mount("/static", StaticFiles(directory=STATIC), name="static")
