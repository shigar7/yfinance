"""IVV Prediction — an indicative IVV.AX open from the US IVV close, AUD/USD and
the overnight ES move, plus a backtest of the same conversion against real opens.

Run:  ./run.sh   Then: http://127.0.0.1:9003
"""
from __future__ import annotations

import math
import threading
import time
from datetime import datetime, time as clock, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse

ROOT = Path(__file__).parent
ET = ZoneInfo("America/New_York")
SYDNEY = ZoneInfo("Australia/Sydney")

UNITS_PER_US_SHARE = 15.0
US_OPEN, US_CLOSE = clock(9, 30), clock(16, 0)
ASX_OPEN, ASX_CLOSE = clock(10, 0), clock(16, 10)

TODAY_TTL = 60
BACKTEST_TTL = 3600
# Yahoo serves hourly FX back about 730 days, which caps the backtest.
FX_MAX_GAP = pd.Timedelta(hours=3)
WINDOWS = {"1M": 30, "3M": 91, "6M": 182, "1Y": 365, "ALL": None}

app = FastAPI(title="IVV Prediction")
app.add_middleware(GZipMiddleware, minimum_size=500)

_cache: dict[str, tuple[float, object]] = {}
_lock = threading.Lock()


def cached(key: str, ttl: int, build):
    # One lock across the build, so a burst of page loads makes one Yahoo fetch.
    with _lock:
        held = _cache.get(key)
        if held and time.time() - held[0] < ttl:
            return held[1]
        value = build()
        _cache[key] = (time.time(), value)
        return value


def history(symbol: str, **kw) -> pd.DataFrame:
    # auto_adjust=False: these are real traded prices. Adjusted ones would move
    # past opens by later dividends, and the two funds pay on different dates.
    frame = yf.Ticker(symbol).history(auto_adjust=False, **kw)
    if frame.empty:
        raise HTTPException(502, f"Yahoo returned no data for {symbol}")
    return frame.dropna(subset=["Close"])


def at(day, when: clock, zone: ZoneInfo) -> pd.Timestamp:
    return pd.Timestamp(datetime.combine(day, when, zone))


def iso(ts) -> str:
    return pd.Timestamp(ts).isoformat()


def round_or_none(value, digits: int = 4):
    return None if value is None or not math.isfinite(value) else round(float(value), digits)


# --------------------------------------------------------------------------
# today's indicative open
# --------------------------------------------------------------------------
def next_asx_open(now: pd.Timestamp) -> pd.Timestamp:
    """The next weekday 10:00 in Sydney. ASX holidays are not known here."""
    day = now.tz_convert(SYDNEY).date()
    if now >= at(day, ASX_OPEN, SYDNEY) or day.weekday() >= 5:
        day += timedelta(days=1)
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return at(day, ASX_OPEN, SYDNEY)


def us_sessions_before(now: pd.Timestamp, target: pd.Timestamp) -> bool:
    """Will a US close land between now and the target open, assuming no holiday?"""
    day = now.tz_convert(ET).date()
    while at(day, US_CLOSE, ET) < target:
        if day.weekday() < 5 and at(day, US_CLOSE, ET) > now:
            return True
        day += timedelta(days=1)
    return False


def us_reference(now: pd.Timestamp) -> dict:
    """The IVV price to build on, and the instant it was struck."""
    et_now = now.tz_convert(ET)
    if et_now.weekday() < 5 and US_OPEN <= et_now.time() < US_CLOSE:
        bars = yf.Ticker("IVV").history(period="1d", interval="1m", auto_adjust=False).dropna(subset=["Close"])
        # A bar from today is what tells a trading day from a US holiday.
        if not bars.empty and bars.index[-1].tz_convert(ET).date() == et_now.date():
            return {"price": float(bars["Close"].iloc[-1]), "at": bars.index[-1] + pd.Timedelta(minutes=1),
                    "session_open": True}

    daily = history("IVV", period="10d", interval="1d")
    for stamp, row in daily.iloc[::-1].iterrows():
        close_at = at(stamp.tz_convert(ET).date(), US_CLOSE, ET)
        if close_at <= now:
            return {"price": float(row["Close"]), "at": close_at, "session_open": False}
    raise HTTPException(502, "No completed US session in Yahoo's IVV data")


def es_move(since: pd.Timestamp) -> dict | None:
    bars = history("ES=F", period="7d", interval="5m")
    ends = bars.index + pd.Timedelta(minutes=5)
    done = bars[ends <= since]
    if done.empty:
        return None
    ref, latest = done.iloc[-1], bars.iloc[-1]
    return {"ref": float(ref["Close"]), "ref_at": done.index[-1] + pd.Timedelta(minutes=5),
            "now": float(latest["Close"]), "now_at": bars.index[-1] + pd.Timedelta(minutes=5)}


def build_today() -> dict:
    now = pd.Timestamp.now(tz="UTC")
    target = next_asx_open(now)

    us = us_reference(now)
    es = es_move(us["at"])
    drift = es["now"] / es["ref"] - 1 if es else 0.0

    fx_bars = history("AUDUSD=X", period="5d", interval="5m")
    aud_usd = float(fx_bars["Close"].iloc[-1])
    fx_at = fx_bars.index[-1] + pd.Timedelta(minutes=5)

    base = us["price"] / aud_usd / UNITS_PER_US_SHARE
    estimate = base * (1 + drift)

    asx = history("IVV.AX", period="10d", interval="1d")
    closed = [(s, r) for s, r in asx.iterrows()
              if at(s.tz_convert(SYDNEY).date(), ASX_CLOSE, SYDNEY) <= now]
    last_stamp, last_row = closed[-1]

    # Once the ASX has opened today, the real open can sit next to the estimate.
    today_open = None
    latest_stamp, latest_row = asx.index[-1], asx.iloc[-1]
    latest_day = latest_stamp.tz_convert(SYDNEY).date()
    if latest_day == now.tz_convert(SYDNEY).date() and at(latest_day, ASX_OPEN, SYDNEY) <= now:
        today_open = {"price": round_or_none(float(latest_row["Open"])), "date": latest_day.isoformat()}

    return {
        "now": iso(now),
        "target_open": iso(target),
        "estimate": round_or_none(estimate),
        "base_no_es": round_or_none(base),
        "units_per_us_share": UNITS_PER_US_SHARE,
        "us": {"price": round_or_none(us["price"]), "at": iso(us["at"]), "session_open": us["session_open"]},
        "more_us_sessions_before_open": us_sessions_before(now, target),
        "es": None if not es else {"ref": round_or_none(es["ref"], 2), "ref_at": iso(es["ref_at"]),
                                   "now": round_or_none(es["now"], 2), "now_at": iso(es["now_at"]),
                                   "move": round_or_none(drift, 6)},
        "fx": {"aud_usd": round_or_none(aud_usd, 5), "usd_aud": round_or_none(1 / aud_usd, 5), "at": iso(fx_at)},
        "asx": {"last_close": round_or_none(float(last_row["Close"])),
                "last_close_date": last_stamp.tz_convert(SYDNEY).date().isoformat(),
                "today_open": today_open},
    }


# --------------------------------------------------------------------------
# backtest
# --------------------------------------------------------------------------
def build_pairs() -> pd.DataFrame:
    """One row per real ASX open, against the latest US close that came before it.

    Matching on instants rather than on dates is what keeps holidays straight: a
    date join would shift or drop rows whenever only one market was shut.
    """
    now = pd.Timestamp.now(tz="UTC")

    us_daily = history("IVV", period="2y", interval="1d")
    us = pd.DataFrame({
        "us_date": [s.tz_convert(ET).date() for s in us_daily.index],
        "us_close": us_daily["Close"].to_numpy(),
    })
    us["close_at"] = [at(d, US_CLOSE, ET).tz_convert("UTC") for d in us["us_date"]]
    us = us[us["close_at"] <= now].sort_values("close_at")

    asx_daily = history("IVV.AX", period="2y", interval="1d")
    asx = pd.DataFrame({
        "asx_date": [s.tz_convert(SYDNEY).date() for s in asx_daily.index],
        "open": asx_daily["Open"].to_numpy(),
        "asx_close": asx_daily["Close"].to_numpy(),
    })
    asx["prev_asx_close"] = asx["asx_close"].shift(1)
    asx["open_at"] = [at(d, ASX_OPEN, SYDNEY).tz_convert("UTC") for d in asx["asx_date"]]
    asx = asx[(asx["open_at"] <= now) & (asx["open"] > 0)].sort_values("open_at")

    pairs = pd.merge_asof(asx, us, left_on="open_at", right_on="close_at",
                          direction="backward", allow_exact_matches=False).dropna(subset=["us_close"])

    # FX at the US close: the hourly bar that ends at 16:00 ET, so its close is the
    # rate at that moment. Bars are stamped at their start.
    fx_bars = history("AUDUSD=X", period="730d", interval="1h")
    fx = pd.DataFrame({"fx_at": (fx_bars.index + pd.Timedelta(hours=1)).tz_convert("UTC"),
                       "aud_usd": fx_bars["Close"].to_numpy()}).sort_values("fx_at")
    pairs = pd.merge_asof(pairs, fx, left_on="close_at", right_on="fx_at",
                          direction="backward", tolerance=FX_MAX_GAP).dropna(subset=["aud_usd"])

    pairs["predicted"] = pairs["us_close"] / pairs["aud_usd"] / UNITS_PER_US_SHARE
    pairs["error"] = pairs["predicted"] - pairs["open"]

    def expected_us_day(day):
        day -= timedelta(days=1)
        while day.weekday() >= 5:
            day -= timedelta(days=1)
        return day
    # The US was shut the night before, so this open is priced off an older close.
    pairs["stale_us"] = [u != expected_us_day(a) for a, u in zip(pairs["asx_date"], pairs["us_date"])]
    return pairs


def metrics(rows: pd.DataFrame) -> dict:
    pred, actual, err = rows["predicted"], rows["open"], rows["error"]
    r = float(np.corrcoef(pred, actual)[0, 1]) if len(rows) > 2 else float("nan")
    slope, intercept = np.polyfit(pred, actual, 1) if len(rows) > 2 else (float("nan"), float("nan"))
    prev = rows.dropna(subset=["prev_asx_close"])
    gap_predicted, gap_actual = prev["predicted"] - prev["prev_asx_close"], prev["open"] - prev["prev_asx_close"]
    gap_r = float(np.corrcoef(gap_predicted, gap_actual)[0, 1]) if len(prev) > 2 else float("nan")
    return {
        "n": len(rows),
        "mae": round_or_none(float(err.abs().mean())),
        "mae_pct": round_or_none(float((err.abs() / actual).mean() * 100), 3),
        "rmse": round_or_none(float(np.sqrt((err ** 2).mean()))),
        "bias": round_or_none(float(err.mean())),
        "max_abs_error": round_or_none(float(err.abs().max())),
        "r": round_or_none(r),
        "r2": round_or_none(r * r),
        "slope": round_or_none(float(slope)),
        "intercept": round_or_none(float(intercept)),
        # Levels trend together over months, so r above flatters any model. These
        # ask the harder question: did it call the overnight gap?
        "gap_r2": round_or_none(gap_r * gap_r),
        "naive_mae": round_or_none(float(gap_actual.abs().mean())),
        "median_implied_ratio": round_or_none(float((rows["us_close"] / rows["aud_usd"] / rows["open"]).median())),
        "stale_us_days": int(rows["stale_us"].sum()),
    }


def build_backtest(window: str) -> dict:
    pairs = cached("pairs", BACKTEST_TTL, build_pairs)
    days = WINDOWS[window]
    rows = pairs if days is None else pairs[pairs["open_at"] >= pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)]
    if rows.empty:
        raise HTTPException(502, "No backtest rows in that window")
    return {
        "window": window,
        "from": rows["asx_date"].iloc[0].isoformat(),
        "to": rows["asx_date"].iloc[-1].isoformat(),
        "metrics": metrics(rows),
        "rows": [{"asx_date": a.isoformat(), "us_date": u.isoformat(), "us_close": round(c, 2),
                  "aud_usd": round(f, 5), "predicted": round(p, 3), "open": round(o, 3),
                  "error": round(e, 3), "stale_us": bool(s)}
                 for a, u, c, f, p, o, e, s in zip(rows["asx_date"], rows["us_date"], rows["us_close"],
                                                   rows["aud_usd"], rows["predicted"], rows["open"],
                                                   rows["error"], rows["stale_us"])],
    }


# --------------------------------------------------------------------------
# routes
# --------------------------------------------------------------------------
@app.get("/api/today")
def today():
    return cached("today", TODAY_TTL, build_today)


@app.get("/api/backtest")
def backtest(window: str = "1Y"):
    if window not in WINDOWS:
        raise HTTPException(400, f"window must be one of {sorted(WINDOWS)}")
    return build_backtest(window)


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/")
def page():
    return FileResponse(ROOT / "index.html", headers={"Cache-Control": "no-cache"})
