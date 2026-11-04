import hmac
import json
import os
import time
from datetime import date, timedelta
from typing import Literal

import uvicorn
import yfinance as yf
from fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

import eodhd
import bdi
import ecb
import store

# keep yfinance's cookie + ticker-timezone cache across rebuilds (cold lookups cost extra requests)
yf.set_tz_cache_location(os.path.join(eodhd.DATA_DIR, "yfinance"))

TOKEN = os.environ.get("MCP_TOKEN", "").strip()
if len(TOKEN) < 32:  # an empty token would make "Bearer " a valid credential
    raise SystemExit("MCP_TOKEN must be set to at least 32 random characters (openssl rand -hex 32)")
# private ISIN -> Yahoo ticker map, kept in the data volume (see isins.example.json)
try:
    with open(os.path.join(eodhd.DATA_DIR, "isins.json")) as f:
        ISINS: dict[str, str] = json.load(f)
except FileNotFoundError:
    ISINS = {}
LEARNED_PATH = os.path.join(eodhd.DATA_DIR, "isins_learned.json")
try:
    with open(LEARNED_PATH) as f:
        LEARNED: dict[str, str] = json.load(f)
except Exception:
    LEARNED = {}

TTL = 120  # seconds; Yahoo rate-limits bursts
_cache: dict[str, tuple[float, dict]] = {}
Source = Literal["auto", "yahoo", "eodhd"]

# Every tool only reads public market data; declaring it lets clients skip write-confirmation prompts.
READ_ONLY = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}

mcp = FastMCP(
    "market-data",
    instructions=(
        "Prices about 15 minutes delayed. Primary source Yahoo Finance; EODHD (free plan, about 18 "
        "calls/day) is the automatic fallback and resolves unknown ISINs, so avoid source='eodhd' "
        "for bulk requests. Accepts Yahoo tickers (EUNL.DE, AAPL, 0700.HK) or ISINs (IE00B4L5Y983). "
        "Xetra (.DE) ETF quotes are in EUR; stocks are in their listing currency (AAPL and MSFT in "
        "USD, 0700.HK in HKD). To convert: for a value on a past date or an end-of-day figure use the "
        "official ECB reference rate (ecb_rate, ecb_rate_history, to_eur); for Italian tax figures use "
        "the Banca d'Italia rate (bdi_rate, or to_eur with source='bdi'), identical to ECB for major "
        "currencies; for an intraday conversion of a live price use fx (Yahoo spot, e.g. EURUSD) and "
        "divide. Historical closes are split-adjusted; corporate_actions lists splits and dividends."
    ),
)


_unresolved: dict[str, date] = {}


def _is_isin(s: str) -> bool:
    return len(s) == 12 and s[:2].isalpha() and s[2:].isalnum() and s[-1].isdigit()


def _isin_checksum_ok(s: str) -> bool:
    """ISIN check digit (Luhn over the letters expanded to numbers): catches typos for free."""
    digits = "".join(str(int(c, 36)) for c in s[:-1])
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch) * (2 if i % 2 == 0 else 1)
        total += n // 10 + n % 10
    return (10 - total % 10) % 10 == int(s[-1])


_canary = (0.0, True)  # (checked_at, yahoo_alive)


def _yahoo_alive() -> bool:
    """Is Yahoo answering at all? Checked with EURUSD, cached 5 min. When Yahoo works and still has
    no data for a symbol, the symbol is unknown and the EODHD fallback would only burn quota."""
    global _canary
    if time.time() - _canary[0] < 300:
        return _canary[1]
    try:
        alive = not yf.Ticker("EURUSD=X").history(period="5d").empty
    except Exception:
        alive = False
    _canary = (time.time(), alive)
    return alive


def _use_eodhd_fallback(sym: str) -> bool:
    return eodhd.enabled() and not _yahoo_alive()


def resolve(symbol: str) -> str:
    s = symbol.strip().upper()
    if s in ISINS:
        return ISINS[s]
    if s in LEARNED:
        return LEARNED[s]
    if not _is_isin(s):
        return s
    if not _isin_checksum_ok(s):
        raise ValueError(f"{s} is not a valid ISIN (check digit mismatch); check for a typo")
    if _unresolved.get(s) == date.today():
        raise ValueError(f"ISIN {s} not resolvable today (cached miss); add it to data/isins.json")
    if eodhd.enabled():
        try:
            hit = eodhd.best_listing(s)
            if hit:
                LEARNED[s] = hit["yahoo"]
                os.makedirs(eodhd.DATA_DIR, exist_ok=True)
                with open(LEARNED_PATH, "w") as f:
                    json.dump(LEARNED, f, indent=1)
                return hit["yahoo"]
        except Exception:
            pass
    try:
        r = yf.Search(s, max_results=1).quotes
        if r:
            return r[0]["symbol"]
    except Exception:
        pass
    _unresolved[s] = date.today()  # don't spend EODHD quota on the same unknown ISIN again today
    raise ValueError(f"ISIN {s} not resolvable; add it to data/isins.json")


STALE_MAX = 3 * 86400  # serve a last-good quote (flagged stale, with age) up to 3 days old, covers weekends
_last_good: dict[str, tuple[float, dict]] = {}
_last_reset = 0.0


def _reset_yahoo() -> None:
    """Drop yfinance's cookie/crumb. Yahoo sometimes stops honouring the persisted cookie and
    every request then returns empty data ('possibly delisted') until the cookie is refreshed."""
    global _last_reset
    if time.time() - _last_reset < 60:  # at most once a minute
        return
    _last_reset = time.time()
    try:
        from yfinance.data import YfData

        d = YfData()
        with d._cookie_lock:
            d._cookie = None
            d._crumb = None
            if d._session is not None:
                d._session.cookies.clear()
        yf.cache.get_cookie_cache().store("curlCffi", None)
    except Exception:
        pass


def _yahoo(fn, *args):
    """Run a Yahoo fetch; on failure reset the session and retry once."""
    try:
        return fn(*args)
    except Exception:
        _reset_yahoo()
        time.sleep(1)
        return fn(*args)


def _yahoo_quote(sym: str) -> dict:
    now = time.time()
    hit = _cache.get(sym)
    if hit and now - hit[0] < TTL:
        return hit[1]
    # One chart request (5 daily bars + metadata). fast_info pulls a full year per ticker,
    # breaks on thin listings ('currentTradingPeriod') and gave an imprecise previous_close.
    t = yf.Ticker(sym)
    h = t.history(period="5d", auto_adjust=False)
    if h.empty:
        raise ValueError(f"no data for {sym} from Yahoo")
    meta = t.history_metadata or {}
    price = meta.get("regularMarketPrice") or h["Close"].iloc[-1]
    prev = h["Close"].iloc[-2] if len(h) > 1 else meta.get("chartPreviousClose")
    currency, exchange = meta.get("currency"), meta.get("exchangeName")
    if price is None or price != price:  # None or NaN
        raise ValueError(f"no price for {sym}")
    # as_of_utc = when the price was traded (yesterday's close before the market opens), not fetch time
    mt = meta.get("regularMarketTime")
    try:
        traded = float(mt.timestamp()) if hasattr(mt, "timestamp") else float(mt)
    except Exception:
        traded = now
    out = {
        "symbol": sym,
        "price": round(float(price), 4),
        "currency": currency,
        "previous_close": round(float(prev), 4) if prev else None,
        "exchange": exchange,
        "as_of_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(traded)),
        "fetched_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        "delayed": True,
        "source": "yahoo",
    }
    _cache[sym] = (now, out)
    return out


def _stale(sym: str, why: str) -> dict:
    hit = _last_good.get(sym)
    if not hit:
        try:
            hit = store.last_quote(sym)  # survives restarts
        except Exception:
            hit = None
    if not hit or time.time() - hit[0] > STALE_MAX:
        raise ValueError(why)
    return {**hit[1], "stale": True, "stale_age_s": int(time.time() - hit[0]), "stale_reason": why}


def _remember(out: dict) -> dict:
    _last_good[out["symbol"]] = (time.time(), out)
    try:
        store.save_quote(out)
    except Exception:
        pass  # the cache must never break a quote
    return out


def _quote(sym: str, source: Source = "auto") -> dict:
    if source == "eodhd":
        return _remember(eodhd.quote(sym))
    try:
        out = _yahoo(_yahoo_quote, sym)
    except Exception as e:
        if source == "yahoo" or not eodhd.enabled():
            return _stale(sym, f"yahoo: {e}")
        if not _use_eodhd_fallback(sym):
            return _stale(sym, f"yahoo: {e}. Yahoo is up, so the symbol is likely wrong; EODHD not queried "
                               "(pass source='eodhd' to force it)")
        try:
            out = eodhd.quote(sym)
        except Exception as e2:
            return _stale(sym, f"yahoo: {e}; eodhd: {e2}")
    return _remember(out)


def _yahoo_closes(sym: str, start: date, end_incl: date) -> list[dict]:
    h = yf.Ticker(sym).history(
        start=start.isoformat(), end=(end_incl + timedelta(days=1)).isoformat(), auto_adjust=False
    )
    if h.empty:
        raise ValueError(f"no data for {sym} from Yahoo")
    return [{"date": i.date().isoformat(), "close": round(float(r["Close"]), 4)} for i, r in h.iterrows()]


def _fetch_closes(sym: str, start: date, end_incl: date, source: Source) -> tuple[list[dict], str]:
    if source != "eodhd":
        try:
            return _yahoo(_yahoo_closes, sym, start, end_incl), "yahoo"
        except Exception as e:
            if source == "yahoo" or not eodhd.enabled():
                raise
            if not _use_eodhd_fallback(sym):
                raise ValueError(f"{e}. Yahoo is up, so the symbol is likely wrong; EODHD not queried "
                                 "(pass source='eodhd' to force it)") from None
    return eodhd.eod(sym, start.isoformat(), end_incl.isoformat()), "eodhd"


def _has_weekday(a: date, b: date) -> bool:
    return any((a + timedelta(days=i)).weekday() < 5 for i in range(min((b - a).days + 1, 7)))


def _closes(sym: str, start: date, end_incl: date, source: Source) -> tuple[list[dict], str]:
    """Completed days come from SQLite; only missing spans are fetched. An explicit source
    (yahoo/eodhd) bypasses the cache so it works as a cross-check."""
    if source != "auto":
        return _fetch_closes(sym, start, end_incl, source)
    if start > end_incl:
        return [], "cache"
    today = date.today()
    last_done = min(end_incl, today - timedelta(days=1))
    used = {"cache"}
    if start <= last_done:
        cov = store.coverage(sym)
        if cov is None:
            gaps = [(start, last_done)]
        else:
            gaps = []
            if start < cov[0]:
                gaps.append((start, cov[0] - timedelta(days=1)))
            if last_done > cov[1]:
                gaps.append((cov[1] + timedelta(days=1), last_done))
        for a, b in gaps:
            if not _has_weekday(a, b):  # weekend-only gap: nothing to fetch
                store.save_closes(sym, [], "none", (a, b))
                continue
            # pad back a week so a holiday-only gap still returns rows instead of "no data"
            rows, src = _fetch_closes(sym, a - timedelta(days=7), b, "auto")
            store.save_closes(sym, rows, src, (a - timedelta(days=7), b))
            used.add(src)
    rows = store.get_closes(sym, start, last_done) if start <= last_done else []
    if end_incl >= today and today.weekday() < 5:
        live, src = _fetch_closes(sym, today - timedelta(days=7), today, "auto")
        rows += [r for r in live if r["date"] == today.isoformat()]  # today's bar is not final
        used.add(src)
    return rows, "+".join(sorted(used))


@mcp.tool(annotations=READ_ONLY)
def quote(symbol: str, source: Source = "auto") -> dict:
    """Latest price (about 15 min delayed) for one Yahoo ticker or ISIN. as_of_utc is when it
    traded: before a market opens it is the previous close. Currency is the listing
    currency: EUR for .DE ETFs, USD for AAPL/MSFT, HKD for 0700.HK. Use fx() to convert.
    source: auto (Yahoo, EODHD on failure), yahoo, or eodhd (for cross-checks; uses scarce quota)."""
    return _quote(resolve(symbol), source)


@mcp.tool(annotations=READ_ONLY)
def quotes(symbols: list[str]) -> list[dict]:
    """Latest prices for several tickers/ISINs (Yahoo, EODHD fallback per item). Errors are
    returned per item, not raised. Each item carries its listing currency; use fx() to convert."""
    res = []
    for s in symbols:
        try:
            res.append({"input": s, **_quote(resolve(s))})
        except Exception as e:
            res.append({"input": s, "error": str(e)})
    return res


@mcp.tool(annotations=READ_ONLY)
def close_on(symbol: str, day: str, source: Source = "auto") -> dict:
    """Closing price on a given date (YYYY-MM-DD), or on the last trading day before it.
    Split-adjusted: compare with old trade prices only after dividing by later split ratios."""
    sym = resolve(symbol)
    d = date.fromisoformat(day)
    rows, src = _closes(sym, d - timedelta(days=14), d, source)
    rows = [r for r in rows if r["date"] <= d.isoformat()]
    if not rows:
        raise ValueError(f"no data for {sym} up to {day}")
    return {"symbol": sym, **rows[-1], "source": src}


@mcp.tool(annotations=READ_ONLY)
def history(symbol: str, start: str, end: str, source: Source = "auto") -> list[dict]:
    """Daily closes between two dates (YYYY-MM-DD), both inclusive. Closes are split-adjusted
    (e.g. NVDA before its June 2024 10:1 split shows ~1/10 of the traded price), not dividend-adjusted."""
    rows, _ = _closes(resolve(symbol), date.fromisoformat(start), date.fromisoformat(end), source)
    return rows


@mcp.tool(annotations=READ_ONLY)
def fx(pair: str = "EURUSD", source: Source = "auto") -> dict:
    """Spot FX rate, e.g. EURUSD (USD per 1 EUR), EURHKD. EUR value = USD price / EURUSD."""
    return _quote(f"{pair.strip().upper()}=X", source)


@mcp.tool(annotations=READ_ONLY)
def ecb_rate(currency: str = "USD", day: str | None = None) -> dict:
    """Official ECB euro reference rate: units of `currency` per 1 EUR (e.g. USD 1.1179), for `day`
    (YYYY-MM-DD) or the last ECB publication before it; default latest. Published once per business
    day around 16:00 CET, so it is not intraday. About 30 currencies (USD, HKD, JPY, DKK, CHF, GBP...)."""
    return ecb.rate(currency, day)


@mcp.tool(annotations=READ_ONLY)
def ecb_rate_history(currency: str, start: str, end: str) -> list[dict]:
    """ECB reference rates (units per 1 EUR) between two dates, inclusive, one row per publication day."""
    return ecb.history(currency, start, end)


@mcp.tool(annotations=READ_ONLY)
def to_eur(amount: float, currency: str, day: str | None = None,
           source: Literal["ecb", "bdi"] = "ecb") -> dict:
    """Convert an amount in `currency` to EUR at the reference rate of `day` (or the last
    publication before it; default latest). source: ecb (default) or bdi (Banca d'Italia, the rate
    Italian tax law uses; covers ~170 currencies). E.g. a USD close on 2026-11-03 -> EUR."""
    r = (bdi if source == "bdi" else ecb).rate(currency, day)
    return {"amount": amount, "currency": r["currency"], "eur": round(amount / r["rate"], 4),
            "rate": r["rate"], "rate_date": r["date"], "source": r["source"]}


@mcp.tool(annotations=READ_ONLY)
def bdi_rate(currency: str = "USD", day: str | None = None) -> dict:
    """Banca d'Italia exchange rate: units of `currency` per 1 EUR for `day` (YYYY-MM-DD) or the
    last publication before it; default latest. The official rate for Italian tax purposes; equals the
    ECB rate for major currencies and covers ~170 currencies."""
    return bdi.rate(currency, day)


@mcp.tool(annotations=READ_ONLY)
def bdi_rate_history(currency: str, start: str, end: str) -> list[dict]:
    """Banca d'Italia rates (units per 1 EUR) between two dates, inclusive."""
    return bdi.history(currency, start, end)


_actions_cache: dict[str, tuple[float, dict]] = {}


def _yahoo_actions(sym: str) -> dict:
    t = yf.Ticker(sym)
    a = t.actions  # one 'max' chart request: dividends, splits (and capital gains for funds)
    meta = t.history_metadata or {}
    if a is None or (a.empty and not meta):
        raise ValueError(f"no data for {sym} from Yahoo")
    divs, splits = [], []
    for ts, row in a.iterrows():
        d = ts.date().isoformat()
        if row.get("Dividends", 0):
            divs.append({"ex_date": d, "amount": round(float(row["Dividends"]), 6)})
        if row.get("Stock Splits", 0):
            splits.append({"date": d, "ratio": float(row["Stock Splits"])})
    return {"symbol": sym, "currency": meta.get("currency"), "dividends": divs, "splits": splits}


@mcp.tool(annotations=READ_ONLY)
def corporate_actions(symbol: str, start: str = "2015-01-01", end: str | None = None) -> dict:
    """Dividends (per share, listing currency, by ex-date) and stock splits between two dates.
    Use it to reconcile old trade prices with split-adjusted closes (divide a pre-split trade price
    by the product of the later split ratios) and to see payouts of distributing ETFs and stocks.
    Dividend amounts are split-adjusted like the closes (NVDA's 0.04 paid in March 2024 shows as
    0.004): to get the amount actually paid, multiply by the ratios of every split after its ex-date
    (those in `splits` plus split_factor_after_end)."""
    sym = resolve(symbol)
    hit = _actions_cache.get(sym)
    if hit and time.time() - hit[0] < 12 * 3600:
        data = hit[1]
    else:
        data = _yahoo(_yahoo_actions, sym)
        _actions_cache[sym] = (time.time(), data)
    end = end or date.today().isoformat()
    divs = [d for d in data["dividends"] if start <= d["ex_date"] <= end]
    splits = [s for s in data["splits"] if start <= s["date"] <= end]
    later = 1.0
    for s in data["splits"]:
        if s["date"] > end:
            later *= s["ratio"]
    return {"symbol": sym, "currency": data["currency"], "dividends": divs,
            "dividends_total": round(sum(d["amount"] for d in divs), 6), "splits": splits,
            "split_factor_after_end": later, "source": "yahoo"}


@mcp.tool(annotations=READ_ONLY)
def isin_map() -> dict:
    """The ISIN -> ticker map: seeded entries plus ones learned from EODHD lookups."""
    return {"seed": ISINS, "learned": LEARNED}


@mcp.tool(annotations=READ_ONLY)
def sources_status() -> dict:
    """EODHD quota used today, and the local SQLite cache size (quotes logged, daily closes stored)."""
    try:
        db = store.stats()
    except Exception as e:
        db = {"error": str(e)}
    try:
        ecb_s = ecb.status()
    except Exception as e:
        ecb_s = {"error": str(e)}
    try:
        bdi_s = bdi.status()
    except Exception as e:
        bdi_s = {"error": str(e)}
    return {"yahoo": {"yfinance": yf.__version__}, "eodhd": eodhd.usage(), "ecb_fx": ecb_s,
            "bdi_fx": bdi_s, "cache_db": db}


class BearerAuth(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/healthz":
            return JSONResponse({"ok": True})
        got = request.headers.get("authorization", "")
        if not hmac.compare_digest(got.encode(), f"Bearer {TOKEN}".encode()):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)


app = mcp.http_app(path="/mcp")
app.add_middleware(BearerAuth)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8765)
