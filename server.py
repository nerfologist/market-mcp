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
        "divide. Historical closes are split-adjusted; corporate_actions lists splits and dividends. "
        "EUR views: quote_eur/quotes_eur (spot for the price, previous ECB fix for the previous close, "
        "so fx_contribution_pct is the FX part of the day move). portfolio() values the hand-maintained "
        "holdings file (live, or at a past close with day=); checkpoint(day) stores a valuation, "
        "checkpoints() lists them, dietz() computes Modified Dietz between two with the caller's flows; "
        "dividends(symbol, year) converts payouts at the Banca d'Italia rate. The server holds no targets "
        "or advice: it values what it is given."
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
    # an in-progress or halted day can carry a NaN close; SQLite would store it as NULL
    return [{"date": i.date().isoformat(), "close": round(float(r["Close"]), 4)}
            for i, r in h.iterrows() if r["Close"] == r["Close"]]


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


# ----------------------------------------------------------------------------------------------
# v2: EUR views, holdings valuation, checkpoints, Modified Dietz, dividends in EUR.
# The server holds no opinions: no targets, no buy/sell logic. It values what holdings.json says.
# ----------------------------------------------------------------------------------------------

HOLDINGS_PATH = os.environ.get("HOLDINGS_PATH", "/config/holdings.json")
HOLDINGS_MAX_AGE_DAYS = 7
# checkpoint() writes to the cache DB; everything else stays READ_ONLY
WRITES = {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}


def _pct(a: float | None, b: float | None) -> float | None:
    return round((a / b - 1) * 100, 4) if a is not None and b else None


def _fx_spot(ccy: str) -> tuple[float, str]:
    """Yahoo spot: units of `ccy` per 1 EUR. EURHKD is not always quoted, so fall back to
    EURUSD x USDHKD (and the same for any other currency)."""
    ccy = ccy.upper()
    if ccy == "EUR":
        return 1.0, "none"
    try:
        q = _quote(f"EUR{ccy}=X")
        if q["price"] and not q.get("stale"):
            return float(q["price"]), f"EUR{ccy}"
    except Exception:
        q = None
    try:
        usd = _quote("EURUSD=X")
        cross = _quote(f"USD{ccy}=X")
        return float(usd["price"]) * float(cross["price"]), f"EURUSD*USD{ccy}"
    except Exception as e:
        if q:  # stale EURccy beats nothing
            return float(q["price"]), f"EUR{ccy}(stale)"
        raise ValueError(f"no spot rate for {ccy}: {e}")


def _prev_fix(ccy: str, as_of: str) -> dict:
    """ECB reference rate of the business day before `as_of` (YYYY-MM-DD): the fix the previous
    close was last marked at, so the day's EUR move splits into price move + FX move."""
    d = date.fromisoformat(as_of) - timedelta(days=1)
    return ecb.rate(ccy, d.isoformat())


def _quote_eur(sym: str) -> dict:
    q = _quote(sym)
    ccy = (q.get("currency") or "EUR").upper()
    out = {**q}
    if ccy == "EUR":
        out.update(price_eur=q["price"], previous_close_eur=q.get("previous_close"), fx_rate_used=1.0,
                   fx_pair_used="none", prev_fx_rate_used=1.0)
    else:
        spot, pair = _fx_spot(ccy)
        prev_fix = _prev_fix(ccy, q["as_of_utc"][:10])
        out.update(price_eur=round(q["price"] / spot, 4),
                   previous_close_eur=round(q["previous_close"] / prev_fix["rate"], 4) if q.get("previous_close") else None,
                   fx_rate_used=round(spot, 6), fx_pair_used=pair,
                   prev_fx_rate_used=prev_fix["rate"], prev_fx_date=prev_fix["date"])
    out["day_change_pct"] = _pct(q["price"], q.get("previous_close"))
    out["day_change_eur_pct"] = _pct(out["price_eur"], out["previous_close_eur"])
    if out["day_change_pct"] is not None and out["day_change_eur_pct"] is not None:
        out["fx_contribution_pct"] = round(out["day_change_eur_pct"] - out["day_change_pct"], 4)
    else:
        out["fx_contribution_pct"] = None
    return out


@mcp.tool(annotations=READ_ONLY)
def quote_eur(symbol: str) -> dict:
    """quote() plus EUR fields: price_eur, previous_close_eur, day_change_pct (listing currency),
    day_change_eur_pct, fx_rate_used, fx_contribution_pct = day_change_eur_pct - day_change_pct.
    Convention: price_eur uses the live Yahoo spot (EURUSD, EURHKD or EURUSD x USDHKD when EURHKD is
    missing); previous_close_eur uses the ECB reference rate of the business day before the quote's
    as_of date, so the FX contribution is spot-vs-previous-fix, the same split DEGIRO's day P/L
    implies. EUR listings: fx_rate_used 1, contribution 0."""
    return _quote_eur(resolve(symbol))


@mcp.tool(annotations=READ_ONLY)
def quotes_eur(symbols: list[str]) -> list[dict]:
    """quotes() with the EUR fields of quote_eur() on every item (same FX convention). Errors are
    returned per item, not raised."""
    res = []
    for s in symbols:
        try:
            res.append({"input": s, **_quote_eur(resolve(s))})
        except Exception as e:
            res.append({"input": s, "error": str(e)})
    return res


@mcp.tool(annotations=READ_ONLY)
def closes_on(symbols: list[str], day: str) -> list[dict]:
    """close_on() for several tickers/ISINs on one date (YYYY-MM-DD, or the last trading day before
    it). Errors are returned per item. For a counterfactual column: closes_on(sold_lines, day)."""
    res = []
    for s in symbols:
        try:
            res.append({"input": s, **close_on(s, day)})
        except Exception as e:
            res.append({"input": s, "error": str(e)})
    return res


def _load_holdings() -> dict:
    try:
        with open(HOLDINGS_PATH) as f:
            h = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"holdings file not found at {HOLDINGS_PATH}; see config/holdings.example.json")
    for k in ("as_of", "positions"):
        if k not in h:
            raise ValueError(f"holdings file lacks '{k}'")
    h.setdefault("cash_eur", 0.0)
    h.setdefault("buckets", {})
    return h


def _currency(sym: str) -> str:
    return (_quote(sym).get("currency") or "EUR").upper()


def _value_on(sym: str, day: str) -> dict:
    """Close on `day` (or the last trading day before) and the close before that, both in EUR at the
    ECB reference rate of their own date."""
    d = date.fromisoformat(day)
    rows, src = _closes(sym, d - timedelta(days=21), d, "auto")
    rows = [r for r in rows if r["date"] <= day]
    if not rows:
        raise ValueError(f"no close for {sym} up to {day}")
    if rows[-1]["close"] != rows[-1]["close"]:
        raise ValueError(f"close for {sym} on {rows[-1]['date']} is not a number")
    ccy = _currency(sym)
    last, prev = rows[-1], (rows[-2] if len(rows) > 1 else None)
    fix = ecb.rate(ccy, last["date"])
    out = {"symbol": sym, "currency": ccy, "price": last["close"], "price_date": last["date"],
           "price_eur": round(last["close"] / fix["rate"], 4), "fx_rate_used": fix["rate"],
           "fx_date": fix["date"], "fx_source": "ecb", "source": src}
    if prev:
        pfix = ecb.rate(ccy, prev["date"])
        out.update(previous_close=prev["close"], previous_close_date=prev["date"],
                   previous_close_eur=round(prev["close"] / pfix["rate"], 4), prev_fx_rate_used=pfix["rate"])
    else:
        out.update(previous_close=None, previous_close_eur=None)
    return out


def _value_holdings(h: dict, day: str | None) -> dict:
    """Value every position of a holdings dict, live (quotes_eur) or at the close of `day`."""
    bucket_of = {isin: b for b, isins in h["buckets"].items() for isin in isins}
    positions, warnings = [], []
    for p in h["positions"]:
        isin, qty = p["isin"].strip().upper(), float(p["qty"])
        row = {"isin": isin, "name": p.get("name"), "bucket": bucket_of.get(isin, "Unassigned"), "qty": qty,
               "avg_cost_eur": p.get("avg_cost_eur")}
        try:
            sym = resolve(isin)
            v = _value_on(sym, day) if day else _quote_eur(sym)
            row.update(symbol=sym, currency=v.get("currency"), price=v["price"], price_eur=v["price_eur"],
                       fx_rate_used=v["fx_rate_used"], value_eur=round(qty * v["price_eur"], 2))
            if day:
                row["price_date"] = v["price_date"]
            else:
                row["as_of_utc"] = v.get("as_of_utc")
                if v.get("stale"):
                    row["stale"] = True
                    warnings.append(f"{sym}: stale quote ({v.get('stale_reason')})")
            prev_eur = v.get("previous_close_eur")
            if prev_eur:
                row["day_change_eur"] = round(qty * (v["price_eur"] - prev_eur), 2)
                row["day_change_pct"] = _pct(v["price"], v.get("previous_close"))
                row["day_change_eur_pct"] = _pct(v["price_eur"], prev_eur)
            else:
                row["day_change_eur"] = None
            if p.get("avg_cost_eur"):
                row["unrealised_pl_eur"] = round(qty * (v["price_eur"] - float(p["avg_cost_eur"])), 2)
                row["unrealised_pl_pct"] = _pct(v["price_eur"], float(p["avg_cost_eur"]))
        except Exception as e:
            row["error"] = str(e)
            warnings.append(f"{isin}: {e}")
        positions.append(row)
    valued = [r for r in positions if "error" not in r]
    cash = float(h.get("cash_eur") or 0.0)
    total = round(sum(r["value_eur"] for r in valued) + cash, 2)
    day_chg = round(sum(r["day_change_eur"] or 0.0 for r in valued), 2)
    invested = sum(r["qty"] * float(r["avg_cost_eur"]) for r in valued if r.get("avg_cost_eur"))
    for r in valued:
        r["weight_pct"] = round(r["value_eur"] / total * 100, 2) if total else None
    buckets = {}
    for name in list(h["buckets"]) + (["Unassigned"] if any(r["bucket"] == "Unassigned" for r in valued) else []):
        rows = [r for r in valued if r["bucket"] == name]
        val = round(sum(r["value_eur"] for r in rows), 2)
        buckets[name] = {"value_eur": val, "weight_pct": round(val / total * 100, 2) if total else None,
                         "day_change_eur": round(sum(r["day_change_eur"] or 0.0 for r in rows), 2),
                         "positions": [r["symbol"] for r in rows]}
    if cash:
        buckets["Cash"] = {"value_eur": round(cash, 2), "weight_pct": round(cash / total * 100, 2) if total else None,
                           "day_change_eur": 0.0, "positions": []}
    as_of = date.fromisoformat(h["as_of"])
    age = (date.today() - as_of).days
    if age > HOLDINGS_MAX_AGE_DAYS:
        warnings.insert(0, f"holdings file as_of {h['as_of']} is {age} days old; update it from DEGIRO")
    return {
        "valuation": f"close {day}" if day else "live (about 15 min delayed)",
        "holdings_as_of": h["as_of"], "holdings_age_days": age,
        "totals": {"value_eur": total, "cash_eur": round(cash, 2), "invested_eur": round(invested, 2),
                   "unrealised_pl_eur": round(total - cash - invested, 2) if invested else None,
                   "day_change_eur": day_chg,
                   "day_change_pct": round(day_chg / (total - day_chg) * 100, 4) if total - day_chg else None,
                   "positions_valued": len(valued), "positions_failed": len(positions) - len(valued)},
        "buckets": buckets, "positions": positions, "warnings": warnings,
    }


@mcp.tool(annotations=READ_ONLY)
def portfolio(day: str | None = None) -> dict:
    """Value the holdings file (maintained by hand from DEGIRO, never written by the server): per
    position symbol, qty, price_eur, value_eur, weight_pct, unrealised_pl_eur/pct vs avg_cost_eur,
    day_change_eur; per bucket value_eur, weight_pct, day_change_eur; totals incl. cash. Without
    `day`: live prices via the quotes_eur convention (Yahoo spot, previous close at the previous ECB
    fix). With `day` (YYYY-MM-DD): closes of that day converted at the ECB reference rate of the day.
    Warns when the holdings file is older than 7 days. A `day` before the last trade silently values
    TODAY's holdings at that day's prices (the file has no history): for a past total from before the
    holdings changed, store the broker figure with checkpoint(day, total_eur=...) instead.
    No targets or advice: it values what it is given."""
    return _value_holdings(_load_holdings(), day)


@mcp.tool(annotations=WRITES)
def checkpoint(day: str, total_eur: float | None = None, note: str | None = None) -> dict:
    """Store a portfolio valuation for `day` (YYYY-MM-DD) in SQLite, for checkpoints() and dietz().
    Default: values the current holdings file at that day's closes (ECB rate of the day for USD/HKD)
    and stores total and per-line values. Pass `total_eur` to store a known figure instead (e.g. a
    DEGIRO export from before the holdings changed). Idempotent: re-running a day overwrites it."""
    date.fromisoformat(day)
    if total_eur is not None:
        store.save_checkpoint(day, round(float(total_eur), 2), None, None, "manual", note, None)
        return {"day": day, "total_eur": round(float(total_eur), 2), "source": "manual", "note": note, "stored": True}
    h = _load_holdings()
    v = _value_holdings(h, day)
    if v["totals"]["positions_failed"]:
        raise ValueError("not stored: some positions could not be valued: " + "; ".join(v["warnings"]))
    detail = {"positions": [{k: r.get(k) for k in ("isin", "symbol", "qty", "price", "currency", "price_date",
                                                   "fx_rate_used", "price_eur", "value_eur")} for r in v["positions"]],
              "buckets": {b: x["value_eur"] for b, x in v["buckets"].items()}}
    store.save_checkpoint(day, v["totals"]["value_eur"], v["totals"]["cash_eur"], h["as_of"], "valued", note, detail)
    return {"day": day, "total_eur": v["totals"]["value_eur"], "cash_eur": v["totals"]["cash_eur"],
            "holdings_as_of": h["as_of"], "source": "valued", "note": note, "stored": True,
            "buckets": detail["buckets"], "positions": detail["positions"], "warnings": v["warnings"]}


@mcp.tool(annotations=READ_ONLY)
def checkpoints(day: str | None = None) -> list[dict] | dict:
    """Stored checkpoints oldest-first (day, total_eur, cash_eur, holdings_as_of, source valued|manual,
    note). Pass `day` to get one row with its per-line detail."""
    if day:
        r = store.get_checkpoint(day)
        if not r:
            raise ValueError(f"no checkpoint stored for {day}")
        return r
    return store.list_checkpoints()


@mcp.tool(annotations=READ_ONLY)
def dietz(start_day: str, end_day: str, flows: list[dict] | None = None) -> dict:
    """Modified Dietz return between two stored checkpoints. flows: external cash flows in between,
    [{"day": "YYYY-MM-DD", "amount_eur": 2000}], deposits positive, withdrawals negative (the caller
    supplies them; holdings changes are not tracked). Each flow is weighted by the fraction of the
    period remaining after its day. R = (V1 - V0 - F) / (V0 + sum(w_i * F_i))."""
    v0, v1 = store.get_checkpoint(start_day), store.get_checkpoint(end_day)
    if not v0 or not v1:
        missing = [d for d, v in ((start_day, v0), (end_day, v1)) if not v]
        raise ValueError(f"no checkpoint stored for {', '.join(missing)}; run checkpoint(day) first")
    t0, t1 = date.fromisoformat(start_day), date.fromisoformat(end_day)
    T = (t1 - t0).days
    if T <= 0:
        raise ValueError("end_day must be after start_day")
    total_f, weighted, rows = 0.0, 0.0, []
    for f in flows or []:
        fd, amt = date.fromisoformat(f["day"]), float(f["amount_eur"])
        if not (t0 <= fd <= t1):
            raise ValueError(f"flow on {f['day']} is outside {start_day}..{end_day}")
        w = (T - (fd - t0).days) / T
        total_f += amt
        weighted += w * amt
        rows.append({"day": f["day"], "amount_eur": amt, "weight": round(w, 6)})
    denom = v0["total_eur"] + weighted
    r = (v1["total_eur"] - v0["total_eur"] - total_f) / denom
    return {"start_day": start_day, "end_day": end_day, "days": T, "start_eur": v0["total_eur"],
            "end_eur": v1["total_eur"], "flows": rows, "flows_total_eur": round(total_f, 2),
            "gain_eur": round(v1["total_eur"] - v0["total_eur"] - total_f, 2), "return_pct": round(r * 100, 4),
            "method": "modified dietz, day-weighted flows"}


@mcp.tool(annotations=READ_ONLY)
def dividends(symbol: str, year: int, qty: float | None = None,
              pay_dates: dict[str, str] | None = None) -> dict:
    """Cash dividends of one ticker/ISIN in a calendar year, each converted to EUR at the Banca
    d'Italia rate (the rate Italian tax law uses). Yahoo reports the ex-date, not the payment date:
    by default the ex-date rate is used. For a tax filing the payment date is the taxable event, so
    pass `pay_dates` = {"<ex_date>": "<pay_date>", ...} from the broker's dividend statement and
    those payments convert at the pay-date rate (`rate_basis` says which was used per payment).
    Amounts are per share, actually paid (split-adjustment undone); pass `qty` to add gross totals
    for a holding. EUR listings convert at 1. The broker statement stays the source of record; this
    is a cross-check."""
    sym = resolve(symbol)
    ca = corporate_actions(sym, f"{year}-01-01", f"{year}-12-31")
    ccy = (ca.get("currency") or "EUR").upper()
    rows, tot_ccy, tot_eur = [], 0.0, 0.0
    for d in ca["dividends"]:
        factor = ca["split_factor_after_end"]
        for s in ca["splits"]:
            if s["date"] > d["ex_date"]:
                factor *= s["ratio"]
        amt = round(d["amount"] * factor, 6)
        pay = (pay_dates or {}).get(d["ex_date"])
        if pay:
            date.fromisoformat(pay)
        basis_day = pay or d["ex_date"]
        if ccy == "EUR":
            rate, rate_date, eur = 1.0, basis_day, amt
        else:
            r = bdi.rate(ccy, basis_day)
            rate, rate_date, eur = r["rate"], r["date"], round(amt / r["rate"], 6)
        row = {"ex_date": d["ex_date"], "pay_date": pay, "amount": amt, "currency": ccy, "rate": rate,
               "rate_date": rate_date, "rate_basis": "pay_date" if pay else "ex_date", "amount_eur": eur}
        if qty:
            row["gross"] = round(amt * qty, 2)
            row["gross_eur"] = round(eur * qty, 2)
        rows.append(row)
        tot_ccy += amt
        tot_eur += eur
    out = {"symbol": sym, "year": year, "currency": ccy, "payments": rows, "count": len(rows),
           "total_per_share": round(tot_ccy, 6), "total_per_share_eur": round(tot_eur, 6),
           "fx_source": "bdi", "note": "ex-dates from Yahoo, which has no payment dates; pass pay_dates "
                                      "from the broker statement to convert at the pay-date rate"}
    if qty:
        out["qty"] = qty
        out["gross_total"] = round(tot_ccy * qty, 2)
        out["gross_total_eur"] = round(tot_eur * qty, 2)
    return out


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
