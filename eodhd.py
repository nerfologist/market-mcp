"""EODHD client used as fallback and ISIN resolver. Free plan: 20 calls/day, so every call is
budgeted (persistent counter under /data) and cached."""
import json
import os
import threading
import time
import urllib.parse
import urllib.request
from datetime import date

KEY = os.environ.get("EODHD_API_KEY", "").strip().strip('"')
DAILY_BUDGET = int(os.environ.get("EODHD_DAILY_BUDGET", "18"))  # keep 2 spare of the 20
DATA_DIR = os.environ.get("DATA_DIR", "/data")
_STATE = os.path.join(DATA_DIR, "eodhd_usage.json")
_lock = threading.Lock()
_rt_cache: dict[str, tuple[float, dict]] = {}
_eod_cache: dict[tuple[str, str, str], list] = {}
_misses: dict[str, str] = {}  # code -> date of a failed lookup
RT_TTL = 900

# Yahoo suffix -> EODHD exchange code, and the currency each listing trades in
SUFFIX = {"DE": "XETRA", "F": "F", "MI": "MI", "SW": "SW", "PA": "PA", "AS": "AS", "L": "LSE", "HK": "HK",
          "CO": "CO", "T": "TSE"}
EX_TO_SUFFIX = {v: k for k, v in SUFFIX.items()}
EX_CCY = {"XETRA": "EUR", "F": "EUR", "MI": "EUR", "PA": "EUR", "AS": "EUR", "HK": "HKD", "US": "USD",
          "CO": "DKK", "TSE": "JPY"}


def enabled() -> bool:
    return bool(KEY)


def to_eodhd(yahoo: str) -> str:
    if yahoo.endswith("=X"):
        return f"{yahoo[:-2]}.FOREX"
    if "." in yahoo:
        base, suf = yahoo.rsplit(".", 1)
        if suf in SUFFIX:
            return f"{base}.{SUFFIX[suf]}"
    return f"{yahoo}.US"


def to_yahoo(code: str, exchange: str) -> str | None:
    if exchange == "US":
        return code
    suf = EX_TO_SUFFIX.get(exchange)
    return f"{code}.{suf}" if suf else None


def _load() -> dict:
    try:
        with open(_STATE) as f:
            s = json.load(f)
    except Exception:
        s = {}
    if s.get("date") != date.today().isoformat():
        s = {"date": date.today().isoformat(), "calls": 0}
    return s


def usage() -> dict:
    with _lock:
        s = _load()
    return {"enabled": enabled(), "date": s["date"], "calls_today": s["calls"], "daily_budget": DAILY_BUDGET}


def _get(path: str, **params):
    if not enabled():
        raise RuntimeError("EODHD not configured")
    with _lock:
        s = _load()
        if s["calls"] >= DAILY_BUDGET:
            raise RuntimeError(f"EODHD daily budget used ({s['calls']}/{DAILY_BUDGET}); resets at midnight")
        s["calls"] += 1
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(_STATE, "w") as f:
            json.dump(s, f)
    q = urllib.parse.urlencode({**params, "api_token": KEY, "fmt": "json"})
    try:
        with urllib.request.urlopen(f"https://eodhd.com/api/{path}?{q}", timeout=15) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"EODHD HTTP {e.code} for {path}") from None  # never echo the URL (has the key)
    except Exception as e:
        raise RuntimeError(f"EODHD request failed for {path}: {type(e).__name__}") from None


def search_isin(isin: str) -> list[dict]:
    return _get(f"search/{isin}") or []


def best_listing(isin: str) -> dict | None:
    """Pick a listing for an ISIN. ETFs/ETCs: Xetra first, then other EUR venues (UCITS products
    are quoted in EUR there). Stocks: the primary listing (most liquid, reference price)."""
    hits = search_isin(isin)
    hits = [h for h in hits if to_yahoo(h["Code"], h["Exchange"])]
    if not hits:
        return None
    is_fund = any(h.get("Type") in ("ETF", "FUND", "ETC") for h in hits)
    rank = {"XETRA": 0, "MI": 1, "PA": 2, "AS": 3, "F": 4, "US": 5}
    if is_fund:
        hits.sort(key=lambda h: (rank.get(h["Exchange"], 9), not h.get("isPrimary")))
    else:
        hits.sort(key=lambda h: (not h.get("isPrimary"), h["Exchange"] != "US", rank.get(h["Exchange"], 9)))
    h = hits[0]
    return {"yahoo": to_yahoo(h["Code"], h["Exchange"]), "name": h.get("Name"), "currency": h.get("Currency")}


def quote(yahoo: str) -> dict:
    code = to_eodhd(yahoo)
    now = time.time()
    hit = _rt_cache.get(code)
    if hit and now - hit[0] < RT_TTL:
        return hit[1]
    today = date.today().isoformat()
    if _misses.get(code) == today:
        raise ValueError(f"EODHD has no price for {code} (cached miss)")
    d = _get(f"real-time/{code}")
    if not isinstance(d, dict) or d.get("close") in (None, "NA"):
        _misses[code] = today  # don't spend quota on the same bad symbol again today
        raise ValueError(f"EODHD has no price for {code}")
    ex = code.rsplit(".", 1)[1]
    out = {
        "symbol": yahoo,
        "price": round(float(d["close"]), 4),
        "currency": "USD" if ex == "FOREX" else EX_CCY.get(ex),
        "previous_close": round(float(d["previousClose"]), 4) if d.get("previousClose") not in (None, "NA") else None,
        "exchange": ex,
        "as_of_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(d["timestamp"]))),
        "delayed": True,
        "source": "eodhd",
    }
    _rt_cache[code] = (now, out)
    return out


def eod(yahoo: str, start: str, end: str) -> list[dict]:
    """Daily bars, start and end inclusive."""
    code = to_eodhd(yahoo)
    k = (code, start, end)
    if k not in _eod_cache:
        _eod_cache[k] = _get(f"eod/{code}", **{"from": start, "to": end}) or []
    return [{"date": r["date"], "close": round(float(r["close"]), 4)} for r in _eod_cache[k]]
