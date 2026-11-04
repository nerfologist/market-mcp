"""Banca d'Italia exchange rates (tassidicambio.bancaditalia.it, free, no key), cached in SQLite.

These are the rates Italian tax law refers to for converting foreign-currency amounts. For the ~30
currencies the ECB publishes they equal the ECB reference rate; BdI adds ~140 more. Rates are units
of currency per 1 EUR. Each currency's series is fetched once (from 2010) and then topped up."""
import json
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

BASE = "https://tassidicambio.bancaditalia.it/terzevalute-wf-web/rest/v1.0"
DB_PATH = os.path.join(os.environ.get("DATA_DIR", "/data"), "market.db")
FIRST = date(2010, 1, 1)
CET = ZoneInfo("Europe/Rome")
_lock = threading.Lock()
_conn: sqlite3.Connection | None = None
_last_try: dict[str, float] = {}


def _db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.executescript(
            "CREATE TABLE IF NOT EXISTS bdi_fx (currency TEXT NOT NULL, date TEXT NOT NULL, rate REAL NOT NULL,"
            " PRIMARY KEY (currency, date)) WITHOUT ROWID;"
            "CREATE TABLE IF NOT EXISTS bdi_coverage (currency TEXT PRIMARY KEY, start TEXT NOT NULL, end TEXT NOT NULL);"
        )
    return _conn


def _get(path: str, **params) -> dict:
    url = f"{BASE}/{path}?{urllib.parse.urlencode({**params, 'lang': 'en'})}"
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "market-mcp/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def _fetch(ccy: str, start: date, end: date) -> None:
    d = _get("dailyTimeSeries", startDate=start.isoformat(), endDate=end.isoformat(),
             baseCurrencyIsoCode=ccy, currencyIsoCode="EUR")
    rows = [(ccy, r["referenceDate"], float(r["avgRate"])) for r in d.get("rates") or [] if r.get("avgRate")]
    db = _db()
    db.execute("BEGIN")
    db.executemany("INSERT OR REPLACE INTO bdi_fx VALUES (?,?,?)", rows)
    cov = db.execute("SELECT start, end FROM bdi_coverage WHERE currency=?", (ccy,)).fetchone()
    a, b = start, end
    if cov:
        a, b = min(a, date.fromisoformat(cov[0])), max(b, date.fromisoformat(cov[1]))
    db.execute("INSERT OR REPLACE INTO bdi_coverage VALUES (?,?,?)", (ccy, a.isoformat(), b.isoformat()))
    db.execute("COMMIT")


def _ensure(ccy: str, start: date, end: date) -> None:
    """Make sure [start, end] has been fetched; the recent edge is re-checked at most hourly
    because BdI publishes some currencies a day late."""
    today = datetime.now(CET).date()
    end = min(end, today)
    with _lock:
        cov = _db().execute("SELECT start, end FROM bdi_coverage WHERE currency=?", (ccy,)).fetchone()
        if cov is None:
            _fetch(ccy, min(start, FIRST), today)
            _last_try[ccy] = time.time()
            return
        c0, c1 = date.fromisoformat(cov[0]), date.fromisoformat(cov[1])
        if start < c0:
            _fetch(ccy, start, c0)
        if end >= c1 - timedelta(days=3) and time.time() - _last_try.get(ccy, 0) > 3600:
            _last_try[ccy] = time.time()
            try:
                _fetch(ccy, c1 - timedelta(days=7), today)  # re-read the last week: late publications
            except Exception:
                pass  # serve what is stored


def rate(currency: str, day: str | None = None) -> dict:
    ccy = currency.strip().upper()
    if ccy == "EUR":
        return {"currency": "EUR", "date": day or date.today().isoformat(), "rate": 1.0, "source": "bdi"}
    d = date.fromisoformat(day) if day else datetime.now(CET).date()
    _ensure(ccy, d - timedelta(days=15), d)
    with _lock:
        r = _db().execute(
            "SELECT date, rate FROM bdi_fx WHERE currency=? AND date<=? ORDER BY date DESC LIMIT 1",
            (ccy, d.isoformat()),
        ).fetchone()
    if not r:
        raise ValueError(f"no Banca d'Italia rate for {ccy} on or before {d}")
    return {"currency": ccy, "date": r[0], "rate": r[1], "per": "1 EUR", "source": "bdi"}


def history(currency: str, start: str, end: str) -> list[dict]:
    ccy = currency.strip().upper()
    _ensure(ccy, date.fromisoformat(start), date.fromisoformat(end))
    with _lock:
        rows = _db().execute(
            "SELECT date, rate FROM bdi_fx WHERE currency=? AND date BETWEEN ? AND ? ORDER BY date",
            (ccy, start, end),
        ).fetchall()
    return [{"date": d, "rate": v} for d, v in rows]


def status() -> dict:
    with _lock:
        rows = _db().execute("SELECT currency, start, end FROM bdi_coverage ORDER BY currency").fetchall()
        n = _db().execute("SELECT COUNT(*) FROM bdi_fx").fetchone()[0]
    return {"currencies_cached": {c: f"{a}..{b}" for c, a, b in rows}, "rows": n}
