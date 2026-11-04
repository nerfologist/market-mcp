"""ECB euro foreign exchange reference rates (official, free, no key), cached in SQLite.

Rates are units of currency per 1 EUR, published once per TARGET business day around 16:00 CET.
The full history zip (~640 KB, every currency since 1999) is downloaded at most once an hour,
and only when the stored data is older than the latest rate that should be out."""
import csv
import io
import os
import sqlite3
import threading
import time
import urllib.request
import zipfile
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

HIST_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
DB_PATH = os.path.join(os.environ.get("DATA_DIR", "/data"), "market.db")
CET = ZoneInfo("Europe/Berlin")
_lock = threading.Lock()
_conn: sqlite3.Connection | None = None
_last_attempt = 0.0


def _db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute(
            "CREATE TABLE IF NOT EXISTS ecb_fx (date TEXT NOT NULL, currency TEXT NOT NULL, rate REAL NOT NULL,"
            " PRIMARY KEY (currency, date)) WITHOUT ROWID"
        )
    return _conn


def _expected_latest() -> date:
    """Latest date a rate should exist for: today after 16:30 CET, else the previous weekday."""
    now = datetime.now(CET)
    d = now.date() if now.hour * 60 + now.minute >= 16 * 60 + 30 else now.date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _stored_latest() -> date | None:
    r = _db().execute("SELECT MAX(date) FROM ecb_fx").fetchone()
    return date.fromisoformat(r[0]) if r and r[0] else None


def _refresh() -> None:
    global _last_attempt
    _last_attempt = time.time()
    req = urllib.request.Request(HIST_URL, headers={"User-Agent": "market-mcp/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        z = zipfile.ZipFile(io.BytesIO(r.read()))
    text = z.read(z.namelist()[0]).decode()
    rows = csv.reader(io.StringIO(text))
    header = [h.strip() for h in next(rows)]
    data = []
    for row in rows:
        if not row or not row[0]:
            continue
        d = row[0].strip()
        for ccy, v in zip(header[1:], row[1:]):
            v = v.strip()
            if ccy and v and v != "N/A":
                data.append((d, ccy, float(v)))
    db = _db()
    db.execute("BEGIN")
    db.executemany("INSERT OR REPLACE INTO ecb_fx VALUES (?,?,?)", data)
    db.execute("COMMIT")


def ensure_fresh() -> None:
    with _lock:
        latest = _stored_latest()
        if latest is not None and latest >= _expected_latest():
            return
        if time.time() - _last_attempt < 3600 and latest is not None:
            return  # ECB holiday or late publication: don't hammer, serve what we have
        try:
            _refresh()
        except Exception:
            if latest is None:
                raise


def rate(currency: str, day: str | None = None) -> dict:
    ccy = currency.strip().upper()
    if ccy == "EUR":
        return {"currency": "EUR", "date": day or date.today().isoformat(), "rate": 1.0, "source": "ecb"}
    ensure_fresh()
    d = day or date.today().isoformat()
    with _lock:
        r = _db().execute(
            "SELECT date, rate FROM ecb_fx WHERE currency=? AND date<=? ORDER BY date DESC LIMIT 1", (ccy, d)
        ).fetchone()
    if not r:
        raise ValueError(f"no ECB rate for {ccy} on or before {d} (ECB publishes ~30 currencies, not all)")
    return {"currency": ccy, "date": r[0], "rate": r[1], "per": "1 EUR", "source": "ecb"}


def history(currency: str, start: str, end: str) -> list[dict]:
    ensure_fresh()
    with _lock:
        rows = _db().execute(
            "SELECT date, rate FROM ecb_fx WHERE currency=? AND date BETWEEN ? AND ? ORDER BY date",
            (currency.strip().upper(), start, end),
        ).fetchall()
    return [{"date": d, "rate": v} for d, v in rows]


def status() -> dict:
    with _lock:
        latest = _stored_latest()
        n = _db().execute("SELECT COUNT(*) FROM ecb_fx").fetchone()[0]
    return {"latest_date": latest.isoformat() if latest else None, "rows": n,
            "expected_latest": _expected_latest().isoformat()}
