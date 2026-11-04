"""SQLite cache in /data/market.db: every fetched quote (append-only log) and completed daily
closes with per-symbol coverage, so history is served locally and only gaps are fetched."""
import os
import sqlite3
import threading
import time
from datetime import date, timedelta

DB_PATH = os.path.join(os.environ.get("DATA_DIR", "/data"), "market.db")
_lock = threading.Lock()
_conn: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS quotes (
  symbol TEXT NOT NULL, fetched_at REAL NOT NULL, price REAL NOT NULL, currency TEXT,
  previous_close REAL, exchange TEXT, source TEXT NOT NULL, as_of_utc TEXT
);
CREATE INDEX IF NOT EXISTS quotes_sym_time ON quotes(symbol, fetched_at);
CREATE TABLE IF NOT EXISTS closes (
  symbol TEXT NOT NULL, date TEXT NOT NULL, close REAL NOT NULL, source TEXT NOT NULL,
  PRIMARY KEY (symbol, date)
) WITHOUT ROWID;
-- [start, end] date span per symbol whose closes are fully stored (holidays simply have no row)
CREATE TABLE IF NOT EXISTS coverage (
  symbol TEXT PRIMARY KEY, start TEXT NOT NULL, end TEXT NOT NULL
);
"""


def _db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.executescript(SCHEMA)
    return _conn


def save_quote(q: dict) -> None:
    with _lock:
        last = _db().execute(
            "SELECT as_of_utc, source, price FROM quotes WHERE symbol=? ORDER BY fetched_at DESC LIMIT 1",
            (q["symbol"],),
        ).fetchone()
        if last == (q.get("as_of_utc"), q.get("source", "yahoo"), q["price"]):
            return  # same in-memory cache hit served again; don't log duplicates
        _db().execute(
            "INSERT INTO quotes VALUES (?,?,?,?,?,?,?,?)",
            (q["symbol"], time.time(), q["price"], q.get("currency"), q.get("previous_close"),
             q.get("exchange"), q.get("source", "yahoo"), q.get("as_of_utc")),
        )


def last_quote(symbol: str) -> tuple[float, dict] | None:
    with _lock:
        r = _db().execute(
            "SELECT fetched_at, price, currency, previous_close, exchange, source, as_of_utc "
            "FROM quotes WHERE symbol=? ORDER BY fetched_at DESC LIMIT 1", (symbol,)
        ).fetchone()
    if not r:
        return None
    return r[0], {"symbol": symbol, "price": r[1], "currency": r[2], "previous_close": r[3],
                  "exchange": r[4], "as_of_utc": r[6], "delayed": True, "source": r[5]}


def get_closes(symbol: str, start: date, end: date) -> list[dict]:
    with _lock:
        rows = _db().execute(
            "SELECT date, close FROM closes WHERE symbol=? AND date BETWEEN ? AND ? ORDER BY date",
            (symbol, start.isoformat(), end.isoformat()),
        ).fetchall()
    return [{"date": d, "close": c} for d, c in rows]


def coverage(symbol: str) -> tuple[date, date] | None:
    with _lock:
        r = _db().execute("SELECT start, end FROM coverage WHERE symbol=?", (symbol,)).fetchone()
    return (date.fromisoformat(r[0]), date.fromisoformat(r[1])) if r else None


def save_closes(symbol: str, rows: list[dict], source: str, covered: tuple[date, date] | None) -> None:
    """Store completed days only (date < today) and widen coverage by `covered`, which must
    touch or overlap the existing span so coverage stays contiguous."""
    today = date.today().isoformat()
    final = [(symbol, r["date"], r["close"], source) for r in rows if r["date"] < today]
    with _lock:
        db = _db()
        db.execute("BEGIN")
        try:
            db.executemany("INSERT OR REPLACE INTO closes VALUES (?,?,?,?)", final)
            if covered:
                a, b = covered
                b = min(b, date.today() - timedelta(days=1))
                if a <= b:
                    r = db.execute("SELECT start, end FROM coverage WHERE symbol=?", (symbol,)).fetchone()
                    if r:
                        a = min(a, date.fromisoformat(r[0]))
                        b = max(b, date.fromisoformat(r[1]))
                    db.execute("INSERT OR REPLACE INTO coverage VALUES (?,?,?)",
                               (symbol, a.isoformat(), b.isoformat()))
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise


def stats() -> dict:
    with _lock:
        db = _db()
        q = db.execute("SELECT COUNT(*), COUNT(DISTINCT symbol) FROM quotes").fetchone()
        c = db.execute("SELECT COUNT(*), COUNT(DISTINCT symbol) FROM closes").fetchone()
    size = sum(os.path.getsize(p) for p in (DB_PATH, DB_PATH + "-wal") if os.path.exists(p))
    return {"path": DB_PATH, "size_kb": size // 1024, "quotes_logged": q[0], "quote_symbols": q[1],
            "closes_stored": c[0], "close_symbols": c[1]}
