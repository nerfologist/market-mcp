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
-- portfolio valuations stored by checkpoint(): one row per day, re-running a day overwrites it
CREATE TABLE IF NOT EXISTS checkpoints (
  day TEXT PRIMARY KEY, total_eur REAL NOT NULL, cash_eur REAL, holdings_as_of TEXT,
  source TEXT NOT NULL, note TEXT, created_at TEXT NOT NULL, detail TEXT
) WITHOUT ROWID;
-- alerts.py: one row per threshold currently in the "fired" state (cleared when the price re-arms)
CREATE TABLE IF NOT EXISTS alert_state (
  key TEXT PRIMARY KEY, fired_at TEXT NOT NULL, fired_price REAL NOT NULL
) WITHOUT ROWID;
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


def save_checkpoint(day: str, total_eur: float, cash_eur: float | None, holdings_as_of: str | None,
                    source: str, note: str | None, detail: dict | None) -> None:
    import json
    with _lock:
        _db().execute(
            "INSERT OR REPLACE INTO checkpoints VALUES (?,?,?,?,?,?,?,?)",
            (day, total_eur, cash_eur, holdings_as_of, source, note,
             time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), json.dumps(detail) if detail else None),
        )


def get_checkpoint(day: str) -> dict | None:
    import json
    with _lock:
        r = _db().execute(
            "SELECT day, total_eur, cash_eur, holdings_as_of, source, note, created_at, detail "
            "FROM checkpoints WHERE day=?", (day,)
        ).fetchone()
    if not r:
        return None
    return {"day": r[0], "total_eur": r[1], "cash_eur": r[2], "holdings_as_of": r[3], "source": r[4],
            "note": r[5], "created_at": r[6], "detail": json.loads(r[7]) if r[7] else None}


def list_checkpoints() -> list[dict]:
    with _lock:
        rows = _db().execute(
            "SELECT day, total_eur, cash_eur, holdings_as_of, source, note, created_at "
            "FROM checkpoints ORDER BY day"
        ).fetchall()
    return [{"day": r[0], "total_eur": r[1], "cash_eur": r[2], "holdings_as_of": r[3], "source": r[4],
             "note": r[5], "created_at": r[6]} for r in rows]


def delete_checkpoint(day: str) -> bool:
    with _lock:
        n = _db().execute("DELETE FROM checkpoints WHERE day=?", (day,)).rowcount
    return n > 0


def alert_states() -> dict[str, dict]:
    with _lock:
        rows = _db().execute("SELECT key, fired_at, fired_price FROM alert_state").fetchall()
    return {k: {"fired_at": a, "fired_price": p} for k, a, p in rows}


def alert_fire(key: str, price: float) -> None:
    with _lock:
        _db().execute("INSERT OR REPLACE INTO alert_state VALUES (?,?,?)",
                      (key, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), price))


def alert_rearm(key: str) -> None:
    with _lock:
        _db().execute("DELETE FROM alert_state WHERE key=?", (key,))
