"""Check the market-data MCP against a DEGIRO export, through the Claude Desktop bridge.

usage: python3 -I tests/portfolio_check.py Portfolio.csv Transactions.csv
Talks MCP over stdio to the same command Claude Desktop runs, so the CSVs stay on the Mac.
"""
import csv, json, os, select, subprocess, sys, time
from collections import defaultdict
from datetime import date, datetime, timedelta

CFG = os.path.expanduser("~/Library/Application Support/Claude/claude_desktop_config.json")


class Bridge:
    def __init__(self):
        c = json.load(open(CFG))["mcpServers"]["market-data"]
        env = {"HOME": os.environ["HOME"], "PATH": "/usr/bin:/bin", **c["env"]}
        self.p = subprocess.Popen([c["command"], *c["args"]], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.DEVNULL, env=env, text=True)
        self.n = 0
        self.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                "clientInfo": {"name": "portfolio-check", "version": "1"}})
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def _send(self, m):
        self.p.stdin.write(json.dumps(m) + "\n"); self.p.stdin.flush()

    def rpc(self, method, params, timeout=180):
        self.n += 1
        self._send({"jsonrpc": "2.0", "id": self.n, "method": method, "params": params})
        end = time.time() + timeout
        while time.time() < end:
            if select.select([self.p.stdout], [], [], 1)[0]:
                m = json.loads(self.p.stdout.readline())
                if m.get("id") == self.n:
                    if "error" in m:
                        raise RuntimeError(m["error"])
                    return m["result"]
        raise TimeoutError(method)

    def tool(self, name, **args):
        r = self.rpc("tools/call", {"name": name, "arguments": args})
        if r.get("isError"):
            raise RuntimeError(r["content"][0]["text"][:200])
        sc = r.get("structuredContent")
        return sc.get("result", sc) if isinstance(sc, dict) else json.loads(r["content"][0]["text"])


def num(s):
    return float(s.replace(".", "").replace(",", ".")) if s else None


def pct(a, b):
    return (a / b - 1) * 100


def main(portfolio, transactions):
    b = Bridge()
    t0 = time.time()
    port = [r for r in csv.reader(open(portfolio, encoding="utf-8-sig"))][1:]
    port = [(r[0], r[1], num(r[2]), num(r[3])) for r in port if r[1]]
    raw = list(csv.reader(open(transactions, encoding="utf-8-sig")))
    hdr = raw[0]
    tx = [{**dict(zip(hdr, r)), "price_ccy": r[hdr.index("Price") + 1]} for r in raw[1:] if r]
    names = {r["ISIN"]: r["Product"] for r in tx}
    names.update({i: n for n, i, *_ in port})
    isins = list(dict.fromkeys([p[1] for p in port] + [r["ISIN"] for r in tx]))

    # 1. resolve + quote every ISIN in one call
    q = {r["input"]: r for r in b.tool("quotes", symbols=isins)}
    print(f"\n1) quotes for {len(isins)} ISINs ({time.time()-t0:.1f}s)")
    for i in isins:
        r = q[i]
        flag = "ERROR " + r["error"][:70] if "error" in r else (
            f"{r['symbol']:10} {r['price']:>10.3f} {r['currency']}  {r['source']}{' STALE' if r.get('stale') else ''}")
        print(f"   {i}  {names[i][:34]:34}  {flag}")

    # FX closes for conversion (history of the pair, daily)
    first = min(datetime.strptime(r["Date"], "%d-%m-%Y").date() for r in tx)
    fx_hist = {}
    for ccy in sorted({r["currency"] for r in q.values() if "currency" in r} - {"EUR", None}):
        rows = b.tool("history", symbol=f"EUR{ccy}=X", start=(first - timedelta(days=7)).isoformat(),
                      end=date.today().isoformat())
        fx_hist[ccy] = {x["date"]: x["close"] for x in rows}

    def fx_on(ccy, d):
        if ccy == "EUR":
            return 1.0
        h = fx_hist[ccy]
        ds = [k for k in h if k <= d]
        return h[max(ds)] if ds else None

    # 2. open positions: DEGIRO 'Closing' (EUR, Tradegate) vs MCP previous weekday close and now, in EUR
    prev = date.today() - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    print(f"\n2) open positions vs DEGIRO 'Closing' (EUR), MCP close on {prev}")
    for name, isin, qty, closing in port:
        r = q[isin]
        if "error" in r:
            print(f"   {isin} {name[:30]:30} ERROR"); continue
        c = b.tool("close_on", symbol=isin, day=prev.isoformat())
        ccy = r["currency"]
        c_eur = c["close"] / fx_on(ccy, c["date"])
        now_eur = r["price"] / (b.tool("fx", pair=f"EUR{ccy}")["price"] if ccy != "EUR" else 1)
        best = min((abs(pct(c_eur, closing)), "prev"), (abs(pct(now_eur, closing)), "now"))
        mark = "OK " if best[0] <= 1.0 else "CHK"
        print(f"   {mark} {r['symbol']:9} DEGIRO {closing:8.2f} | MCP prev {c_eur:8.2f} ({pct(c_eur, closing):+5.2f}%)"
              f" now {now_eur:8.2f} ({pct(now_eur, closing):+5.2f}%)  [{ccy}]")

    # 3. every trade price vs that day's close, converted to EUR
    print(f"\n3) {len(tx)} trades vs MCP close on the trade date")
    by_isin = defaultdict(list)
    for r in tx:
        by_isin[r["ISIN"]].append(r)
    worst, checked, skipped = [], 0, 0
    for isin, rows in by_isin.items():
        if "error" in q[isin]:
            skipped += len(rows); continue
        ds = sorted(datetime.strptime(r["Date"], "%d-%m-%Y").date() for r in rows)
        try:
            hist = b.tool("history", symbol=isin, start=(ds[0] - timedelta(days=7)).isoformat(), end=ds[-1].isoformat())
        except Exception as e:
            print(f"   {isin} history ERROR {str(e)[:80]}"); skipped += len(rows); continue
        closes = {x["date"]: x["close"] for x in (hist or [])}
        ccy = q[isin]["currency"]
        for r in rows:
            d = datetime.strptime(r["Date"], "%d-%m-%Y").date().isoformat()
            prior = [k for k in closes if k <= d]
            if not prior:
                skipped += 1; continue
            trade_ccy = r["price_ccy"] or "EUR"  # unnamed column right after Price
            price = num(r["Price"])
            close = closes[max(prior)]
            close_in_trade_ccy = close if ccy == trade_ccy else close / fx_on(ccy, d) * (1 if trade_ccy == "EUR" else fx_on(trade_ccy, d))
            dev = pct(price, close_in_trade_ccy)
            checked += 1
            worst.append((abs(dev), dev, d, q[isin]["symbol"], price, close_in_trade_ccy, trade_ccy))
    worst.sort(reverse=True)
    n_ok = sum(1 for w in worst if w[0] <= 3)
    print(f"   checked {checked}, skipped {skipped}; within 3% of the close: {n_ok}/{checked}")
    for _, dev, d, sym, price, close, tc in worst[:8]:
        print(f"   {'CHK' if abs(dev) > 3 else 'ok '} {d} {sym:9} trade {price:9.3f} vs close {close:9.3f} {tc} ({dev:+.2f}%)")

    s = b.tool("sources_status")
    print(f"\nEODHD calls today: {s['eodhd']['calls_today']}/{s['eodhd']['daily_budget']} | cache: {s['cache_db']}")
    print(f"total {time.time()-t0:.1f}s")
    b.p.terminate()


if __name__ == "__main__":
    main(os.path.expanduser(sys.argv[1]), os.path.expanduser(sys.argv[2]))
