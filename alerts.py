"""Threshold alerts, run outside the MCP by a scheduler (DSM Task Scheduler every 30 min on trading
days):  docker exec market-mcp python alerts.py

Reads /config/thresholds.json: [{"isin": "...", "below": 70.0}, {"isin": "...", "above": 95.0,
"ref": 76.88, "ref_label": "05/11 fill"}]. Asks the running server for each quote over HTTP (bearer
token from MCP_TOKEN), pushes one notification per crossing and remembers it in SQLite; the
threshold re-arms when the price recrosses the level by REARM_PCT. Never trades.

Delivery (set in .env): ALERT_NTFY_URL=https://ntfy.sh/<topic> (optional ALERT_NTFY_TOKEN), and/or
ALERT_HA_WEBHOOK=http://homeassistant:8123/api/webhook/<id> (JSON {"title","message"}). With
neither set, crossings are printed and the exit code is 2. ALERT_DRY_RUN=1 prints without storing.
"""
import asyncio
import json
import os
import sys
import urllib.request
from datetime import date

import store

THRESHOLDS_PATH = os.environ.get("THRESHOLDS_PATH", "/config/thresholds.json")
HOLDINGS_PATH = os.environ.get("HOLDINGS_PATH", "/config/holdings.json")
MCP_URL = os.environ.get("ALERT_MCP_URL", "http://127.0.0.1:8765/mcp")
TOKEN = os.environ.get("MCP_TOKEN", "").strip()
NTFY_URL = os.environ.get("ALERT_NTFY_URL", "").strip()
NTFY_TOKEN = os.environ.get("ALERT_NTFY_TOKEN", "").strip()
HA_WEBHOOK = os.environ.get("ALERT_HA_WEBHOOK", "").strip()
DRY_RUN = os.environ.get("ALERT_DRY_RUN", "") == "1"
REARM_PCT = float(os.environ.get("ALERT_REARM_PCT", "2"))


def _post(url: str, data: bytes, headers: dict) -> None:
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=15) as r:
        r.read()


def notify(title: str, message: str) -> bool:
    sent = False
    if NTFY_URL:
        h = {"Title": title, "Tags": "chart_with_upwards_trend", "Content-Type": "text/plain; charset=utf-8"}
        if NTFY_TOKEN:
            h["Authorization"] = f"Bearer {NTFY_TOKEN}"
        _post(NTFY_URL, message.encode(), h)
        sent = True
    if HA_WEBHOOK:
        _post(HA_WEBHOOK, json.dumps({"title": title, "message": message}).encode(),
              {"Content-Type": "application/json"})
        sent = True
    return sent


async def fetch_quotes(isins: list[str]) -> dict[str, dict]:
    from fastmcp import Client
    from fastmcp.client.transports import StreamableHttpTransport

    t = StreamableHttpTransport(MCP_URL, headers={"Authorization": f"Bearer {TOKEN}"})
    async with Client(t) as c:
        rows = (await c.call_tool("quotes", {"symbols": isins})).data
    return {r["input"]: r for r in rows}


def main() -> int:
    if date.today().weekday() >= 5:
        print("weekend, nothing to do")
        return 0
    with open(THRESHOLDS_PATH) as f:
        thresholds = json.load(f)
    if not thresholds:
        print("no thresholds")
        return 0
    try:
        with open(HOLDINGS_PATH) as f:
            cost = {p["isin"].upper(): p.get("avg_cost_eur") for p in json.load(f)["positions"]}
    except Exception:
        cost = {}
    isins = sorted({t["isin"].strip().upper() for t in thresholds})
    quotes = asyncio.run(fetch_quotes(isins))
    state = store.alert_states()
    fired, rearmed, failures = [], [], []
    for t in thresholds:
        isin = t["isin"].strip().upper()
        q = quotes.get(isin) or {}
        if "error" in q or "price" not in q:
            failures.append(f"{isin}: {q.get('error', 'no quote')}")
            continue
        price, sym, ccy = float(q["price"]), q["symbol"], q.get("currency") or ""
        for side in ("below", "above"):
            if side not in t:
                continue
            level = float(t[side])
            key = f"{isin}|{side}|{level}"
            crossed = price < level if side == "below" else price > level
            back = price >= level * (1 + REARM_PCT / 100) if side == "below" else price <= level * (1 - REARM_PCT / 100)
            if key in state:
                if back:
                    if not DRY_RUN:
                        store.alert_rearm(key)
                    rearmed.append(f"{sym} {price:.2f} re-armed {side} {level:.2f}")
                continue
            if not crossed:
                continue
            ref, label = t.get("ref"), t.get("ref_label")
            if ref is None and cost.get(isin):
                ref, label = cost[isin], "BEP"
            msg = f"{sym} {price:.2f} {'<' if side == 'below' else '>'} {level:.2f}"
            if ref:
                msg += f" ({(price / float(ref) - 1) * 100:+.1f}% vs {label or 'ref'} {float(ref):.2f})"
            if q.get("stale"):
                msg += " [stale quote]"
            if DRY_RUN:
                print("DRY RUN:", msg)
                continue
            delivered = notify(f"{sym} {side} {level:.2f}", msg)
            print(("sent: " if delivered else "NOT DELIVERED (no ALERT_NTFY_URL/ALERT_HA_WEBHOOK): ") + msg)
            store.alert_fire(key, price)  # stored even when undelivered, so a misconfiguration does not spam later
            fired.append(msg)
    for r in rearmed:
        print(r)
    for f in failures:
        print("quote failed:", f, file=sys.stderr)
    if not fired and not rearmed:
        print(f"checked {len(thresholds)} thresholds, nothing crossed")
    if fired and not (NTFY_URL or HA_WEBHOOK):
        return 2
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
