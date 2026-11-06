"""v2 smoke test, run next to the container (same network namespace, same .env):
  docker run --rm --network container:market-mcp --env-file .env -v $PWD/tests:/tests \
    market-mcp:latest python /tests/v2test.py [broker_total_eur]
Env: CHECK_DAY (default today), V2_CHECKPOINTS, V2_FLOWS, V2_DIV_SYMBOL, V2_DIV_YEAR (see below).
"""
import asyncio, json, os, sys, time
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

NEW = {"quote_eur", "quotes_eur", "closes_on", "portfolio", "checkpoint", "checkpoints", "dietz", "dividends"}


async def main():
    t = StreamableHttpTransport("http://127.0.0.1:8765/mcp", headers={"Authorization": f"Bearer {os.environ['MCP_TOKEN']}"})
    async with Client(t) as c:
        names = {x.name for x in await c.list_tools()}
        print("tools:", sorted(names))
        assert NEW <= names, NEW - names
        call = lambda n, **a: c.call_tool(n, a)

        t0 = time.time()
        p = (await call("portfolio")).data
        print(f"\nportfolio() live in {time.time()-t0:.1f}s  as_of {p['holdings_as_of']} ({p['holdings_age_days']}d)")
        print(" totals:", json.dumps(p["totals"]))
        for b, x in p["buckets"].items():
            print(f"  {b:18} {x['value_eur']:10.2f}  {x['weight_pct']:5.2f}%  day {x['day_change_eur']:+8.2f}")
        for r in p["positions"]:
            if "error" in r:
                print("  ERR", r["isin"], r["error"]); continue
            print(f"  {r['symbol']:9} {r['qty']:6.0f} x {r['price']:9.3f} {r['currency']} fx {r['fx_rate_used']:<8} = {r['value_eur']:9.2f} "
                  f"({r['weight_pct']:5.2f}%) day {r['day_change_eur'] if r['day_change_eur'] is not None else 0:+8.2f} "
                  f"PL {r.get('unrealised_pl_eur', 0):+9.2f} ({r.get('unrealised_pl_pct', 0):+6.2f}%){' STALE' if r.get('stale') else ''}")
        print(" warnings:", p["warnings"])
        if len(sys.argv) > 1:
            exp = float(sys.argv[1]); got = p["totals"]["value_eur"]
            print(f" vs DEGIRO {exp:.2f}: {(got/exp-1)*100:+.3f}%")

        isins = [r["isin"] for r in p["positions"]]
        q = (await call("quotes_eur", symbols=isins)).data
        print("\nquotes_eur")
        for r in q:
            if "error" in r:
                print("  ERR", r["input"], r["error"]); continue
            assert r["currency"] != "EUR" or r["fx_rate_used"] == 1.0
            print(f"  {r['symbol']:9} {r['price']:9.3f} {r['currency']} -> {r['price_eur']:9.3f} EUR via {r['fx_pair_used']:<14} "
                  f"day {r['day_change_pct']:+6.2f}% eur {r['day_change_eur_pct']:+6.2f}% fx {r['fx_contribution_pct']:+6.2f}%")

        day = os.environ.get("CHECK_DAY", time.strftime("%Y-%m-%d"))
        t0 = time.time()
        ck = (await call("checkpoint", day=day, note="v2test")).data
        print(f"\ncheckpoint({day}) in {time.time()-t0:.1f}s total {ck['total_eur']:.2f} buckets {ck['buckets']}")
        print("  closes_on sample:", (await call("closes_on", symbols=isins[:3], day=day)).data)

        # optional: known past totals (e.g. broker exports from before the holdings changed) + a flow
        #   V2_CHECKPOINTS="2026-10-07=68000.00,2026-11-03=65800.00" V2_FLOWS="2026-10-19=2000"
        manual = [x.split("=") for x in os.environ.get("V2_CHECKPOINTS", "").split(",") if x]
        for d_, tot in manual:
            await call("checkpoint", day=d_, total_eur=float(tot), note="v2test manual")
        cps = (await call("checkpoints")).data
        print("checkpoints:", [(r["day"], r["total_eur"], r["source"]) for r in cps])
        if len(cps) >= 2:
            flows = [{"day": a, "amount_eur": float(b)} for a, b in (x.split("=") for x in os.environ.get("V2_FLOWS", "").split(",") if x)]
            a, b = (manual[0][0], manual[-1][0]) if len(manual) >= 2 else (cps[-2]["day"], cps[-1]["day"])
            flows = [f for f in flows if a <= f["day"] <= b]
            d = (await call("dietz", start_day=a, end_day=b, flows=flows)).data
            print(f"dietz {a} -> {b} flows {d['flows_total_eur']:+.2f}: {d['return_pct']:+.3f}%  (gain {d['gain_eur']:+.2f})")

        div_sym = os.environ.get("V2_DIV_SYMBOL") or isins[0]
        dv = (await call("dividends", symbol=div_sym, year=int(os.environ.get("V2_DIV_YEAR", day[:4])))).data
        print(f"\ndividends {dv['symbol']} {dv['year']}: {dv['count']} payments, {dv['total_per_share']} {dv['currency']}/sh = {dv['total_per_share_eur']} EUR/sh")
        for r in dv["payments"]:
            print("  ", r["ex_date"], r["amount"], r["currency"], "@", r["rate"], "=", r["amount_eur"], "EUR")
        print("\nstatus:", (await call("sources_status")).data["eodhd"])

asyncio.run(main())
