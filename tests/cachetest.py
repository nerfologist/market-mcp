"""Run inside the container: python /tmp/c.py"""
import asyncio, os, time
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

async def main():
    t = StreamableHttpTransport("http://127.0.0.1:8765/mcp", headers={"Authorization": f"Bearer {os.environ['MCP_TOKEN']}"})
    async with Client(t) as c:
        async def call(n, a):
            t0 = time.time(); r = (await c.call_tool(n, a)).data; return r, round(time.time() - t0, 2)
        h1, s1 = await call("history", {"symbol": "EUNL.DE", "start": "2026-10-01", "end": "2026-11-03"})
        h2, s2 = await call("history", {"symbol": "EUNL.DE", "start": "2026-10-01", "end": "2026-11-03"})
        print("history cold", len(h1), f"{s1}s | warm", len(h2), f"{s2}s | identical:", h1 == h2)
        h3, s3 = await call("history", {"symbol": "EUNL.DE", "start": "2026-09-01", "end": "2026-10-10"})
        print("left extension", len(h3), f"{s3}s", h3[0], h3[-1])
        y, _ = await call("history", {"symbol": "EUNL.DE", "start": "2026-09-01", "end": "2026-11-03", "source": "yahoo"})
        cached, _ = await call("history", {"symbol": "EUNL.DE", "start": "2026-09-01", "end": "2026-11-03"})
        print("cache vs fresh yahoo, Sep 1 to Nov 3: identical:", y == cached, len(y), len(cached))
        for d in ("2026-09-15", "2026-10-11"):  # weekday, Sunday
            print("close_on", d, *(await call("close_on", {"symbol": "IE00B4L5Y983", "day": d})))
        print("history incl. today", (await call("history", {"symbol": "EUNL.DE", "start": "2026-10-30", "end": "2026-11-04"}))[0])
        q, _ = await call("quotes", {"symbols": ["EUNL.DE", "AAPL", "0700.HK"]})
        q2, _ = await call("quotes", {"symbols": ["EUNL.DE", "AAPL", "0700.HK"]})
        print("quotes ok:", all("price" in x for x in q))
        print("status", (await call("sources_status", {}))[0])
asyncio.run(main())
