import asyncio, json, os
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

async def main():
    t = StreamableHttpTransport("http://127.0.0.1:8765/mcp", headers={"Authorization": f"Bearer {os.environ['MCP_TOKEN']}"})
    async with Client(t) as c:
        print("tools:", [x.name for x in await c.list_tools()])
        isins = (await c.call_tool("isin_map", {})).data["seed"]
        q = (await c.call_tool("quotes", {"symbols": list(isins)})).data
        for r in q:
            print(json.dumps(r))
        for d in ("2026-09-15", "2026-10-01", "2026-11-03"):
            print("close_on", d, (await c.call_tool("close_on", {"symbol": "IE00B4L5Y983", "day": d})).data)
        print("fx", (await c.call_tool("fx", {"pair": "EURUSD"})).data)
        print("hist", (await c.call_tool("history", {"symbol": "EUNL.DE", "start": "2026-10-28", "end": "2026-11-03"})).data)
asyncio.run(main())
