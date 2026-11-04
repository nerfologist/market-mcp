import asyncio, os
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
async def main():
    t = StreamableHttpTransport("http://127.0.0.1:8765/mcp", headers={"Authorization": f"Bearer {os.environ['MCP_TOKEN']}"})
    async with Client(t) as c:
        call = lambda n, a: c.call_tool(n, a)
        print([x.name for x in await c.list_tools()])
        print("yahoo", (await call("quote", {"symbol": "EUNL.DE"})).data)
        print("eodhd", (await call("quote", {"symbol": "EUNL.DE", "source": "eodhd"})).data)
        print("eodhd close", (await call("close_on", {"symbol": "IE00B4L5Y983", "day": "2026-09-15", "source": "eodhd"})).data)
        print("new isin", (await call("quote", {"symbol": "IE00B5BMR087"})).data)
        print("fx eodhd", (await call("fx", {"pair": "EURUSD", "source": "eodhd"})).data)
        print("learned", (await call("isin_map", {})).data["learned"])
        print("status", (await call("sources_status", {})).data)
asyncio.run(main())
