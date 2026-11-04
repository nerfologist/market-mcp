# market-mcp

Read-only MCP server with stock prices, FX rates and corporate actions, running in Docker on a home server (`http://<host>:8765/mcp`, LAN/VPN only, bearer token).

## Sources

- **Yahoo Finance** (yfinance): quotes, daily closes, splits and dividends. Primary source.
- **EODHD** free plan: fallback quotes and ISIN resolution, capped at 18 calls/day.
- **ECB** reference rates: all ECB currencies since 1999.
- **Banca d'Italia** rates: ~170 currencies, the official rate for Italian tax.

Completed daily closes and FX rates are cached in SQLite (`data/market.db`).

Meant for personal use on a private network. Yahoo Finance data comes through the unofficial yfinance library and is subject to Yahoo's terms; check them (and EODHD's) before using it for anything else.

## Files

- `server.py`: FastMCP app, tools, ISIN resolution, Yahoo client, bearer auth
- `eodhd.py`, `ecb.py`, `bdi.py`: source clients
- `store.py`: SQLite cache for quotes and closes
- `isins.example.json`: format of the ISIN to Yahoo ticker map. Your own map goes in `data/isins.json` (not in git); ISINs missing from it are resolved through EODHD and saved to `data/isins_learned.json`
- `tests/`: smoke tests and `portfolio_check.py` (checks a DEGIRO Portfolio/Transactions export through the Claude Desktop bridge)

## Deploy

```sh
cp deploy.env.example deploy.env   # once: set your ssh host and directory
./deploy.sh
```

Rsyncs to `$DEPLOY_HOST:$DEPLOY_DIR`, rebuilds with compose and checks `/healthz`. `data/isins.json` is synced if present; the rest of `data/` belongs to the container.

Create `.env` on the host only, never in git:

```sh
MCP_TOKEN=<64 random hex chars, e.g. openssl rand -hex 32>
EODHD_API_KEY=<optional, free plan works>
```

## Test

```sh
python3 -I tests/portfolio_check.py Portfolio.csv Transactions.csv
```

## License

MIT, see `LICENSE`.
