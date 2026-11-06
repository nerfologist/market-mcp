# market-mcp

Read-only MCP server with stock prices, FX rates and corporate actions, running in Docker on a home server (`http://<host>:8765/mcp`, LAN/VPN only, bearer token).

## Tools

- Prices: `quote`, `quotes`, `quote_eur`, `quotes_eur`, `close_on`, `closes_on`, `history`, `corporate_actions`, `dividends`
- FX: `fx` (Yahoo spot), `ecb_rate`, `ecb_rate_history`, `bdi_rate`, `bdi_rate_history`, `to_eur`
- Holdings: `portfolio` (values `config/holdings.json`, live or at a past close), `checkpoint` (stores a valuation in SQLite), `checkpoints`, `dietz` (Modified Dietz between two checkpoints with caller-supplied flows)
- `isin_map`, `sources_status`

The server holds no opinions: no targets, no buy or sell logic. It values what the holdings file says.

EUR convention for non-EUR listings: `price_eur` uses the live Yahoo spot (EURUSD, EURHKD, or EURUSD x USDHKD when EURHKD is missing); `previous_close_eur` uses the ECB reference rate of the business day before the quote, so `fx_contribution_pct` is spot-vs-previous-fix. Past closes (`portfolio(day=...)`, `checkpoint`) use the ECB rate of the day; `dividends` uses the Banca d'Italia rate of the ex-date, or of the payment date when `pay_dates` are given.

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
- `store.py`: SQLite cache for quotes and closes, stored checkpoints, alert state
- `alerts.py`: threshold alerts (ntfy or Home Assistant webhook), run by a scheduler outside the MCP, see below
- `config/holdings.example.json`, `config/thresholds.example.json`: formats of the hand-maintained holdings and alert thresholds. Your own go in `config/holdings.json` and `config/thresholds.json` (not in git), mounted read-only at `/config`; the server re-reads them on every call and never writes them
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

After a trade: edit `config/holdings.json` (qty and avg cost from the broker's break-even price), bump `as_of`, run `./deploy.sh config`. `portfolio()` warns when `as_of` is older than 7 days.

## Alerts

`alerts.py` reads `config/thresholds.json` (`[{"isin": "...", "below": 70.0}, {"isin": "...", "above": 95.0, "ref": 76.88, "ref_label": "05/11 fill"}]`), asks the running server for the quotes and pushes one notification per crossing, e.g. `VVSM.DE 69.80 < 70.00 (-9.2% vs 05/11 fill 76.88)`. A fired threshold is remembered in SQLite and re-arms when the price recrosses the level by 2%. Delivery is set in `.env`: `ALERT_NTFY_URL=https://ntfy.sh/<topic>` (plus `ALERT_NTFY_TOKEN` if needed) and/or `ALERT_HA_WEBHOOK=http://<home-assistant>:8123/api/webhook/<id>`. Schedule it on the host, e.g. every 30 minutes on trading days:

```sh
docker exec market-mcp python alerts.py
```

`ALERT_DRY_RUN=1` prints crossings without storing or sending. `ALERT_HEARTBEAT_URL` (for example a Home Assistant webhook) receives a POST after every successful run, so an automation can notice when the job or the container is down.

## Test

```sh
python3 -I tests/portfolio_check.py Portfolio.csv Transactions.csv
```

v2 tools, next to the container on the host:

```sh
docker run --rm --network container:market-mcp --env-file .env -v $PWD/tests:/tests market-mcp:latest python /tests/v2test.py
```

## License

MIT, see `LICENSE`.
