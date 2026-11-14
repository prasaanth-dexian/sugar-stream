<!--
Copyright (c) 2026 Dexian India Technologies Private Limited.
All rights reserved.

This file is proprietary and confidential. Unauthorised copying,
distribution or use of this file, via any medium, is strictly
prohibited without the prior written permission of Dexian India
Technologies Private Limited.
-->

# THIN PI edge ingest gateway

Receives readings from THIN PI plant collectors and stores them in the
StreamPipes data lake with **zero loss**: a collector deletes nothing until this
gateway has confirmed that the readings are durably stored.

```
THIN PI (SQLite buffer) ──HTTPS──▶ gateway ──sync write──▶ StreamPipes data lake (InfluxDB)
  marks rows synced                 │                         ▲ dashboards
  only after a full 200             └──fresh rows──▶ NATS ──▶ "THIN PI Live" adapter ──▶ pipelines
```

## Delivery contract

1. Every reading has a `seq`, unique and never reused within the collector
   database's `epoch`.
2. The gateway answers **200** only after the batch is written to the data lake
   and recorded in its ledger. The reply is
   `{"accepted": <rows received>, "rejected": [[seq, reason], ...], "stored": n}`.
   The collector marks rows synced only if `accepted` equals the rows it sent.
3. Any storage failure answers **503**. The collector keeps the rows and resends
   them. A resend overwrites the same InfluxDB point (time + collector + node +
   tag), so it never creates a duplicate.
4. Invalid rows (non-finite value, timestamp before 2000 or more than a day in the
   future, non-integer tag id or quality) are **rejected**, not failed: the
   collector moves them to its `dead_letter` table so they never block the queue.
5. Fresh rows (last `LIVE_WINDOW_S`, default 30 s) are also published to NATS
   for live pipelines. This copy is best effort; storage never depends on it.

## Proof of zero loss

Each batch carries `min_unsynced_seq`: the lowest seq the collector has not yet
had confirmed. Every seq below it was confirmed by this gateway earlier, so the
ledger must contain it. `GET /health` and `GET /metrics` report
`confirmed_missing` per collector and epoch. **It must always be 0.** Alert on
`thinpi_gateway_confirmed_missing > 0`, on `thinpi_gateway_backlog_rows` growing,
and on `thinpi_gateway_last_batch_age_seconds` > 60.

## Wire format (v1)

`POST /v1/telemetry/ingest`, `Authorization: Bearer <key>`, gzip JSON:

```json
{"schema": 1, "collector_id": "mill1-thinpi-01", "epoch": 123456789, "kind": "live",
 "min_unsynced_seq": 1001, "max_seq": 1500,
 "tags": {"7": {"node_id": "ns=2;s=Boiler.Pressure", "name": "Boiler Pressure", "unit": "bar"}},
 "rows": [[1001, 1763100000000, 7, 63.5, 192]]}
```

Rows are `[seq, timestamp_ms, tag_id, value, quality]`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_API_KEYS` / `GATEWAY_API_KEYS_FILE` | (required) | Comma-separated collector tokens |
| `GATEWAY_ALLOW_NO_AUTH` | `0` | `1` only for local development |
| `SP_URL` | `http://backend:8030/streampipes-backend` | StreamPipes backend |
| `SP_API_USER`, `SP_API_KEY` / `SP_API_KEY_FILE` | (required) | StreamPipes API user and key |
| `SP_DATASET` | `thinpi_readings` | Data lake dataset |
| `NATS_URL`, `LIVE_SUBJECT` | `nats://nats:4222`, `thinpi.live` | Live copy (empty URL disables) |
| `LIVE_WINDOW_S` | `30` | Only rows newer than this go to NATS |
| `STATE_DB` | `/data/gateway.db` | Ledger (keep on a persistent volume) |
| `GATEWAY_PORT` | `8092` | Listen port |

Collector side (`THINPI_*` environment variables): `THINPI_SYNC_URL`,
`THINPI_SYNC_API_KEY`, `THINPI_COLLECTOR_ID` (unique per collector),
`THINPI_DISK_BUDGET_MB` (default 20480, about half the free disk),
`THINPI_RETAIN_SYNCED_HOURS` (default 72), `THINPI_BACKFILL_BATCH` (10000),
`THINPI_BACKFILL_PARALLEL` (2), `THINPI_AUTOSTART` (`simulator` or an OPC endpoint).

## Setup

```sh
docker build -t thinpi-gateway thin-pi/edge-gateway
# once per StreamPipes installation: dataset + live adapter
docker run --rm --network <streampipes network> -e SP_API_USER=... -e SP_API_KEY=... \
  thinpi-gateway python bootstrap.py --live-adapter
docker run -d --network <streampipes network> -p 8092:8092 -v thinpi-gateway:/data \
  -e SP_API_USER=... -e SP_API_KEY=... -e GATEWAY_API_KEYS=... thinpi-gateway
```

Put TLS in front (reverse proxy on 443) for collectors outside the edge host.
Use a dedicated StreamPipes service user with write access to the dataset only.

## Tests

```sh
cd thin-pi/edge-gateway
pip install -r requirements-dev.txt
python -m pytest
```
