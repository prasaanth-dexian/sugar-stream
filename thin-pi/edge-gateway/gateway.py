# Copyright (c) 2026 Dexian India Technologies Private Limited.
# All rights reserved.
#
# This file is proprietary and confidential. Unauthorised copying,
# distribution or use of this file, via any medium, is strictly
# prohibited without the prior written permission of Dexian India
# Technologies Private Limited.

"""THIN PI edge ingest gateway.

POST /v1/telemetry/ingest   (gzip JSON, wire format v1, see README.md)
  1. authenticate the collector (Bearer token)
  2. validate every row; invalid rows are listed as `rejected` (the collector
     moves them to its dead-letter table) instead of failing the batch
  3. write the valid rows to the StreamPipes data lake SYNCHRONOUSLY
  4. record the stored seqs in the durable delivery ledger
  5. only then answer 200 {"accepted": <rows received>, "rejected": [[seq, reason]]}
     Any failure in 3-4 answers 503 and the collector keeps and resends the rows.
     Resends are harmless: a reading is identified in InfluxDB by its time, its
     collector and its tag, so a resend overwrites the same point.
  6. best effort: publish fresh rows to NATS for live StreamPipes pipelines
GET /health   JSON status per collector, including confirmed missing readings
GET /metrics  Prometheus text format
"""

from __future__ import annotations

import asyncio
import gzip
import hmac
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass

import aiohttp
from aiohttp import web

from ledger import Ledger

log = logging.getLogger("thinpi-gateway")

COLLECTOR_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
MIN_TS_MS = 946_684_800_000  # 2000-01-01
MAX_ROWS = 50_000
SP_CHUNK = 10_000
HEADERS = ["timestamp", "collector_id", "node_id", "tag_name", "eng_unit", "tag_id",
           "value", "quality", "epoch", "seq", "gw_ts"]


def _read_secret(var: str) -> str:
    """Value of VAR, or the contents of the file named by VAR_FILE."""
    path = os.environ.get(f"{var}_FILE")
    if path:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    return os.environ.get(var, "").strip()


@dataclass
class Config:
    port: int = 8092
    api_keys: tuple[str, ...] = ()
    allow_no_auth: bool = False
    sp_url: str = "http://backend:8030/streampipes-backend"
    sp_user: str = ""
    sp_key: str = ""
    dataset: str = "thinpi_readings"
    nats_url: str = "nats://nats:4222"
    live_subject: str = "thinpi.live"
    live_window_s: float = 30.0
    max_future_s: float = 86_400.0
    state_db: str = "/data/gateway.db"
    sp_timeout_s: float = 60.0

    @classmethod
    def from_env(cls) -> "Config":
        keys = tuple(k.strip() for k in _read_secret("GATEWAY_API_KEYS").split(",") if k.strip())
        return cls(
            port=int(os.environ.get("GATEWAY_PORT", "8092")),
            api_keys=keys,
            allow_no_auth=os.environ.get("GATEWAY_ALLOW_NO_AUTH") == "1",
            sp_url=os.environ.get("SP_URL", cls.sp_url).rstrip("/"),
            sp_user=os.environ.get("SP_API_USER", ""),
            sp_key=_read_secret("SP_API_KEY"),
            dataset=os.environ.get("SP_DATASET", cls.dataset),
            nats_url=os.environ.get("NATS_URL", cls.nats_url),
            live_subject=os.environ.get("LIVE_SUBJECT", cls.live_subject),
            live_window_s=float(os.environ.get("LIVE_WINDOW_S", "30")),
            max_future_s=float(os.environ.get("MAX_FUTURE_S", "86400")),
            state_db=os.environ.get("STATE_DB", cls.state_db),
        )


class BadRequest(Exception):
    pass


def parse_batch(body: dict) -> tuple[str, int, int, int, dict, list]:
    """Check the envelope. Raises BadRequest for anything a correct collector never sends."""
    if not isinstance(body, dict) or body.get("schema", 1) != 1:
        raise BadRequest("unsupported payload schema")
    cid = body.get("collector_id")
    if not isinstance(cid, str) or not COLLECTOR_ID_RE.match(cid):
        raise BadRequest("collector_id missing or invalid")
    epoch = body.get("epoch")
    rows = body.get("rows")
    if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch <= 0:
        raise BadRequest("epoch missing or invalid")
    if not isinstance(rows, list) or len(rows) > MAX_ROWS:
        raise BadRequest(f"rows missing or more than {MAX_ROWS}")
    tags = body.get("tags") or {}
    if not isinstance(tags, dict):
        raise BadRequest("tags must be an object")
    min_unsynced = body.get("min_unsynced_seq", 0)
    max_seq = body.get("max_seq", 0)
    if not all(isinstance(v, int) and not isinstance(v, bool) for v in (min_unsynced, max_seq)):
        raise BadRequest("min_unsynced_seq/max_seq must be integers")
    for r in rows:
        if not isinstance(r, list) or len(r) != 5 or not _is_int(r[0]) or r[0] <= 0:
            raise BadRequest("every row must be [seq, timestamp, tag_id, value, quality]")
    return cid, epoch, min_unsynced, max_seq, tags, rows


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def validate_rows(rows: list, now_ms: int, max_future_s: float) -> tuple[list, list]:
    """Split rows into (good, rejected[[seq, reason]])."""
    good, rejected = [], []
    max_ts = now_ms + int(max_future_s * 1000)
    for seq, ts, tag_id, value, quality in rows:
        if not _is_int(ts) or not MIN_TS_MS <= ts <= max_ts:
            rejected.append([seq, "timestamp out of range"])
        elif not _is_int(tag_id):
            rejected.append([seq, "tag_id not an integer"])
        elif isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            rejected.append([seq, "value not a finite number"])
        elif not _is_int(quality) or quality < 0:
            rejected.append([seq, "quality not a non-negative integer"])
        else:
            good.append((seq, ts, tag_id, float(value), quality))
    return good, rejected


def to_datalake_rows(cid: str, epoch: int, tags: dict, good: list, now_ms: int) -> list[list]:
    out = []
    for seq, ts, tag_id, value, quality in good:
        meta = tags.get(str(tag_id)) or {}
        out.append([ts, cid, str(meta.get("node_id") or ""), str(meta.get("name") or f"tag_{tag_id}"),
                    str(meta.get("unit") or ""), tag_id, value, quality, epoch, seq, now_ms])
    return out


class Gateway:
    def __init__(self, cfg: Config, ledger: Ledger):
        self.cfg = cfg
        self.ledger = ledger
        self.http: aiohttp.ClientSession | None = None
        self.nc = None
        self.stats = {"batches": 0, "sp_errors": 0, "last_sp_error": None, "last_sp_error_at": 0.0,
                      "sp_write_seconds_sum": 0.0, "sp_write_count": 0, "live_published": 0,
                      "live_publish_errors": 0}

    # ── storage ──────────────────────────────────────────────────────────────
    async def write_datalake(self, rows: list[list]) -> None:
        url = f"{self.cfg.sp_url}/api/v4/datalake/measurements/{self.cfg.dataset}"
        headers = {"Content-Type": "application/json", "X-API-USER": self.cfg.sp_user,
                   "X-API-KEY": self.cfg.sp_key}
        for i in range(0, len(rows), SP_CHUNK):
            chunk = rows[i:i + SP_CHUNK]
            payload = json.dumps({"total": len(chunk), "headers": HEADERS,
                                  "allDataSeries": [{"total": len(chunk), "headers": HEADERS,
                                                     "rows": chunk}]})
            t0 = time.perf_counter()
            async with self.http.post(url, data=payload, headers=headers) as r:
                text = await r.text()
                if r.status != 200:
                    raise RuntimeError(f"data lake answered {r.status}: {text[:200]}")
            self.stats["sp_write_seconds_sum"] += time.perf_counter() - t0
            self.stats["sp_write_count"] += 1

    async def publish_live(self, cid: str, epoch: int, tags: dict, good: list, now_ms: int) -> None:
        if self.nc is None or not self.nc.is_connected:
            return
        cut = now_ms - self.cfg.live_window_s * 1000
        try:
            for seq, ts, tag_id, value, quality in good:
                if ts < cut:
                    continue
                meta = tags.get(str(tag_id)) or {}
                event = {"timestamp": ts, "collector_id": cid, "node_id": meta.get("node_id") or "",
                         "tag_name": meta.get("name") or f"tag_{tag_id}", "eng_unit": meta.get("unit") or "",
                         "tag_id": tag_id, "value": value, "quality": quality, "epoch": epoch, "seq": seq}
                await self.nc.publish(self.cfg.live_subject, json.dumps(event).encode())
                self.stats["live_published"] += 1
        except Exception as e:  # live copy is best effort; storage already confirmed
            self.stats["live_publish_errors"] += 1
            log.debug("live publish failed: %s", e)

    # ── handlers ─────────────────────────────────────────────────────────────
    def authorized(self, request: web.Request) -> bool:
        if not self.cfg.api_keys:
            return self.cfg.allow_no_auth
        auth = request.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else ""
        return any(hmac.compare_digest(token, k) for k in self.cfg.api_keys)

    async def ingest(self, request: web.Request) -> web.Response:
        if not self.authorized(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        raw = await request.read()
        try:
            if raw[:2] == b"\x1f\x8b":  # aiohttp normally inflates already
                raw = gzip.decompress(raw)
            cid, epoch, min_unsynced, max_seq, tags, rows = parse_batch(json.loads(raw))
        except (BadRequest, ValueError, OSError) as e:
            log.error("rejected batch: %s", e)
            return web.json_response({"error": str(e)}, status=400)

        now_ms = int(time.time() * 1000)
        good, rejected = validate_rows(rows, now_ms, self.cfg.max_future_s)
        try:
            if good:
                await self.write_datalake(to_datalake_rows(cid, epoch, tags, good, now_ms))
            st, new_missing = await asyncio.to_thread(
                self.ledger.record, cid, epoch, [r[0] for r in rows], len(rejected), min_unsynced, max_seq)
        except Exception as e:
            self.stats["sp_errors"] += 1
            self.stats["last_sp_error"] = str(e)[:300]
            self.stats["last_sp_error_at"] = time.time()
            log.warning("batch from %s not stored, collector will resend: %s", cid, e)
            return web.json_response({"error": "storage unavailable"}, status=503)

        if new_missing:
            log.error("CONFIRMED MISSING readings for %s epoch %s: %d total, ranges %s",
                      cid, epoch, st.reported_missing, new_missing[:10])
        if rejected:
            log.warning("%s: %d rows rejected, e.g. %s", cid, len(rejected), rejected[0])
        await self.publish_live(cid, epoch, tags, good, now_ms)
        self.stats["batches"] += 1
        return web.json_response({"accepted": len(rows), "rejected": rejected, "stored": len(good)})

    def collector_report(self) -> list[dict]:
        now = time.time()
        out = []
        for st in self.ledger.states.values():
            out.append({"collector_id": st.collector_id, "epoch": st.epoch, "rows_stored": st.rows_stored,
                        "rows_rejected": st.rows_rejected, "confirmed_missing": st.missing_count(),
                        "missing_ranges": st.missing()[:20], "backlog": st.backlog(),
                        "last_batch_age_s": round(now - st.last_batch_at, 1) if st.last_batch_at else None})
        return out

    async def health(self, _request: web.Request) -> web.Response:
        collectors = self.collector_report()
        degraded = any(c["confirmed_missing"] for c in collectors) or (
            time.time() - self.stats["last_sp_error_at"] < 60)
        return web.json_response({"status": "degraded" if degraded else "ok",
                                  "nats_connected": bool(self.nc and self.nc.is_connected),
                                  "collectors": collectors,
                                  **{k: v for k, v in self.stats.items() if k != "last_sp_error_at"}})

    async def metrics(self, _request: web.Request) -> web.Response:
        lines = [
            "# TYPE thinpi_gateway_rows_stored_total counter",
            "# TYPE thinpi_gateway_confirmed_missing gauge",
            "# TYPE thinpi_gateway_backlog_rows gauge",
            "# TYPE thinpi_gateway_last_batch_age_seconds gauge",
        ]
        for c in self.collector_report():
            lbl = f'collector="{c["collector_id"]}",epoch="{c["epoch"]}"'
            lines += [f"thinpi_gateway_rows_stored_total{{{lbl}}} {c['rows_stored']}",
                      f"thinpi_gateway_rows_rejected_total{{{lbl}}} {c['rows_rejected']}",
                      f"thinpi_gateway_confirmed_missing{{{lbl}}} {c['confirmed_missing']}",
                      f"thinpi_gateway_backlog_rows{{{lbl}}} {c['backlog']}",
                      f"thinpi_gateway_last_batch_age_seconds{{{lbl}}} {c['last_batch_age_s'] or 0}"]
        s = self.stats
        lines += [f"thinpi_gateway_storage_errors_total {s['sp_errors']}",
                  f"thinpi_gateway_sp_write_seconds_sum {s['sp_write_seconds_sum']:.3f}",
                  f"thinpi_gateway_sp_write_seconds_count {s['sp_write_count']}",
                  f"thinpi_gateway_live_published_total {s['live_published']}"]
        return web.Response(text="\n".join(lines) + "\n", content_type="text/plain")

    # ── lifecycle ────────────────────────────────────────────────────────────
    async def on_startup(self, _app) -> None:
        self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.cfg.sp_timeout_s))
        if self.cfg.nats_url:
            asyncio.get_running_loop().create_task(self._connect_nats())

    async def _connect_nats(self) -> None:
        import nats
        while self.nc is None:
            try:
                self.nc = await nats.connect(self.cfg.nats_url, max_reconnect_attempts=-1,
                                             reconnect_time_wait=1)
                log.info("live copy to NATS %s subject %s", self.cfg.nats_url, self.cfg.live_subject)
            except Exception as e:
                log.warning("NATS not reachable yet (%s); live copy paused", e)
                await asyncio.sleep(5)

    async def on_cleanup(self, _app) -> None:
        if self.http:
            await self.http.close()
        if self.nc:
            await self.nc.close()
        self.ledger.close()


def create_app(cfg: Config, ledger: Ledger | None = None) -> web.Application:
    if not cfg.api_keys and not cfg.allow_no_auth:
        raise SystemExit("Set GATEWAY_API_KEYS (or GATEWAY_ALLOW_NO_AUTH=1 for local development)")
    gw = Gateway(cfg, ledger or Ledger(cfg.state_db))
    app = web.Application(client_max_size=64 * 1024 * 1024)
    app["gateway"] = gw
    app.add_routes([web.post("/v1/telemetry/ingest", gw.ingest), web.get("/health", gw.health),
                    web.get("/metrics", gw.metrics)])
    app.on_startup.append(gw.on_startup)
    app.on_cleanup.append(gw.on_cleanup)
    return app


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    cfg = Config.from_env()
    if not cfg.sp_user or not cfg.sp_key:
        raise SystemExit("Set SP_API_USER and SP_API_KEY (or SP_API_KEY_FILE)")
    log.info("THIN PI gateway on :%d -> %s dataset %s", cfg.port, cfg.sp_url, cfg.dataset)
    web.run_app(create_app(cfg), host="0.0.0.0", port=cfg.port, access_log=None)


if __name__ == "__main__":
    main()
