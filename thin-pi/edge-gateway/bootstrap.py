# Copyright (c) 2026 Dexian India Technologies Private Limited.
# All rights reserved.
#
# This file is proprietary and confidential. Unauthorised copying,
# distribution or use of this file, via any medium, is strictly
# prohibited without the prior written permission of Dexian India
# Technologies Private Limited.

"""One-time StreamPipes setup for the gateway (idempotent, safe to re-run).

1. Creates the data lake dataset (SP_DATASET) with an explicit schema.
   collector_id, node_id and tag_name are DIMENSION properties, i.e. InfluxDB
   tags: a point is identified by (time, collector, node, tag), so readings of
   different tags at the same millisecond never overwrite each other, and a
   resent reading overwrites itself (idempotent).
2. With --live-adapter: creates a NATS adapter on LIVE_SUBJECT (not persisted)
   so StreamPipes pipelines can use the live copy the gateway publishes.

Uses the same SP_URL / SP_API_USER / SP_API_KEY(_FILE) settings as the gateway.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import urllib.error
import urllib.request

from gateway import Config, HEADERS

XSD = "http://www.w3.org/2001/XMLSchema#"
TYPES = {"timestamp": "long", "collector_id": "string", "node_id": "string", "tag_name": "string",
         "eng_unit": "string", "tag_id": "long", "value": "double", "quality": "integer",
         "epoch": "long", "seq": "long", "gw_ts": "long"}
DIMENSIONS = {"collector_id", "node_id", "tag_name"}
ADAPTER_NAME = "THIN PI Live"


def call(cfg: Config, method: str, path: str, body=None, timeout: float = 60):
    req = urllib.request.Request(
        cfg.sp_url + path, method=method, data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Accept": "application/json",
                 "X-API-USER": cfg.sp_user, "X-API-KEY": cfg.sp_key})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return json.loads(raw) if raw.strip() else None
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} -> HTTP {e.code}: {e.read()[:300]!r}") from None


def dataset_metadata(name: str) -> dict:
    props = []
    for runtime_name in HEADERS:
        props.append({
            "@class": "org.apache.streampipes.model.schema.EventPropertyPrimitive",
            "runtimeName": runtime_name,
            "runtimeType": XSD + TYPES[runtime_name],
            "propertyScope": ("HEADER_PROPERTY" if runtime_name == "timestamp" else
                              "DIMENSION_PROPERTY" if runtime_name in DIMENSIONS else "MEASUREMENT_PROPERTY"),
            "semanticType": "http://schema.org/DateTime" if runtime_name == "timestamp" else None,
            "additionalMetadata": {},
            "description": "",
        })
    return {"@class": "org.apache.streampipes.model.dataset.DatasetMetadata", "measureName": name,
            "timestampField": "s0::timestamp", "eventSchema": {"eventProperties": props},
            "schemaUpdateStrategy": "UPDATE_SCHEMA"}


def ensure_dataset(cfg: Config) -> None:
    existing = [m for m in call(cfg, "GET", "/api/v4/datalake/measurements") if m["measureName"] == cfg.dataset]
    if existing:
        props = {p["runtimeName"]: p.get("propertyScope") for p in existing[0]["eventSchema"]["eventProperties"]}
        missing = [h for h in HEADERS if h not in props]
        wrong_dims = [d for d in DIMENSIONS if props.get(d) != "DIMENSION_PROPERTY"]
        if missing or wrong_dims:
            sys.exit(f"dataset {cfg.dataset} exists with an incompatible schema "
                     f"(missing {missing}, not dimensions {wrong_dims}); choose another SP_DATASET")
        print(f"dataset {cfg.dataset}: present, schema OK")
        return
    call(cfg, "POST", "/api/v4/datalake/measure", dataset_metadata(cfg.dataset))
    print(f"dataset {cfg.dataset}: created")


async def _sample_publisher(cfg: Config, stop: asyncio.Event) -> None:
    import nats
    nc = await nats.connect(cfg.nats_url)
    i = 0
    while not stop.is_set():
        event = {"timestamp": int(time.time() * 1000), "collector_id": "bootstrap", "node_id": "",
                 "tag_name": "bootstrap", "eng_unit": "", "tag_id": 0, "value": 0.0, "quality": 192,
                 "epoch": 1, "seq": i}
        await nc.publish(cfg.live_subject, json.dumps(event).encode())
        i += 1
        await asyncio.sleep(0.2)
    await nc.close()


async def ensure_live_adapter(cfg: Config) -> None:
    if any(a.get("name") == ADAPTER_NAME for a in call(cfg, "GET", "/api/v2/connect/master/adapters")):
        print(f"adapter '{ADAPTER_NAME}': present")
        return
    # StreamPipes checks the schema against live messages while creating the adapter.
    stop = asyncio.Event()
    pub = asyncio.create_task(_sample_publisher(cfg, stop))
    await asyncio.sleep(2)
    dim = {"propertyScope": "DIMENSION_PROPERTY"}
    mea = {"propertyScope": "MEASUREMENT_PROPERTY"}
    adapter = {
        "name": ADAPTER_NAME,
        "description": "Live copy of THIN PI readings from the edge gateway (storage is done by the gateway)",
        "appId": "org.apache.streampipes.connect.iiot.protocol.stream.nats",
        "configuration": [
            {"natsUrls": cfg.nats_url}, {"subject": cfg.live_subject},
            {"access-mode": "anonymous-alternative"},
            {"connection-properties": "none-properties-alternative"},
            {"format": "Json", "json_options": "object"},
        ],
        "schema": {"timestamp": {"propertyScope": "HEADER_PROPERTY", "semanticType": "http://schema.org/DateTime"},
                   "collector_id": dim, "node_id": dim, "tag_name": dim, "eng_unit": mea, "tag_id": mea,
                   "value": mea, "quality": mea, "epoch": mea, "seq": mea},
        "createOptions": {"persist": False, "start": True},
    }
    try:
        await asyncio.to_thread(call, cfg, "POST", "/api/v2/connect/compact-adapters", adapter, 180)
        print(f"adapter '{ADAPTER_NAME}': created and started")
    finally:
        stop.set()
        await pub


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--live-adapter", action="store_true", help="also create the NATS live adapter")
    args = ap.parse_args()
    cfg = Config.from_env()
    if not cfg.sp_user or not cfg.sp_key:
        sys.exit("Set SP_API_USER and SP_API_KEY (or SP_API_KEY_FILE)")
    ensure_dataset(cfg)
    if args.live_adapter:
        asyncio.run(ensure_live_adapter(cfg))


if __name__ == "__main__":
    main()
