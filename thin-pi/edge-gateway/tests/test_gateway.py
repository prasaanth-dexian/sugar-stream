# Copyright (c) 2026 Dexian India Technologies Private Limited.
# All rights reserved.
#
# This file is proprietary and confidential. Unauthorised copying,
# distribution or use of this file, via any medium, is strictly
# prohibited without the prior written permission of Dexian India
# Technologies Private Limited.

import gzip
import json
import os
import sys
import time

import pytest
from aiohttp import web

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gateway import Config, create_app, validate_rows  # noqa: E402
from ledger import Ledger, holes, merge_ranges  # noqa: E402

NOW = int(time.time() * 1000)


# ── pure functions ──────────────────────────────────────────────────────────
def test_merge_ranges_handles_order_duplicates_and_adjacency():
    r = merge_ranges([], [5, 3, 4, 4, 10])
    assert r == [[3, 5], [10, 10]]
    r = merge_ranges(r, [6, 7, 8, 9])
    assert r == [[3, 10]]
    assert merge_ranges(r, []) == [[3, 10]]


def test_holes_only_inside_the_window():
    ranges = [[1, 3], [6, 8], [12, 12]]
    assert holes(ranges, 1, 13) == [[4, 5], [9, 11]]
    assert holes(ranges, 1, 4) == []
    assert holes(ranges, 1, 15) == [[4, 5], [9, 11], [13, 14]]
    assert holes([], 5, 5) == []


def test_validate_rows_rejects_bad_values_but_keeps_good_ones():
    rows = [[1, NOW, 7, 1.5, 192], [2, NOW, 7, None, 192], [3, NOW, 7, True, 192],
            [4, 123, 7, 1.0, 192], [5, NOW + 10 * 86_400_000, 7, 1.0, 192], [6, NOW, "x", 1.0, 192],
            [7, NOW, 7, 2, -1], [8, NOW, 7, 3, 0]]
    good, rejected = validate_rows(rows, NOW, 86_400)
    assert [g[0] for g in good] == [1, 8]
    assert [r[0] for r in rejected] == [2, 3, 4, 5, 6, 7]


def test_ledger_reports_confirmed_missing_only_below_min_unsynced(tmp_path):
    led = Ledger(str(tmp_path / "l.db"))
    st, new = led.record("c1", 9, list(range(1, 11)), 0, min_unsynced=1, max_seq=10)
    assert st.missing_count() == 0 and new == []
    assert st.backlog() == 0
    # 11..14 still queued at the collector (min_unsynced = 11): not missing
    st, new = led.record("c1", 9, list(range(15, 21)), 0, min_unsynced=11, max_seq=20)
    assert st.missing_count() == 0 and st.backlog() == 4
    # collector says everything below 21 was confirmed, but we never stored 11..14
    st, new = led.record("c1", 9, [21], 0, min_unsynced=21, max_seq=21)
    assert st.missing() == [[11, 14]] and new == [[11, 14]]
    led.close()
    # state is durable
    led2 = Ledger(str(tmp_path / "l.db"))
    assert led2.states[("c1", 9)].missing_count() == 4


def test_ledger_baseline_ignores_rows_synced_before_first_contact(tmp_path):
    led = Ledger(str(tmp_path / "l.db"))
    st, _ = led.record("c1", 3, [500, 501], 0, min_unsynced=500, max_seq=501)
    assert st.baseline == 500 and st.missing_count() == 0


# ── HTTP flow against a fake StreamPipes ────────────────────────────────────
class FakeSP:
    def __init__(self):
        self.fail = False
        self.points = {}

    async def write(self, request):
        if self.fail:
            return web.Response(status=500, text="influx down")
        body = await request.json()
        series = body["allDataSeries"][0]
        h = series["headers"]
        for row in series["rows"]:
            d = dict(zip(h, row))
            self.points[(d["timestamp"], d["collector_id"], d["node_id"], d["tag_name"])] = d
        return web.json_response({})


@pytest.fixture
async def env(aiohttp_client, aiohttp_server, tmp_path):
    sp = FakeSP()
    sp_app = web.Application()
    sp_app.router.add_post("/api/v4/datalake/measurements/{name}", sp.write)
    sp_server = await aiohttp_server(sp_app)
    cfg = Config(api_keys=("k1",), sp_url=str(sp_server.make_url("")).rstrip("/"), sp_user="u",
                 sp_key="k", nats_url="", state_db=str(tmp_path / "gw.db"))
    client = await aiohttp_client(create_app(cfg))
    return sp, client


def batch(seqs, min_unsynced=None, value=1.0, epoch=42):
    return {"schema": 1, "collector_id": "mill-1", "epoch": epoch, "kind": "live",
            "min_unsynced_seq": min(seqs) if min_unsynced is None else min_unsynced, "max_seq": max(seqs),
            "tags": {"7": {"node_id": "ns=2;s=P", "name": "Pressure", "unit": "bar"}},
            "rows": [[s, NOW + s, 7, value, 192] for s in seqs]}


async def post(client, body, key="k1", gz=True):
    data = json.dumps(body).encode()
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if gz:
        data = gzip.compress(data)
        headers["Content-Encoding"] = "gzip"
    return await client.post("/v1/telemetry/ingest", data=data, headers=headers)


async def test_confirms_only_after_storage(env):
    sp, client = env
    r = await post(client, batch([1, 2, 3]))
    assert r.status == 200
    assert (await r.json()) == {"accepted": 3, "rejected": [], "stored": 3}
    assert len(sp.points) == 3


async def test_storage_failure_is_503_and_nothing_is_recorded(env):
    sp, client = env
    sp.fail = True
    r = await post(client, batch([1, 2, 3]))
    assert r.status == 503
    health = await (await client.get("/health")).json()
    assert health["collectors"] == [] and health["sp_errors"] == 1
    sp.fail = False
    assert (await post(client, batch([1, 2, 3]))).status == 200


async def test_resend_is_idempotent(env):
    sp, client = env
    for _ in range(3):
        assert (await post(client, batch([1, 2, 3]))).status == 200
    assert len(sp.points) == 3
    health = await (await client.get("/health")).json()
    assert health["collectors"][0]["confirmed_missing"] == 0


async def test_bad_rows_are_rejected_not_retried(env):
    sp, client = env
    body = batch([1, 2])
    body["rows"][1][3] = None
    r = await post(client, body)
    assert r.status == 200
    j = await r.json()
    assert j["accepted"] == 2 and j["rejected"] == [[2, "value not a finite number"]] and j["stored"] == 1


async def test_auth_and_malformed_batches(env):
    _, client = env
    assert (await post(client, batch([1]), key="wrong")).status == 401
    assert (await post(client, {"collector_id": "mill-1"})).status == 400
    assert (await post(client, {**batch([1]), "collector_id": "bad id/"})).status == 400
    body = batch([1])
    body["rows"] = [[1, 2, 3]]
    assert (await post(client, body)).status == 400


async def test_plain_json_without_gzip_is_accepted(env):
    _, client = env
    assert (await post(client, batch([1]), gz=False)).status == 200


async def test_health_reports_confirmed_loss(env):
    _, client = env
    await post(client, batch([1, 2, 3]))
    await post(client, batch([6], min_unsynced=6))
    health = await (await client.get("/health")).json()
    c = health["collectors"][0]
    assert c["confirmed_missing"] == 2 and c["missing_ranges"] == [[4, 5]]
    assert health["status"] == "degraded"
    metrics = await (await client.get("/metrics")).text()
    assert 'thinpi_gateway_confirmed_missing{collector="mill-1",epoch="42"} 2' in metrics
