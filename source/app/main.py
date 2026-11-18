"""模拟源站:市环境监测中心 PM2.5 修订发布服务。

- 首版(v1)为全量站点快照;此后每版只列新增、改值(upsert)和删除(delete)。
- 每版分页发布,响应携带连续版号 version、页号 page、总页数 total_pages。
- 管理接口支持:发布新修订(可切换修订)、重置、注入缺页/超时/同版同站冲突故障。
"""
from __future__ import annotations

import os
import random
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "8"))
FAULT_SLEEP = float(os.environ.get("FAULT_SLEEP_SECONDS", "10"))
DEFAULT_STATIONS = int(os.environ.get("STATION_COUNT", "20"))
DEFAULT_SEED = int(os.environ.get("SEED", "20261118"))

LOCK = threading.Lock()

# ---------------- 内部状态(仅源站可见的"真实值"与已发布修订) ----------------
STATE = {
    "seed": DEFAULT_SEED,
    "rng": random.Random(DEFAULT_SEED),
    "truth": {},      # station_id -> pm25,当前最新全量
    "versions": {},   # version -> {version, items, pages, total_pages, created_at}
    "current": 0,
    "faults": [],     # [{version, page, kind, times}], times=-1 表示一直生效
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _paginate(items: list[dict]) -> tuple[int, dict[int, list[dict]]]:
    total = max(1, (len(items) + PAGE_SIZE - 1) // PAGE_SIZE)
    return total, {p: items[(p - 1) * PAGE_SIZE: p * PAGE_SIZE] for p in range(1, total + 1)}


def _reset(station_count: int, seed: int) -> None:
    rng = random.Random(seed)
    truth = {f"S{i:02d}": round(rng.uniform(5, 150), 1) for i in range(1, station_count + 1)}
    items = [{"station_id": sid, "op": "upsert", "pm25": pm} for sid, pm in sorted(truth.items())]
    total, pages = _paginate(items)
    STATE["seed"] = seed
    STATE["rng"] = rng
    STATE["truth"] = truth
    STATE["versions"] = {
        1: {"version": 1, "items": items, "pages": pages, "total_pages": total, "created_at": _now()}
    }
    STATE["current"] = 1
    STATE["faults"] = []


def _auto_delta(version: int) -> list[dict]:
    """确定性地生成一条增量:3 个改值,偶数版删 1 站,并新增 1 个站。"""
    rng = STATE["rng"]
    truth = STATE["truth"]
    ids = sorted(truth)
    ops: list[dict] = []
    chosen = rng.sample(ids, k=min(3, len(ids))) if ids else []
    for sid in chosen:
        ops.append({"station_id": sid, "op": "upsert", "pm25": round(rng.uniform(5, 150), 1)})
    if version % 2 == 0:
        remaining = [s for s in ids if s not in chosen]
        if len(remaining) > 1:
            ops.append({"station_id": rng.choice(remaining), "op": "delete"})
    new_id = f"N{version:02d}"
    if new_id not in truth:
        ops.append({"station_id": new_id, "op": "upsert", "pm25": round(rng.uniform(5, 150), 1)})
    return ops


def _apply_ops(ops: list[dict]) -> None:
    truth = STATE["truth"]
    for it in ops:
        if it["op"] == "upsert":
            truth[it["station_id"]] = it["pm25"]
        else:
            truth.pop(it["station_id"], None)


def _consume_fault(version: int, page: int) -> Optional[str]:
    for f in list(STATE["faults"]):
        if f["version"] == version and f["page"] == page and f["times"] != 0:
            if f["times"] > 0:
                f["times"] -= 1
                if f["times"] == 0:
                    STATE["faults"].remove(f)
            return f["kind"]
    return None


# ---------------- 请求模型 ----------------
class Op(BaseModel):
    station_id: str = Field(min_length=1)
    op: Literal["upsert", "delete"]
    pm25: Optional[float] = Field(default=None, ge=0)


class PublishRequest(BaseModel):
    ops: Optional[list[Op]] = None  # 缺省则自动生成一条确定性增量


class ResetRequest(BaseModel):
    station_count: int = Field(default=DEFAULT_STATIONS, ge=1, le=500)
    seed: int = DEFAULT_SEED


class Fault(BaseModel):
    version: int
    page: int = Field(default=1, ge=1)
    kind: Literal["missing", "timeout", "conflict"]
    times: int = 1  # 触发次数;-1 表示一直生效


class FaultsRequest(BaseModel):
    faults: list[Fault]


def _validate_explicit(ops_in: list[Op]) -> list[dict]:
    seen: dict[str, tuple] = {}
    ops: list[dict] = []
    for o in ops_in:
        if o.op == "upsert" and o.pm25 is None:
            raise HTTPException(400, f"upsert for {o.station_id} requires pm25")
        if o.op == "delete" and o.pm25 is not None:
            raise HTTPException(400, f"delete for {o.station_id} must not carry pm25")
        key = (o.op, o.pm25)
        if o.station_id in seen:
            if seen[o.station_id] != key:
                raise HTTPException(400, f"conflicting ops for station {o.station_id}")
            continue
        seen[o.station_id] = key
        item = {"station_id": o.station_id, "op": o.op}
        if o.op == "upsert":
            item["pm25"] = o.pm25
        ops.append(item)
    return ops


# ---------------- 应用 ----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    with LOCK:
        _reset(DEFAULT_STATIONS, DEFAULT_SEED)
    yield


app = FastAPI(title="PM2.5 Mock Source", lifespan=lifespan)


@app.get("/")
def index():
    return {"service": "pm25-mock-source", "docs": "/docs"}


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


# ---------------- 数据接口(采集方使用) ----------------
@app.get("/api/revisions/current")
def current_revision():
    with LOCK:
        return {"current_version": STATE["current"]}


@app.get("/api/revisions")
def list_revisions():
    with LOCK:
        return {
            "current_version": STATE["current"],
            "versions": [
                {
                    "version": v,
                    "total_pages": ver["total_pages"],
                    "item_count": len(ver["items"]),
                    "created_at": ver["created_at"],
                }
                for v, ver in sorted(STATE["versions"].items())
            ],
        }


@app.get("/api/revisions/{version}/pages/{page}")
def get_page(version: int, page: int):
    with LOCK:
        ver = STATE["versions"].get(version)
        if ver is None:
            raise HTTPException(404, f"version {version} not found")
        if page < 1 or page > ver["total_pages"]:
            raise HTTPException(404, f"page {page} of version {version} not found")
        items = [dict(i) for i in ver["pages"][page]]
        total = ver["total_pages"]
        first_item = dict(ver["items"][0]) if ver["items"] else None
        fault = _consume_fault(version, page)
    if fault == "missing":
        raise HTTPException(404, f"page {page} of version {version} not found (injected)")
    if fault == "timeout":
        time.sleep(FAULT_SLEEP)
    if fault == "conflict" and first_item is not None:
        # 同版同站冲突:追加一条与本版首条同站但取值不同的记录
        items.append(
            {
                "station_id": first_item["station_id"],
                "op": "upsert",
                "pm25": round((first_item.get("pm25") or 0) + 50.0, 1),
            }
        )
    return {"version": version, "page": page, "total_pages": total, "items": items}


# ---------------- 管理接口(切换修订 / 故障注入) ----------------
@app.post("/admin/reset")
def admin_reset(req: ResetRequest = ResetRequest()):
    with LOCK:
        _reset(req.station_count, req.seed)
        return {
            "current_version": STATE["current"],
            "station_count": len(STATE["truth"]),
            "seed": STATE["seed"],
        }


@app.post("/admin/publish")
def admin_publish(req: PublishRequest = PublishRequest()):
    with LOCK:
        version = STATE["current"] + 1
        ops = _auto_delta(version) if req.ops is None else _validate_explicit(req.ops)
        _apply_ops(ops)
        total, pages = _paginate(ops)
        STATE["versions"][version] = {
            "version": version,
            "items": ops,
            "pages": pages,
            "total_pages": total,
            "created_at": _now(),
        }
        STATE["current"] = version
        return {"version": version, "total_pages": total, "item_count": len(ops), "ops": ops}


@app.get("/admin/faults")
def get_faults():
    with LOCK:
        return {"faults": [dict(f) for f in STATE["faults"]]}


@app.put("/admin/faults")
def put_faults(req: FaultsRequest):
    with LOCK:
        STATE["faults"] = [f.model_dump() for f in req.faults]
        return {"faults": [dict(f) for f in STATE["faults"]]}


@app.delete("/admin/faults")
def delete_faults():
    with LOCK:
        STATE["faults"] = []
        return {"faults": []}


@app.get("/admin/state")
def admin_state():
    with LOCK:
        return {
            "current_version": STATE["current"],
            "station_count": len(STATE["truth"]),
            "seed": STATE["seed"],
            "versions": [
                {"version": v, "total_pages": ver["total_pages"], "item_count": len(ver["items"])}
                for v, ver in sorted(STATE["versions"].items())
            ],
            "faults": [dict(f) for f in STATE["faults"]],
        }
