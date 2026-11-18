"""采集与查询 API。"""
from __future__ import annotations

import re
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from . import db as dbmod
from . import service

HOUR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}$")


def _check_hour(hour: str) -> str:
    hour = hour.strip()
    if not HOUR_RE.match(hour):
        raise HTTPException(400, "hour must be a UTC hour like '2026-11-18T10'")
    try:
        datetime.strptime(hour, "%Y-%m-%dT%H")
    except ValueError:
        raise HTTPException(400, f"invalid UTC hour: {hour}")
    return hour


@asynccontextmanager
async def lifespan(app: FastAPI):
    dbmod.init()
    yield


app = FastAPI(title="PM2.5 Revision Collector", lifespan=lifespan)


class CollectRequest(BaseModel):
    hour: str


@app.get("/")
def index():
    return {"service": "pm25-collector", "docs": "/docs"}


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/collections")
def create_collection(req: CollectRequest):
    """采集某 UTC 小时:先固定源站当前版,再逐版补齐到该版。"""
    hour = _check_hour(req.hour)
    try:
        return service.collect_hour(hour)
    except service.HourBusy:
        raise HTTPException(409, f"collection already running for hour {hour}")


@app.get("/collections/{hour}")
def collection_status(hour: str):
    hour = _check_hour(hour)
    db = dbmod.connect()
    try:
        row = dbmod.get_hour(db, hour)
        if row is None:
            raise HTTPException(404, f"hour {hour} never collected")
        return dict(row)
    finally:
        db.close()


@app.get("/hours")
def list_hours():
    db = dbmod.connect()
    try:
        rows = db.execute("SELECT * FROM hours ORDER BY hour").fetchall()
        return {"hours": [dict(r) for r in rows]}
    finally:
        db.close()


@app.get("/hours/{hour}/versions")
def list_versions(hour: str):
    hour = _check_hour(hour)
    db = dbmod.connect()
    try:
        # 指针、版本列表与站点数取自同一个已提交快照
        with dbmod.read_tx(db):
            row = dbmod.get_hour(db, hour)
            if row is None:
                raise HTTPException(404, f"hour {hour} never collected")
            vers = db.execute(
                "SELECT * FROM versions WHERE hour=? ORDER BY version", (hour,)
            ).fetchall()
            return {
                "hour": hour,
                "current_version": row["current_version"],
                "versions": [dict(v) for v in vers],
            }
    finally:
        db.close()


@app.get("/hours/{hour}/data")
def hour_data(
    hour: str,
    version: Optional[int] = Query(default=None, description="缺省取当前版指针"),
    station_id: Optional[str] = Query(default=None),
):
    """查询某已确认版当时的完整数据。"""
    hour = _check_hour(hour)
    db = dbmod.connect()
    try:
        # 版号、读数与站点数来自同一个已提交快照:采集并发提交也不会撕裂
        with dbmod.read_tx(db):
            row = dbmod.get_hour(db, hour)
            if row is None:
                raise HTTPException(404, f"hour {hour} never collected")
            v = version if version is not None else row["current_version"]
            if v == 0:
                raise HTTPException(404, f"hour {hour} has no confirmed version yet")
            vrow = dbmod.get_version(db, hour, v)
            if vrow is None:
                raise HTTPException(404, f"version {v} of hour {hour} is not confirmed")
            # 默认查当前版:直接读物化快照,不逐版回放历史。
            # 历史版:以旧全量版本为基线叠加稀疏变更还原。
            if v == row["current_version"] == row["current_state_version"]:
                snap = dbmod.current_snapshot(db, hour)
            else:
                snap = dbmod.reconstruct_snapshot(db, hour, v)
            if station_id is not None:
                if station_id not in snap:
                    raise HTTPException(
                        404, f"station {station_id} not present in version {v}"
                    )
                stations = [{"station_id": station_id, "pm25": snap[station_id]}]
            else:
                stations = [
                    {"station_id": sid, "pm25": pm} for sid, pm in sorted(snap.items())
                ]
            return {
                "hour": hour,
                "version": v,
                "is_current": v == row["current_version"],
                "station_count": len(stations),
                "stations": stations,
            }
    finally:
        db.close()
