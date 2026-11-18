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
        row = dbmod.get_hour(db, hour)
        if row is None:
            raise HTTPException(404, f"hour {hour} never collected")
        v = version if version is not None else row["current_version"]
        if v == 0:
            raise HTTPException(404, f"hour {hour} has no confirmed version yet")
        vrow = db.execute(
            "SELECT * FROM versions WHERE hour=? AND version=?", (hour, v)
        ).fetchone()
        if vrow is None:
            raise HTTPException(404, f"version {v} of hour {hour} is not confirmed")
        if station_id is not None:
            rows = db.execute(
                "SELECT station_id, pm25 FROM entries WHERE hour=? AND version=? AND station_id=?",
                (hour, v, station_id),
            ).fetchall()
            if not rows:
                raise HTTPException(404, f"station {station_id} not present in version {v}")
        else:
            rows = db.execute(
                "SELECT station_id, pm25 FROM entries WHERE hour=? AND version=? "
                "ORDER BY station_id",
                (hour, v),
            ).fetchall()
        return {
            "hour": hour,
            "version": v,
            "is_current": v == row["current_version"],
            "station_count": len(rows),
            "stations": [{"station_id": r["station_id"], "pm25": r["pm25"]} for r in rows],
        }
    finally:
        db.close()
