"""采集逻辑:先固定源站当前版,再逐版补齐;失败保留原供数版。"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone

import httpx

from . import db as dbmod

SOURCE_BASE_URL = os.environ.get("SOURCE_BASE_URL", "http://localhost:8001")
PAGE_TIMEOUT = float(os.environ.get("PAGE_TIMEOUT_SECONDS", "3"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))
RETRY_BACKOFF = float(os.environ.get("RETRY_BACKOFF_SECONDS", "0.5"))


class CollectError(Exception):
    """采集过程中可重试/可降级的错误。"""


class MissingPage(CollectError):
    """缺页。"""


class PageTimeout(CollectError):
    """超时。"""


class VersionConflict(CollectError):
    """同版同站冲突。"""


class SourceError(CollectError):
    """源站响应异常。"""


class HourBusy(Exception):
    """同一小时已有采集在进行。"""


_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(hour: str) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(hour, threading.Lock())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------- 源站访问 ----------------
def _get_json(client: httpx.Client, path: str) -> dict:
    try:
        resp = client.get(path)
    except httpx.TimeoutException as e:
        raise PageTimeout(f"timeout fetching {path}") from e
    except httpx.HTTPError as e:
        raise SourceError(f"transport error fetching {path}: {e}") from e
    if resp.status_code == 404:
        raise MissingPage(f"missing page: {path}")
    if resp.status_code != 200:
        raise SourceError(f"source returned {resp.status_code} for {path}")
    try:
        return resp.json()
    except ValueError as e:
        raise SourceError(f"invalid JSON from {path}") from e


def _check_envelope(pg: dict, version: int, page: int) -> None:
    if not isinstance(pg, dict):
        raise SourceError(f"version {version} page {page}: response is not an object")
    if pg.get("version") != version:
        raise SourceError(f"page {page}: version mismatch {pg.get('version')!r} != {version}")
    if pg.get("page") != page:
        raise SourceError(f"page mismatch: got {pg.get('page')!r}, want {page}")
    total = pg.get("total_pages")
    if not isinstance(total, int) or isinstance(total, bool) or total < 1:
        raise SourceError(f"page {page}: bad total_pages {total!r}")
    if not isinstance(pg.get("items"), list):
        raise SourceError(f"page {page}: items is not a list")


def _validate_item(it: object, version: int) -> tuple[str, str, float | None]:
    if not isinstance(it, dict):
        raise SourceError(f"version {version}: item is not an object")
    sid = it.get("station_id")
    op = it.get("op")
    if not isinstance(sid, str) or not sid:
        raise SourceError(f"version {version}: bad station_id {sid!r}")
    if op not in ("upsert", "delete"):
        raise SourceError(f"version {version} station {sid}: bad op {op!r}")
    if op == "upsert":
        pm = it.get("pm25")
        if not isinstance(pm, (int, float)) or isinstance(pm, bool) or pm < 0:
            raise SourceError(f"version {version} station {sid}: bad pm25 {pm!r}")
        return sid, "upsert", float(pm)
    return sid, "delete", None


def _fetch_version(client: httpx.Client, version: int) -> tuple[int, list[dict]]:
    """整版抓取:每次调用都从第 1 页重新取,绝不沿用上次尝试的残页。"""
    first = _get_json(client, f"/api/revisions/{version}/pages/1")
    _check_envelope(first, version, 1)
    total = first["total_pages"]
    raw = list(first["items"])
    for page in range(2, total + 1):
        pg = _get_json(client, f"/api/revisions/{version}/pages/{page}")
        _check_envelope(pg, version, page)
        if pg["total_pages"] != total:
            raise SourceError(f"version {version}: total_pages changed between pages")
        raw.extend(pg["items"])
    # 同版同站冲突检测(完全相同的重复记录视为幂等,去噪)
    seen: dict[str, tuple] = {}
    items: list[dict] = []
    for it in raw:
        sid, op, pm = _validate_item(it, version)
        if sid in seen:
            if seen[sid] != (op, pm):
                raise VersionConflict(
                    f"version {version} station {sid}: conflicting entries in same version"
                )
            continue
        seen[sid] = (op, pm)
        items.append({"station_id": sid, "op": op, "pm25": pm})
    return total, items


def _apply(base: dict[str, float], items: list[dict]) -> dict[str, float]:
    snap = dict(base)
    for it in items:
        if it["op"] == "upsert":
            snap[it["station_id"]] = it["pm25"]
        else:
            snap.pop(it["station_id"], None)
    return snap


# ---------------- 采集主流程 ----------------
def collect_hour(hour: str) -> dict:
    lock = _lock_for(hour)
    if not lock.acquire(blocking=False):
        raise HourBusy(hour)
    try:
        return _collect(hour)
    finally:
        lock.release()


def _mark(db, hour: str, status: str, last_error: str | None = None) -> None:
    # 仅更新状态元信息,绝不动 current_version 指针
    db.execute(
        "UPDATE hours SET status=?, last_error=?, updated_at=? WHERE hour=?",
        (status, last_error, _now(), hour),
    )


def _summary(hour, status, pinned, confirmed_from, confirmed_to, error, attempts) -> dict:
    return {
        "hour": hour,
        "status": status,
        "pinned_version": pinned,
        "confirmed_from": confirmed_from,
        "confirmed_to": confirmed_to,
        "current_version": confirmed_to,
        "attempts": {str(v): n for v, n in attempts.items()},
        "error": error,
    }


def _collect(hour: str) -> dict:
    db = dbmod.connect()
    attempts: dict[int, int] = {}
    try:
        dbmod.ensure_hour(db, hour, _now())
        row = dbmod.get_hour(db, hour)
        prev_current = row["current_version"]

        with httpx.Client(base_url=SOURCE_BASE_URL, timeout=PAGE_TIMEOUT) as client:
            # 1) 先固定源站当前版
            try:
                pinned = int(_get_json(client, "/api/revisions/current")["current_version"])
            except (CollectError, KeyError, TypeError, ValueError) as e:
                msg = f"pin current version failed: {e}"
                _mark(db, hour, "failed", msg)
                return _summary(hour, "failed", None, prev_current, prev_current, msg, attempts)

            db.execute(
                "UPDATE hours SET status='collecting', pinned_version=?, last_error=NULL, "
                "updated_at=? WHERE hour=?",
                (pinned, _now(), hour),
            )

            if pinned < prev_current:
                msg = f"source current {pinned} regressed below confirmed {prev_current}"
                _mark(db, hour, "failed", msg)
                return _summary(hour, "source_regressed", pinned, prev_current, prev_current, msg, attempts)
            if pinned == prev_current:
                _mark(db, hour, "ok")
                return _summary(hour, "up_to_date", pinned, prev_current, prev_current, None, attempts)

            # 2) 逐版补齐:prev_current+1 .. pinned
            confirmed = prev_current
            for version in range(prev_current + 1, pinned + 1):
                base = dbmod.snapshot(db, hour, version - 1)
                last_err: Exception | None = None
                ok = False
                for attempt in range(1, MAX_ATTEMPTS + 1):
                    try:
                        total, items = _fetch_version(client, version)
                        new_snap = _apply(base, items)
                        # 3) 数据与当前版指针在同一事务生效
                        with dbmod.tx(db):
                            db.execute(
                                "INSERT INTO versions(hour, version, total_pages, item_count, "
                                "station_count, confirmed_at) VALUES (?,?,?,?,?,?)",
                                (hour, version, total, len(items), len(new_snap), _now()),
                            )
                            db.executemany(
                                "INSERT INTO entries(hour, version, station_id, pm25) "
                                "VALUES (?,?,?,?)",
                                [(hour, version, sid, pm) for sid, pm in sorted(new_snap.items())],
                            )
                            db.execute(
                                "UPDATE hours SET current_version=?, updated_at=? WHERE hour=?",
                                (version, _now(), hour),
                            )
                        confirmed = version
                        attempts[version] = attempt
                        ok = True
                        break
                    except CollectError as e:
                        last_err = e
                        if attempt < MAX_ATTEMPTS:
                            time.sleep(RETRY_BACKOFF * attempt)
                if not ok:
                    # 缺页/超时/冲突:保留原供数版,指针不动,停止后续版本
                    msg = f"version {version} failed after {MAX_ATTEMPTS} attempts: {last_err}"
                    _mark(db, hour, "failed", msg)
                    return _summary(hour, "failed", pinned, prev_current, confirmed, msg, attempts)

            _mark(db, hour, "ok")
            return _summary(hour, "ok", pinned, prev_current, confirmed, None, attempts)
    finally:
        db.close()
