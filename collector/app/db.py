"""SQLite 存储层。

存储布局(同一小时的多次修订不再逐版复制未变站点):

- ``entries``:旧版采集写入的**全量**版本快照,保持只读、继续可查,并作为
  后续稀疏版本的基线。
- ``version_changes``:新确认的版本只记录该版实际提交的变更,
  ``op='upsert'`` 带新读数,``op='delete'`` 是删除标记(pm25 为 NULL)。
  某版没有变更的站点不出现在这里,因此不会复制整张站点表。
- ``current_entries``:每小时一份"当前确认版"的物化快照。它由
  ``version_changes``/``entries`` 在同一事务里增量维护,默认查询直接读它,
  无需逐版回放历史。

不变式:
- 一版的变更、``current_entries`` 增量、版本登记(versions)与当前版指针
  (hours.current_version)在同一事务里提交;失败或重启都不会暴露"半版",
  也不会改变原已确认版本。
- ``hours.current_state_version`` 标明 ``current_entries`` 对应哪个确认版;
  启动时据此校验/重建,崩溃不会留下陈旧或部分物化的当前快照。
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", "/data/collector.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS hours (
    hour            TEXT PRIMARY KEY,          -- UTC 小时,如 2026-11-18T10
    current_version INTEGER NOT NULL DEFAULT 0, -- 当前版指针:0 表示尚无确认版
    status          TEXT NOT NULL DEFAULT 'idle',
    pinned_version  INTEGER,
    last_error      TEXT,
    updated_at      TEXT NOT NULL,
    -- current_entries 对应的确认版;重启时据此校验/重建物化快照
    current_state_version INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS versions (
    hour           TEXT NOT NULL,
    version        INTEGER NOT NULL,
    total_pages    INTEGER NOT NULL,
    item_count     INTEGER NOT NULL,
    station_count  INTEGER NOT NULL,
    confirmed_at   TEXT NOT NULL,
    -- full: 旧版全量快照(数据在 entries);sparse: 仅变更(数据在 version_changes)
    storage        TEXT NOT NULL DEFAULT 'sparse',
    PRIMARY KEY (hour, version)
);
-- 旧版写入的全量快照,保持原样供查询/作为基线。
CREATE TABLE IF NOT EXISTS entries (
    hour       TEXT NOT NULL,
    version    INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    pm25       REAL NOT NULL,
    PRIMARY KEY (hour, version, station_id)
);
-- 新确认版只存变化值和删除标记。
CREATE TABLE IF NOT EXISTS version_changes (
    hour       TEXT NOT NULL,
    version    INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    op         TEXT NOT NULL CHECK (op IN ('upsert', 'delete')),
    pm25       REAL,
    PRIMARY KEY (hour, version, station_id),
    CHECK ((op = 'upsert' AND pm25 IS NOT NULL) OR (op = 'delete' AND pm25 IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_version_changes_hour_version
    ON version_changes(hour, version);
-- 每小时当前确认版的物化快照(仅一份,不按版本复制历史)。
CREATE TABLE IF NOT EXISTS current_entries (
    hour       TEXT NOT NULL,
    station_id TEXT NOT NULL,
    pm25       REAL NOT NULL,
    PRIMARY KEY (hour, station_id)
);
"""


def connect() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)  # 自动提交,显式事务见 tx()
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


@contextmanager
def tx(db: sqlite3.Connection):
    """写事务:要么整体提交,要么整体回滚。"""
    db.execute("BEGIN IMMEDIATE")
    try:
        yield db
    except Exception:
        db.execute("ROLLBACK")
        raise
    else:
        try:
            db.execute("COMMIT")
        except Exception:
            # 提交失败(如磁盘满)时保证不残留未结束的事务
            db.execute("ROLLBACK")
            raise


@contextmanager
def read_tx(db: sqlite3.Connection):
    """只读事务:多次 SELECT 共享同一个已提交快照。

    查询期间若有采集提交新版本,版号、读数和站点数仍然取自同一个已确认状态。
    """
    db.execute("BEGIN")
    try:
        yield db
    except Exception:
        db.execute("ROLLBACK")
        raise
    else:
        try:
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise


def init() -> None:
    directory = os.path.dirname(DB_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    db = connect()
    try:
        db.executescript(SCHEMA)
        with tx(db):
            _migrate_columns(db)
            _sync_current_state(db)
            # 重启清理:把中断在 collecting 的小时复位为 idle。
            # 数据本身由事务保证完整,这里只修正状态元信息。
            db.execute("UPDATE hours SET status='idle' WHERE status='collecting'")
    finally:
        db.close()


def _columns(db: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in db.execute(f"PRAGMA table_info({table})").fetchall()}


def _migrate_columns(db: sqlite3.Connection) -> None:
    """兼容旧库:补上派生列。

    ``versions.storage`` 区分旧的全量版本(``full``,数据在 ``entries``)与
    新的稀疏版本(``sparse``,数据在 ``version_changes``)。
    ``hours.current_state_version`` 记录 ``current_entries`` 对应的版本。
    """
    if "storage" not in _columns(db, "versions"):
        # 旧库里的 versions 都是全量快照
        db.execute("ALTER TABLE versions ADD COLUMN storage TEXT NOT NULL DEFAULT 'full'")
    if "current_state_version" not in _columns(db, "hours"):
        db.execute(
            "ALTER TABLE hours ADD COLUMN current_state_version INTEGER NOT NULL DEFAULT 0"
        )


def _rebuild_current_state(db: sqlite3.Connection, hour: str, current: int) -> None:
    """用已确认版本重建该小时的当前快照与版本标记(调用方持有写事务)。"""
    db.execute("DELETE FROM current_entries WHERE hour=?", (hour,))
    if current > 0:
        snap = reconstruct_snapshot(db, hour, current)
        db.executemany(
            "INSERT INTO current_entries(hour, station_id, pm25) VALUES (?,?,?)",
            [(hour, sid, pm) for sid, pm in sorted(snap.items())],
        )
    db.execute("UPDATE hours SET current_state_version=? WHERE hour=?", (current, hour))


def _sync_current_state(db: sqlite3.Connection) -> None:
    """校验并重建 ``current_entries``,使其严格对应 ``current_version``。

    旧库升级时 ``current_state_version`` 为 0,会由旧的全量快照/稀疏变更重建;
    正常重启时若两者一致则什么都不做。整个过程在调用方的写事务里完成,
    不会留下半份当前快照。
    """
    hours = db.execute(
        "SELECT hour, current_version, current_state_version FROM hours"
    ).fetchall()
    for h in hours:
        if h["current_version"] != h["current_state_version"]:
            _rebuild_current_state(db, h["hour"], h["current_version"])


def ensure_current_state(db: sqlite3.Connection, hour: str) -> None:
    """采集前确保物化快照与指针一致(如遇旧库/未迁移状态就地自愈)。"""
    row = get_hour(db, hour)
    if row is None or row["current_version"] == row["current_state_version"]:
        return
    with tx(db):
        row = get_hour(db, hour)
        if row is not None and row["current_version"] != row["current_state_version"]:
            _rebuild_current_state(db, hour, row["current_version"])


def ensure_hour(db: sqlite3.Connection, hour: str, now: str) -> None:
    db.execute(
        "INSERT OR IGNORE INTO hours(hour, current_version, status, updated_at) "
        "VALUES (?, 0, 'idle', ?)",
        (hour, now),
    )


def get_hour(db: sqlite3.Connection, hour: str) -> sqlite3.Row | None:
    return db.execute("SELECT * FROM hours WHERE hour=?", (hour,)).fetchone()


def get_version(db: sqlite3.Connection, hour: str, version: int) -> sqlite3.Row | None:
    return db.execute(
        "SELECT * FROM versions WHERE hour=? AND version=?", (hour, version)
    ).fetchone()


class StaleBase(Exception):
    """提交时当前版指针已被其他采集推进,本次提交作废。"""


def commit_sparse_version(
    db: sqlite3.Connection,
    hour: str,
    version: int,
    *,
    total_pages: int,
    items: list[dict],
    new_snapshot: dict[str, float],
    now: str,
) -> None:
    """原子确认一个稀疏版本。

    同一事务内写入:该版变更值/删除标记(``version_changes``)、当前快照的
    增量(``current_entries``)、版本登记(``versions``)与当前版指针
    (``hours.current_version``)。任一步失败都整体回滚,原供数版不受影响。

    指针更新带 ``current_version = version-1`` 条件:若基线在抓取期间被
    其他进程推进,则整笔作废并抛 ``StaleBase``,绝不把基于旧基线的快照
    写成已确认版本。
    """
    changes = {
        it["station_id"]: (it["op"], it["pm25"] if it["op"] == "upsert" else None)
        for it in items
    }
    with tx(db):
        cur = db.execute(
            "UPDATE hours SET current_version=?, current_state_version=?, updated_at=? "
            "WHERE hour=? AND current_version=?",
            (version, version, now, hour, version - 1),
        )
        if cur.rowcount != 1:
            raise StaleBase(
                f"hour {hour} advanced by another collector; "
                f"expected current_version={version - 1}"
            )
        db.executemany(
            "INSERT INTO version_changes(hour, version, station_id, op, pm25) "
            "VALUES (?,?,?,?,?)",
            [(hour, version, sid, op, pm) for sid, (op, pm) in sorted(changes.items())],
        )
        db.executemany(
            "INSERT INTO current_entries(hour, station_id, pm25) VALUES (?,?,?) "
            "ON CONFLICT(hour, station_id) DO UPDATE SET pm25=excluded.pm25",
            [
                (hour, sid, pm)
                for sid, (op, pm) in sorted(changes.items())
                if op == "upsert"
            ],
        )
        db.executemany(
            "DELETE FROM current_entries WHERE hour=? AND station_id=?",
            [(hour, sid) for sid, (op, _) in sorted(changes.items()) if op == "delete"],
        )
        db.execute(
            "INSERT INTO versions(hour, version, total_pages, item_count, "
            "station_count, confirmed_at, storage) VALUES (?,?,?,?,?,?,'sparse')",
            (hour, version, total_pages, len(items), len(new_snapshot), now),
        )


def current_snapshot(db: sqlite3.Connection, hour: str) -> dict[str, float]:
    """当前确认版的物化快照(默认查询路径,不回放历史)。"""
    rows = db.execute(
        "SELECT station_id, pm25 FROM current_entries WHERE hour=?", (hour,)
    ).fetchall()
    return {r["station_id"]: r["pm25"] for r in rows}


def _full_snapshot(db: sqlite3.Connection, hour: str, version: int) -> dict[str, float]:
    rows = db.execute(
        "SELECT station_id, pm25 FROM entries WHERE hour=? AND version=?",
        (hour, version),
    ).fetchall()
    return {r["station_id"]: r["pm25"] for r in rows}


def _latest_full_version(
    db: sqlite3.Connection, hour: str, version: int
) -> int | None:
    row = db.execute(
        "SELECT MAX(version) AS v FROM versions "
        "WHERE hour=? AND version<=? AND storage='full'",
        (hour, version),
    ).fetchone()
    return row["v"] if row and row["v"] is not None else None


def reconstruct_snapshot(
    db: sqlite3.Connection, hour: str, version: int
) -> dict[str, float]:
    """还原某确认版的完整站点集。

    以不晚于该版的最近一个旧全量版本为基线,再叠加其后的稀疏变更。
    每个站点只取该版之前最后一次变更:删除标记会遮住更早的读数,
    删除后重现的站则以重现时的 upsert 为准,不会串用旧版读数。
    """
    full = _latest_full_version(db, hour, version)
    if full == version:
        return _full_snapshot(db, hour, version)

    snap = _full_snapshot(db, hour, full) if full is not None else {}
    rows = db.execute(
        """
        SELECT station_id, op, pm25 FROM (
            SELECT station_id, op, pm25,
                   ROW_NUMBER() OVER (
                       PARTITION BY station_id ORDER BY version DESC
                   ) AS rn
            FROM version_changes
            WHERE hour=? AND version>? AND version<=?
        ) WHERE rn=1
        """,
        (hour, full if full is not None else 0, version),
    ).fetchall()
    for r in rows:
        if r["op"] == "upsert":
            snap[r["station_id"]] = r["pm25"]
        else:
            snap.pop(r["station_id"], None)
    return snap
