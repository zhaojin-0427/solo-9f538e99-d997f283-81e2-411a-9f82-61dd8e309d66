#!/usr/bin/env python3
"""端到端冒烟测试:对运行中的 source + collector 验证全部关键行为。

用法:
    python3 scripts/smoke.py                 # 默认打 http://localhost:8000 / 8001
    COLLECTOR_URL=... SOURCE_URL=... python3 scripts/smoke.py
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

COLLECTOR = os.environ.get("COLLECTOR_URL", "http://localhost:8000").rstrip("/")
SOURCE = os.environ.get("SOURCE_URL", "http://localhost:8001").rstrip("/")
HOUR = os.environ.get("SMOKE_HOUR", "2026-11-18T10")

_checks = 0


def req(method: str, base: str, path: str, body=None, expect: int | None = 200):
    url = base + path
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=60) as resp:
            code, payload = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        code, payload = e.code, e.read()
    if expect is not None and code != expect:
        raise AssertionError(f"{method} {url} -> {code}, want {expect}: {payload[:400]!r}")
    return code, (json.loads(payload) if payload else {})


def check(name: str, cond: bool, extra: str = "") -> None:
    global _checks
    if not cond:
        raise AssertionError(f"CHECK FAILED: {name} {extra}")
    _checks += 1
    print(f"  ok [{_checks:02d}] {name}")


def stations(base: str, hour: str, version: int | None = None) -> dict:
    q = f"?version={version}" if version is not None else ""
    _, data = req("GET", base, f"/hours/{hour}/data{q}")
    return {s["station_id"]: s["pm25"] for s in data["stations"]}


def main() -> int:
    print(f"== smoke: collector={COLLECTOR} source={SOURCE} hour={HOUR}")

    print("-- 0. 健康检查")
    req("GET", SOURCE, "/healthz")
    req("GET", COLLECTOR, "/healthz")

    print("-- 1. 重置源站,采集首版(全量)")
    _, reset = req("POST", SOURCE, "/admin/reset", {"station_count": 20, "seed": 20261118})
    check("reset -> current=1", reset["current_version"] == 1)
    _, r = req("POST", COLLECTOR, "/collections", {"hour": HOUR})
    check("collect v1 ok", r["status"] == "ok" and r["confirmed_to"] == 1, json.dumps(r))
    v1 = stations(COLLECTOR, HOUR, 1)
    check("v1 全量 20 站", len(v1) == 20)
    check("默认查询=当前版指针", stations(COLLECTOR, HOUR) == v1)

    print("-- 2. 源站发布 v2/v3(增量),逐版补齐")
    _, p2 = req("POST", SOURCE, "/admin/publish")
    _, p3 = req("POST", SOURCE, "/admin/publish")
    deleted_v2 = [o["station_id"] for o in p2["ops"] if o["op"] == "delete"]
    _, r = req("POST", COLLECTOR, "/collections", {"hour": HOUR})
    check("补齐到 v3", r["status"] == "ok" and r["confirmed_from"] == 1 and r["confirmed_to"] == 3,
          json.dumps(r))
    v2, v3 = stations(COLLECTOR, HOUR, 2), stations(COLLECTOR, HOUR, 3)
    check("v2 删除了目标站", all(s not in v2 for s in deleted_v2) and deleted_v2,
          f"deleted={deleted_v2}")
    check("v1 仍含被删站(已确认版均可查当时数据)", all(s in v1 for s in deleted_v2))
    check("v3 含新站 N03", "N03" in v3 and "N03" not in v1)
    check("v1 数据未被后续版本污染", stations(COLLECTOR, HOUR, 1) == v1)

    print("-- 3. 重复采集:已是最新")
    _, r = req("POST", COLLECTOR, "/collections", {"hour": HOUR})
    check("up_to_date", r["status"] == "up_to_date" and r["current_version"] == 3, json.dumps(r))

    print("-- 4. 超时故障:一次性,重试后成功(重试不混残页)")
    req("PUT", SOURCE, "/admin/faults",
        {"faults": [{"version": 4, "page": 1, "kind": "timeout", "times": 1}]})
    req("POST", SOURCE, "/admin/publish")
    _, r = req("POST", COLLECTOR, "/collections", {"hour": HOUR})
    check("v4 重试后成功", r["status"] == "ok" and r["confirmed_to"] == 4, json.dumps(r))
    check("v4 第 2 次尝试才成功", r["attempts"].get("4") == 2, json.dumps(r["attempts"]))

    print("-- 5. 缺页故障:持续缺页 -> 保留原供数版")
    req("PUT", SOURCE, "/admin/faults",
        {"faults": [{"version": 5, "page": 1, "kind": "missing", "times": -1}]})
    req("POST", SOURCE, "/admin/publish")
    _, r = req("POST", COLLECTOR, "/collections", {"hour": HOUR})
    check("v5 缺页采集失败", r["status"] == "failed" and "missing page" in (r["error"] or ""),
          json.dumps(r))
    check("指针保留在 v4", r["current_version"] == 4, json.dumps(r))
    _, cur = req("GET", COLLECTOR, f"/hours/{HOUR}/data")
    check("默认查询仍是 v4(未暴露半版)", cur["version"] == 4 and cur["is_current"])
    req("GET", COLLECTOR, f"/hours/{HOUR}/data?version=5", expect=404)
    check("v5 不可查", True)
    req("DELETE", SOURCE, "/admin/faults")
    _, r = req("POST", COLLECTOR, "/collections", {"hour": HOUR})
    check("清障后补齐 v5", r["status"] == "ok" and r["confirmed_to"] == 5, json.dumps(r))

    print("-- 6. 同版同站冲突:保留原供数版")
    req("PUT", SOURCE, "/admin/faults",
        {"faults": [{"version": 6, "page": 1, "kind": "conflict", "times": -1}]})
    req("POST", SOURCE, "/admin/publish")
    _, r = req("POST", COLLECTOR, "/collections", {"hour": HOUR})
    check("v6 冲突采集失败", r["status"] == "failed" and "conflict" in (r["error"] or "").lower(),
          json.dumps(r))
    check("指针保留在 v5", r["current_version"] == 5, json.dumps(r))
    req("GET", COLLECTOR, f"/hours/{HOUR}/data?version=6", expect=404)
    check("v6 不可查", True)
    req("DELETE", SOURCE, "/admin/faults")
    _, r = req("POST", COLLECTOR, "/collections", {"hour": HOUR})
    check("清障后补齐 v6", r["status"] == "ok" and r["confirmed_to"] == 6, json.dumps(r))

    print("-- 7. 已确认版完整性:逐版快照与版本登记一致")
    _, vers = req("GET", COLLECTOR, f"/hours/{HOUR}/versions")
    check("确认版 1..6 连续", [v["version"] for v in vers["versions"]] == [1, 2, 3, 4, 5, 6])
    for v in vers["versions"]:
        snap = stations(COLLECTOR, HOUR, v["version"])
        check(f"v{v['version']} 快照站数={v['station_count']}", len(snap) == v["station_count"])

    print("-- 8. 源站回退保护")
    req("POST", SOURCE, "/admin/reset", {"station_count": 20, "seed": 1})
    _, r = req("POST", COLLECTOR, "/collections", {"hour": HOUR})
    check("源站回退被拒绝", r["status"] == "source_regressed" and r["current_version"] == 6,
          json.dumps(r))
    check("回退后历史版仍可查", len(stations(COLLECTOR, HOUR, 6)) > 0)

    print(f"== smoke 全部通过 ({_checks} 项检查)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
