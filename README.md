# PM2.5 修订采集与查询平台

市环境监测中心会补正各站点的小时 PM2.5,分析作业需按原版复算。本平台包含:

- **source**(模拟源站,FastAPI):分页发布修订。首版(v1)含全量站点,此后每版只列
  新增、改值(`upsert`)和删除(`delete`),响应带连续版号 `version`、页号 `page`、
  总页数 `total_pages`。管理接口可发布/切换修订、注入缺页、超时、同版同站冲突故障。
- **collector**(采集与查询 API,FastAPI + SQLite):采集某 UTC 小时前先固定源站当前版,
  再逐版补齐;每个确认版的完整快照均可查询;数据与当前版指针在同一事务生效。

## 快速开始

```bash
docker compose up --build
```

- 采集/查询 API: http://localhost:8000 (文档 `/docs`)
- 模拟源站: http://localhost:8001 (文档 `/docs`)

端到端冒烟(35+ 项断言,覆盖缺页/超时/冲突/回退):

```bash
python3 scripts/smoke.py
```

## 典型流程

```bash
# 1. 采集某 UTC 小时(先固定源站当前版,再逐版补齐)
curl -X POST localhost:8000/collections -H 'Content-Type: application/json' \
     -d '{"hour": "2026-11-18T10"}'

# 2. 源站发布新修订(可切换修订;也可带 {"ops":[...]} 显式指定)
curl -X POST localhost:8001/admin/publish

# 3. 再次采集,逐版补齐到新固定版
curl -X POST localhost:8000/collections -H 'Content-Type: application/json' \
     -d '{"hour": "2026-11-18T10"}'

# 4. 查询:当前版(默认)或任一已确认版当时的完整数据
curl localhost:8000/hours/2026-11-18T10/data
curl 'localhost:8000/hours/2026-11-18T10/data?version=1'
curl 'localhost:8000/hours/2026-11-18T10/data?version=3&station_id=S07'
curl localhost:8000/hours/2026-11-18T10/versions
```

## 故障注入演示(缺页 / 超时 / 同版同站冲突)

```bash
# 下一版第 1 页:超时 1 次(重试可成功,且不混入上次残页)
curl -X PUT localhost:8001/admin/faults -H 'Content-Type: application/json' -d \
  '{"faults":[{"version":2,"page":1,"kind":"timeout","times":1}]}'

# kind 还可取 missing(缺页 404)、conflict(同版同站不同值);times=-1 表示持续生效
curl -X DELETE localhost:8001/admin/faults   # 清除全部故障
```

采集端对每版最多重试 `MAX_ATTEMPTS` 次;仍失败则**保留原供数版**(当前版指针不动),
该小时状态记为 `failed`,清除故障后再次采集即可继续补齐。

## 需求映射

| 需求 | 实现 |
| --- | --- |
| 先固定源站当前版,再逐版补齐 | `collector/app/service.py::_collect`:开头一次 `GET /api/revisions/current` 固定目标,之后 `prev+1..pinned` 逐版抓取 |
| 缺页/超时/同版同站冲突时保留原供数版 | 错误只更新 `hours.status/last_error`,`current_version` 指针不动;失败版及后续版不确认 |
| 重试不能混入残页 | 页面只存内存、从不落库;每次重试从第 1 页整版重取(`_fetch_version`) |
| 已确认版均可查当时完整数据 | 每版确认时物化全量快照到 `entries`,`GET /hours/{hour}/data?version=N` |
| 数据与当前版指针一同生效 | 单事务写入 `versions` + `entries` + `hours.current_version`(`db.py::tx`) |
| 失败或重启不得暴露半版 | 事务原子提交;启动时仅复位 `collecting` 状态元信息;SQLite WAL + 数据卷持久化 |

## API 一览

**collector(8000)**

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/collections` | 采集某 UTC 小时,body `{"hour":"YYYY-MM-DDTHH"}`;采集中返回 409 |
| GET | `/collections/{hour}` | 该小时采集状态/最近错误 |
| GET | `/hours` | 全部小时及当前版指针 |
| GET | `/hours/{hour}/versions` | 已确认版列表 |
| GET | `/hours/{hour}/data` | 某版完整数据;`version` 缺省取当前版,`station_id` 可单站过滤 |

**source(8001)**

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/revisions/current` | 当前版号 |
| GET | `/api/revisions` | 全部修订元信息 |
| GET | `/api/revisions/{v}/pages/{p}` | 某版某页(`version`/`page`/`total_pages`/`items`) |
| POST | `/admin/publish` | 发布下一修订(缺省自动生成确定性增量,可传 `ops`) |
| POST | `/admin/reset` | 重置回首版(可指定站点数与种子) |
| GET/PUT/DELETE | `/admin/faults` | 故障注入: `missing`/`timeout`/`conflict`,`times` 控制触发次数 |
| GET | `/admin/state` | 源站内部状态 |

## 本地开发(不用 Docker)

```bash
pip install -r source/requirements.txt -r collector/requirements.txt
(cd source && uvicorn app.main:app --port 8001) &
(cd collector && SOURCE_BASE_URL=http://localhost:8001 DB_PATH=/tmp/collector.db \
    uvicorn app.main:app --port 8000) &
python3 scripts/smoke.py
```
