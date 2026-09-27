# Airgap Sync Monitoring V1 设计需求与讨论纪要

> 用途：作为后续 Codex / Claude Code 开发 Monitoring V1 的长期上下文文档。  
> 当前正式基线：`381cef1c4a8713289aad0ec98582f785e0c4602f`  
> Commit：`Fix Windows deployment and MySQL 5.6 compatibility`

---

## 1. 当前系统状态

Airgap Sync 当前核心同步链路已经基本稳定，并经过真实环境验证。

现有生产链路：

```text
Source MySQL
→ Windows Source Agent
→ HTTP Relay
→ Relay FTP Server
→ 隔离侧外部 FTP Client
→ CentOS Destination incoming
→ Destination Agent
→ Target MySQL / RDS
```

真实环境已经验证：

- Windows Server 2019 Source；
- CentOS 7.9 Destination；
- Source MySQL 5.7；
- Destination 阿里私有云 RDS MySQL 5.6.46；
- 611 万行大表完整同步成功；
- 700 多万行大表完整同步成功；
- 其余表也已完成同步；
- 系统已经连续整晚自动运行；
- Source Worker 已通过 Windows Task Scheduler 后台长期运行；
- Destination Worker 使用 systemd 后台运行；
- `381cef1` 作为当前新的正式基线。

Monitoring 后续开发必须尽量避免扰动已经稳定的同步核心。

---

## 2. Monitoring V1 核心目标

Monitoring V1 不是为了做复杂监控平台，而是为了让运维人员在 Destination 一侧打开网页，就能判断整个 Airgap Sync 是否健康。

必须能够回答：

```text
Source 机器还活着吗？
Source Worker 还在运行吗？
Source 磁盘快满了吗？
Destination Worker 是否正常？
Destination 本机磁盘是否正常？
RDS 是否可连接？
最近一次同步是否成功？
当前有没有正在同步的表？
哪张表已经很久没有更新？
当前有没有 FAILED / MISMATCH / DISK_PRESSURE？
下一轮什么时候运行？
当前同步慢在哪一个阶段？
```

重点：

```text
可见性
故障发现
磁盘预警
数据新鲜度
同步过程诊断
```

---

# 3. 顶层设计原则

## 3.1 严格单向

必须继续保持：

```text
Source → Destination
```

不允许为了 Monitoring 引入：

```text
Destination → Source
```

Destination Web 不能主动请求 Windows Source。

Source 所有监控信息必须仍然通过现有单向隔离链路送到 Destination。

## 3.2 Monitoring 不能影响业务同步

必须保证：

```text
Monitoring failure != Sync failure
```

以下情况都不能导致业务失败：

```text
Heartbeat 上传失败
Relay 返回 429
Monitor JSON 丢失
monitor.db 写失败
Web 挂掉
```

Telemetry 必须是 BEST EFFORT。

优先级：

```text
业务 Snapshot / Manifest   最高
关键 Monitoring Event      中
Heartbeat                  最低
```

## 3.3 尽量不侵入同步核心

Monitoring 主要读取：

```text
Source SQLite meta.db
Destination airgap_sync_meta
Destination incoming
系统状态
```

不要深度修改：

```text
SnapshotRunner
DeliveryRunner
CycleRunner
DestinationProcessor
Verification
Promotion
```

第二阶段确实需要实时进度时，再做最小 instrumentation。

## 3.4 V1 Web 只读

V1 只允许：

```text
查看
查询
诊断
告警
历史趋势
```

不做：

```text
Retry Run
Force Promote
Delete Run
Clean Now
修改 YAML
远程控制 Source
```

---

# 4. 推荐总体架构

```text
                    单向网络
Windows Source
┌────────────────────────────────┐
│ Airgap Sync Source Worker      │
│                                │
│ Source SQLite meta.db          │
│   cycles / runs / artifacts    │
│                                │
│ Source Monitor Reporter        │
│   独立计划任务                 │
└───────────────┬────────────────┘
                │ telemetry JSON
                ▼
            HTTP Relay
                │
                ▼
             FTP Relay
                │
                ▼
CentOS Destination
┌──────────────────────────────────────────┐
│ telemetry incoming                       │
│       ↓                                  │
│ Monitor Ingest                           │
│       ↓                                  │
│ /var/lib/airgap-sync-monitor/monitor.db  │
│                                          │
│ Destination Agent → airgap_sync_meta     │
│                                          │
│ Monitor Web + Alert Engine               │
│   ├─ monitor.db                          │
│   ├─ airgap_sync_meta                    │
│   ├─ incoming                            │
│   └─ Destination local system            │
└───────────────────┬──────────────────────┘
                    ▼
                  Browser
```

---

# 5. Source Monitoring 运行模型

Source Monitor 不要直接塞进 Source Worker。

推荐：

```text
Airgap Sync Source Worker
→ 开机启动
→ 常驻运行
→ 执行业务 Cycle

Airgap Sync Source Monitor
→ Windows Task Scheduler
→ 每 5 分钟运行一次
→ one-shot 后退出
```

好处：

```text
Host ONLINE
Monitor OK
Worker DOWN
Disk OK
```

可以清楚区分机器宕机、Worker 宕机、Monitor 自身问题。

建议新增命令：

```powershell
airgap-sync source monitor-report --config <source.yaml>
```

执行流程：

```text
读取 Source SQLite
→ 读取 Windows 系统状态
→ 检查 Source Worker
→ 生成 telemetry JSON
→ best-effort 上传
→ 退出
```

推荐 Task Scheduler：

```text
Task name: Airgap Sync Source Monitor
Account: SYSTEM
RunLevel: Highest
Trigger: Every 5 minutes
MultipleInstances: IgnoreNew
Execution: one-shot
```

---

# 6. Source Telemetry 协议

建议文件名：

```text
airgap-monitor-v1--source-01--20260917T091500Z--a82c31.json
```

Telemetry 示例：

```json
{
  "schema_version": 1,
  "node_id": "source-01",
  "role": "SOURCE",
  "captured_at": "2026-09-17T09:15:00Z",
  "agent_version": "0.1.0-381cef1",
  "host": {
    "hostname": "SOURCE-WIN01",
    "os": "Windows Server 2019",
    "uptime_seconds": 238283,
    "boot_time": "2026-09-14T15:03:00Z"
  },
  "worker": {
    "status": "RUNNING",
    "pid": 1234,
    "task_status": "Running",
    "schedule_enabled": true,
    "next_action_at": "2026-09-24T03:51:22Z"
  },
  "cycle": {
    "cycle_id": "cycle-20260917T...",
    "status": "RUNNING",
    "tables_total": 18,
    "tables_delivered": 11,
    "tables_failed": 0
  },
  "current_run": {
    "run_id": "20260917T...",
    "table": "std_xxx",
    "status": "UPLOADING",
    "rows_scanned": 4210000,
    "chunks_created": 423,
    "chunks_uploaded": 417,
    "pending_bytes": 382993281,
    "raw_bytes": 28482993482,
    "compressed_bytes": 3282389231,
    "elapsed_seconds": 912
  },
  "filesystems": [
    {
      "mount": "C:",
      "total_bytes": 128000000000,
      "free_bytes": 43000000000,
      "free_percent": 33.6
    },
    {
      "mount": "D:",
      "total_bytes": 214748364800,
      "free_bytes": 41000000000,
      "free_percent": 19.1
    }
  ],
  "managed_storage": {
    "outbox_bytes": 4900000000,
    "failed_runs_bytes": 9200000000,
    "backups_bytes": 6100000000,
    "logs_bytes": 820000000
  },
  "system": {
    "cpu_percent": 13.2,
    "memory_total_bytes": 34359738368,
    "memory_available_bytes": 19327352832,
    "memory_used_percent": 43.8
  },
  "connectivity": {
    "source_mysql": "OK",
    "relay": "OK"
  },
  "last_error": null
}
```

绝对禁止包含：

```text
MySQL password
Relay Token
环境变量完整值
业务行数据
SQL 正文
API Key
```

---

# 7. Source 状态数据来源

现有 Source SQLite 已可提供：

```text
sync_cycles
cycle_tables
sync_runs
run_artifacts
table_state
```

包括：

```text
Cycle status
started/completed
next_attempt_at
Cycle table status
attempts
last_run_id
last_error
delivered_at

Run status
created_at
snapshot_completed_at
delivered_at
row_count
chunk_count
raw_bytes
compressed_bytes
last_error

Artifact upload_status
attempts
request_id
uploaded_at
cleanup_error
```

Monitoring V1 优先读取现有状态，不复制业务状态机。

---

# 8. Destination Monitoring 数据来源

Destination Web 可以读取：

```text
airgap_sync_meta
Destination incoming
本地系统状态
monitor.db
```

现有 Destination 已经有：

```text
latest VERIFIED
FAILED / MISMATCH
cleanup pending
Run metadata
chunk metadata
row_count
actual_row_count
chunk_count
import_started_at
import_completed_at
digest_verified_at
applied_at
last_error
```

现有 statistics 继续使用：

```text
当前总行数
本次净增
月度净增
```

统一使用“净增”，不用“新增”。

---

# 9. 指标优先级

```text
1. Heartbeat
2. Worker 状态
3. Disk
4. 数据新鲜度
5. FAILED / MISMATCH / DISK_PRESSURE
6. 当前同步状态
7. Relay / RDS 连通性
8. 性能
9. CPU / Memory
```

---

# 10. Source / Destination 系统监控

Source Windows 建议采集：

```text
Host online / heartbeat
Hostname
OS version
Agent version
Uptime
Last boot
Task Scheduler status
Worker process status
Worker PID
CPU
Memory
Pagefile
Filesystem total/free/%
Managed directory usage
Source MySQL connectivity
Relay health
Last monitor sample
```

Destination CentOS 建议采集：

```text
Hostname
OS version
Agent version
Uptime
Last boot
Destination Worker status
Destination Worker PID
Monitor Web status
CPU
Memory
Swap
Filesystem
incoming usage
logs usage
old release usage
RDS connectivity
MySQL version
Target database
Metadata schema version
incoming candidate count
cleanup pending
```

---

# 11. 磁盘监控

磁盘是最高优先级之一。

同时判断：

```text
Free %
Free bytes
```

推荐默认：

```yaml
monitoring:
  disk:
    warning:
      free_percent: 20
      free_bytes: 50GiB
    critical:
      free_percent: 10
      free_bytes: 20GiB
    emergency:
      free_percent: 5
      free_bytes: 10GiB
```

规则：

```text
任意阈值命中 → 状态升级
```

Source 不要硬编码 C:/D:，自动发现：

```text
Windows system volume
InstallRoot volume
ConfigRoot volume
data_dir volume
TEMP volume
```

按 volume 去重。

Destination 自动识别：

```text
/
incoming_dir filesystem
/opt/airgap-sync filesystem
monitor.db filesystem
log filesystem
```

按 device/filesystem 去重。

---

# 12. Managed Storage Usage

Source：

```text
outbox
failed run files
backups
logs
old releases
pending spool
```

Destination：

```text
incoming
cleanup pending files
backups
logs
old releases
monitor telemetry incoming
monitor.db
```

页面示例：

```text
Source D: 38 GiB free   WARNING

Pending spool       4.8 GiB
Failed runs         9.2 GiB
Backups             6.1 GiB
Logs              820 MiB
Old releases        2.7 GiB
```

目录大小不必每 5 分钟递归扫描。

推荐：

```text
heartbeat: 5m
managed storage scan: 30～60m
```

---

# 13. 数据新鲜度

每张表展示：

```text
latest VERIFIED
row_count
本次净增
月度净增
data age
最近同步结果
下一次预计同步
```

数据新鲜度比 CPU 使用率更重要。

---

# 14. Overview 首页

第一屏必须回答：

```text
系统健康吗？
Source 在线吗？
Destination 在线吗？
磁盘安全吗？
同步有没有失败？
数据是否过期？
现在正在干什么？
```

推荐：

```text
┌──────────────────────────────────────────────────────────────┐
│ Airgap Sync                 Overall ● HEALTHY                │
│ Latest verified 04:51       18/18 tables healthy            │
└──────────────────────────────────────────────────────────────┘

┌───────────────────────┐   ┌───────────────────────┐
│ SOURCE WINDOWS        │   │ DESTINATION CENTOS    │
│ ● ONLINE              │   │ ● ONLINE              │
│ Worker ● RUNNING      │   │ Worker ● RUNNING      │
│ Heartbeat 2m ago      │   │ RDS ● CONNECTED       │
│ D: 38 GiB free ⚠      │   │ / 312 GiB free        │
│ Version 381cef1       │   │ Version 381cef1       │
└───────────────────────┘   └───────────────────────┘

Data age max 6d 2h
Active run none
Alerts 1 warning

⚠ Source D: free space below 50 GiB
```

---

# 15. 页面结构

V1 控制在 5 个页面：

```text
Overview
Tables
Runs
System
Alerts
```

## Overview

```text
Overall status
Source heartbeat
Destination status
Source disk
Destination disk
latest completed cycle
current active run
FAILED/MISMATCH count
max data age
open alerts
version match
```

## Tables

```text
table
latest VERIFIED
row_count
本次净增
月度净增
data age
latest run status
latest run duration
latest error
```

## Runs

```text
Run ID
table
Source status
Destination status
rows
chunks
raw bytes
compressed bytes
created
delivered
imported
verified
applied
end-to-end duration
error
```

## System

Source 和 Destination 的系统、Worker、磁盘、连接状态。

## Alerts

```text
OPEN
RECOVERED
```

未来可增加 ACKNOWLEDGED。

---

# 16. 端到端 Run Timeline

示例：

```text
03:12:20  Source Snapshot Started
03:34:11  Source Snapshot Completed
03:34:11  Upload Started
03:42:37  Manifest Delivered

03:47:20  Destination First Seen
03:47:22  Import Started
04:18:03  Import Completed
04:18:03  Verification Started
04:29:41  Verification Passed
04:29:42  Promoted
04:29:42  VERIFIED
```

自动计算：

```text
Snapshot       21m 51s
Upload          8m 26s
Transport       4m 43s
Import         30m 41s
Verify         11m 38s
End-to-end      1h 17m
```

用于快速定位瓶颈：

```text
Source MySQL
Compression
HTTP Relay
FTP
RDS INSERT
RDS VERIFY
```

---

# 17. 大表运行进度

对于 600～700 万行表：

```text
Source
Status              DELIVERED
Rows                6,112,200
Chunks              731
Uploaded            731 / 731
Raw                 45.6 GiB
Compressed           5.2 GiB
Compression ratio    11.5%
Duration             21m 46s

Destination
Status              VERIFYING
Imported chunks     731 / 731
Imported rows       6,112,200
Import duration      38m 12s
Verification        RUNNING
Elapsed             11m 20s
```

不要为了 UI 额外执行 Source `COUNT(*)`。

Source 进行中优先显示：

```text
rows scanned
chunks generated
chunks uploaded
bytes
speed
elapsed
```

如果用上一次 VERIFIED row_count 估算百分比，必须标 `Estimated`。

---

# 18. Heartbeat

推荐：

```text
interval: 5m
warning_after: 10m
critical_after: 20m
```

例如：

```text
2m ago   HEALTHY
12m ago  WARNING
27m ago  CRITICAL
```

业务周期是 7 天，因此不能用最近同步时间判断 Source 是否在线。

---

# 19. Worker 状态

Source Monitor 同时检查：

```text
Task Scheduler
真实 airgap-sync worker process
```

例如：

```text
Scheduled Task  Running
Process         airgap-sync.exe
PID             1234
```

Host 在线但 Worker 不运行：

```text
CRITICAL
Source worker is not running
```

---

# 20. Version Monitoring

显示：

```text
Source version       381cef1
Destination version  381cef1
```

不一致时：

```text
VERSION MISMATCH
```

至少 WARNING。

---

# 21. RDS 监控边界

Destination 本机 filesystem 可以直接监控。

RDS 第一版只展示：

```text
CONNECTED / DISCONNECTED
MySQL version
Target database
Metadata schema version
Query health
```

除非以后有云 API / 权限，否则不要伪造 RDS disk free。

---

# 22. Monitoring SQLite

建议独立：

```text
/var/lib/airgap-sync-monitor/monitor.db
```

不要把高频 heartbeat 大量写入 `airgap_sync_meta`。

理由：

```text
Monitoring 挂 → 不影响业务 metadata
RDS 挂 → Web 仍能显示 Source heartbeat / Destination 本机状态
Heartbeat 高频 → 不增加 RDS 写压力
```

建议表：

```text
nodes
node_samples
filesystem_samples
managed_storage_samples
sync_events
alerts
```

---

# 23. Schema 建议

## nodes

```text
node_id
role
hostname
first_seen_at
last_seen_at
agent_version
```

role：

```text
SOURCE
DESTINATION
```

## node_samples

```text
id
node_id
captured_at
uptime_seconds
cpu_percent
memory_total_bytes
memory_available_bytes
memory_used_percent
worker_status
worker_pid
mysql_status
relay_status
rds_status
agent_version
payload_version
```

## filesystem_samples

```text
id
node_id
captured_at
device
mount
total_bytes
free_bytes
free_percent
```

## managed_storage_samples

```text
id
node_id
captured_at
category
path
bytes
```

category：

```text
outbox
failed_runs
backups
logs
old_releases
incoming
monitor_incoming
monitor_db
```

## sync_events

事件建议：

```text
CYCLE_STARTED
CYCLE_COMPLETED
CYCLE_RETRY_WAIT
RUN_STARTED
RUN_DELIVERED
RUN_FAILED
DESTINATION_FIRST_SEEN
IMPORT_STARTED
IMPORT_COMPLETED
VERIFY_STARTED
VERIFY_COMPLETED
PROMOTED
VERIFIED
DISK_PRESSURE
```

## alerts

```text
id
fingerprint
node_id
severity
alert_type
status
message
details_json
opened_at
last_seen_at
recovered_at
```

状态：

```text
OPEN
RECOVERED
```

以后可增加 ACKNOWLEDGED。

---

# 24. Alert 状态机

```text
Condition false
→ 无 Alert

Condition true
→ OPEN

持续 true
→ 更新 last_seen_at

Condition false
→ RECOVERED
```

同一问题用稳定 fingerprint，例如：

```text
source-01:disk:D:warning
```

避免每 5 分钟新增一条 Alert。

---

# 25. 推荐 Alert 规则

Source heartbeat：

```text
WARNING > 10m
CRITICAL > 20m
```

Worker：

```text
Host online AND Worker down
→ CRITICAL
```

Disk：

```text
WARNING
free < 20%
OR free < 50GiB

CRITICAL
free < 10%
OR free < 20GiB

EMERGENCY
free < 5%
OR free < 10GiB
```

数据新鲜度：

```text
WARNING 8d
CRITICAL 10d
```

可配置。

Run：

```text
FAILED        CRITICAL
MISMATCH      CRITICAL
DISK_PRESSURE CRITICAL
RETRY_WAIT    WARNING
```

Incoming stale：

```text
完整 Manifest 已出现但 >2h 未处理
→ WARNING
```

Version mismatch：

```text
Source != Destination
→ WARNING
```

---

# 26. 磁盘趋势

保留历史 sample：

```text
24h
7d
30d
```

展示：

```text
Current free        38.2 GiB
24h change          -6.4 GiB
7d average          -3.1 GiB/day
```

可以简单线性估算：

```text
Estimated:
<20 GiB in ~5.8 days
```

必须标 `Estimated`。

---

# 27. Telemetry 生命周期

5 分钟 heartbeat 一年约 10 万文件，因此必须从 V1 定义清理。

Destination：

```text
telemetry received
→ validate
→ insert monitor.db
→ delete telemetry incoming file
```

Relay / FTP 也要确认下载后是否删除远端，或增加 7～30 天 retention。

不能让 Monitoring 自己成为磁盘问题。

---

# 28. Telemetry 上传失败语义

Heartbeat：

```text
失败
→ 本地 warning
→ 丢弃本次 sample
→ 下个 interval 再试
```

不要堆积大量 heartbeat spool。

关键 event 可以 bounded retry，但仍不能影响业务同步。

---

# 29. Monitor Web 技术方案

环境：

```text
CentOS 7
离线
单机
内部低并发
```

推荐：

```text
FastAPI / Starlette
+ Jinja2
+ 原生 JavaScript
+ 本地 CSS
```

要求：

```text
不依赖 CDN
不依赖公网
不需要 Node runtime
静态资源全部打入 Python package
```

页面可每 15～30 秒调用：

```text
/api/dashboard
```

刷新。

---

# 30. Monitor Web 部署

建议独立：

```text
airgap-sync-destination.service
airgap-sync-monitor.service
```

Web 默认：

```text
127.0.0.1:8080
```

由 nginx 反代。

不要与业务 Destination Worker 同进程。

---

# 31. Source 两个任务

最终 Windows：

```text
Airgap Sync Source Worker
→ AtStartup
→ SYSTEM
→ long-running

Airgap Sync Source Monitor
→ Every 5 minutes
→ SYSTEM
→ one-shot
```

---

# 32. Destination Monitor

V1 可以一个 Monitor service 同时负责：

```text
telemetry ingest
alert evaluation
Web
```

业务 `destination worker` 必须保持独立。

---

# 33. UI 统一同步阶段

建议：

```text
SOURCE_SCAN
SOURCE_COMPRESS
SOURCE_UPLOAD
SOURCE_DELIVERED
TRANSPORT_WAIT
DEST_IMPORT
DEST_VERIFY
DEST_PROMOTE
VERIFIED
```

Source 无法知道 Destination VERIFIED。

最终端到端状态由 Destination 通过：

```text
Source telemetry + Destination metadata
```

关联展示。

---

# 34. Monitoring V1 分阶段

## M1 — Destination Read-only Dashboard

```text
读取 airgap_sync_meta
读取 incoming
读取 Destination 系统状态
Overview
Tables
Runs
Destination System
Problems
```

完全不动 Source 和 transport。

## M2 — Source Telemetry Reporter

新增：

```text
source monitor-report
Source heartbeat
Worker
Disk
System
Cycle
Current Run
Managed Storage
Version
```

## M3 — Telemetry Ingest + monitor.db

```text
telemetry validation
idempotent ingest
monitor.db
retention
source system history
```

## M4 — Alert Engine

```text
disk
heartbeat
worker
freshness
FAILED/MISMATCH
version mismatch
incoming stale
```

## M5 — Run Timeline + Performance

```text
Source timestamps
Destination timestamps
End-to-end timeline
stage duration
throughput
compression ratio
```

## M6 — Enhanced Real-time Progress

如有必要，再做最小 instrumentation：

```text
rows_scanned
verify_rows_read
current throughput
```

必须单独 review。

---

# 35. V1 Non-Goals

明确不做：

```text
Prometheus
Grafana
Redis
MQ
Kafka
Elasticsearch
远程控制 Source
Destination → Source API
网页重试
网页删除
网页清理
网页修改配置
自动删除业务数据
复杂权限系统
复杂多用户系统
公网 SaaS
机器学习预测
```

保持轻量。

---

# 36. 配置草案

```yaml
monitoring:
  enabled: true
  node_id: source-01

  heartbeat:
    interval: 5m
    warning_after: 10m
    critical_after: 20m

  system:
    collect_cpu: true
    collect_memory: true
    collect_uptime: true

  disk:
    warning:
      free_percent: 20
      free_bytes: 50GiB
    critical:
      free_percent: 10
      free_bytes: 20GiB
    emergency:
      free_percent: 5
      free_bytes: 10GiB

  managed_storage:
    enabled: true
    interval: 1h

  freshness:
    warning_after: 8d
    critical_after: 10d

  telemetry:
    schema_version: 1
    best_effort: true
```

后续可拆分 Source / Destination 配置。

---

# 37. V1 验收标准

Destination 浏览器中能够看到：

## Source

```text
ONLINE / OFFLINE
最后 heartbeat
hostname
OS
Agent version
Task Scheduler Worker
Worker process
CPU
Memory
C:/D: 或实际 volume
managed storage
Source MySQL health
Relay health
最新 Cycle
next action
当前 Run
最近错误
```

## Destination

```text
CentOS 状态
Destination Worker
Monitor Service
CPU
Memory
filesystem
incoming
RDS health
Agent version
```

## Tables

```text
latest VERIFIED
row_count
本次净增
月度净增
data age
health
```

## Runs

```text
Run status
Rows
Chunks
Raw/Compressed size
Source timestamps
Destination timestamps
End-to-end duration
Last error
```

## Alerts

正确产生和恢复：

```text
Source heartbeat lost
Worker down
Disk warning
Disk critical
FAILED
MISMATCH
DISK_PRESSURE
Freshness warning
Version mismatch
```

---

# 38. 安全与可靠性

必须：

```text
Monitoring 与同步核心隔离
Monitoring failure 不影响 Sync
严格单向
Telemetry 不含秘密
JSON schema validation
原子文件写入 / rename
monitor.db 使用事务
网页只读
Web 默认绑定 localhost
内部时间 UTC
页面按配置时区展示
```

---

# 39. 重点测试

必须覆盖：

```text
heartbeat missing
heartbeat out of order
duplicate telemetry
malformed telemetry
unknown schema_version
disk thresholds
alert open
alert remains open
alert recovered
version mismatch
worker down
Source offline
monitor.db restart recovery
telemetry file cleanup
telemetry upload failure 不影响业务 Run
no large telemetry backlog
same telemetry re-ingest idempotency
filesystem dedup
managed storage scan failure
RDS unavailable
Source DB unavailable
```

---

# 40. 当前正式基线

所有 Monitoring 开发基于：

```text
381cef1c4a8713289aad0ec98582f785e0c4602f
```

已确认稳定能力：

```text
Full Snapshot
Source Worker
Destination Worker
Windows Task Scheduler
CentOS systemd
Source SQLite state
Destination metadata
Statistics
Retry / crash recovery
Windows external Python
MySQL 5.6 compatibility
真实 611 万行同步
真实 700 多万行同步
整晚无人值守运行
```

Monitoring 不得破坏这些能力。

---

# 41. Codex 工作方式

继续分阶段：

```text
设计
→ 实现
→ 测试
→ 独立 commit
→ Review
→ 下一 Phase
```

不要把：

```text
Reporter
Ingest
Web
Alerts
实时进度
```

一次性做成一个巨大 commit。

第一阶段优先：

```text
M1 Destination Read-only Dashboard
```

---

# 42. 最终产品定位

Monitoring Web 的定位是：

> Airgap Sync 运维控制台。

它首先回答：

```text
同步系统健康吗？
数据够新吗？
Source 无人值守机器还好吗？
磁盘快满了吗？
最近一次失败是什么？
当前 Run 在哪个阶段？
为什么这一轮比上一轮慢？
```

优先级始终保持：

```text
Heartbeat
Worker
Disk
Freshness
Sync Status
Errors
RDS / Relay
Performance
CPU / Memory
```
