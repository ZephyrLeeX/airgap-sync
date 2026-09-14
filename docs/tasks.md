# Airgap Sync 开发任务 V1

## 开发原则

开发顺序以：

**先证明快照生成正确 → 再打通传输 → 再打通目标端 → 最后补运维**

为原则。

V1 统一采用 Full Snapshot 全量快照同步。旧设计中的 keyed / row_multiset / 逻辑唯一键 / 行 Hash 状态 / INSERT-UPDATE-DELETE Diff / 增量同步已从 V1 方案中移除，相关任务已删除。

---

# Phase 1：项目基础 ✅ 已完成

* T001 项目初始化（Python 项目、配置、日志、MySQL 只读连接、SQLite 状态库）。
* T002 表配置模型（Phase 2 起简化为 `name` + `enabled`，`mode` / `key` 已删除）。

提交：`90e1274`、`371aa68`。

---

# Phase 2：Full Snapshot Source 核心 ← 当前阶段

## T101 表配置简化

* `TableConfig` 只保留 `name` / `enabled`；
* 删除 `TableMode`、key 校验、重复 key 校验、`KEY_COLUMN_NOT_FOUND`；
* 旧配置（`mode` / `key`）因 `extra=forbid` 明确失败；
* 新增 `snapshot.fetch_size`、`chunk.max_rows`、`chunk.max_uncompressed_bytes`、`chunk.compression_level` 配置。

### 验收

* 新配置可加载；旧 `mode`/`key` 配置明确报错；
* example config / config tests / CLI 输出 / README 同步更新。

## T102 Row Codec

公共行编码模块：

* JSON Lines + 类型 tag（`$decimal` / `$bytes` / `$datetime` / `$date` / `$time` / `$timedelta` / `$float`）；
* NULL ≠ ""、0 ≠ "0"、Decimal 不转 float、bytes 走 Base64；
* 编码 deterministic；Source 编码与 Destination 解码共用同一模块。

### 验收

全部类型 round-trip；相同数据编码结果 byte 级一致。

## T103 Snapshot Multiset Digest

order-independent、multiplicity-sensitive 的验证摘要：

* 双 domain SHA256 + 模 2^256 加法累加；
* 输出 `row_count` + `digest_a` + `digest_b`；
* O(1) 内存。

### 验收

* `A B C` 与 `C A B` 结果相同；
* `A A B` 与 `A B` 结果不同；
* 任意一行变化结果不同；
* 空表有确定结果。

## T104 MySQL 专用安全 API

* `quote_identifier`（反引号 + 内部反引号翻倍）；
* `get_create_table(table)`（专用 SHOW CREATE TABLE，不开放任意 SQL）；
* `stream_table(table, fetch_size)`（SSCursor + fetchmany 的 `SELECT *`）；
* 表类型检查（仅 BASE TABLE，VIEW 报 `UNSUPPORTED_TABLE_TYPE`）。

### 验收

identifier quoting 覆盖特殊字符与反引号；双层只读保护不放宽。

## T105 Chunk Writer

* 行数 / 未压缩字节双阈值；
* 单行超限允许单行 Chunk；
* zstd 压缩（可配置级别）；
* `.part` → fsync → 原子 rename；
* SHA256 针对最终压缩文件字节。

### 验收

阈值切分正确；`.part` 不被视为完成；解压后 JSONL 内容与 row count 正确。

## T106 Snapshot Runner

* Run ID 生成（UTC timestamp + random suffix）；
* 目录 `<data_dir>/outbox/<table>/<run_id>/`；
* DDL 扫描前后一致 → COMPLETED（写 schema.sql + manifest）；
* DDL 变化 → FAILED（`SCHEMA_CHANGED_DURING_SNAPSHOT`，无 manifest）；
* 扫描异常 → FAILED；
* 失败不推进 `current_run_id`；
* SQLite schema v2（删除 `mode`，简单 v1→v2 迁移）。

## T107 `source snapshot` CLI

```bash
airgap-sync source snapshot --config config.yaml --table TABLE_NAME
```

输出 Run / Rows / Chunks / Raw size / Compressed / Status / Output 摘要，不输出业务数据。

### 验收

本地生成完整 Run 目录：

```text
outbox/<table>/<run_id>/schema.sql + chunk-*.jsonl.zst + manifest.json
```

## T102a 测试

单元测试（配置、quoting、codec、digest、chunk writer、manifest、runner）+ 真实 MySQL 集成测试（无主键表、NULL、重复行、Decimal、datetime、binary）。

---

# Phase 3：HTTP 上传 + Source 磁盘流水线

* T201 HTTP Uploader（超时、重试、网络异常恢复；服务器确认可靠接收后才允许删除本地 Chunk）；
* T202 流水线生成与上传（Chunk 生成 → 上传 → 删除 → 继续扫描，不要求整个 Run 先全部落盘）；
* T203 磁盘保护（剩余空间检测、spool 限制、DISK_PRESSURE 状态）。

---

# Phase 4：Destination 接收 + staging 导入

* T301 incoming 文件管理（incoming / pending / processing / applied / failed）；
* T302 文件与 Manifest 校验（SHA256、缺失文件、顺序）；
* T303 目标同步元数据（runs / chunks / table_versions）；
* T304 基于 schema.sql 创建 staging 表并按 manifest columns 完整导入；
* T305 Chunk 幂等（重复文件不重复写入）。

---

# Phase 5：一致性验证 + 正式表切换

* T401 Destination 复算 Multiset Digest 并与 Manifest 比对（VERIFIED / MISMATCH）；
* T402 验证通过后原子替换正式表；
* T403 MISMATCH 提示人工重新生成该表快照。

---

# Phase 6：调度、状态、异常恢复、大表压力测试

* T501 Source 表级调度（同表不并发，默认单大表串行）；
* T502 Destination Worker（自动发现文件、按 Run 处理）；
* T503 状态命令（表 / Run / 状态 / Chunk / 错误）；
* T504 大表性能测试（700 万行：内存、耗时、压缩比、磁盘占用）；
* T505 故障测试（杀进程、写一半的文件、上传失败、MySQL 中断、验证失败；不允许静默漏数据）。

---

# 最小可用版本里程碑

## Milestone 1（Phase 2 完成）

> 源表 → 流式扫描 → zstd Chunk + schema + manifest 本地生成

## Milestone 2（Phase 3 完成）

> Chunk → HTTP → 确认 → 删除，Source 磁盘受控

## Milestone 3（Phase 4-5 完成）

> 文件 → Destination staging → 验证 → 替换正式表 → VERIFIED

## Milestone 4（Phase 6 完成）

> 自动运行 + 压力测试 + 故障恢复，进入正式部署

---

# 推荐实际开发顺序

第一轮开发不要碰全部表。先选一张小表贯通：

```text
Source Snapshot → HTTP → FTP → Destination staging → 验证 → 替换
```

完整链路跑通后，再接 700 万级大表调优参数。
