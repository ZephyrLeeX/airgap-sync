# 单向隔离网络 MySQL 数据同步系统需求说明

## 1. 项目背景

现有业务需要将源 MySQL 数据库中的指定表数据，同步到隔离网络中的另一套 MySQL 数据库。

现有网络链路如下：

源 MySQL
→ 源端同步程序
→ HTTP 文件上传接口
→ 中转服务器
→ FTP 服务
→ 隔离网络内 FTP 客户端
→ 目的服务器
→ 目标 MySQL

网络为严格单向链路。

隔离网络中的程序只能从 FTP 服务端获取文件，不能向源端或中转端返回同步结果、ACK、状态信息或其他数据。

源端只能确认文件是否已经通过 HTTP 接口可靠上传到中转服务器，无法直接知道目标数据库是否已经完成同步。

---

## 2. 当前环境和限制

### 2.1 源数据库

源数据库为 MySQL。

当前已知：

* MySQL 5.7.35；
* 账号权限受限，不能读取 Binlog。

源业务表不能修改，包括：

* 不增加字段；
* 不修改主键；
* 不增加同步字段；
* 不依赖 Trigger 修改业务表行为。

允许同步程序在自己的环境中建立状态库、状态表或其他辅助数据。

### 2.2 源服务器

源端运行在 Windows Server。

可用磁盘空间约 200GB，因此：

* 不能长期保存大量历史同步文件；
* 不能要求一次生成完整的超大同步包后再上传；
* 同步文件应支持边生成、边上传、成功后释放本地空间。

### 2.3 数据特点

需要同步的表数据量差异较大，部分表可能达到 700 万行以上。

源数据由其他系统维护，我们无法保证：

* 主键一定存在；
* 业务唯一值一直唯一；
* 逻辑键永远非 NULL；
* 更新时间可靠；
* 数据结构永远不变化。

源数据存在以下实际情况：

* 部分表有数据库主键；
* 部分表没有数据库主键；
* 部分表存在重复行；
* 更新时间字段不可靠。

---

# 3. 建设目标

系统需要实现：

1. 将指定源 MySQL 表可靠同步到隔离网络中的目标 MySQL。
2. 支持没有数据库主键的表。
3. 支持重复行、NULL、空字符串等一切源数据形态。
4. 700 万行级别表能够流式处理，不要求整体加载内存。
5. 大量数据必须拆分传输，不能形成一个超大文件或一个超大数据库事务。
6. 程序中断后的恢复以"重新生成完整快照"为单位，简单可靠。
7. 目标端能够自行验证最终数据是否与源端一致。
8. 数据不一致时提供简单的人工修复方式（重新生成该表快照）。

系统不要求实现源端和目标端的实时强一致。

同时业务允许较低的同步频率，例如：

10～15 天同步一次

目标是一套：

**可靠的最终一致性数据同步系统。**

## 3.1 Phase 3：Source 到 HTTP Relay 的可靠交付

`source sync` 必须把 SSCursor 扫描/编码/压缩与一个 HTTP 上传 worker 重叠执行。
Chunk 原子关闭后先把 metadata 写入 SQLite，再入队；Relay 严格确认后先标记
`UPLOADED`，再删除本地 Chunk。积压字节或磁盘可用空间越过配置阈值时直接以
`DISK_PRESSURE` / `UPLOAD_BACKLOG_LIMIT` 结束 Run，不允许无限阻塞 SSCursor。

Relay 是扁平命名空间，远端名称固定为
`airgap-v1--<run_id>--<logical_name>`；logical name 仅允许六位序号 Chunk、
`schema.sql` 和 `manifest.json`。原始表名不得进入 transport filename。

HTTP 合同：`PUT /api/v1/upload/{filename}`，body 为文件原始二进制（禁止
multipart），请求必须携带 Bearer Authorization、`application/octet-stream`、真实
`Content-Length` 和 `X-File-SHA256`。只有 HTTP 201 且 JSON 中 `success=true`、
filename/size/sha256 与本地完全一致、request_id 非空才算确认。409 表示
`REMOTE_FILE_EXISTS_AMBIGUOUS`，不得推断为成功。

扫描完成且 DDL 二次检查通过后等待全部 Chunk 确认，再依次上传 schema 和
manifest；manifest 必须最后提交。`DELIVERED` 仅表示 Relay 已可靠接收，不表示
Destination 已 Apply 或 VERIFIED。

## 3.2 Destination Phase 4 已实现行为

外部 FTP Client 把 Relay 文件下载到 Destination `incoming/`；本项目不实现 FTP 客户端。
Destination 只以 transport manifest 为 Run 候选，等待 manifest 声明的 schema 和全部 Chunk
存在且整组文件稳定，再严格检查 protocol v1、FULL_SNAPSHOT、multiset_digest_v1、run_id、
Chunk 序号/逻辑名/行数、文件 size/SHA256 与 schema 表名。

Destination 不要求表级策略配置。源端新增同步表后，目标端从 `manifest.source.table`、
`manifest.columns` 与 `schema.sql` 自动创建 staging。Phase 4 只把完整数据导入确定命名的
staging 并标记 `STAGED`；正式业务表不被创建、修改、清空或删除，incoming 文件也保留。

Chunk 使用 zstd/Row Codec 流式解码和有限 batch `executemany`，一个 Chunk 是一个 MySQL
事务，staging 数据与 `IMPORTED` metadata 同事务提交。已导入 Chunk 重试时跳过。

## 3.3 后续阶段已确定的行为

Phase 6 的 Source 调度采用 fixed-delay：一个 Cycle 可靠交付完成后等待可配置间隔
（建议默认 `delay_after_success=7d`）再启动下一 Cycle；失败使用独立较短重试间隔。
本阶段不实现 Scheduler。

Phase 5 对 Phase 4 的 staging 从 MySQL 重新读取并复算 digest，验证通过后才原子切换
正式表、删除本地 Snapshot 文件并记录统计。失败时保留文件和上一版正式表。

Phase 6 的 Destination Worker 在完整 Run 到达后立即调用 Phase 4 的 discover / validate /
stage-import 核心，不采用每天固定时刻批量导入。

V1 领导统计只提供：总行数、本次净增、月度净增。总行数是最近 VERIFIED Snapshot
的 row_count；本次净增是本次减上次 VERIFIED row_count；月度净增是本月最后一次
减上月最后一次 VERIFIED row_count。它们是“净增”，不是“真实新增记录数”。

---

# 4. V1 同步方案：Full Snapshot 全量快照

## 4.1 为什么使用 Full Snapshot

由于源数据具备第 2.3 节描述的特点，任何依赖业务数据约束的增量方案都必须假设：

* 存在可靠唯一键；
* 或可靠的变更标记；
* 或稳定的表结构。

这些假设在本项目环境中都不成立。

而业务允许较低同步频率，全量传输的数据量增加可以接受。

因此 V1 优先选择：

**简单、可靠、不依赖业务数据约束的 Full Snapshot。**

## 4.2 核心原则

每次同步某张表：

```text
Source MySQL
    ↓
单次流式 SELECT *
    ↓
完整 Snapshot
    ↓
分 Chunk
    ↓
HTTP
    ↓
FTP 单向传输
    ↓
Destination staging
    ↓
完整导入
    ↓
一致性验证
    ↓
替换正式表
```

V1 不再识别：

```text
INSERT
UPDATE
DELETE
```

也不判断：

```text
哪些行发生变化
```

每一个 Run 的含义都是：

> 这是源数据库该表在本次扫描得到的完整版本。

目标端最终需要变成这个完整版本。

V1 只有一种同步类型：

```text
FULL_SNAPSHOT
```

不保留第二套同步方案。

## 4.3 代价

全量快照的代价是每次同步都传输整表数据，传输数据量大于增量方案。

考虑到：

* 同步频率低（10～15 天一次）；
* 链路为中转文件传输，不是实时通道；
* 换来的是对源数据形态零假设；

该代价可以接受。

---

# 5. 快照扫描要求

## 5.1 单次流式查询

一张表使用一条 SELECT 完整读取：

```sql
SELECT * FROM `table`
```

必须使用 server-side / unbuffered cursor 流式读取，客户端分批 fetch。

禁止：

* `fetchall()` 读取百万行；
* `LIMIT ... OFFSET ...` 分页全表。

原因：

* 大 OFFSET 性能差；
* 无主键表无法可靠分页；
* 多条 SELECT 可能观察到不同时间点的数据。

正确模型：

```text
一个 SELECT
    ↓
server-side cursor
    ↓
fetchmany
    ↓
客户端分 Chunk
```

这样一张表的 Snapshot 来自同一个查询。

## 5.2 不要求 ORDER BY

Snapshot 不依赖主键，也不依赖稳定排序：

```sql
SELECT * FROM table
```

即可。

不为了获得稳定顺序而 ORDER BY 某个业务字段，因为：

* 有些表没有唯一键；
* 排序大表成本高；
* 可能产生磁盘临时表；
* 没有必要。

后续的数据验证设计为与行顺序无关。

## 5.3 Schema 一致性保护

Snapshot 必须避免出现"DDL 是旧结构、数据却是新结构"。

最低要求：

* 扫描前读取一次 `SHOW CREATE TABLE`；
* 扫描完成后再次读取 `SHOW CREATE TABLE`；
* 两次 DDL 不一致：本次 Run 失败（`SCHEMA_CHANGED_DURING_SNAPSHOT`），不生成 Manifest。

不尝试自动修复，下一次重新生成即可。

## 5.4 只支持真实表

V1 Snapshot 针对：

```text
BASE TABLE
```

不把 VIEW、PROCEDURE、FUNCTION 当普通数据表处理。

用户配置了 VIEW 时明确失败（`UNSUPPORTED_TABLE_TYPE`），不静默处理。

## 5.5 携带 DDL

每个 Snapshot 必须携带源表 DDL（`SHOW CREATE TABLE` 的实际输出），供目标端自动建表。

---

# 6. Run 和 Chunk

## 6.1 Run

一次同步一张表产生一个 Run。

Run ID 要求：

* 唯一；
* 文件名安全；
* 不依赖数据库主键。

例如：

```text
20260914T213500Z-a1b2c3d4
```

实现采用 UTC timestamp + random suffix，不需要分布式 ID 服务。

每个 Run 的含义是源表的一个完整版本。

## 6.2 Chunk

一个 Run 的数据拆成多个 Chunk 文件：

* Chunk 同时受最大行数和最大未压缩字节数限制，任一达到阈值即封闭；
* 单行本身超过 byte 阈值时允许生成单行超限 Chunk，不丢数据；
* Chunk 文件使用 zstd 压缩；
* 每个最终压缩文件计算 SHA256。

具体阈值是可配置默认值，不是协议硬限制。

## 6.3 本地目录结构

```text
<data_dir>/
└── outbox/
    └── table_name/
        └── run_id/
            ├── schema.sql
            ├── chunk-000001.jsonl.zst
            ├── chunk-000002.jsonl.zst
            └── manifest.json
```

`manifest.json` 存在等价于 Run 已完整生成，因此 Manifest 最后原子写入。

---

# 7. 数据表达

## 7.1 文件格式

V1 使用 JSON Lines + zstd：每一行对应数据库的一行。

行使用 `SELECT *` 返回的列顺序，以 array 表达；列名记录在 Manifest 中，不在每行重复。

## 7.2 类型无损

编码必须无损表达 Source MySQL 常见类型：

* NULL 与空字符串严格区分；
* bytes / binary 使用 Base64 等安全形式；
* Decimal 不转成 float；
* datetime / date / time 不丢精度；
* 不允许使用 pickle。

同一份数据必须始终编码为完全相同的 bytes。

Source 编码与 Destination 解码使用同一模块。

具体 tag 格式见技术设计文档。

---

# 8. 数据一致性验证摘要

表可能：

* 无主键；
* 有重复行；
* 行顺序不稳定。

因此验证摘要必须是：

**order-independent、multiplicity-sensitive 的 multiset summary。**

至少包含：

```text
row_count
digest_a
digest_b
```

要求：

* 行顺序变化不影响结果；
* 重复行按出现次数参与结果；
* 不能使用简单 XOR（偶数个相同行会抵消）；
* 内存占用 O(1)，与总行数无关。

Source 在生成 Snapshot 时计算，Destination 导入后用相同算法复算比对。

该摘要不是数据库主键 Hash，只是 Snapshot Multiset Digest。

---

# 9. 源端磁盘控制

由于源 Windows Server 只有约 200GB 空间，同步程序必须限制自身磁盘占用（属于后续阶段实现）：

* Chunk 生成后尽快上传；
* HTTP 服务确认文件可靠接收后，可以删除源端该 Chunk；
* 不长期保存已经成功上传的历史同步文件；
* 程序不得因为同步任务耗尽系统磁盘空间。

因此 Snapshot 的生成实现不能假设"所有 Chunk 必须同时存在于本地，Manifest 才能工作"。Manifest 的 Chunk metadata 在生成过程中独立记录。

---

# 10. 单向网络处理原则

由于网络政策明确禁止任何反向信息，系统不尝试实现网络层面的端到端 ACK。

责任边界：

### 源端

* 正确扫描源数据库；
* 正确生成完整 Snapshot 与校验信息；
* 将文件可靠交给 HTTP 中转服务器。

源端的 `current_run_id` 只表示最近成功生成的完整 Snapshot Run，不表示目标端已经同步。

### 目标端

* 获取完整文件；
* 校验文件；
* 写入 staging；
* 自行验证数据；
* 验证通过后替换正式表。

---

# 11. 目标端文件处理

FTP 客户端从中转服务器下载文件后，目标端必须首先将文件可靠保存到自己的磁盘，数据库处理完成以前不删除。

目标端按 Run 接收完整文件集合（Chunk + schema + manifest），校验通过后：

1. 基于 schema.sql 创建 staging 表；
2. 完整导入所有 Chunk；
3. 复算验证摘要并与 Manifest 比对；
4. 一致则替换正式表。

（属于后续阶段实现。）

---

# 12. 人工操作要求

正常情况下不需要人工操作。

发生数据不一致时，人工只需要在源端重新触发该表的 Snapshot：

```text
重新同步 XXX 表
```

然后走原有单向链路。

不要求操作人员手工编写 SQL、手工比较数据。

---

# 13. 失败处理原则

Snapshot 中途失败：

```text
Run = FAILED
```

不做 Chunk 级断点续传，下一次重新生成一个新 Run。

理由：

* 没有可靠 key，没有 Binlog；
* 断点续扫无法保证快照一致性；
* 简单可靠优先。

失败 Run 不推进 `current_run_id`。

残留的 `.part` 临时文件不视为有效数据，可被清理或忽略。

---

# 14. 运行状态

源端至少需要能够查看：

* 表；
* 当前 Run；
* 状态；
* 行数；
* Chunk 数；
* 错误信息。

目标端至少需要能够查看：

* 表名；
* Run；
* 接收/导入状态；
* 一致性验证结果。

界面以命令行为主，以简单易用为原则。

---

# 15. V1 不包含的内容

V1 暂不要求：

* Binlog CDC；
* 增量同步 / Diff / 逻辑键；
* 实时秒级同步；
* 双向数据同步；
* 目标端向源端自动返回 ACK；
* Snapshot 断点续扫；
* 分布式集群；
* 复杂消息队列；
* 保存数据库每一次历史修改记录；
* 修改现有源业务表结构；
* 复杂 Web 管理后台。

优先保证：

**简单、可靠、可维护。**

---

# 16. 验收标准

系统满足以下条件即可认为 V1 达到基本要求：

1. 能对指定表生成完整 FULL_SNAPSHOT Run。
2. 不需要主键、逻辑唯一值；NULL、重复行、空表均可正确同步。
3. 700 万行级别表能够流式读取，不整体加载内存。
4. 数据分 Chunk，不产生单个超大文件。
5. 每个表携带 DDL，目标端可据此建表。
6. 文件损坏能够被发现（SHA256）。
7. 最终数据摘要与行顺序无关，目标端可自动比对。
8. 扫描期间表结构变化能够被发现（Run 失败而不是产生错配数据）。
9. 源端全程只读，不修改源业务数据库。
10. HTTP 上传成功后可以安全释放源端对应文件。
11. 目标端完整导入并验证通过后才替换正式表。
12. 正常情况下无需人工参与；不一致时人工只需重新生成该表快照。

---

## 17. 总体原则

本系统 V1 以满足当前实际业务需求为目标。

设计原则：

**能简单实现的，不增加复杂组件。**

**能自动完成的，不增加人工操作。**

**严格单向网络无法解决的问题，不通过复杂设计强行绕过。**

**通过源端可靠生成完整快照、文件可靠传输、目标端自动校验和简单人工重同步，保证系统最终数据一致。**
