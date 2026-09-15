# Airgap Sync 技术设计 V1

## 1. 设计目标

Airgap Sync 用于将源 MySQL 中的指定表，通过现有单向文件链路同步到隔离网络中的目标 MySQL。

V1 同步方式统一为：

**Full Snapshot 全量快照**

V1 优先保证：

* 正确；
* 简单；
* 可验证；
* 不修改源业务表；
* 不依赖 Binlog；
* 不依赖主键 / 逻辑键 / 更新时间；
* 能处理数百万行数据；
* 能适应源服务器约 200GB 的磁盘限制。

不追求实时同步，也不引入消息队列、Redis、分布式系统等额外组件。

---

# 2. 技术栈

## 2.1 开发语言

使用 Python 3.12+。

原因：

* Windows Server 部署方便；
* MySQL、HTTP、文件处理生态成熟；
* 适合流式读取和批量处理；
* Source 和 Destination 可以共用大量代码。

## 2.2 依赖

* PyMySQL —— MySQL 连接（`SSCursor` server-side cursor 流式读取）；
* Pydantic —— 配置与 Manifest 模型；
* PyYAML —— 配置文件；
* zstandard —— Chunk 压缩；
* Click —— CLI；
* Requests —— HTTP/HTTPS 流式 PUT（文件对象请求体）。

## 2.3 Source 本地状态

使用 SQLite（`<data_dir>/state/meta.db`）。

只保存控制信息：

* 表状态；
* Run 状态；
* （后续阶段）上传状态。

不保存业务行数据、行 Hash 或任何数据指纹镜像。

---

# 3. 项目结构

```text
src/airgap_sync/
├── cli.py
├── common/
│   ├── config.py          # YAML 配置加载
│   ├── models.py          # 配置模型
│   ├── logging.py         # 日志
│   ├── row_codec.py       # 行编码/解码 (Source/Destination 共用)
│   ├── verification.py    # Snapshot Multiset Digest
│   └── manifest.py        # Manifest 模型与原子读写
└── source/
    ├── mysql.py           # 只读连接 + identifier quoting + DDL + 流式扫描
    ├── state.py           # SQLite 状态库 (schema v3)
    ├── scanner.py         # 单表扫描 (流式读取 → 编码 → chunk/digest)
    ├── chunk_writer.py    # zstd Chunk 原子写入
    ├── snapshot.py        # 本地 Snapshot 生命周期编排
    ├── uploader.py        # Relay 流式 PUT、严格确认、重试
    └── delivery.py        # producer + 单 upload worker
```

Source 和 Destination 使用同一套项目代码，通过配置决定角色。Destination 端模块在 Phase 4 实现。

---

# 4. 总体架构

```text
                    SOURCE NETWORK

                 Source MySQL (READ ONLY)
                      │
        SHOW CREATE TABLE (第 1 次)
                      │
        单条 SELECT * (SSCursor 流式)
                      │
                 fetchmany 分批
                      │
          Row Codec 编码 → JSON Lines
                      │
           ┌──────────┴──────────┐
           │                     │
      Chunk Writer         Multiset Digest
      (zstd + sha256,      (order-independent,
       原子 rename)         O(1) 内存)
           │                     │
        chunk-*.jsonl.zst         │
           │                     │
        SHOW CREATE TABLE (第 2 次) —— 不一致则 Run FAILED
           │                     │
        schema.sql          verification summary
           └──────────┬──────────┘
                   manifest.json (最后原子写入)
                      │
                HTTP Upload (Phase 3 已实现)
                      │
                     FTP
════════════════ 单向隔离边界 ════════════════
                      │
                Destination (Phase 4/5)
                      │
              staging 导入 → 验证 → 替换正式表
```

---

# 5. 表配置

每张表只需要：

```yaml
tables:
  - name: std_scjgj_zhgsxt_qyjbxx_all
    enabled: true

  - name: dwd_frk_jbxx_djxx_frjbxx
    enabled: true
```

所有表统一 FULL_SNAPSHOT。不要求用户配置：

* 主键；
* 逻辑键；
* 同步模式；
* NULL 规则。

顶层配置结构：

```yaml
role: source

mysql:
  host: ...
  port: 3306
  database: ...
  user: ...
  password_env: ...
  connect_timeout: 10

paths:
  data_dir: D:/airgap-sync/data

snapshot:
  fetch_size: 2000          # 每批从 server-side cursor 读取的行数

chunk:
  max_rows: 50000           # 单个 Chunk 最大行数
  max_uncompressed_bytes: 67108864   # 单个 Chunk 最大未压缩字节数 (64 MiB)
  compression_level: 3      # zstd 级别, 默认 3 (速度与压缩比均衡)

tables:
  - name: ...
    enabled: true
```

配置模型 `extra="forbid"`：旧的 `mode` / `key` 字段会直接校验失败，避免用户误以为仍然有效。

---

# 6. Source MySQL 安全边界

Phase 1 的双层只读保护继续有效，且不允许为 Phase 2 功能放宽：

1. **应用层**：`fetch_all()` 只允许 SELECT；非 SELECT 在发送前拒绝。
2. **MySQL 层**：连接建立后 `SET SESSION TRANSACTION READ ONLY` 并验证。

Phase 2 新增的能力全部通过**专用方法**实现，不开放任意 SQL 入口：

## 6.1 Identifier quoting

```python
quote_identifier("normal_table")     # `normal_table`
quote_identifier("table-name")       # `table-name`
quote_identifier("we`rd")            # we``rd
```

规则：反引号包裹，名称内的反引号翻倍。禁止任何 `f"SELECT * FROM {table_name}"` 式的直接拼接。

## 6.2 获取 DDL

```python
get_create_table(table_name) -> str
```

内部只构造：

```sql
SHOW CREATE TABLE `安全引用后的表名`
```

不提供通用 SHOW / SET / 任意 SQL 接口。

## 6.3 流式扫描

```python
connection.stream_table(table_name, fetch_size) -> TableStream
```

内部只构造：

```sql
SELECT * FROM `安全引用后的表名`
```

使用 `pymysql.cursors.SSCursor`（server-side / unbuffered cursor），`fetchmany(fetch_size)` 分批读取。

不支持 WHERE / ORDER BY / JOIN / 用户 SQL。V1 只同步完整物理表。

## 6.4 表类型检查

`source check` 通过 information_schema 检查：

* 表存在（`TABLE_NOT_FOUND`）；
* 表类型为 `BASE TABLE`（VIEW 等为 `UNSUPPORTED_TABLE_TYPE`）。

---

# 7. Snapshot Run

## 7.1 Run ID

```text
<UTC timestamp>-<random suffix>
例如: 20260914T213500Z-a1b2c3d4
```

* 时间戳：`YYYYMMDDTHHMMSSZ`；
* 后缀：`secrets.token_hex(4)`；
* 文件名安全（仅 `[0-9A-Za-z-]`）；
* 不依赖数据库。

## 7.2 Run 目录

```text
<data_dir>/outbox/<table_name>/<run_id>/
├── schema.sql
├── chunk-000001.jsonl.zst
├── chunk-000002.jsonl.zst
└── manifest.json       # 最后原子写入
```

`manifest.json` 存在 = Run 已完整生成。

Manifest 的 Chunk metadata 在生成过程中独立记录（不依赖所有 Chunk 同时存在于磁盘），为 Phase 3 的"边生成、边上传、边删除"流水线预留。

## 7.3 Run 状态机

```text
RUNNING → COMPLETED   (DDL 一致 + manifest 原子写入成功)
RUNNING → FAILED      (任何失败: 连接错误 / 扫描异常 / DDL 变化 / 写文件失败)
```

失败 Run：

* 不生成 manifest；
* 不写 schema.sql（DDL 检查通过后才写）；
* 不推进 `current_run_id`；
* 残留 `.part` / chunk 文件不视为有效数据。

不做 Chunk 级断点续传；下次运行生成全新 Run。

---

# 8. Source State (SQLite schema v3)

```sql
CREATE TABLE table_state (
    table_name      TEXT PRIMARY KEY,
    last_snapshot_run_id TEXT,           -- 最近成功完成生成语义的 Run
    last_delivered_run_id TEXT,          -- 最近被 Relay 可靠确认的 Run
    status          TEXT NOT NULL,       -- IDLE / RUNNING / COMPLETED / FAILED
    last_run_id     TEXT,                -- 最近一次尝试的 Run (含失败)
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
```

状态库只保存控制信息，不保存业务行 Hash（V1 已删除行级状态设计）。

Schema 版本从 v1（含 `mode` 列）升级到 v2：项目尚未生产运行，采用简单迁移——检测到 v1 时重建 `table_state`（丢弃 `mode`），单事务完成。不做通用 migration framework。

另有 `sync_runs` 记录 Run 的 `GENERATING / UPLOADING / FINALIZING /
SNAPSHOT_READY / DELIVERED / FAILED / DISK_PRESSURE`、行数/Chunk 数/字节数和时间；
`run_artifacts` 按 Chunk/schema/manifest 记录 logical/transport name、size、SHA256、
attempts、request_id、uploaded_at、last_error 和 cleanup_error。SQLite 不保存业务数据。

`last_snapshot_run_id` 语义：

> Source 最近成功生成的完整 Snapshot Run。

`last_delivered_run_id` 只表示 Relay 已可靠接收全部 artifact 且 manifest 最后确认；
严格单向网络下 Source 永远无法知道目标端 Apply/VERIFIED 状态。Python 读取对象暂时
提供 `current_run_id` 兼容属性，但 v3 SQLite 只持久化上述两个明确字段。

## 8.1 schema v2 → v3

迁移在单个 SQLite 事务中重建 `table_state`，把旧 `current_run_id` 复制到
`last_snapshot_run_id`，`last_delivered_run_id` 初始为空，并创建 Run/artifact 表。
重复初始化幂等，不引入 ORM 或 Alembic。

# 9. Phase 3 HTTP Relay 与磁盘流水线

## 9.1 协议与安全

上传地址为 `{base_url}/api/v1/upload/{transport_filename}`。Requests 接收打开的二进制
文件对象作为 body，因此大文件按客户端缓冲流式读取；显式发送文件 stat 得到的
`Content-Length` 及预先生成的 SHA256。Token 只从 `relay.token_env` 指定的环境变量
读取，不进入配置 repr、SQLite、异常或日志。HTTPS 默认使用系统 CA；配置 `ca_file`
时使用指定 CA，不提供关闭 TLS 校验选项。HTTP 继续支持内网 Relay。

成功必须同时满足 201、`success=true`、transport filename/size/sha256 精确匹配及非空
request_id。网络错误、connect/read timeout、429、500 和所有合理 5xx（含 507）有限
指数退避重试；401/411/413/415/422 等确定性 4xx 不重试。409 无法核验已有文件，
以 `REMOTE_FILE_EXISTS_AMBIGUOUS` 失败并保留本地文件。

## 9.2 producer / worker

Scanner 在 Chunk 原子关闭时快速执行 callback：metadata 持久化、磁盘阈值检查、入队，
随后继续消费 SSCursor。唯一的 upload worker 顺序 PUT；确认后先写 `UPLOADED` 和
request_id，再 unlink。删除失败只记 `cleanup_error`，不重新上传。这样 MySQL 可生成
Chunk N+1，同时 HTTP 上传 Chunk N，但不会触发 Relay 的多文件并发 429 限制。

待上传本地字节数受 `spool.max_pending_bytes` 限制，可用空间受
`spool.min_free_bytes` 限制；越界直接停止扫描而非等待队列。上传最终失败通过 batch
边界检查尽快中止 scanner。失败 Run 不传 manifest，已上传文件成为 Destination 不得
应用的 orphan。

## 9.3 最终提交和文件生命周期

顺序固定为：全部 Chunk 确认 → DDL 一致 → schema 确认 → manifest 最后确认 →
`DELIVERED`。Manifest 从内存/SQLite ChunkMeta 构建，不依赖 Chunk 文件仍在本地。
成功后再次清理确认过但先前 unlink 失败的文件及空 run_dir；失败 Run 保留未确认文件。

---

# 10. Chunk Writer

## 10.1 阈值

Chunk 同时受：

```text
max_rows
max_uncompressed_bytes
```

限制，任一达到阈值即封闭当前 Chunk。

单行超过 byte 阈值：允许生成单行超限 Chunk（先写入再封闭），不丢数据、不死循环。

空表：0 个 Chunk。

## 10.2 原子生成

```text
chunk-000001.jsonl.zst.part    (写入中)
        ↓ 写入 + 关闭 compressor + flush + fsync + 关闭文件
os.replace → chunk-000001.jsonl.zst
```

只有最终文件名代表完整 Chunk；`.part` 不是有效数据。

## 10.3 压缩与校验

* zstd（`zstandard` 库，可配置级别，默认 3）；
* SHA256 针对最终压缩文件字节计算（边写边算，不回读文件）；
* Manifest 记录每个 Chunk 的：

```text
sequence
file
rows
uncompressed_bytes
compressed_bytes
sha256
```

---

# 11. Row Codec

## 11.1 格式

每行编码为 JSON array（列顺序 = `SELECT *` 返回顺序），一行一个 JSON 对象加换行符写入 JSONL。

编码参数：

```python
json.dumps(values, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
.encode("utf-8")
```

同一份数据始终编码为完全相同的 bytes。

## 11.2 类型映射

| MySQL/PyMySQL 值 | JSON 表达 | 说明 |
|---|---|---|
| `None` | `null` | NULL |
| `bool` | `true` / `false` | |
| `int` | 数字 | 任意精度 (Python bigint) |
| `float` | 数字 (repr) | 有限值直接数字 |
| 非有限 float | `{"$float": "inf"}` 等 | NaN/Infinity 不是合法 JSON |
| `str` | 字符串 | Unicode 原样 |
| `Decimal` | `{"$decimal": "123.4500"}` | 字符串形式, 不丢精度, 不转 float |
| `bytes` / `bytearray` | `{"$bytes": "<base64>"}` | Base64 |
| `date` | `{"$date": "2026-09-14"}` | ISO 格式 |
| `datetime` | `{"$datetime": "2026-09-14T20:00:00.123456"}` | 无时区, ISO 格式 |
| `time` | `{"$time": "12:34:56.000001"}` | ISO 格式 |
| `timedelta` | `{"$timedelta": "<days>:<seconds>:<microseconds>"}` | PyMySQL TIME 列默认返回 timedelta |

语义保证：

* `NULL != ""`（`null` vs `""`）；
* `0 != "0"`（数字 vs 字符串）；
* tag 对象只出现在对象位置，且 key 以 `$` 开头，与普通数据不混淆；
* 不使用 pickle。

## 11.3 解码

Destination 使用同一模块 `decode_row()` 还原 Python 值（`Decimal`、`bytes`、`datetime` 等按 tag 还原），round-trip 无损。

---

# 12. Snapshot Multiset Digest

表可能无主键、有重复行、行顺序不稳定，因此验证摘要必须：

* order-independent：行顺序变化结果不变；
* multiplicity-sensitive：重复行按出现次数参与结果；
* 非 XOR：XOR 会被偶数个相同行抵消；
* O(1) 内存：与总行数无关。

## 12.1 算法

对每一行：

```text
row_bytes = canonical encoded row (Row Codec 输出, 不含换行)

h1 = SHA256(domain_a || row_bytes)
h2 = SHA256(domain_b || row_bytes)

digest_a = Σ int(h1) mod 2^256
digest_b = Σ int(h2) mod 2^256
```

`domain_a` / `domain_b` 是两个固定的域分隔常量，保证两条摘要独立。

最终保存：

```text
row_count   行数
digest_a    64 hex 字符
digest_b    64 hex 字符
```

## 12.2 性质

* 模加交换 → 顺序无关；
* 每行贡献固定非零值的加法项 → 重复行可区分（两条相同行 ≠ 一条）；
* 双 domain → 显著降低构造碰撞的可能性；
* 空表得到确定结果（全零 digest + row_count 0）；
* 每行只需两次 SHA256 与一个大整数加法，流式 O(1) 内存。

## 12.3 复用

Source 生成 Snapshot 时计算；Destination（Phase 5）导入 staging 后用**完全相同实现**复算比对。

注意：这不是数据库主键 Hash，只是 Snapshot Multiset Digest。

---

# 13. Manifest

```json
{
  "protocol_version": 1,
  "run_id": "20260914T213500Z-a1b2c3d4",
  "run_type": "FULL_SNAPSHOT",
  "source": {
    "database": "sgaj_data",
    "table": "std_xxx"
  },
  "schema": {
    "file": "schema.sql",
    "sha256": "..."
  },
  "columns": ["id", "name", "created_at"],
  "row_count": 7123456,
  "chunks": [
    {
      "sequence": 1,
      "file": "chunk-000001.jsonl.zst",
      "rows": 50000,
      "uncompressed_bytes": 123,
      "compressed_bytes": 45,
      "sha256": "..."
    }
  ],
  "verification": {
    "algorithm": "multiset_digest_v1",
    "row_count": 7123456,
    "digest_a": "...",
    "digest_b": "..."
  },
  "created_at": "2026-09-14T21:35:00+00:00"
}
```

写入方式：`manifest.json.part` → fsync → 原子 rename → `manifest.json`。

Manifest 必须最后生成：`manifest.json` 存在即代表 Run 完整。

## 13.1 schema.sql

* 内容 = `SHOW CREATE TABLE` 的实际输出（第一次读取结果）；
* 不重新构造 CREATE TABLE，不丢字段类型 / DEFAULT / NULL / INDEX / PRIMARY KEY / charset / collation / comment；
* UTF-8、`\n` 换行、末尾统一 `;\n`；
* 计算 SHA256 记入 Manifest。

目标端（Phase 4）基于该 DDL 创建 staging 表；本阶段只负责携带。

## 13.2 columns

`SELECT *` 返回的列名顺序。每条 JSON row 是 array，不重复列名。Destination 严格按 manifest columns 写入 staging。

---

# 14. Snapshot 执行流程

`airgap-sync source snapshot --config config.yaml --table TABLE_NAME`

1. 验证配置、`role=source`、表在 enabled tables 中；
2. 建立只读连接；
3. 状态库登记表，状态 → `RUNNING`；
4. 第一次 `SHOW CREATE TABLE`；
5. 单条 `SELECT *` 流式扫描（fetchmany 分批）；
6. 每行：Row Codec 编码 → Chunk Writer 写入 + Multiset Digest 累加；
7. 扫描完成，封闭最后一个 Chunk；
8. 第二次 `SHOW CREATE TABLE`；
9. DDL 一致 → 写 `schema.sql` + 原子写 `manifest.json`；DDL 变化 → Run FAILED；
10. 成功 → `current_run_id = run_id`，状态 `COMPLETED`。

### DDL 一致性比较

比较两次 `SHOW CREATE TABLE` 时，忽略表选项 `AUTO_INCREMENT=<n>` 的具体数值（该计数器会随并发插入变化，不代表结构变化），其余任何差异（列、索引、charset、comment 等）都判定为结构变化。比较在规范化换行后进行；存储到 `schema.sql` 的仍是原始 DDL 文本。

---

# 15. 性能与内存模型

Snapshot 全链路没有 `list(all_rows)` / `fetchall()`：

* server-side cursor + `fetchmany(fetch_size)`：内存只与单批行数相关；
* Chunk Writer：逐行写入压缩流，同时只在内存保留当前 Chunk 的 metadata（行数、字节数、sha256 状态）；
* Multiset Digest：固定 O(1) 状态；
* Manifest chunks 列表：每个 Chunk 数十字节，与总行数无关。

数据量从 10 行增长到 700 万行，内存占用不随总行数线性增长。

---

# 16. 日志

Snapshot 日志只记录：

```text
table, run_id, rows processed, chunk sequence,
raw bytes, compressed bytes, elapsed time, status, error
```

不记录完整行、身份证号、手机号、longtext 或其他业务字段值。

---

# 17. 后续阶段

* **Phase 3**：HTTP 上传 + Source 磁盘流水线（已实现）；
* **Phase 4**：Destination 接收 + staging 导入；
* **Phase 5**：一致性验证（复算 Multiset Digest）+ 正式表切换；
* **Phase 6**：调度、状态、异常恢复、大表压力测试。

---

# 18. V1 不实现

* Binlog CDC；
* 增量同步 / Diff / 逻辑键 / 行级 Hash 状态；
* Snapshot 断点续扫；
* Redis / RabbitMQ / Kafka / Kubernetes；
* 双向通信、远程 ACK；
* 复杂 Web 管理后台。

如果未来确有增量需求，重新设计。
