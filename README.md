# Airgap Sync

单向隔离网络 MySQL 数据同步工具。

将源 MySQL 中的指定表，通过 `源端程序 → HTTP 上传 → 中转服务器 → FTP 单向传输 → 目标端程序` 的链路同步到隔离网络中的目标 MySQL。V1 统一采用 **Full Snapshot 全量快照**同步：每次同步某张表都生成该表的完整版本（DDL + 数据 Chunk + 校验信息），不依赖 Binlog、主键、逻辑唯一键或更新时间。

## 当前状态

Phase 1 至 Phase 4（Destination staging 导入）已完成，当前支持：

- YAML 配置加载与强类型校验（表配置只需 `name` + `enabled`，所有表统一 FULL_SNAPSHOT；旧 `mode` / `key` 字段已删除，配置中残留会明确报错）；
- `airgap-sync config validate` 配置校验；
- `airgap-sync source check`：MySQL 只读连接测试、同步表存在性与表类型检查（仅支持 BASE TABLE，配置 VIEW 报 `UNSUPPORTED_TABLE_TYPE`）、SQLite 状态库初始化；
- `airgap-sync source snapshot --table TABLE`：单条流式 `SELECT *` 全表扫描 → JSONL + zstd 分 Chunk → `schema.sql` + `manifest.json`，输出到 `<data_dir>/outbox/<table>/<run_id>/`。
- `airgap-sync source sync --table TABLE`：扫描与单路 HTTP PUT 并行，Relay 严格确认每个
  Chunk 后即释放 Source 文件，最后依次提交 schema 和 manifest；
- `airgap-sync source relay-check`：只调用 `GET /health` 的诊断命令。
- `airgap-sync destination check`：检查 incoming、目标 MySQL 与 metadata schema；
- `airgap-sync destination process --run RUN_ID`：严格校验完整 Run，按 Chunk 流式导入
  staging，最高状态为 `STAGED`；
- `airgap-sync destination process-once`：扫描当前全部 manifest 一次；不完整 Run 跳过，
  不会阻断其他 Run。

### 源数据库只读保护

Airgap Sync 不修改源业务表。`SourceMySQLConnection` 提供**双层只读保护**：

1. **应用层**：对外只提供 SELECT 查询能力（`fetch_all` / `ping`），`INSERT` / `UPDATE` / `DELETE` / `SHOW` / `SET` 等非 SELECT SQL 在发送给 MySQL 之前直接抛出 `SourceMySQLError` 拒绝；
2. **MySQL 层**：连接建立后立即执行 `SET SESSION TRANSACTION READ ONLY` 并验证生效（只影响当前 Session，不改 GLOBAL，不需要 SUPER）。即使数据库账号具有写权限，写操作也会被 MySQL 以错误 1792 拒绝。

Phase 2 新增的 DDL 获取与全表扫描通过**专用安全方法**实现（内部构造反引号安全引用的 `SHOW CREATE TABLE \`tbl\`` 与 `SELECT * FROM \`tbl\``），不开放任意 SQL 入口，双层只读保护不变。

生产代码不提供任何执行 DML/DDL 或暴露原始连接的公共 API。需要真实 MySQL 的集成测试通过独立的 PyMySQL 管理连接创建/清理测试表，`SourceMySQLConnection` 在测试中也保持只读。

### Snapshot Run 结构

```text
outbox/<table>/<run_id>/
├── schema.sql                 # SHOW CREATE TABLE 原始 DDL
├── chunk-000001.jsonl.zst     # JSON Lines + zstd 分块数据
├── chunk-000002.jsonl.zst
└── manifest.json              # 最后原子写入; 存在即代表 Run 完整
```

特性：

- 单条 `SELECT *` + server-side cursor 流式读取，内存占用与总行数无关（700 万行级别不需要整表进内存）；
- 不要求主键、唯一键、ORDER BY；NULL、重复行、空表均可正确同步；
- 每行 JSON array 编码，列顺序记录在 manifest；`Decimal` / `bytes` / `datetime` 等类型无损（tag 格式见 `docs/technical-design.md`）；
- Chunk 同时受行数与未压缩字节数双阈值限制，`.part` 临时文件原子 rename，最终压缩文件计算 SHA256；
- 扫描前后各取一次 DDL，结构变化则 Run FAILED（`SCHEMA_CHANGED_DURING_SNAPSHOT`），不产生结构错配的快照；
- 验证摘要是与行顺序无关的 multiset digest（`row_count` + `digest_a` + `digest_b`），供目标端导入后复算比对。

以下能力**尚未实现**：staging 内容的最终 digest VERIFY、正式表切换、incoming Snapshot
删除、总行数/本次净增/月度净增统计、Destination 常驻 Worker 和 Source fixed-delay
Scheduler。FTP Client 是现有外部链路组件，不属于本项目。

## 环境要求

- Python 3.12+
- 使用 [mise](https://mise.jdx.dev/) 管理 [uv](https://docs.astral.sh/uv/) 与 Python

## 安装

```bash
mise install      # 安装 uv 和 Python
uv sync           # 创建虚拟环境并安装依赖
```

## 创建配置

```bash
cp config/config.example.yaml config/config.yaml
# Destination 使用：
cp config/config.destination.example.yaml config/config.yaml
```

按实际环境修改 `mysql`、`paths`、`snapshot`、`chunk`、`relay`、`spool`、`tables` 配置。注意：

- 数据库密码**不写入 YAML**，只通过 `mysql.password_env` 指定的环境变量提供；
- Relay Token 也**不写入 YAML**，只通过 `relay.token_env` 指定的环境变量提供；
- `paths.data_dir` 支持 Windows 风格路径，例如 `D:/airgap-sync/data`；
- 每张同步表只需要 `name` 和 `enabled`，所有表统一 FULL_SNAPSHOT，不需要配置主键或同步模式。
- Destination 可省略 `tables`（或写 `tables: []`），新表会随完整 Snapshot 自动进入 staging。

## 设置数据库密码环境变量

```bash
# Linux / macOS
export AIRGAP_SYNC_MYSQL_PASSWORD='真实密码'
export AIRGAP_SYNC_UPLOAD_TOKEN='真实Relay Token'

# Windows PowerShell
$env:AIRGAP_SYNC_MYSQL_PASSWORD = '真实密码'
$env:AIRGAP_SYNC_UPLOAD_TOKEN = '真实Relay Token'
```

环境变量未设置时程序会明确报错；密码和 Token 不会出现在日志或错误输出中。

## 使用

```bash
uv run airgap-sync --version
uv run airgap-sync config validate --config config/config.yaml
uv run airgap-sync source check --config config/config.yaml
uv run airgap-sync source snapshot --config config/config.yaml --table std_scjgj_zhgsxt_qyjbxx_all
uv run airgap-sync source relay-check --config config/config.yaml
uv run airgap-sync source sync --config config/config.yaml --table std_scjgj_zhgsxt_qyjbxx_all
uv run airgap-sync destination check --config config/config.yaml
uv run airgap-sync destination process --config config/config.yaml --run 20260915T030000Z-a1b2c3d4
uv run airgap-sync destination process-once --config config/config.yaml
```

`source check` 输出示例：

```text
Configuration       OK
MySQL connection    OK
MySQL server        5.7.35-log
Database            sgaj_data
SQLite state        OK (D:/airgap-sync/data/state/meta.db, schema v3)

Tables:
std_scjgj_zhgsxt_qyjbxx_all   OK
dwd_frk_jbxx_djxx_frjbxx      OK
some_legacy_table             SKIP (disabled)
```

`source snapshot` 输出示例：

```text
Table           std_scjgj_zhgsxt_qyjbxx_all
Run             20260914T213500Z-a1b2c3d4
Rows            7,123,456
Chunks          143
Raw size        3.2 GiB
Compressed      1.1 GiB
Status          COMPLETED
Output          D:/airgap-sync/data/outbox/std_scjgj_zhgsxt_qyjbxx_all/20260914T213500Z-a1b2c3d4
```

SQLite 状态库自动创建在 `<data_dir>/state/meta.db`。`last_snapshot_run_id` 表示最近成功
生成的 Snapshot，`last_delivered_run_id` 表示最近被 HTTP Relay 可靠确认的 Run；后者
也不表示 Destination 已 Apply/VERIFIED。失败不推进 delivered 指针，下一次运行生成全新
Run（V1 不做断点续扫）。

## HTTP Relay 协议与流水线

Relay 上传使用 `PUT /api/v1/upload/{filename}`，body 是文件原始二进制，不使用
multipart。客户端通过 Requests 直接传打开的文件对象，显式设置真实
`Content-Length` 和 `X-File-SHA256`，因此内存不会随 Chunk 大小线性增长。transport
filename 为 `airgap-v1--<run_id>--<logical_name>`，不包含原始表名。

只有 HTTP 201 且响应 `success`、filename、size、sha256、request_id 全部验证通过才会
记录 `UPLOADED` 并删除本地文件。网络超时、429 和 5xx 有限指数退避；确定性 4xx 不
重试；409 以 `REMOTE_FILE_EXISTS_AMBIGUOUS` 失败并保留文件。HTTPS 使用系统 CA，或
通过 `relay.ca_file` 指定私有 CA；不支持跳过 TLS 验证。

Scanner 每关闭一个 Chunk 只持久化 metadata 并入队，然后继续读取 SSCursor；一个固定
HTTP worker 顺序上传，所以扫描/压缩可与 PUT 重叠，但不会并发多个 PUT。待上传字节超过
`spool.max_pending_bytes` 或可用空间低于 `spool.min_free_bytes` 时 Run 立即失败，不无限
阻塞数据库读取。全部 Chunk 确认且 DDL 二次检查通过后才上传 schema，manifest 永远最后。

`DELIVERED` 后清理本地 Run 文件；删除失败只作为本地 cleanup error 记录，不会重新 PUT。

## Destination：incoming → STAGED

外部 FTP Client 把扁平 transport 文件下载到 `destination.incoming_dir`，文件名固定为
`airgap-v1--<run_id>--<logical_name>`。Destination 只从 `manifest.json` transport 文件发现
Run：孤立 Chunk 不会建表。manifest 出现但预期文件缺失、文件小于声明大小，或整组文件在
`settle_seconds` 两个观察点间发生变化时，本次结果为可重试的 `INCOMPLETE`；稳定后的协议、
结构、大小或 SHA256 矛盾是永久 `FAILED`。

只接受 protocol v1、`FULL_SNAPSHOT` 和 `multiset_digest_v1`。Chunk 序号必须从 1 连续，
逻辑文件名、行数和 manifest 内部 row count 必须一致。SHA256 以固定缓冲流式计算，不把
大 Chunk 整体读入内存。

目标 MySQL 使用独立写连接，metadata 默认保存在 `airgap_sync_meta`（schema v1）。
`schema.sql` 必须是与 manifest 表名一致的单条 `CREATE TABLE`；程序只重写 CREATE TABLE
后的第一个 identifier 为确定、短且与源表长度无关的 `__airgap_stg_<run>_<hash>`。
FOREIGN KEY / CONSTRAINT 当前明确报 `UNSUPPORTED_SCHEMA_FEATURE`。PK、UNIQUE、INDEX、
AUTO_INCREMENT、charset、collation 和 comment 保持原样。正式表无论是否已存在都不会被
CREATE、DROP、TRUNCATE、ALTER 或 INSERT。

Chunk 使用 zstd streaming decompressor 逐行读取，复用公共 Row Codec 解码；INSERT 始终
使用经过核对的显式列名，生成列从 INSERT 列表排除。`executemany` 受
`destination.insert_batch_rows` 控制，但整个 Chunk 只有一个事务；staging 行和 Chunk
`IMPORTED` metadata 在同一 MySQL 事务提交。失败会整 Chunk 回滚，已 `IMPORTED` Chunk
重试时直接跳过。同 Run 和同一源表的不同 Run 都使用 MySQL advisory lock 避免并发。
所有 Chunk 行数合计匹配后 Run 才成为 `STAGED`。incoming 文件保留，最终数据库 digest、
正式表原子切换与清理属于 Phase 5。

## 真实 Relay 手工测试

自动化测试不会访问 `10.9.195.133:50552`。设置 Token 后可先检查健康，再选择一张很小
的测试表执行同步：

```bash
export AIRGAP_SYNC_UPLOAD_TOKEN='...'
uv run airgap-sync source relay-check --config config/config.yaml
uv run airgap-sync source sync --config config/config.yaml --table small_test_table
```

Relay 单文件上限 20 GiB，不支持覆盖或断点续传；Chunk 参数必须保证实际压缩文件低于
该限制。

## 日志级别

```bash
export AIRGAP_SYNC_LOG_LEVEL=DEBUG   # 或 INFO / WARNING / ERROR
uv run airgap-sync --log-level DEBUG source snapshot --config config/config.yaml --table std_xxx
```

## 开发

```bash
uv run pytest                    # 单元测试 (默认跳过需要真实 MySQL 的集成测试)
uv run ruff check src tests      # 静态检查
uv run ruff format --check src tests
```

需要真实 MySQL 的集成测试：

```bash
export AIRGAP_TEST_MYSQL_HOST=127.0.0.1
export AIRGAP_TEST_MYSQL_PORT=3306
export AIRGAP_TEST_MYSQL_DATABASE=airgap_sync_it   # 一次性测试库, 会被写入
export AIRGAP_TEST_MYSQL_USER=root
export AIRGAP_TEST_MYSQL_PASSWORD=...
uv run pytest -m integration
```
