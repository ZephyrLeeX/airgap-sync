# Airgap Sync

单向隔离网络 MySQL 数据同步工具。

将源 MySQL 中的指定表，通过 `源端程序 → HTTP 上传 → 中转服务器 → FTP 单向传输 → 目标端程序` 的链路同步到隔离网络中的目标 MySQL。V1 统一采用 **Full Snapshot 全量快照**同步：每次同步某张表都生成该表的完整版本（DDL + 数据 Chunk + 校验信息），不依赖 Binlog、主键、逻辑唯一键或更新时间。

## 当前状态

Phase 1（项目基础）与 Phase 2（Full Snapshot Source 核心）已完成，当前支持：

- YAML 配置加载与强类型校验（表配置只需 `name` + `enabled`，所有表统一 FULL_SNAPSHOT；旧 `mode` / `key` 字段已删除，配置中残留会明确报错）；
- `airgap-sync config validate` 配置校验；
- `airgap-sync source check`：MySQL 只读连接测试、同步表存在性与表类型检查（仅支持 BASE TABLE，配置 VIEW 报 `UNSUPPORTED_TABLE_TYPE`）、SQLite 状态库初始化；
- `airgap-sync source snapshot --table TABLE`：单条流式 `SELECT *` 全表扫描 → JSONL + zstd 分 Chunk → `schema.sql` + `manifest.json`，输出到 `<data_dir>/outbox/<table>/<run_id>/`。

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

以下能力**尚未实现**，属于后续阶段：HTTP 上传、Destination 端、staging 导入、一致性验证、调度器。

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
```

按实际环境修改 `mysql`、`paths`、`snapshot`、`chunk`、`tables` 配置。注意：

- 数据库密码**不写入 YAML**，只通过 `mysql.password_env` 指定的环境变量提供；
- `paths.data_dir` 支持 Windows 风格路径，例如 `D:/airgap-sync/data`；
- 每张同步表只需要 `name` 和 `enabled`，所有表统一 FULL_SNAPSHOT，不需要配置主键或同步模式。

## 设置数据库密码环境变量

```bash
# Linux / macOS
export AIRGAP_SYNC_MYSQL_PASSWORD='真实密码'

# Windows PowerShell
$env:AIRGAP_SYNC_MYSQL_PASSWORD = '真实密码'
```

环境变量未设置时程序会明确报错；密码不会出现在日志或错误输出中。

## 使用

```bash
uv run airgap-sync --version
uv run airgap-sync config validate --config config/config.yaml
uv run airgap-sync source check --config config/config.yaml
uv run airgap-sync source snapshot --config config/config.yaml --table std_scjgj_zhgsxt_qyjbxx_all
```

`source check` 输出示例：

```text
Configuration       OK
MySQL connection    OK
MySQL server        5.7.35-log
Database            sgaj_data
SQLite state        OK (D:/airgap-sync/data/state/meta.db, schema v2)

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

SQLite 状态库自动创建在 `<data_dir>/state/meta.db`。`table_state.current_run_id` 表示 Source 最近成功生成的完整 Snapshot Run——由于网络严格单向，它不代表目标端已经同步完成。Snapshot 中途失败的 Run 状态为 FAILED，不生成 manifest，也不推进 `current_run_id`；下一次运行会生成全新 Run（V1 不做断点续扫）。

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
