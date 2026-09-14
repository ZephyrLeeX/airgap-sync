# Airgap Sync

单向隔离网络 MySQL 数据同步工具。

将源 MySQL 中的指定表，通过 `源端程序 → HTTP 上传 → 中转服务器 → FTP 单向传输 → 目标端程序` 的链路同步到隔离网络中的目标 MySQL。V1 采用全表扫描 + 本地状态比较识别增量，不依赖 Binlog，也不依赖源表更新时间。

## 当前状态

Phase 1（项目基础）已完成，当前支持：

- YAML 配置加载与强类型校验（表配置支持 `keyed` / `row_multiset` 两种模式，key 字段不允许重复）；
- `airgap-sync config validate` 配置校验；
- `airgap-sync source check`：Source MySQL 只读连接测试、同步表与 key 字段元数据检查、本地 SQLite 状态库初始化。

### 源数据库只读保护

Airgap Sync 不修改源业务表。`SourceMySQLConnection` 提供**双层只读保护**：

1. **应用层**：对外只提供 SELECT 查询能力（`fetch_all` / `ping`），`INSERT` / `UPDATE` / `DELETE` / `CREATE` / `DROP` 等非 SELECT SQL 在发送给 MySQL 之前直接抛出 `SourceMySQLError` 拒绝；
2. **MySQL 层**：连接建立后立即执行 `SET SESSION TRANSACTION READ ONLY` 并验证生效（只影响当前 Session，不改 GLOBAL，不需要 SUPER）。即使数据库账号具有写权限，写操作也会被 MySQL 以错误 1792 拒绝。设置失败时连接初始化直接失败，不会静默降级为可写连接。

生产代码不提供任何执行 DML/DDL 或暴露原始连接的公共 API。需要真实 MySQL 的集成测试通过独立的 PyMySQL 管理连接创建/清理测试表，`SourceMySQLConnection` 在测试中也保持只读。

以下能力**尚未实现**，属于后续阶段：数据扫描、行 Hash、Diff、Chunk、HTTP 上传、Destination 端、一致性校验等。

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

按实际环境修改 `mysql`、`paths`、`tables` 配置。注意：

- 数据库密码**不写入 YAML**，只通过 `mysql.password_env` 指定的环境变量提供；
- `paths.data_dir` 支持 Windows 风格路径，例如 `D:/airgap-sync/data`；
- 每张同步表必须声明 `mode`：`keyed`（必须提供至少一个 `key` 字段，支持组合键）或 `row_multiset`（无需 key）。

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
```

`source check` 输出示例：

```text
Configuration       OK
MySQL connection    OK
MySQL server        5.7.35-log
Database            sgaj_data
SQLite state        OK (D:/airgap-sync/data/state/meta.db, schema v1)

Tables:
std_scjgj_zhgsxt_qyjbxx_all   OK   mode=keyed key=etps_id
dwd_frk_jbxx_djxx_frjbxx      OK   mode=keyed key=zjid
some_legacy_table             SKIP mode=row_multiset (disabled)
```

本阶段 `source check` 只做元数据检查（表存在、key 字段存在），不做全表扫描，也不检查 key 是否真正唯一。

任何检查失败时命令返回非 0 退出码并给出明确错误（如 `TABLE_NOT_FOUND`、`KEY_COLUMN_NOT_FOUND`）。

SQLite 状态库自动创建在 `<data_dir>/state/meta.db`，首次运行自动初始化 schema，重复运行幂等。已产生同步基线（`current_run_id` 非空）的表不允许修改同步 `mode`，需通过显式 reset / snapshot 机制处理（本阶段尚未实现）。

## 日志级别

```bash
export AIRGAP_SYNC_LOG_LEVEL=DEBUG   # 或 INFO / WARNING / ERROR
uv run airgap-sync --log-level DEBUG source check --config config/config.yaml
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
