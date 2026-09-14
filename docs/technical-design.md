# Airgap Sync 技术设计 V1

## 1. 设计目标

Airgap Sync 用于将源 MySQL 中的指定表，通过现有单向文件链路同步到隔离网络中的目标 MySQL。

V1 优先保证：

* 正确；
* 简单；
* 可恢复；
* 可验证；
* 不修改源业务表；
* 不依赖 Binlog；
* 不依赖源表更新时间；
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

## 2.2 Source 本地状态

使用 SQLite。

SQLite 只保存：

* 同步任务状态；
* Run 信息；
* 上一版本数据指纹；
* 当前扫描生成的下一版本数据指纹。

不保存完整源数据库镜像。

对于单张超大表，可以使用独立状态数据库文件，避免所有表集中在一个超大 SQLite 文件中。

## 2.3 Destination 同步元数据

在目标 MySQL 中建立独立 schema，例如：

`airgap_sync_meta`

只存放：

* 已处理 Run；
* 已处理 Chunk；
* 当前表版本；
* 校验结果；
* 错误信息。

不修改目标业务数据字段。

---

# 3. 项目结构

建议：

```text
airgap-sync/
├── README.md
├── pyproject.toml
├── config/
│   └── config.example.yaml
├── docs/
│   ├── requirements.md
│   ├── technical-design.md
│   └── tasks.md
├── src/
│   └── airgap_sync/
│       ├── common/
│       │   ├── config.py
│       │   ├── hashing.py
│       │   ├── manifest.py
│       │   └── models.py
│       ├── source/
│       │   ├── scanner.py
│       │   ├── state.py
│       │   ├── diff.py
│       │   ├── chunk_writer.py
│       │   ├── uploader.py
│       │   └── service.py
│       └── destination/
│           ├── watcher.py
│           ├── verifier.py
│           ├── applier.py
│           ├── reconcile.py
│           └── service.py
└── tests/
```

Source 和 Destination 使用同一套项目代码，通过配置决定角色。

---

# 4. 总体架构

```text
                    SOURCE NETWORK

                 Source MySQL
                      │
                streaming SELECT
                      │
                      ▼
               Source Scanner
                      │
               canonical row
                      │
                key / hash
                      │
          ┌───────────┴───────────┐
          │                       │
    Current State            Next State
      SQLite                   SQLite
          │                       │
          └────────── Diff ───────┘
                      │
                      ▼
                 Delta Run
                      │
                 Chunk files
                      │
                HTTP Upload
                      │
                      ▼
               Relay Server
                      │
                     FTP
════════════════ 单向隔离边界 ════════════════
                      │
                      ▼
             Existing FTP Client
                      │
                      ▼
              incoming directory
                      │
                      ▼
             Destination Agent
                      │
                 MySQL Apply
                      │
                  Verify
                      │
                      ▼
                Target MySQL
```

FTP 下载过程由现有环境负责。

Airgap Sync 的 Destination Agent 只需要监控 FTP 客户端已经放到本地的目录。

---

# 5. 表配置

每张表独立配置。

示例：

```yaml
tables:
  - name: std_scjgj_zhgsxt_qyjbxx_all
    mode: keyed
    key:
      - etps_id

  - name: dwd_frk_jbxx_djxx_frjbxx
    mode: keyed
    key:
      - zjid

  - name: some_legacy_table
    mode: row_multiset
```

支持两种核心模式。

## 5.1 keyed

用于：

* 数据库真实主键；
* 已验证唯一的逻辑业务键；
* 多字段组合唯一键。

## 5.2 row_multiset

用于真正没有可靠唯一键的表。

使用完整行数据指纹和出现次数判断：

* 新增；
* 删除；
* 数据变化。

大表应优先寻找可靠逻辑键，只有确实没有逻辑键时才使用 `row_multiset`。

---

# 6. 数据指纹

所有比较都基于程序生成的规范化数据。

必须正确区分：

* NULL；
* 空字符串；
* 数字；
* 字符串；
* datetime；
* decimal；
* binary；
* text。

不能简单使用：

`CONCAT(column1, column2, ...)`

生成 Hash。

V1 使用 SHA-256。

具体二进制编码格式由公共 hashing 模块统一实现，Source 和 Destination 必须使用完全相同的规则。

---

# 7. Source State

## 7.1 keyed 表

状态至少保存：

```text
key
row_hash
```

例如：

```text
100001 → 6f15...
100002 → f18a...
100003 → 09bd...
```

## 7.2 无唯一键表

保存：

```text
row_hash
count
必要的旧行数据
```

用于识别重复行以及后续删除。

---

# 8. Current / Next 双状态设计

为了避免扫描中途异常破坏基线，Source 不直接修改当前状态。

每张表存在：

```text
Current State
Next State
```

开始扫描：

```text
Current State
      │
      ├── 比较
      │
Source MySQL
      │
      └── 写入 Next State
```

如果扫描中途失败：

```text
删除 Next State
Current State 不变
```

重新扫描即可。

只有当：

1. 整张表扫描完成；
2. 所有 Chunk 生成成功；
3. 所有 Chunk 上传成功；
4. Manifest 上传成功；

之后才允许：

```text
Next State
→
Current State
```

这样源端不会因为断电、程序崩溃等原因丢失同步变化。

---

# 9. Source 扫描

源数据库使用只读账号。

采用流式读取，不允许一次将整张表装入内存。

概念上：

```text
读取一批
↓
计算 key/hash
↓
与 Current State 比较
↓
写 Next State
↓
有变化则写 Delta
↓
继续下一批
```

默认同一时间只扫描一张大表，以降低源数据库负载。

V1 不依赖：

* update_time；
* create_time；
* 最大 ID。

这些字段只能作为业务数据，不作为同步正确性的依据。

---

# 10. Run

每完成一次表扫描，产生一个独立 Run。

例如：

```text
table:
std_xxx

run_id:
20260914-00000125

base_run_id:
20260914-00000124
```

Run 代表：

> 本次扫描观察到的整张表状态。

不同表维护独立 Run 序号。

一张超大表不会阻塞其他表的版本。

---

# 11. Chunk

一个 Run 的变化拆成多个 Chunk。

例如：

```text
run-125/
├── chunk-000001.data.zst
├── chunk-000002.data.zst
├── chunk-000003.data.zst
└── manifest.json
```

Chunk 同时受：

* 最大记录数量；
* 最大未压缩数据量；

限制。

具体参数通过性能测试确定，不在设计阶段写死。

文件默认使用 zstd 压缩。

---

# 12. Delta 数据

Delta 至少支持：

```text
INSERT
UPDATE
DELETE
```

对于 keyed 表：

```text
UPDATE
=
key + new row
```

对于 DELETE：

```text
key
```

对于无唯一键表：

变化可以表达成：

```text
DELETE old row / row hash
INSERT new row
```

无需人为构造不存在的主键。

---

# 13. Source 流水线和 200GB 空间限制

Source 不等待整个 Run 的全部文件生成完成以后再上传。

流程：

```text
扫描
↓
生成 Chunk 1
↓
HTTP 上传
↓
Relay 返回可靠接收成功
↓
删除本地 Chunk 1

继续扫描
↓
生成 Chunk 2
↓
上传
↓
删除
...
```

因此即使一个 Run 最终有几十 GB 或上百 GB：

Source 也只需要保留少量当前文件。

必须设置磁盘保护。

当剩余磁盘低于安全阈值：

* 暂停生成新 Chunk；
* 优先上传已有 Chunk；
* 禁止耗尽 Windows 系统盘。

---

# 14. Manifest

一个 Run 扫描全部完成后，最后生成 Manifest。

示例结构：

```json
{
  "protocol_version": 1,
  "table": "std_xxx",
  "run_id": "20260914-00000125",
  "base_run_id": "20260914-00000124",
  "type": "DELTA",
  "source_row_count": 7123456,
  "insert_count": 1234,
  "update_count": 5678,
  "delete_count": 12,
  "chunks": [
    {
      "sequence": 1,
      "file": "chunk-000001.data.zst",
      "rows": 50000,
      "sha256": "..."
    }
  ],
  "verification": {
    "..."
  }
}
```

Manifest 永远最后上传。

因此：

```text
只有 Chunk，没有 Manifest
```

代表：

> Run 尚未完成，不能应用。

---

# 15. Source 异常情况

如果扫描在中途失败：

已上传的 Chunk 可能已经通过单向链路进入 Destination。

由于没有 Manifest：

Destination 不应用这些文件。

Source 保留原 Current State。

下一次重新建立新 Run。

Destination 可以定期清理长时间没有对应 Manifest 的孤立 Chunk。

---

# 16. Destination 文件目录

建议：

```text
data/
├── incoming/
├── pending/
├── processing/
├── applied/
└── failed/
```

FTP 客户端将文件放入：

`incoming/`

Destination Agent 接管后移动到自己管理的目录。

FTP Server 上原文件被删除，不影响 Destination 重试。

---

# 17. Destination Run 处理

收到 Manifest 后：

1. 检查所有要求的 Chunk 是否存在；
2. 检查文件大小；
3. 校验 SHA256；
4. 检查 Run 顺序；
5. 检查 base_run_id；
6. 全部通过后开始 Apply。

缺少 Chunk：

```text
WAITING_FILES
```

文件损坏：

```text
FILE_ERROR
```

缺少前一个 Run：

```text
WAITING_PREVIOUS_RUN
```

不会错误地跳过版本。

---

# 18. Destination MySQL 元数据

建立：

```text
airgap_sync_meta.runs
airgap_sync_meta.chunks
airgap_sync_meta.table_versions
```

至少记录：

### runs

```text
table_name
run_id
base_run_id
run_type
status
source_row_count
started_at
finished_at
error
```

### chunks

```text
table_name
run_id
chunk_sequence
sha256
status
applied_at
```

### table_versions

```text
table_name
current_run_id
verified_at
verification_status
```

用于：

* 防止重复应用；
* 恢复；
* 排查；
* 判断当前目标版本。

---

# 19. Chunk 幂等

Destination 处理 Chunk 前查询同步元数据。

如果：

```text
run_id + chunk_sequence + sha256
```

已经成功：

直接跳过。

因此即使文件重复出现，也不会重复写入目标业务数据。

---

# 20. MySQL Apply

一个 Chunk 不作为一个超大事务。

例如：

```text
Chunk
50,000 rows
```

可以进一步拆成：

```text
DB Batch 1
DB Batch 2
DB Batch 3
...
```

每个数据库 Batch 独立事务。

具体每批行数通过性能测试确定。

---

# 21. keyed 表写入

INSERT / UPDATE 优先使用：

```text
INSERT ... ON DUPLICATE KEY UPDATE
```

前提是目标表本身存在对应唯一约束。

如果同步使用的是逻辑键但目标表没有 UNIQUE，则 Destination 按该逻辑键执行：

```text
SELECT / UPDATE / INSERT
```

或者使用 staging 方式批量合并。

具体方式按实际表结构选择。

DELETE 使用配置的逻辑键。

---

# 22. 无唯一键表写入

无唯一键表：

* INSERT：直接插入；
* DELETE：根据旧行完整内容匹配并删除相应数量；
* UPDATE：表现为旧行删除 + 新行插入。

这类表同步性能通常低于 keyed 表。

因此配置前应尽可能验证是否存在可靠逻辑键。

---

# 23. 数据一致性验证

每个完整 Run 附带源端校验摘要。

Destination Apply 完成后，使用相同算法计算目标表摘要。

校验至少包含：

```text
row_count
bucket digest
```

结果：

```text
VERIFIED
MISMATCH
```

只有：

`VERIFIED`

才表示该版本真正同步完成。

---

# 24. 分桶校验

为了避免一个 Hash 不一致后无法定位问题，使用固定分桶。

每个桶记录：

```text
bucket_id
row_count
digest
```

Source 生成摘要。

Destination 独立计算。

如果：

```text
所有 bucket 一致
```

结果：

`VERIFIED`

否则：

`MISMATCH`

并记录不一致的 Bucket。

具体桶数量属于性能参数，通过测试确定。

---

# 25. Snapshot / 全量修复

Run 支持两种类型：

```text
DELTA
SNAPSHOT
```

DELTA：

正常增量同步。

SNAPSHOT：

* 第一次同步；
* 人工全量修复；
* 状态无法恢复。

Snapshot 同样拆 Chunk。

Destination 应尽量先写入 staging table。

完整导入并验证通过后，再替换正式数据。

避免全量同步失败导致正式目标表处于半完成状态。

---

# 26. MISMATCH 人工修复

Destination 检测：

```text
MISMATCH
```

只需要显示：

```text
表：
std_xxx

Run：
125

状态：
数据不一致

处理建议：
对该表执行全量重新同步
```

操作人员在 Source 执行：

```text
生成 XXX 表全量修复
```

Source 创建：

```text
SNAPSHOT Run
```

随后仍然走原有单向文件链路。

不设计复杂自动反向修复协议。

---

# 27. 调度

Source：

* 表级调度；
* 默认同一时间只扫描一个大型表；
* 每张表可独立配置扫描周期；
* 上一个 Run 还没有成功完成时，不重复并发扫描同一张表。

Destination：

* 自动监控 incoming；
* 自动处理完整 Run；
* 同一张表严格按 Run 顺序执行。

---

# 28. 日志

日志必须包含：

```text
table
run_id
chunk
阶段
耗时
读取行数
变化行数
文件大小
上传结果
数据库写入数量
验证结果
错误
```

日志中不得记录完整敏感业务数据。

---

# 29. 配置

统一 YAML 配置。

主要包括：

```yaml
role: source

mysql:
  ...

paths:
  ...

tables:
  ...

scan:
  ...

chunk:
  ...

storage:
  ...

http:
  ...
```

数据库密码等敏感信息支持通过环境变量提供。

---

# 30. Source 状态

主要状态：

```text
IDLE
SCANNING
UPLOADING
FINALIZING
COMPLETED
FAILED
DISK_PRESSURE
```

---

# 31. Destination 状态

主要状态：

```text
WAITING_FILES
READY
APPLYING
VERIFYING
VERIFIED
MISMATCH
FAILED
```

---

# 32. V1 不实现

V1 不实现：

* Binlog CDC；
* Redis；
* RabbitMQ/Kafka；
* Kubernetes；
* WebSocket；
* 分布式锁；
* 双向通信；
* 远程 ACK；
* 自动反向修复；
* 多节点集群；
* 数据历史版本查询；
* 复杂 Web 管理后台。

如果后期确有需要再增加。

---

# 33. V1 完成后的运行模型

正常情况下：

```text
Source 定时扫描
      ↓
发现变化
      ↓
生成 Chunk
      ↓
上传
      ↓
Source 更新自己的 Current State

单向传输

      ↓
Destination 自动接收
      ↓
Apply
      ↓
Verify
      ↓
VERIFIED
```

运维人员只关注：

```text
绿色：VERIFIED
黄色：处理中
红色：MISMATCH / FAILED
```

出现红色时才进行人工处理。

这就是 V1 的完整技术边界。
