# Airgap Sync 开发任务 V1

## 开发原则

开发顺序以：

**先证明同步正确 → 再解决大数据量 → 再补可靠性和运维**

为原则。

不要一开始开发完整 UI、复杂调度器或大量优化功能。

---

# Phase 1：项目基础

## T001 项目初始化

建立：

* Python 项目；
* `pyproject.toml`；
* 基础目录结构；
* 日志模块；
* YAML 配置；
* MySQL 连接；
* SQLite 状态库。

完成后应能：

```bash
airgap-sync --version
```

并能够成功读取配置。

### 验收

* Windows 可以运行；
* Source MySQL 可以建立只读连接；
* SQLite 可以初始化；
* 日志正常输出。

---

## T002 表配置模型

实现每张表配置：

```text
table name
mode
key columns
enabled
scan settings
```

支持：

```text
keyed
row_multiset
```

### 验收

能够根据 YAML 正确加载多张表配置并检查字段是否合法。

---

# Phase 2：核心 Diff

## T003 统一数据规范化和 Hash

实现公共：

```text
canonical row encoding
SHA-256
```

正确处理：

* NULL；
* varchar；
* text；
* integer；
* decimal；
* datetime；
* binary。

### 验收

相同数据始终产生相同 Hash。

以下数据必须产生不同结果：

```text
NULL
""
0
"0"
```

Source 和 Destination 使用同一实现。

---

## T004 keyed 表扫描

实现：

```text
MySQL streaming SELECT
```

逐批读取，不一次装入内存。

生成：

```text
Next State
```

并与 Current State 比较。

识别：

```text
INSERT
UPDATE
UNCHANGED
```

### 验收

使用测试表：

* 初次扫描全部 INSERT；
* 第二次不修改数据时变化为 0；
* 修改数据时只识别对应行。

---

## T005 keyed 表 DELETE 检测

完成整表扫描后比较：

```text
Current State
vs
Next State
```

识别源库已不存在的数据。

### 验收

删除测试数据后能够准确产生 DELETE。

---

## T006 Current / Next 状态切换

实现安全状态切换。

必须保证：

扫描失败时：

```text
Current 不变
Next 可删除
```

完整 Run 成功后才：

```text
Next → Current
```

### 验收

在扫描过程中强制结束程序。

重新运行后不能漏掉任何变化。

---

## T007 row_multiset 模式

为真正无唯一键表实现：

```text
row_hash
count
old row payload
```

识别：

* 新增行；
* 删除行；
* 重复行数量变化。

### 验收

包含重复数据的无主键测试表能够正确同步。

---

# Phase 3：文件协议

## T008 Run 模型

实现：

```text
run_id
base_run_id
table
type
status
```

支持：

```text
DELTA
SNAPSHOT
```

每张表维护独立版本。

### 验收

连续扫描能够正确产生：

```text
Run 1
Run 2
Run 3
```

并记录父版本。

---

## T009 Chunk Writer

将变化数据拆分成多个 Chunk。

支持：

* 行数限制；
* 文件大小限制；
* zstd 压缩；
* SHA256。

### 验收

模拟 100 万条变化时：

* 不产生单个超大文件；
* 内存稳定；
* 可以生成多个连续 Chunk。

---

## T010 Manifest

实现 Manifest。

包含至少：

```text
protocol_version
table
run_id
base_run_id
type
row counts
chunk list
chunk SHA256
source verification data
```

Manifest 必须最后生成。

### 验收

只有完整 Run 才能生成合法 Manifest。

---

# Phase 4：Source 上传和磁盘控制

## T011 HTTP Uploader

接入现有 HTTP 文件上传接口。

只有服务器明确返回可靠接收成功后：

允许删除 Source 本地 Chunk。

支持：

* 超时；
* 重试；
* 网络异常恢复。

### 验收

模拟上传失败后：

* 文件不会被删除；
* 恢复网络后能够继续上传；
* 成功后自动释放文件。

---

## T012 流水线生成和上传

实现：

```text
扫描
→ Chunk
→ 上传
→ 删除
→ 继续
```

不能要求整个 Run 全部生成后才开始上传。

### 验收

模拟产生超过 Source 可用缓存空间的数据。

程序仍能持续运行，不需要本地同时保存全部数据。

---

## T013 磁盘保护

实现：

* 剩余空间检测；
* spool 使用限制；
* DISK_PRESSURE 状态。

空间不足时：

```text
停止继续生成
优先上传已有文件
```

### 验收

模拟低磁盘空间时程序不会把系统盘写满。

---

# Phase 5：Destination

## T014 Incoming 文件管理

监控 FTP 客户端落地目录。

Destination 接管文件后管理：

```text
incoming
pending
processing
applied
failed
```

### 验收

FTP 原始文件即使随后被删除，Destination 仍能独立处理和重试。

---

## T015 文件与 Manifest 校验

实现：

* Manifest 解析；
* Chunk 完整性检查；
* SHA256；
* 缺失文件检测；
* Run 顺序检查。

### 验收

以下情况必须拒绝 Apply：

* 文件缺失；
* 文件损坏；
* SHA256 不匹配；
* Manifest 不完整。

---

## T016 目标同步元数据

创建：

```text
airgap_sync_meta
```

实现：

```text
runs
chunks
table_versions
```

### 验收

能够查询：

* 当前表版本；
* 已应用 Run；
* 已应用 Chunk；
* 最近错误。

---

## T017 keyed Delta Apply

实现：

```text
INSERT
UPDATE
DELETE
```

Chunk 内部进一步拆分 DB Batch。

### 验收

源端一组增删改操作通过完整文件流程后：

目标数据正确。

---

## T018 Chunk 幂等

同一个：

```text
run_id
chunk
sha256
```

重复出现时不能再次修改数据。

### 验收

同一 Chunk 投递两次，目标最终数据只变化一次。

---

## T019 row_multiset Apply

实现无唯一键数据：

* INSERT；
* DELETE；
* 重复行数量变化。

### 验收

无主键重复数据表同步结果正确。

---

# Phase 6：一致性校验

## T020 Source Verification Summary

Source 在每次完整扫描时生成：

```text
source row count
bucket summaries
```

并写入 Manifest。

### 验收

同一份数据重复扫描生成相同摘要。

---

## T021 Destination Verification

Destination Apply 完成后计算相同摘要。

比较：

```text
Source
vs
Destination
```

产生：

```text
VERIFIED
MISMATCH
```

### 验收

完全一致：

```text
VERIFIED
```

人工修改目标任意一条数据：

```text
MISMATCH
```

---

# Phase 7：全量同步和人工修复

## T022 Snapshot

实现：

```text
SNAPSHOT Run
```

支持：

* 第一次初始化；
* 单表全量重新同步。

同样：

* 流式；
* 分 Chunk；
* 不生成单个超大文件。

### 验收

空目标数据库能够通过 Snapshot 完整建立数据。

---

## T023 安全全量替换

Destination 接收 Snapshot 后：

先写 staging。

只有：

```text
完整导入
+
校验成功
```

才替换当前正式数据。

### 验收

Snapshot 导入过程中人为中断，原正式数据仍然可用。

---

## T024 人工 Repair 命令

Source 提供简单命令，例如：

```bash
airgap-sync source snapshot --table TABLE_NAME
```

Destination MISMATCH 时明确提示：

```text
需要重新全量同步 TABLE_NAME
```

### 验收

运维人员无需写 SQL 即可完成一次表级修复。

---

# Phase 8：调度和运行

## T025 Source Scheduler

实现简单的表级定时扫描。

要求：

* 同一张表不并发扫描；
* 默认只运行一个大型扫描任务；
* 单表失败不影响其他表。

不引入外部调度系统。

---

## T026 Destination Worker

持续监控 incoming。

自动执行：

```text
发现文件
→ 整理
→ 等待完整 Run
→ Apply
→ Verify
```

---

## T027 状态命令

Source 至少支持查看：

```text
表
当前 Run
扫描状态
变化数量
Chunk 数
上传状态
磁盘状态
```

Destination 至少支持查看：

```text
表
当前版本
Run 状态
Chunk 状态
VERIFIED / MISMATCH
```

CLI 即可。

V1 不要求 Web 后台。

---

# Phase 9：真实数据验证

## T028 候选逻辑键检查工具

提供命令验证：

```text
NULL 数量
总行数
唯一值数量
重复值
```

用于确认：

```text
zjid
id
hh
hh + fyrq
tyshxydm
```

等候选字段是否真的可以作为同步键。

### 验收

最终为每一张正式同步表确定：

```text
keyed
或
row_multiset
```

及对应 key。

---

## T029 大表性能测试

至少针对一张接近真实规模的大表测试：

```text
700万 rows
```

测试：

* 扫描时间；
* Source MySQL 负载；
* Source CPU；
* Source RAM；
* SQLite 状态大小；
* Chunk 总大小；
* 压缩比；
* 上传速度；
* Destination 导入速度；
* Verification 时间。

根据结果再确定：

```text
scan fetch size
chunk rows
chunk bytes
DB batch size
bucket count
```

这些参数不提前拍脑袋固定。

---

## T030 700 万变化压力测试

构造：

```text
700 万条全部发生变化
```

验证：

* Source 不爆内存；
* Source 不需要保存全部 Chunk；
* 200GB 环境能够持续处理；
* Chunk 正常流水上传；
* Destination 小批量落库；
* 最终 VERIFIED。

---

# Phase 10：异常恢复测试

## T031 故障测试

至少测试：

### Source

* 扫描过程中杀进程；
* Chunk 写到一半；
* HTTP 上传失败；
* HTTP 上传成功后重启；
* 磁盘空间不足。

### Destination

* 文件损坏；
* 缺少 Chunk；
* 文件重复；
* Run 乱序；
* MySQL 中断；
* Apply 中杀进程；
* Verification 失败。

### 验收

任何测试都不能出现：

```text
静默漏数据
```

错误必须：

* 自动恢复；
* 或进入明确 FAILED / MISMATCH 状态。

---

# 最小可用版本里程碑

建议不要等 T001-T031 全部完成才第一次运行。

## Milestone 1

完成：

```text
T001-T006
```

能够：

> 源表 → 扫描 → Hash Diff

先证明变化识别正确。

---

## Milestone 2

完成：

```text
T008-T013
```

能够：

> Diff → Chunk → HTTP

先打通 Source。

---

## Milestone 3

完成：

```text
T014-T018
```

能够：

> 文件 → Destination → MySQL

第一次打通完整链路。

---

## Milestone 4

完成：

```text
T020-T024
```

达到：

> 自动一致性验证 + 全量修复

这时系统已经具备正式使用所需的核心能力。

---

## Milestone 5

完成：

```text
T025-T031
```

补齐：

* 自动运行；
* 大数据测试；
* 故障恢复；
* 生产参数。

然后进入正式部署。

---

# 推荐实际开发顺序

第一轮开发不要碰全部 11 张表。

先选择：

```text
一张有明确 PRIMARY KEY 的小表
```

贯通：

```text
Source
→ Diff
→ Chunk
→ HTTP
→ FTP
→ Destination
→ MySQL
→ VERIFIED
```

完整链路跑通后，再接：

```text
一张700万级 keyed 大表
```

最后才处理：

```text
真正没有唯一键的表
```

这是 V1 风险最低、返工最少的开发顺序。
