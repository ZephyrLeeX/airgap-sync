# Monitoring M6 — 阶段观测与进度

M6 在业务调用路径上采集有限的计数和单调时钟耗时，单独写入每 Run 至多 16 KiB 的阶段快照文件。Source Reporter 只读既有 Source 元数据和快照，不扫描业务表；Destination Web/API 只读。旧 Run 不回填新字段，缺失样本显示 Unknown。监控文件读写失败不会改变 Worker 的业务成功、失败、重试或一致性判定。

## 采集点和口径

| 阶段 | 采集位置 | 计数及时间口径 | 可靠总量 |
| --- | --- | --- | --- |
| `source_read_wait` | `scan_table` 逐批调用流迭代器 | `next()` 等待的单调时钟秒数，包含数据库读取与驱动等待；不含编码。只在成功取得批次时推进编码行数 | 无 |
| `source_encode_write` | 每批 `encode_row`、ChunkWriter zstd 写入、摘要更新 | 成功处理的行、编码后含换行的字节、最终 chunk 数；单调时钟秒数包含编码、压缩、文件写入、摘要及批内 Python 开销 | 无 |
| `source_upload_call` | RelayUploader 单文件 `upload()` 调用 | 已确认文件的压缩/附件字节和 chunk 数；调用耗时包含请求、重试、退避及响应校验。`attempts` 是发起请求次数；`retry_bytes` 是重试请求的**名义文件大小之和**，不是实测网卡字节 | 无 |
| `destination_import` | 每个 chunk 的事务成功提交后 | 逻辑完成量包含先前已提交的 chunk；本次处理量只包含本次实际导入的行和压缩字节。秒数只包括本次尝试实际导入的 chunk 调用 | 已验证 Manifest 的预期行、chunk 数 |
| `destination_verify` | DestinationVerifier 回读并计算摘要，每 `verify_fetch_size` 行采样 | 已回读行数和验证调用单调时钟秒数；包含 MySQL 等待、编码与摘要计算 | 已验证 Manifest 的预期行数 |

`source_read_wait` 与 `source_encode_write` 顺序交替；上传线程可与两者同时运行。`source_encode_write` 含压缩调用，无法从当前流式写入接口低开销地可靠拆出纯压缩 CPU 时间。Destination 导入和验证按顺序运行。不得将重叠阶段相加当作 Run 总耗时，M5 的端到端 UTC 事件仍独立显示。相同进程的阶段秒数来自 `time.monotonic()`，文件内 `observed_at` 及跨端事件为 UTC。M5 的负跨端时间差仍以时钟/顺序异常保留。

阶段状态只描述该阶段本身：阶段首次进入 RUNNING 后才有记录；本次执行失败时只将仍为 RUNNING 的阶段置为 INTERRUPTED。不适用的中断请求（阶段不存在、快照属于旧尝试、本次尝试尚未启动该阶段，或该阶段已处于终态）在任何阶段对象被创建、替换或修改之前被拒绝，完全无副作用：不改变内存状态、旧尝试快照、计数、attempt、generation、sequence 或采样基线，后续其他阶段的写入也不会把伪造阶段落盘。COMPLETE 是该阶段的终态，不会被后续阶段错误覆盖；旧尝试的 COMPLETE 一直保留，直到本次尝试真正开始同一阶段；尚未开始的阶段仍为 Unknown。Destination 的验证阶段 COMPLETE 表示摘要计算结束，不表示摘要与 Manifest 匹配，匹配结果仍由独立的 Run 状态决定。新的 Worker 尝试使用新的 attempt 和递增 generation，允许阶段重新开始；旧 attempt 的快照不会被原地改写。状态写入仍经 v4 generation/sequence 排序及每秒限频，监控写入失败不会影响业务成功、重试或一致性。

行、chunk、字节在阶段内分别是本次尝试的处理量或已确认的逻辑完成量。Source 上传 `bytes` 只在 Relay 确认后增加；失败或重复请求只增加 `attempts` 和名义 `retry_bytes`，不会重复增加已确认量。Destination 重启后的新尝试使用新 `attempt` ID，已导入 chunk 从业务元数据确认后计入新的进度，因此完成百分比仍可解释；导入耗时只计本次调用。验证中断时已回读行数是尝试量，不是验证成功量。Manifest 预期量与实际回读不符时保留原计数；超过预期总量则不显示百分比。Source 扫描行总量与上传实际传输字节数均未知，不显示百分比或“纯网络吞吐”。

每个阶段快照保留一次有效采样窗口中本次实际处理的行/字节差与**同一 Run、阶段、执行尝试**的单调时钟间隔；仅间隔大于零、本次有实际处理量且计数未重置才计算窗口平均率；恢复的逻辑完成量只用于百分比，不进入速率分子。新执行尝试首个样本、零时长、计数倒退或乱序样本的速率为 Unknown。窗口速率不是瞬时速度。Source 页面只标 `LAST_OBSERVED`，因为独立 Reporter 周期、Relay 和单向 FTP 延迟不可界定。Destination 本地运行样本 5 秒内标 `RUNNING`，之后为 `LAST_OBSERVED`，超过 15 分钟为 `STALE`；最终快照标 `COMPLETE` 或 `INTERRUPTED`；没有数据标 `UNKNOWN`。进程突然终止而无法写最终状态时，最后 RUNNING 样本自然转为 STALE。

## 存储、协议、升级

Source Worker 写 `<data_dir>/monitor/progress/source`；Destination Worker 写 `<monitor_ingest.db_path 的父目录>/progress/destination`。Destination ingest 将 v3/v4 Source 阶段样本独立合并至同一父目录的 `progress/source`。文件名是四元身份的 SHA256，不包含未经处理的业务文本；内容只有身份、阶段、数值与 UTC 时间，不含 SQL、行内容、凭据或原始错误。每个记录器最多约每秒写一次，阶段转换另外写一次；原子替换，文件体不超过 16 KiB。快照文件数量随 Run 数增长，现场应按既有 Run 保留周期清理超过 35 天且不再需要的阶段文件；不得清理活动 Run。Web GET 不写文件或迁移数据库。Destination 缺少 `monitor_ingest` 配置时阶段文件不写入，UI 显示 Unknown。

严格 v4 JSON 在 v3 阶段字段之外增加 `generation`（持久的执行尝试代数）、`sequence`（阶段样本序号）和本次处理量计数。v1/v2/v3 仍按原契约严格接受；v3 旧样本只能按时间和计数合并，无法可靠解决同秒最终快照或跨尝试乱序。v4 按代数、尝试 ID、序号排序，同尝试计数倒退也拒绝；收到 v4 后，迟到的 v3 不能覆盖它。已有本地快照没有代数时，新记录器从 1 开始；Reporter 只传输已具备 v4 排序字段的阶段，旧阶段在新写入后才进入 v4 传输。时钟仅用于新鲜度显示，不用于 v4 排序。

Run facts 与进度各有持久 keyset 游标。每轮分别读取至多 256 行 Source 元数据、形成至多 20 个候选；进度最多检查并读取 20 个快照、发送 5 个；达到发送上限就停在最后已检查的 Run。上传成功后才推进进度游标，失败后重试；回绕后重访 35 天恢复窗口。缺文件、非法及超预算快照有界跳过。先装入 heartbeat 和至多 20 条 Run facts，再给进度至多 32 KiB；整体限制为 **64 KiB 实际 UTF-8**。进度不阻塞 heartbeat 或 Run facts，也不扫描快照目录。Relay 接收不保证 Destination 已入库。

**先升级 Destination Monitor 与 Destination Worker，后升级 Source Worker 和 Reporter。** 升级前按 M5 方法备份 `monitor.db` 及 WAL；本次不更改 `monitor.db` schema，也不迁移业务 MySQL 元数据。确认外部 Relay/FTP 放行 `airgap-monitor-v4--...json` 和 64 KiB，确认 Source 与 Destination 阶段目录的服务账户权限，再启用 v4 Reporter。旧 Monitor 不能解析 v4，会在宽限后隔离文件。回滚至 M5 Reporter 时阶段传输停止，M5 Run facts 继续；若回滚 Destination Monitor，先停 v4 Reporter 或改回 v2 Reporter。保留阶段文件以便再升级，不要删除或重建 `monitor.db`。在 35 天窗口之外未送达的阶段样本无法恢复。

## 验证与开销

本地 Linux Python 3.12 的可重复测量：`PYTHONPATH=src .venv/bin/python scripts/benchmark_monitor_progress.py`。脚本五轮取中位数，每轮 10,000 次内存更新、100 次强制文件写，以及用 5,000 个合成整数行分别执行有/无进度的 scanner 和 Destination verifier。一次测量得到内存更新增量 **4.76 µs/批**、强制写 **0.077 ms/次**、5,000 行扫描 **20.494 ms 对 19.983 ms（+2.6%）**、5,000 行验证 **16.363 ms 对 16.627 ms（-1.6%，在噪声范围内）**。这是本机小数据微基准，不代表 Windows 磁盘、RDS 或真实大表性能；监控文件写入失败的耗时与现场 Reporter/FTP 延迟仍需验收。

现场应验证：源端不同表大小下的批处理开销；Windows Worker/Reporter 对快照目录的权限；持续运行、重试、重启和突然终止时的状态；Manifest 行数与导入/验证计数差异；Reporter 周期与单向传输延迟；v1/v2/v3 接收与 Relay allowlist；Source/Destination 时钟偏差；目录清理和回滚后旧 Run 仍为 Unknown。自动测试覆盖正常完成、失败隔离、重启计数重置、重复/乱序合并、过期样本、协议边界及 M5 单侧降级；它们不能代替这些现场检查。
