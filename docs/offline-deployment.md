# 离线部署指南（Offline Deployment）

面向完全无互联网环境的 Release / Install / Upgrade / Rollback 流程。

目标环境：

| 端 | 系统 | 说明 |
| --- | --- | --- |
| Source | Windows Server 2019 x64 | 管理员预装 CPython 3.13.15 x64，并加入系统 PATH |
| Destination | CentOS 7.9 x86_64（glibc 2.17） | python-build-standalone portable Python |

部署机器不需要 git、uv、gcc、make、PyPI 或任何网络访问。

---

## 1. 联网开发机：构建 Release Bundle

```bash
# checkout 目标 commit, 确认工作区干净
git checkout <release-commit>
git status --porcelain        # 必须为空

uv run python scripts/offline/build_release.py
```

输出（默认不含 pytest；如需在隔离环境跑真实 MySQL 集成测试，加
`--include-tests`）：

```text
dist/offline/
├── airgap-sync-<version>-<gitsha>-windows-amd64.zip     # 传给 Windows Source
├── airgap-sync-<version>-<gitsha>-centos7-x86_64.tar.gz # 传给 CentOS Destination
└── release-summary.json
```

构建脚本保证：

- 依赖版本完全来自当前 `uv.lock`（`uv export --locked` 并与 lock 闭包交叉核对）；
- 两个平台的 wheelhouse 只含 binary wheel，任何依赖只有 sdist 时构建失败；
- Linux wheelhouse 只接受 CentOS 7 兼容 wheel：每个非 universal wheel 的
  platform tag 中至少有一个 glibc <= 2.17 兼容 tag（manylinux2014 / 2_17 /
  2_12 / 2010 / 1 / 2_5）。仅有 generic `linux_x86_64`、`musllinux` 或
  `manylinux_2_18+` tag 的 wheel 会让构建失败（它们对 glibc 2.17 没有 ABI
  承诺，加载即崩）；
- Linux runtime（python-build-standalone）与上游 `SHA256SUMS` 逐一校验；
- Windows bundle 不含 Python installer 或 runtime archive，只包含 Windows
  CPython 3.13 wheelhouse；
- `release.json` / `release.env` 中的 schema 版本直接读取
  `airgap_sync.source.state.SCHEMA_VERSION` 与
  `airgap_sync.destination.mysql.METADATA_SCHEMA_VERSION`（单一来源）。

Python runtime 版本集中定义在 `scripts/offline/runtime-versions.json`，
升级 Python 只改这一个文件。

把对应压缩包通过 U 盘 / 单向光闸等渠道传入隔离环境。记录
`release-summary.json` 中的 archive SHA256 以便人工比对。

## 2. Bundle 内容

```text
airgap-sync-release/
├── release.json          # Release manifest（版本、git sha、schema 版本、平台）
├── release.env           # 同内容的 shell-sourceable 版本（构建时同步生成）
├── SHA256SUMS            # 全部 payload 的 SHA256（不含自身）
├── runtime/              # 仅 Linux bundle：portable Python runtime
├── app/                  # airgap_sync-<version>-py3-none-any.whl
├── wheelhouse/           # 锁定版本的依赖 binary wheels
├── test-wheelhouse/      # 仅 --include-tests：pytest 及其依赖
├── tests/                # 仅 --include-tests
├── config/               # source.example.yaml / destination.example.yaml
├── service/              # systemd unit 示例 + Windows worker wrapper
├── airgap-sync-deploy.sh   # Linux 部署脚本
├── airgap-sync-deploy.ps1  # Windows 部署脚本
├── release_manifest.py   # 部署脚本使用的共享 helper
└── OFFLINE-DEPLOYMENT.md # 本文档副本
```

Bundle 不含任何真实密码 / Token；示例配置只引用 `password_env` /
`token_env` 环境变量名。

---

## 3. Windows Server 2019（Source）

Airgap Sync 不负责安装 Windows Python。管理员必须先安装 **Python 3.13.15
x64**，并确保 `python.exe` 可通过系统 `PATH` 发现。部署前检查：

```powershell
python --version
python -c "import struct; print(struct.calcsize('P')*8)"
```

输出必须分别为 `Python 3.13.15` 和 `64`。然后以管理员 PowerShell 运行
（默认路径写入 `C:\Program Files` 与
`C:\ProgramData`；所有路径都可用参数覆盖）。

```powershell
# 解压 zip 到任意位置（U 盘、D:\、C:\temp 均可），进入目录
.\airgap-sync-deploy.ps1 -Action Verify
.\airgap-sync-deploy.ps1 -Action Install
# 可选参数：-InstallRoot / -ConfigRoot / -DataRoot
```

Install 顺序：Verify bundle → 平台检查 → 检查已有部署 → 从 PATH 严格检查
Python 3.13.15 x64、标准库模块和真实临时 venv → 独立 release 目录 + venv →
离线 pip 安装（`PIP_NO_INDEX=1 --no-index --find-links wheelhouse`）→ import
smoke test → `airgap-sync --version` → 写 `installed.json` → 建立
`current` directory junction。**安装后不会自动启动 worker。**

Upgrade 也使用 PATH 中严格匹配 release 要求的 Python 创建新 release venv；
旧 release venv 保持不动。Rollback 直接复用目标 release 已安装的 venv，
不检查当前 PATH Python。`-InstallRoot "D:\AirgapApp"` 等现有自定义安装路径
继续支持。

**Install 仅用于首次安装。**机器上已有 `current`（指向任何 release）时：

- `current` 已指向本 bundle 的同一 release → 幂等 no-op（不重装、不切换）；
- `current` 指向其他 release → `INSTALL_BLOCKED_EXISTING_DEPLOYMENT`，
  提示 `Existing deployment detected. Use upgrade instead of install.`
  不会安装新 release、不会修改 `current`、也不会自动转成 upgrade——
  必须由操作员显式执行 `upgrade`（从而保留 worker 停止、config/SQLite
  备份、schema 兼容检查与失败恢复的全部保障）。

首次初始化配置（已存在则拒绝覆盖；升级永不修改现有 YAML）：

```powershell
.\airgap-sync-deploy.ps1 -Action InitConfig -Role source
notepad C:\ProgramData\AirgapSync\config\source.yaml
```

设置 secret 环境变量（值不落盘到部署目录）：

```powershell
setx AIRGAP_SYNC_MYSQL_PASSWORD "..."    # mysql.password_env 指定的名字
setx AIRGAP_SYNC_UPLOAD_TOKEN "..."      # relay.token_env 指定的名字
```

手工验证（第一轮真实测试不部署 service）：

```powershell
$cli = "C:\Program Files\AirgapSync\current\venv\Scripts\airgap-sync.exe"
& $cli config validate --config C:\ProgramData\AirgapSync\config\source.yaml
& $cli source check     --config C:\ProgramData\AirgapSync\config\source.yaml
& $cli source relay-check --config C:\ProgramData\AirgapSync\config\source.yaml
& $cli source sync --table SMALL_TABLE --config C:\ProgramData\AirgapSync\config\source.yaml
```

日常状态查询：

```powershell
.\airgap-sync-deploy.ps1 -Action Status            # 部署视角
.\airgap-sync-deploy.ps1 -Action VerifyInstalled   # 当前 release 自检（不连库）
```

### Windows 目录布局（默认）

```text
C:\Program Files\AirgapSync\
├── releases\<release-id>\venv\      # 每个 release 独立 venv + installed.json
└── current                          # junction -> releases\<release-id>

C:\ProgramData\AirgapSync\config\    # YAML 配置（不属于任何 release）
D:\airgap-sync\data\                 # paths.data_dir 指定的数据目录
```

---

## 4. CentOS 7.9（Destination）

```bash
# 解压 tar.gz 到任意位置（/mnt/usb、/tmp 均可），进入目录
./airgap-sync-deploy.sh verify
sudo ./airgap-sync-deploy.sh install
# 可选参数：--install-root /opt/airgap-sync  --config-root /etc/airgap-sync
#           --data-root /var/lib/airgap-sync
```

`verify` / `install` / `upgrade` / `rollback` / `init-config` 首先通过
checksum gate：确认 `SHA256SUMS` 与 `release.env` 存在，**在不 source
`release.env` 的情况下** `sha256sum --check --strict` 全量校验，成功后才
source `release.env` 并继续（后续 Verify 阶段会重复校验一次，成本可接受）。

`verify` / `install` 还检查：

```text
uname -m                  # 必须 x86_64
getconf GNU_LIBC_VERSION  # 必须 >= 2.17（bundle manifest 中的 minimum_glibc）
```

不满足立即失败。绝不动 `/usr/bin/python`、`/usr/bin/python2`，也不清空
incoming 目录（incoming 由外部 FTP Client 拥有）。

Linux 端 Install 与 Windows 相同：仅首次安装。已有 `current` 指向其他
release 时以 `INSTALL_BLOCKED_EXISTING_DEPLOYMENT` 拒绝，指向同一 release
时幂等 no-op。

Portable Python 来自 python-build-standalone
（`x86_64-unknown-linux-gnu` / `install_only_stripped`，glibc >= 2.17
兼容），解压到 `/opt/airgap-sync/runtimes/python-3.13.x/`，安装后执行
`ssl / sqlite3 / ctypes / zlib / venv` import 检查并打印 Python /
OpenSSL / SQLite 版本（这些是 portable runtime 自带版本，与系统库无关）。

初始化 Destination 配置并验证：

```bash
sudo ./airgap-sync-deploy.sh init-config --role destination
sudo vi /etc/airgap-sync/destination.yaml
export AIRGAP_SYNC_MYSQL_PASSWORD="..."   # mysql.password_env 指定的名字

CLI=/opt/airgap-sync/current/venv/bin/airgap-sync
$CLI config validate --config /etc/airgap-sync/destination.yaml
$CLI destination check --config /etc/airgap-sync/destination.yaml
$CLI destination process --run RUN_ID --config /etc/airgap-sync/destination.yaml
$CLI destination stats --config /etc/airgap-sync/destination.yaml
```

### Linux 目录布局（默认）

```text
/opt/airgap-sync/
├── runtimes/python-3.13.x/           # portable Python, 各 release 共享
├── releases/<release-id>/venv/       # 独立 venv + installed.json
└── current -> releases/<release-id>  # 原子 symlink（ln + mv -T）

/etc/airgap-sync/         # YAML 配置 + backups/
/var/lib/airgap-sync/     # 数据目录（含 backups/ 下的 SQLite 备份）
```

---

## 5. 第二次升级

```text
1. 联网开发机构建新 Release Bundle 并传入隔离环境
2. 停止 worker
3. verify 新 bundle
4. upgrade
5. verify-installed
6. 启动 worker
```

CentOS（有 systemd service 时）：

```bash
./airgap-sync-deploy.sh verify
sudo ./airgap-sync-deploy.sh upgrade --service-name airgap-sync-destination.service
./airgap-sync-deploy.sh verify-installed
sudo systemctl start airgap-sync-destination
```

没有 service manager 协调时必须显式确认 worker 已手工停止，否则拒绝升级：

```bash
sudo ./airgap-sync-deploy.sh upgrade --assume-worker-stopped
```

Windows 等价：

```powershell
.\airgap-sync-deploy.ps1 -Action Upgrade -ServiceName <name>          # NSSM 等 wrapper
.\airgap-sync-deploy.ps1 `
  -Action Upgrade `
  -InstallRoot "D:\AirgapApp" `
  -ConfigRoot "C:\ProgramData\AirgapSync\config" `
  -DataRoot "D:\airgap-sync\data" `
  -ScheduledTaskName "Airgap Sync Source Worker"
.\airgap-sync-deploy.ps1 -Action Upgrade -AssumeWorkerStopped         # 手工停止
```

长期生产环境优先传 `-ScheduledTaskName`（或实际 service 名），使脚本执行
Stop Scheduled Task → backup → install → schema guard → switch current →
restart task。`-AssumeWorkerStopped` 只用于操作员明确自行管理、且已经停止
worker 的场景。

升级安全设计：

- **side-by-side**：新版本安装到 `releases/<新 release-id>`，旧 release
  目录原样保留；
- 切换 `current` 前依次完成：config 备份（`<config-root>/backups/<ts>/`）、
  Python 前置条件准备（Windows 严格检查 PATH Python；Linux 安装 bundled
  runtime）、Source SQLite
  `state/meta.db` 备份（SQLite backup API，写入
  `<data-root>/backups/<ts>/meta.db`；Destination 端不做 mysqldump，
  数据库级备份由 DBA / 环境备份体系负责）、新 venv + 离线安装 + smoke
  test、**schema 兼容检查**（新 release 的 schema 低于当前 release 即失败）；
- 任何发生在切换 `current` 之前的失败都不影响旧版本；脚本停止过的 service
  会在失败时尝试重新启动；
- 同一 release 重复 upgrade 幂等：已完成 → no-op，未完成（无
  `installed.json`）→ 安全重建；
- Linux 同一 Python patch 版本复用已有 runtime；校验
  `release.env` 与 `release.json` 关键字段一致（`check-env`）。

### Python runtime patch 升级（3.13.x → 3.13.y）

Linux bundle 的 Python patch 版本变化时（例如 `3.13.15 → 3.13.16`），
仍按原有顺序安装 side-by-side portable runtime：

```text
verify bundle → 平台检查 → stop worker → 确定 current → config 备份
→ 安装目标 runtime（side-by-side，runtimes/python-3.13.16/）
→ SQLite backup（此时目标 runtime 已存在，backup 不依赖未安装的解释器）
→ 新 release venv → schema 兼容检查 → 切换 current
```

在 SQLite backup 之前安装 runtime 是安全的：安装 side-by-side runtime
不会运行 Airgap Sync 应用代码，也不会迁移 metadata。所有触碰应用状态的
步骤（新 venv、schema guard、current 切换）仍严格发生在 backup 之后。
旧 runtime 目录（`runtimes/python-3.13.15/`）保留不删，回滚到旧 release
时旧 venv 继续可用。

Windows release 当前严格 pin `3.13.15`。Upgrade 先验证 PATH 中正是要求的
x64 Python，并通过临时 venv smoke，再停止 worker、备份并创建新 release
venv；Airgap Sync 不安装或保留共享 Windows runtime。Rollback 只复用已完整
安装的目标 release venv，因此不依赖 PATH Python。

SQLite backup 失败时（如 `meta.db` 损坏）：升级终止，`current` 仍指向旧
release，新 release 目录不会创建。

### Windows current 切换的失败恢复

Windows 端切换 `current` 使用「临时 junction + GUID 唯一名」：

```text
创建 .current.new.<guid> junction 并确认可解析
→ 旧 current 改名为 .current.old.<guid>
→ .current.new.<guid> 改名为 current 并确认存在
→ 删除 .current.old.<guid>（仅清理；失败只记 WARNING:
   retired junction cleanup deferred，不回滚已成功的切换）
```

删除前脚本验证该路径是目录型 reparse point，再调用
`.NET Directory.Delete(path, false)`。`false` 表示非递归，只解除 junction
本身，不枚举或删除 target 内的任何文件，也不会出现 `Remove-Item` 的
“item has children / Recurse not specified”交互确认。若类型异常或清理失败，
已完成的 active `current` 切换仍保持成功并输出 warning。

若最后一步改名失败：自动把 `.current.old.<guid>` 改回 `current` 恢复旧
指针后抛错，upgrade/rollback 返回失败，`current` 仍指向原 release。

## 6. Rollback

必须显式指定目标版本，不自动猜测 previous：

```bash
sudo systemctl stop airgap-sync-destination        # 或 --assume-worker-stopped
sudo ./airgap-sync-deploy.sh rollback --to-release 0.1.0-cc8e17a --assume-worker-stopped
./airgap-sync-deploy.sh verify-installed
sudo systemctl start airgap-sync-destination
```

Windows：

```powershell
.\airgap-sync-deploy.ps1 -Action Rollback -ToRelease "0.1.0-cc8e17a" -AssumeWorkerStopped
.\airgap-sync-deploy.ps1 -Action VerifyInstalled
```

规则：

- 只修改 `current` 指向，新版本目录保留用于诊断；
- **schema 降级硬阻断**：若当前 release 的 source/destination schema 高于
  回滚目标（例如当前 5、目标 4），ROLLBACK BLOCKED——新版本可能已迁移
  metadata。两端 schema 都不低于当前时才允许 code rollback；
- 目标 release 必须存在且有 `installed.json` 完成标记。

## 7. Worker / Service 部署（第一轮验证之后）

第一轮真实验证**不启动任何 worker**：只手工 `source sync --table
SMALL_TABLE`，确认 `DELIVERED → FTP → VERIFIED` 全链路成功后，再部署
service。Bundle 内置示例：

```text
service/airgap-sync-source.service.example
service/airgap-sync-destination.service.example
service/run-source-worker.cmd
service/run-source-worker.ps1
```

systemd unit 始终调用 `/opt/airgap-sync/current/venv/bin/airgap-sync`，
Windows wrapper 始终调用 `<InstallRoot>\current\venv\Scripts\airgap-sync.exe`，
因此升级只切换 `current`，unit / wrapper 无需修改。项目不依赖 NSSM；
如用 NSSM 或 Windows Service wrapper，把 service 示例中的程序路径指向
`run-source-worker.cmd` / `.ps1` 即可，升级时用 `-ServiceName` 协调。

### Windows Task Scheduler（推荐）

把 `run-source-worker.ps1` 复制到 `<InstallRoot>`，配置：

```text
Task name:          Airgap Sync Source Worker
Account:            SYSTEM
RunLevel:           Highest
Trigger:            AtStartup
MultipleInstances:  IgnoreNew
Restart on failure: 2 minutes
ExecutionTimeLimit: unlimited
```

Action 示例（按现场路径调整，wrapper 不内置盘符）：

```powershell
powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass `
  -File "D:\AirgapApp\run-source-worker.ps1" `
  -Config "C:\ProgramData\AirgapSync\config\source.yaml" `
  -LogFile "D:\AirgapApp\logs\source-worker.log"
```

计划任务只在开机时启动一个常驻 Source Worker，不要另设“每 7 天”触发。
同步周期与失败重试由配置控制：

```yaml
schedule:
  enabled: true
  delay_after_success: 7d
  retry_after_failure: 6h
```

wrapper 在 PowerShell 5.1 下临时把 native stderr 的 error preference 设为
Continue，将 stdout 与 stderr 一并追加到日志，再立即保存 `$LASTEXITCODE`。
因此 Python 的 INFO/WARNING stderr 不会终止 wrapper，而真正非零 native
exit code 仍原样返回；CLI、config 缺失或日志目录创建失败则明确非零退出。

### CentOS systemd 升级后加载新代码

Upgrade 切换 `current` 后必须重启 worker，Python 才会重新 import 新 release：

```bash
sudo systemctl restart airgap-sync-destination
```

只修改 venv 中 Python 文件不需要 `daemon-reload`。只有 `.service` 文件有
修改时才执行：

```bash
sudo systemctl daemon-reload
sudo systemctl restart airgap-sync-destination
```

### 6117c6d 现场问题与修复

- Windows PowerShell 5.1：`Test-ExternalPython` 的 `python -c` 双重 quoting
  会生成非法 Python 源码；现改为 PowerShell 双引号、Python 单引号。
- Windows current：旧版 retired junction cleanup 可能弹出
  `item has children / Recurse not specified`；现使用上文的非递归 helper。
- 阿里私有云 RDS MySQL 5.6：旧版查询不存在的
  `GENERATION_EXPRESSION`，报 1054；现按 information_schema 字段能力探测，
  缺失时使用不含该列的 SQL。探测数据库错误不会静默降级。
- Windows Source wrapper：native stderr 在 `$ErrorActionPreference=Stop` 下
  会被包装为 `NativeCommandError`；现合并写日志并以 native exit code 判定。

## 8. 安全要点

- bundle 完整性：需要 bundle 的动作（`verify` / `install` / `upgrade` /
  `rollback` / `init-config`）先通过 checksum gate——`SHA256SUMS` 全量
  校验通过后才 source `release.env`（Linux）或消费 manifest（Windows），
  任何 mismatch 立即失败，不会继续安装；
- SHA256SUMS 路径约束：清单中的路径必须是 bundle 内的相对路径，绝对路径、
  盘符（`C:\...`）与 `..` 穿越（`../outside.file`）一律拒绝（共享
  `release_manifest.py` 与 Windows 校验器同等强制）；
- `release.env` 与 `release.json` 一致性：runtime 安装后用
  `release_manifest.py check-env` 复核二者关键字段（release id、git
  commit、版本、python 版本、平台、schema 版本、runtime/wheel 文件名）
  一致，不一致拒绝使用该 bundle；
- secrets 永远不进 bundle：示例配置只含 `password_env` / `token_env`
  变量名，部署脚本不询问也不保存密码；status 等输出不含任何 secret；
- 离线 pip：安装始终 `--no-index --find-links <bundle>/wheelhouse` 且设置
  `PIP_NO_INDEX=1`，即使隔离机 DNS 可用也不会访问公网 PyPI；
- Linux wheel ABI：构建期扫描 Linux wheelhouse，非 universal wheel 缺少
  glibc <= 2.17 兼容 tag 即构建失败（generic `linux_x86_64` / `musllinux`
  / `manylinux_2_18+` 不被接受为唯一 tag）；
- 安装器不碰 MySQL：不建库、不建用户、不改表；安装后只提示下一步命令。

## 9. 离线集成测试（可选）

以 `--include-tests` 构建的 bundle 额外携带 `tests/` 与 pytest wheels
（`test-wheelhouse/`）。在隔离机创建测试 venv 后运行：

```bash
/opt/airgap-sync/runtimes/python-3.13.x/python/bin/python3 -m venv /tmp/testenv
/tmp/testenv/bin/python -m pip install --no-index \
  --find-links <bundle>/wheelhouse --find-links <bundle>/test-wheelhouse pytest
cd <bundle>
/tmp/testenv/bin/python -m pytest tests -m 'not integration'    # 单元测试
AIRGAP_TEST_MYSQL=... /tmp/testenv/bin/python -m pytest tests -m integration
```

## 10. 本修复尚需真实目标环境回归的事项

- Windows Server 2019 PATH Python preflight；
- Windows Server 2019 noninteractive junction cleanup；
- Windows Task Scheduler + SYSTEM 常驻 worker；
- Windows worker stdout/stderr 日志与 native exit code；
- 阿里私有云 RDS MySQL 5.6 Destination；
- CentOS 7.9 systemd upgrade 后重启并 import 新代码；
- 611 万行 Source 数据在 Destination 完整导入并达到 VERIFIED。
- 真实 Defender/EDR 条件下的 WinError 32/33 bounded retry；
- CentOS 7.9 portable Python 的 glibc 2.17 实机运行；
- 完全无 DNS / 无 PyPI 条件下的真实离线 wheelhouse 安装。

自动测试或开发机测试不得表述为上述现场验证已完成。

发现问题优先在联网开发机复现并修正 `scripts/offline/` 后重新出包。
