# 个人维护分支

仓库：[weihu353-wq/qoder2api-hub](https://github.com/weihu353-wq/qoder2api-hub)。
上游：[shuishuipingan/qoder2api-hub](https://github.com/shuishuipingan/qoder2api-hub)。
当前上游基线：v1.3.4 / `ae31046ba387392ff6dcc4170a978081c409fdfb`，2026-10-10。
个人维护补丁始于 v1.2.17 / `a2aef03110f3bd603f1a17de6e25e0ff77cc3537`（2026-10-05）；
本次整合保留其协议与调度不变量，并保留上游 MIT 许可与作者说明。
个人版本标识：`1.3.4-codex.1`。运行时版本用于区分上游基线与本分支。
完整改动清单见 [个人维护版变更](CHANGELOG_MAINTAINED.md)。

## 维护范围

本分支保留 Hub 的双区路由、账号轮换、活动平台、额度查询和文本泄漏护栏。
首版重点是让客户端收到完整且前后一致的工具调用，以及让自动签到明确遵循活动窗口。

### 协议验收要求

- 一次工具调用的全部参数分片，包括首帧，都进入完整 JSON 参数。
- 不同工具交错传入时独立累计；流内 added/done 与最终 output 的 item ID 和 call ID 一致。
- `apply_patch` 的 custom delta 拼接为原始补丁文本，最终 input 与它一致。
- function/custom 的执行结果按相同 call ID 回传；命名空间工具还原 name/namespace。
- 参数事件携带 item_id；截断回复用对应 incomplete 终态。
- 正常文本与既有泄漏护栏保持兼容。修复转换链路，不按“命令重复”主观禁止正常复查。

### 调度验收要求

- 以北京时间 10:00 为活动窗口边界，宿主时区不改变窗口日期。
- 自动签到、自动额度刷新可独立关闭；明确的手动动作保持可用。
- 重启读取持久化状态，失败以有界方式恢复；状态查询不触发领取。
- 测试和隔离实例使用独立数据目录，不与正式账号目录共用写入状态。

| 配置 | 默认值 | 作用 |
| --- | --- | --- |
| `QD_SCHEDULER_ENABLED` | 未设置，使用旧 state.enabled | `0` 暂停本进程全部自动任务，保留原持久化 enabled；去掉后恢复 |
| `QD_AUTO_CHECKIN` | `1` | 独立控制自动签到 |
| `QD_AUTO_QUOTA_REFRESH` | `1` | 独立控制自动额度刷新 |
| `QD_CHECKIN_HOURS` | `10` | 北京时间的签到小时，逗号分隔 |
| `QD_KEEPALIVE_HOURS` | `22` | 北京时间的保活小时 |
| `QD_CHECKIN_MAX_ATTEMPTS` | `4` | 每活动窗口自动领取尝试总上限，跨重启保留 |
| `QD_CHECKIN_RETRY_BACKOFF` | `30` | 基础退避秒数 |
| `QD_DESKTOP_DISCOVERY` | `1` | 本机桌面账号发现；隔离实例设 `0` |

账号新增 `lastCheckinWindow` 表示北京时间活动窗口，保留旧 `lastCheckin` 展示时间。
旧记录缺少窗口字段时按旧宿主时间戳推导；跨不同宿主时区搬迁的旧记录须核对，
新领取后写入明确窗口字段。上游批量签到接口没有逐账号结果，因此重试结合领取
状态复核和批次结果，始终受窗口次数上限限制。停止信号在阶段间生效，已经开始的
批量上游请求需结束当前批次。

## 测试

```bash
python tests/run_offline.py
python tests/run_offline.py --verbose
```

入口执行上游 `_test_qoder.py`、`_test_leak_guard.py`、本分支
`tests/_test_responses_protocol.py` 与标准 unittest 套件 `tests/test_*.py`。
每组在单独进程、临时账号与流水目录运行，禁用原生身份探测和自动调度；测试进程
阻断外网连接，只允许本机回环 HTTP 对照。缺少官方协议 fixture 的三条加解密断言会明确显示 SKIP，不能算作通过。
fixture 默认只搜索仓库内目录；需外部合成样本时显式设置 `QD_TEST_FIXTURE_DIR` 后直接
运行 `_test_qoder.py`，不要指向真实账号资料。统一入口刻意不加载外部 fixture。

GitHub Actions 在 Ubuntu / Windows、Python 3.9 / 3.13 上运行相同入口。
离线通过证明转换器与调度逻辑的回归结果，不等同于当前 Qoder/CPA 上游在线验收。

## 来源与设计取舍

| 参考 | 使用的设计 | 取舍 |
| --- | --- | --- |
| [Hub 基线](https://github.com/shuishuipingan/qoder2api-hub/tree/a2aef03110f3bd603f1a17de6e25e0ff77cc3537) | 双区账号、活动签到、工具文本泄漏回读 | 保留完整主体能力，局部修补协议与调度 |
| [Qoder Go 项目](https://github.com/Zhengyuuuui/qoder2api/blob/ae3d42f89b2e23ca3e3a6e1235e1d92e7038f73f/internal/bridge/claude.go) | 按 index 合并工具分片；北京时间 10:00 活动窗口、失败重试 | 在 Python 分支中独立实现和验证，旧 Go 服务作为迁移前基线 |
| [work2api emitter](https://github.com/YuapXc/work2api/blob/1cbfce8ea159f995792cbe2aea0d977f57a874bc/internal/core/protocol/stream_emitter.go) | 参数累积、只发增量、function item 完成事件；独立维护开关 | 最终 ID 一致性由本分支自己的回归检查，参考结构而非整套整合 |
| [cli2api](https://github.com/caigee-cmd/cli2api/tree/d12d390a309320b22d34b543a5a8933d9c8e8539) | 请求粘性、失败切换与协议验收分层 | 使用 Hub 已有会话亲和，先验收协议再验收真实账号，不混为同一个成功指标 |
| [OpenAI 事件定义](https://developers.openai.com/api/reference/resources/responses/streaming-events) | item_id、终态与序列字段 | 以客户端可观察事件验证 |
| [OpenAI namespace 示例](https://developers.openai.com/api/docs/guides/tools-tool-search) | 声明 namespace；结果独立 name/namespace | 支持已声明工具的转换；内建服务端工具不据此推定可执行 |

## 同步上游

本分支保留 `upstream` 指向原作者仓库；`origin` 指向个人 fork。
界面的上游新版本查询只作为基线更新提醒；更新本分支遵循下面的合并与验收流程。

1. 在工作树干净时 `git fetch upstream`，核对 release 与 commit 差异。
2. 建立 `codex/<主题>` 分支合并候选更新，先检查本分支的协议和调度差异。
3. 运行统一离线测试及 GitHub CI，明确 fixture 跳过项。
4. 在独立端口验证真实模型、function/custom 工具闭环、国内额度与签到。
5. 保存版本固定的回滚入口，验收后再发布或切换正式路由。

## 迁移验收

仓库提供 `compose.maintained.yml`：默认只发布回环端口，关闭自动调度及桌面账号
发现，用于隔离验收。它从本仓库源码构建；通过隔离验收后再明确开启调度，并
设置 API Key、管理密码与正式数据挂载。不要以默认密码发布管理面。

部署使用固定版本或 commit；保持管理面只在回环或隧道内可访问。
旧 Go 项目的 accounts/secrets/settings/checkin_history 格式与 Hub 不同，不能直接
把旧目录挂到新服务作为兼容承诺。使用受限运行时迁移映射或重新 OAuth 授权，保留
旧数据及签到历史。新旧服务的自动签到在切换期间只保留一个执行者。

真实 CPA 验收至少覆盖一次文件读取、一次工具结果回传、一次追加任务，以及
`apply_patch` 原文和命名空间工具。余额查询、模型目录 HTTP 200 与生成成功分别记录。
