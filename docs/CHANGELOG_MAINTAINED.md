# 个人维护版变更

## 1.2.17-codex.1 — 2026-10-06

基于 upstream v1.2.17 / `a2aef031`。

### Responses 与工具闭环

- 补齐 function 参数事件的 item_id，首帧与后续参数分片全部保留。
- 流式 added/done 和最终 output 复用一致的 item ID、call ID。
- custom/apply_patch 发解包后的原始文本，delta 拼接与最终 input 一致。
- 截断的工具参数标记 incomplete，不发可执行完成事件；截断响应使用
  response.incomplete。名字晚于参数到达时先缓存，确定类型后再声明工具项。
- 支持 namespace 中的 function/custom 声明、选择器与历史；上游使用符合
  通用字符集/长度限制的 wire 名，客户端还原原始 name/namespace。
- 不支持的内建 web_search/tool_search 等声明明确返回 400，不能据此宣称
  能执行服务端内建工具。custom 输入在收尾前统一下发，工具输入不再逐片展示。

### 签到与生命周期

- 活动窗口统一为北京时间 10:00，账号保存 lastCheckinWindow。
- 自动签到/额度刷新独立开关、进程级完全暂停、每窗口有界尝试与失败续跑。
- 排队重启等待旧 worker 退出，停止可以取消排队交接，避免静默停摆或双 worker。
- 网关关闭时停止调度器；隔离配置关闭桌面账号发现与全部自动任务。

### 维护与验证

- 保留上游加解密和泄漏护栏测试，增加协议、账号窗口、调度生命周期回归。
- 本机 HTTP 测试使用实际 Handler 和两轮工具结果回传，上游为合成桩。
- fixture 不再自动搜索用户目录；统一测试入口隔离数据并阻断外网。
- CI 覆盖 Ubuntu/Windows 和 Python 3.9/3.13，明确输出 SKIP。
- 修正旧离线测试中的宿主时区假设，以及 Windows 测试输出编码。

本变更表描述实现与回归范围，真实上游/CPA 接入结果另行记录。
