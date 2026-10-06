"""qoder_scheduler.py —— 后台定时调度器 (Scheduler)

负责常驻后台自动执行：
1. 每日签到 (Daily Checkin)：默认在**北京时间 10:00** 的官方活动窗口为所有
   账号自动签到领积分（小时可由 QD_CHECKIN_HOURS 覆盖，仍按北京时间解释）。
2. 福利包巡检：巡检时刷新额度与套餐快照（可由 QD_AUTO_QUOTA_REFRESH 独立开关）。
3. Token 保活 (Keepalive)：默认北京时间 22:00 集中刷新；另对剩余寿命不足 4
   小时的账号在任意巡检中提前刷新（drt- / jrt- 按 token 前缀路由，PAT 兜底）。
4. 状态持久化与看板展示：暴露状态、执行记录、支持手动立即触发与开关切换。

时区（本轮修复）：所有排程判断固定使用 **UTC+8（北京时间）**，与官方
「每日 10:00（UTC+8）刷新」口径一致，不依赖宿主 Windows/Linux 本地时区。
窗口日 (window day)：北京时间当日 10:00 之前算作**前一天**的窗口，10:00
（含）之后算当日——启动补签标记按窗口日记录，避免 10:00 之后重启仍误判
「今天已经补过」。

配置（优先级：显式参数 > 环境变量 > 调度 state > 内置默认）：
  QD_SCHEDULER_ENABLED=0     完全暂停：不跑任何自动巡检（含 Token 保活）；
                             仅暂停本次进程，**不覆盖** state 里的 enabled，
                             去掉该变量后恢复原状态。默认未设置 = 用 state。
  QD_AUTO_CHECKIN=0/1        自动签到开关（默认 1，保留原运行行为）
  QD_AUTO_QUOTA_REFRESH=0/1  自动额度/套餐刷新开关（默认 1）
  QD_CHECKIN_HOURS=10        自动签到小时（北京时间，逗号分隔，默认 10）
  QD_KEEPALIVE_HOURS=22      Token 保活小时（北京时间，默认 22）
  QD_CHECKIN_MAX_ATTEMPTS=4  同一窗口日自动签到最多执行次数（有界重试）
  QD_CHECKIN_RETRY_BACKOFF=30  失败重试基础退避（秒）
关闭自动签到/额度刷新**不影响**手动触发 (trigger_now) 与看板的「每日签到」按钮。

可靠性（本轮修复）：
  · 失败有界重试：同一次巡检内，只要仍有账号需要签到且上一轮有失败信号，
    就按退避重试；同一窗口日的自动签到总次数有上限，进程反复重启也不会
    无限重放，但一次瞬时失败不会因先打了标记而整天错过。
  · 窗口判定权威：账号显式窗口字段（Account.last_checkin_window /
    lastCheckinWindow）与 Account.can_checkin() 才是权威；旧 lastCheckin 只作
    展示，**不**用日期前缀否决——9 点领到的是上一个窗口，10 点进入新窗口时
    必须还能领，跨时区宿主上本地日期也不代表北京时间窗口日。
  · 崩溃续跑：同窗口已打启动标记但没做完（attempts 已落盘且未用尽、仍有
    账号需要签到）时，重启可续跑剩余次数；从未动手、全部完成或额度用尽
    一律不重复。
  · 执行生命周期可取消：stop() 后循环退出；巡检在各阶段/重试之间检查取消，
    退避等待可被 stop 立即唤醒。
  · 生命周期交接：stop() 紧跟 start()（proxy 的 restart_scheduler）不会清掉
    共享 stop 事件造成双 worker，也不会静默死亡——交给去重的交接线程，先
    join 旧 worker 再起新 worker；排队期间再 stop() 会取消这次重启。
  · 只读安全：status()/状态读取不触发任何签到或刷新。

状态落盘：enabled / 开关 / 小时 / last_run_time / next_run_time / logs /
启动补签窗口日 / 当日自动签到尝试计数写入 <账号目录>/scheduler/state.json。
  · 放在账号目录的**子目录**里：AccountPool.load() 只扫顶层 *.json
    （qoder_accounts.py:1441-1460），子目录不会被误当成账号；
  · accounts/* 已被 .gitignore 忽略，无需新增忽略规则；
  · 旧 state 字段（enabled / startup_claim_date / last_cycle_date / logs）
    继续读写，新字段缺失时按默认值处理，可直接升级。
"""
import datetime
import json
import os
import sys
import threading
import time

import qoder_accounts
import qoder_tasks
from qoder_tasks import set_logger, run_batch_checkin, run_keepalive

# 状态文件相对账号目录的子路径（避免被 AccountPool 读取为账号 JSON）
STATE_SUBDIR = "scheduler"
STATE_FILE_NAME = "state.json"

# 巡回来由：启动自动 / 整点排程 / 手动触发
CYCLE_STARTUP = "startup"
CYCLE_HOUR = "hour"
CYCLE_MANUAL = "manual"

# 排程固定北京时间 (UTC+8)：官方「每日 10:00（UTC+8）刷新」的口径。
BEIJING_TZ = datetime.timezone(datetime.timedelta(hours=8))
# 窗口判定与 qoder_accounts 同源，不在这里复制算法
CHECKIN_WINDOW_HOUR = qoder_accounts.CHECKIN_WINDOW_HOUR_UTC8

DEFAULT_CHECKIN_HOURS = [CHECKIN_WINDOW_HOUR]
DEFAULT_KEEPALIVE_HOURS = [22]
DEFAULT_MAX_CHECKIN_ATTEMPTS = 4
DEFAULT_RETRY_BACKOFF = 30.0
DEFAULT_QUOTA_REFRESH_GAP = 0.5

# 主循环节奏（测试可注入更短的值）
STARTUP_DELAY_SECONDS = 10
POLL_SECONDS = 30
HOUR_DEDUP_SECONDS = 65

_FLAG_OFF = ("0", "false", "no", "off", "disable", "disabled")


def _parse_flag(raw):
    """宽松布尔：0/false/no/off/disable/disabled（大小写不敏感）= 关。"""
    return str(raw).strip().lower() not in _FLAG_OFF


def _parse_hours(raw, default):
    """解析小时列表（逗号/分号/空格分隔）。非法输入回退 default，不抛异常。"""
    hours = []
    for part in str(raw).replace(";", ",").replace(" ", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            hour = int(part)
        except (TypeError, ValueError):
            return list(default)
        if not 0 <= hour <= 23:
            return list(default)
        if hour not in hours:
            hours.append(hour)
    return sorted(hours) if hours else list(default)


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def window_day(ts=None):
    """北京时间「窗口日」(YYYY-MM-DD)，直接复用 qoder_accounts.checkin_window_day。

    当日 10:00 前算前一天、10:00 后算当日；固定 UTC+8，与宿主本地时区无关
    （容器常见 UTC，不能按本地日期判断）。
    """
    return qoder_accounts.checkin_window_day(ts)


class Scheduler(object):
    def __init__(self, pool, state_dir=None, now_fn=None,
                 auto_checkin=None, auto_quota_refresh=None,
                 checkin_hours=None, keepalive_hours=None,
                 max_checkin_attempts=None, retry_backoff=None):
        self.pool = pool
        # 可注入时钟：测试用假时间；生产用真实时钟（排程判断仍固定 UTC+8）。
        self._now_fn = now_fn or time.time
        self._stop_event = threading.Event()
        self._thread = None
        self._run_lock = threading.Lock()
        # 生命周期：stop() 紧跟 start() 的重启必须等旧 worker 真正退出再起新的，
        # 否则清掉共享 stop 事件会让旧 worker 继续跑（双 worker）或无人接手
        # （旧 worker 退出后调度器静默死亡）。_desired_running 记录最后一次
        # start()/stop() 的意图，供交接线程判断排队中的重启是否已被取消。
        self._lifecycle_lock = threading.RLock()
        self._restart_thread = None
        self._desired_running = False
        # 状态目录：优先显式传入（测试）→ 账号池目录 → 环境变量 → 脚本同级 accounts/
        self.state_dir = (state_dir or getattr(pool, "dir", None)
                          or os.environ.get("ACCOUNTS_DIR")
                          or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "accounts"))
        self._state_lock = threading.Lock()
        st = self._load_state()

        # 排程配置：显式参数 > 环境变量 > state > 默认
        self.checkin_hours = self._resolve_hours(
            checkin_hours, "QD_CHECKIN_HOURS", st.get("checkin_hours"),
            DEFAULT_CHECKIN_HOURS)
        self.keepalive_hours = self._resolve_hours(
            keepalive_hours, "QD_KEEPALIVE_HOURS", st.get("keepalive_hours"),
            DEFAULT_KEEPALIVE_HOURS)
        self.auto_checkin = self._resolve_flag(
            auto_checkin, "QD_AUTO_CHECKIN", st.get("auto_checkin"), True)
        self.auto_quota_refresh = self._resolve_flag(
            auto_quota_refresh, "QD_AUTO_QUOTA_REFRESH",
            st.get("auto_quota_refresh"), True)
        self.checkin_max_attempts = self._resolve_int(
            max_checkin_attempts, "QD_CHECKIN_MAX_ATTEMPTS",
            st.get("checkin_max_attempts"), DEFAULT_MAX_CHECKIN_ATTEMPTS)
        self.retry_backoff = self._resolve_float(
            retry_backoff, "QD_CHECKIN_RETRY_BACKOFF",
            st.get("retry_backoff"), DEFAULT_RETRY_BACKOFF)
        self._refresh_all_hours()

        # enabled：旧字段，语义不变（看板开关写这里，重启后仍生效）。
        # QD_SCHEDULER_ENABLED 是**本次进程**的覆盖层，不写回 state，避免
        # 部署用的暂停开关覆盖用户持久化的开关状态。
        self.enabled = bool(st.get("enabled", True))
        self._enabled_override = self._resolve_override("QD_SCHEDULER_ENABLED")

        self.last_run_time = st.get("last_run_time") or None
        self.next_run_time = None
        self.logs = [str(x) for x in (st.get("logs") or [])][-60:]
        # 启动补签：startup_claim_window_day 才是闸门（按窗口日）；
        # startup_claim_date 是旧字段，保留日历日语义以兼容旧 state/看板。
        self.startup_claim_date = str(st.get("startup_claim_date") or "")
        self.startup_claim_window_day = str(
            st.get("startup_claim_window_day") or "")
        self.checkin_attempt_day = str(st.get("checkin_attempt_day") or "")
        self.checkin_attempts = _safe_int(st.get("checkin_attempts"), 0)
        self.last_cycle_date = str(st.get("last_cycle_date") or "")
        self.last_cycle_window_day = str(
            st.get("last_cycle_window_day") or "")
        self._calc_next_fire()
        # 把任务层失败（死端点、上游结构变化）也打进看板日志。
        set_logger(self.log)
        if st:
            self.log("调度器状态已从 %s 恢复（enabled=%s，上次运行 %s）"
                     % (self._state_path(), self.enabled,
                        self.last_run_time or "-"))

    # -- 配置解析 -----------------------------------------------------------
    @staticmethod
    def _resolve_flag(explicit, env_name, state_value, default):
        if explicit is not None:
            return bool(explicit)
        raw = os.environ.get(env_name)
        if raw is not None and str(raw).strip() != "":
            return _parse_flag(raw)
        if state_value is not None:
            return bool(state_value)
        return bool(default)

    @staticmethod
    def _resolve_override(env_name):
        """三态覆盖：未设置 -> None（用持久化状态）；0 -> False；其它 -> True。"""
        raw = os.environ.get(env_name)
        if raw is None or str(raw).strip() == "":
            return None
        return _parse_flag(raw)

    @staticmethod
    def _resolve_hours(explicit, env_name, state_value, default):
        if explicit:
            return _parse_hours(",".join(str(h) for h in explicit), default)
        raw = os.environ.get(env_name)
        if raw is not None and str(raw).strip() != "":
            return _parse_hours(raw, default)
        if isinstance(state_value, (list, tuple)) and state_value:
            return _parse_hours(",".join(str(h) for h in state_value), default)
        return list(default)

    @staticmethod
    def _resolve_int(explicit, env_name, state_value, default):
        for candidate in (explicit, os.environ.get(env_name), state_value):
            if candidate is None or str(candidate).strip() == "":
                continue
            try:
                value = int(str(candidate).strip())
            except (TypeError, ValueError):
                continue
            if value >= 1:
                return value
        return default

    @staticmethod
    def _resolve_float(explicit, env_name, state_value, default):
        for candidate in (explicit, os.environ.get(env_name), state_value):
            if candidate is None or str(candidate).strip() == "":
                continue
            try:
                value = float(str(candidate).strip())
            except (TypeError, ValueError):
                continue
            if value >= 0.0:
                return value
        return default

    def _refresh_all_hours(self):
        self.all_hours = sorted(set(list(self.checkin_hours)
                                    + list(self.keepalive_hours)))

    @property
    def effective_enabled(self):
        """实际是否跑自动巡检：state 开关 + 环境暂停覆盖。"""
        if self._enabled_override is not None:
            return bool(self._enabled_override)
        return bool(self.enabled)

    # -- 时间（固定北京时间） -----------------------------------------------
    def _now(self):
        return float(self._now_fn())

    def _now8(self, ts=None):
        base = self._now() if ts is None else float(ts)
        return datetime.datetime.fromtimestamp(base, BEIJING_TZ)

    def _fmt8(self, ts=None):
        return self._now8(ts).strftime("%Y-%m-%d %H:%M:%S")

    def _window_day(self, ts=None):
        return window_day(self._now() if ts is None else float(ts))

    @staticmethod
    def _calendar_day():
        """旧字段用的宿主本地日历日（保持既有含义，不改语义）。"""
        return time.strftime("%Y-%m-%d")

    def _sleep(self, seconds):
        """可被 stop() 立即唤醒的等待；返回 True 表示完整等待结束。"""
        try:
            wait = max(0.0, float(seconds))
        except (TypeError, ValueError):
            wait = 0.0
        if wait <= 0:
            return not self._stop_event.is_set()
        return not self._stop_event.wait(wait)

    # -- 状态持久化 ---------------------------------------------------------
    def _state_path(self):
        return os.path.join(self.state_dir, STATE_SUBDIR, STATE_FILE_NAME)

    def _load_state(self):
        """读状态文件；不存在/损坏都返回 {}（按全新进程处理）。"""
        try:
            with open(self._state_path(), encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_state(self):
        """原子落盘（tmp + os.replace，同账号文件的写法）。失败只写 stderr。

        注意：本函数**不得**调用 self.log()，否则与 log→_save_state 形成递归。
        """
        with self._state_lock:
            try:
                path = self._state_path()
                os.makedirs(os.path.dirname(path), exist_ok=True)
                tmp = path + ".tmp"
                data = {
                    "enabled": bool(self.enabled),
                    "auto_checkin": bool(self.auto_checkin),
                    "auto_quota_refresh": bool(self.auto_quota_refresh),
                    "checkin_hours": list(self.checkin_hours),
                    "keepalive_hours": list(self.keepalive_hours),
                    "checkin_max_attempts": int(self.checkin_max_attempts),
                    "retry_backoff": float(self.retry_backoff),
                    "last_run_time": self.last_run_time,
                    "next_run_time": self.next_run_time,
                    "startup_claim_date": self.startup_claim_date,
                    "startup_claim_window_day": self.startup_claim_window_day,
                    "checkin_attempt_day": self.checkin_attempt_day,
                    "checkin_attempts": int(self.checkin_attempts),
                    "last_cycle_date": self.last_cycle_date,
                    "last_cycle_window_day": self.last_cycle_window_day,
                    "logs": self.logs[-60:],
                    "updated_at": self._fmt8(),
                }
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(data, fh, ensure_ascii=False, indent=2)
                os.replace(tmp, path)
            except Exception as exc:
                try:
                    sys.stderr.write("[scheduler] 状态落盘失败: %s\n" % exc)
                    sys.stderr.flush()
                except Exception:
                    pass

    def log(self, msg):
        entry = "[%s] %s" % (self._fmt8(), msg)
        self.logs.append(entry)
        if len(self.logs) > 60:
            self.logs = self.logs[-60:]
        # 日志即状态：顺带落盘，看板开关/上次运行时间在重启后仍然可见
        self._save_state()
        try:
            import qoder_proxy
            qoder_proxy.add_log_entry("[调度器] %s" % msg, tag="scheduler")
        except Exception:
            pass

    # -- 生命周期 -----------------------------------------------------------
    def start(self):
        """启动后台 worker；旧 worker 还没退出时排队交接后再起新的。

        proxy 的 restart_scheduler 是 stop() 紧跟 start()：旧 worker 可能正在
        批内执行，stop() 只置位、它还没退出。此时**不能**清共享 stop 事件
        （旧 worker 会继续跑，形成双 worker），也不能直接返回（旧 worker 退出
        后没人接手，调度器静默死亡）。正确做法是交给一个去重的交接线程：
        先 join 旧 worker，再清事件、起新 worker。
        """
        with self._lifecycle_lock:
            self._desired_running = True
            # 已有交接线程在等旧 worker 退出：一律交给它。否则「旧 worker 刚
            # 退出、helper 还没拿到锁」时这里会抢先起一个 worker，随后 helper
            # 再起一个 -> 双 worker。
            if self._restart_thread and self._restart_thread.is_alive():
                self.log("调度器重启已排队，start() 合并到同一交接线程")
                return
            worker = self._thread
            if worker and worker.is_alive():
                if not self._stop_event.is_set():
                    return                  # 正常运行中：保持原 no-op 语义
                helper = threading.Thread(target=self._handoff_restart,
                                          args=(worker,),
                                          name="qd-scheduler-restart",
                                          daemon=True)
                self._restart_thread = helper
                helper.start()
                self.log("旧调度线程尚未退出，已排队重启交接")
                return
            self._spawn_worker_locked()

    def stop(self):
        with self._lifecycle_lock:
            self._desired_running = False
            self._stop_event.set()
        self.log("后台定时调度器已暂停")

    def _spawn_worker_locked(self):
        """（调用方须持有 _lifecycle_lock）清 stop 事件并起一个全新 worker。"""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop,
                                        name="qd-scheduler", daemon=True)
        self._thread.start()
        self.log("后台定时调度器已启动")

    def _handoff_restart(self, worker):
        """交接线程：等旧 worker 退出；期间又被 stop() 取消则不再起新 worker。"""
        try:
            if worker is not threading.current_thread():
                worker.join()
        except Exception:
            pass
        with self._lifecycle_lock:
            self._restart_thread = None
            # 交接期间若有别的 start() 已经起过 worker（self._thread 被替换），
            # 这里不能再起第二个；只有仍指向我们 join 的那个旧 worker 时才接手。
            if self._thread is not worker:
                self.log("重启交接已被其他 start() 接管，跳过重复启动")
                return
            if not self._desired_running:
                self.log("排队中的调度器重启已被 stop() 取消")
                return
            self._spawn_worker_locked()

    def worker_running(self):
        """只读：是否有存活的自动巡检 worker。"""
        return bool(self._thread and self._thread.is_alive())

    def restart_pending(self):
        """只读：是否有排队中的重启交接线程。"""
        return bool(self._restart_thread and self._restart_thread.is_alive())

    def set_enabled(self, value):
        """看板开关入口（兼容旧写法：直接改 enabled 属性亦可）。"""
        self.enabled = bool(value)
        self.log("调度器状态切换为: %s" % ("启用" if self.enabled else "暂停"))
        return self.status()

    def configure(self, auto_checkin=None, auto_quota_refresh=None,
                  checkin_hours=None, keepalive_hours=None,
                  max_checkin_attempts=None, retry_backoff=None):
        """运行时更新调度配置并落盘，供 settings/环境路由联动。

        None = 保持当前值；显式传入的值优先于环境变量与 state。
        返回最新 status()。
        """
        changes = []
        if auto_checkin is not None and bool(auto_checkin) != self.auto_checkin:
            self.auto_checkin = bool(auto_checkin)
            changes.append("auto_checkin=%s" % self.auto_checkin)
        if auto_quota_refresh is not None \
                and bool(auto_quota_refresh) != self.auto_quota_refresh:
            self.auto_quota_refresh = bool(auto_quota_refresh)
            changes.append("auto_quota_refresh=%s" % self.auto_quota_refresh)
        if checkin_hours is not None:
            hours = _parse_hours(",".join(str(h) for h in checkin_hours),
                                 self.checkin_hours)
            if hours != self.checkin_hours:
                self.checkin_hours = hours
                changes.append("checkin_hours=%s" % hours)
        if keepalive_hours is not None:
            hours = _parse_hours(",".join(str(h) for h in keepalive_hours),
                                 self.keepalive_hours)
            if hours != self.keepalive_hours:
                self.keepalive_hours = hours
                changes.append("keepalive_hours=%s" % hours)
        if max_checkin_attempts is not None:
            value = max(1, _safe_int(max_checkin_attempts,
                                     self.checkin_max_attempts))
            if value != self.checkin_max_attempts:
                self.checkin_max_attempts = value
                changes.append("checkin_max_attempts=%d" % value)
        if retry_backoff is not None:
            try:
                value = max(0.0, float(retry_backoff))
            except (TypeError, ValueError):
                value = self.retry_backoff
            if value != self.retry_backoff:
                self.retry_backoff = value
                changes.append("retry_backoff=%.0f" % value)
        self._refresh_all_hours()
        self._calc_next_fire()
        if changes:
            self.log("调度配置更新: " + "，".join(changes))
        else:
            self._save_state()
        return self.status()

    def _run_loop(self):
        # 启动后先等待主服务就绪（可被 stop 唤醒），然后执行初次检查
        self._sleep(STARTUP_DELAY_SECONDS)
        if self._stop_event.is_set():
            return
        if self.effective_enabled:
            try:
                self._execute_cycle("启动初次初始化巡检", CYCLE_STARTUP)
            except Exception as exc:
                self.log("初次巡检异常: %s" % exc)
        else:
            self.log("调度器处于暂停状态（enabled=%s，QD_SCHEDULER_ENABLED=%s），"
                     "跳过自动巡检（含 Token 保活）"
                     % (self.enabled, self._enabled_override))

        while not self._stop_event.is_set():
            self._calc_next_fire()
            now8 = self._now8()
            if self.effective_enabled and now8.minute == 0 \
                    and now8.hour in self.all_hours:
                reason = "整点排程命中 (%02d:00 北京时间)" % now8.hour
                try:
                    self._execute_cycle(reason, CYCLE_HOUR)
                except Exception as exc:
                    self.log("排程执行异常: %s" % exc)
                self._sleep(HOUR_DEDUP_SECONDS)   # 避开当前这一分钟重复触发
            self._sleep(POLL_SECONDS)

    def _calc_next_fire(self):
        if not self.all_hours:
            self.next_run_time = None
            return
        now8 = self._now8()
        next_hour = None
        for hour in self.all_hours:
            if hour > now8.hour or (hour == now8.hour and now8.minute == 0):
                next_hour = hour
                break
        if next_hour is None:
            target = (now8 + datetime.timedelta(days=1)).replace(
                hour=self.all_hours[0], minute=0, second=0, microsecond=0)
        else:
            target = now8.replace(hour=next_hour, minute=0, second=0,
                                  microsecond=0)
        self.next_run_time = target.strftime("%Y-%m-%d %H:%M:%S")

    def trigger_now(self):
        """手动立即触发一次调度检查（显式请求：不受自动开关限制）。"""
        if self._run_lock.locked():
            return {"ok": False, "msg": "已有巡检正在执行，请稍候再试"}
        threading.Thread(target=self._execute_cycle,
                         args=("手动立即触发", CYCLE_MANUAL),
                         daemon=True).start()
        return {"ok": True, "msg": "已触发后台调度执行"}

    def _execute_cycle(self, trigger_reason="周期巡检", cycle_kind=CYCLE_HOUR):
        if not self._run_lock.acquire(blocking=False):
            self.log("跳过本次巡检 (%s)：上一轮仍在执行" % trigger_reason)
            return
        try:
            self._run_cycle(trigger_reason, cycle_kind)
        finally:
            self._run_lock.release()

    def _allow_complement_checkin(self, cycle_kind):
        """启动巡检的补签闸门：**同一窗口日最多补签一次，但可续跑剩余额度**。

        窗口日 = 北京时间当日 10:00 前算前一天、10:00 后算当日，因此 10:00
        之后重启不会误以为「今天已经补过」。仍是 mark-before-act：先落盘再
        动作；窗口已标记且**没动过手/已完成/额度用尽**时一律拒绝，但上次动手
        后没做完（attempts 已落盘且未用尽、仍有账号需要签到）时允许续跑剩余
        次数，进程崩溃重启不会白白浪费当天剩下的重试额度。
        """
        if cycle_kind != CYCLE_STARTUP:
            return True
        window_day_now = self._window_day()
        if self.startup_claim_window_day == window_day_now:
            return self._can_resume_attempts(window_day_now)
        self.startup_claim_window_day = window_day_now
        self.startup_claim_date = self._calendar_day()   # 旧字段：保留日历日语义
        self._save_state()
        return True

    # -- 巡检执行 -----------------------------------------------------------
    @staticmethod
    def _account_window_day(acc):
        """账号显式记录的「已领取窗口日」；老账号/假账号可能没有 -> 返回 ""。

        Account.last_checkin_window（持久化字段 lastCheckinWindow）才是窗口级
        权威。这里兼容几种属性名，但缺失时不做任何日期猜测——旧 lastCheckin
        只是展示用时间戳，不能拿来否决窗口判定。
        """
        for name in ("last_checkin_window", "lastCheckinWindow",
                     "last_checkin_window_day"):
            value = getattr(acc, name, None)
            if value:
                return str(value)[:10]
        return ""

    def _needs_checkin(self, acc):
        """本窗口是否还需要为该账号发起签到。

        权威顺序：账号显式窗口字段 > Account.can_checkin()。
        刻意**不**用旧 lastCheckin 的日期前缀做否决：9 点领到的是上一个窗口
        （lastCheckin 却显示今天），10 点进入新窗口时必须还能领；跨时区宿主
        上本地日期同样不代表北京时间窗口日。
        """
        window_day_now = self._window_day()
        explicit = self._account_window_day(acc)
        if explicit and explicit == window_day_now:
            return False            # 显式窗口记录已覆盖本窗口：权威否决
        try:
            return bool(acc.can_checkin())
        except Exception:
            return False

    def _pending_accounts(self):
        """当前窗口仍需签到的目标账号（enabled 且有 access_token）。"""
        targets = [a for a in (self.pool.accounts if self.pool else [])
                   if a.enabled and a.access_token]
        return [a for a in targets if self._needs_checkin(a)]

    def _can_resume_attempts(self, window_day_now):
        """同窗口已有启动标记时，是否还能用剩余额度续跑。

        仅当本窗口**确实动过手**（attempt_day 匹配且 0 < attempts < 上限）且仍
        有账号需要签到时才允许：崩溃重启可续跑剩余次数，但从未尝试过、全部
        完成或额度用尽都一律拒绝，杜绝无限重放。
        """
        if self.checkin_attempt_day != window_day_now:
            return False
        if not (0 < self.checkin_attempts < self.checkin_max_attempts):
            return False
        return bool(self._pending_accounts())

    @staticmethod
    def _batch_has_failure(res):
        """批次结果是否含失败信号（run_batch_checkin 只给聚合结果）。

        ok=False（全部失败）或日志里有 '!' 行都算失败；「已领取/名额已发完」
        这类不可领取结论不带 '!'，据此不重试。
        """
        if not (res or {}).get("ok"):
            return True
        for line in (res or {}).get("logs") or []:
            if str(line).lstrip().startswith("!"):
                return True
        return False

    @staticmethod
    def _permanently_unavailable(acc):
        try:
            capable, _ = acc.checkin_capability()
        except Exception:
            return False
        return capable is False

    def _begin_checkin_attempt(self, window_day_now, manual):
        """占用一次自动签到尝试额度；手动触发不占预算。"""
        if manual:
            return True
        if self.checkin_attempt_day != window_day_now:
            self.checkin_attempt_day = window_day_now
            self.checkin_attempts = 0
        if self.checkin_attempts >= self.checkin_max_attempts:
            return False
        self.checkin_attempts += 1
        self._save_state()
        return True

    def _run_checkin_phase(self, pending, window_day_now, manual):
        """有界重试地执行签到；返回 (账号数, 本轮真实新增积分)。"""
        if not pending or self._stop_event.is_set():
            return 0, 0
        attempted = 0
        earned = 0
        current = list(pending)
        while current and not self._stop_event.is_set():
            if not self._begin_checkin_attempt(window_day_now, manual):
                self.log("本窗口日自动签到已达上限（%d 次），停止重试；"
                         "下个窗口日恢复" % self.checkin_max_attempts)
                break
            attempted += 1
            self.log("检测到 %d 个账号需要签到，执行自动签到（第 %d 次尝试）..."
                     % (len(current), attempted))
            res = run_batch_checkin(current, gap=1.0, inter_gap=1.0)
            earned += res.get("credit_added") or 0
            failure = self._batch_has_failure(res)
            for line in res.get("logs") or []:
                # run_batch_checkin 给每个账号的日志加了两空格前缀，
                # 直接 startswith 匹配不到，必须先 lstrip。
                stripped = str(line).lstrip()
                if stripped.startswith("✓") or stripped.startswith("!"):
                    self.log(stripped)
            remaining = [a for a in current if self._needs_checkin(a)]
            if not remaining:
                break
            if not failure:
                self.log("剩余 %d 个账号无失败信号（多为已领取/名额已发完），"
                         "本轮不再重试" % len(remaining))
                break
            if attempted >= self.checkin_max_attempts:
                self.log("仍有 %d 个账号未确认签到，已达单次巡检重试上限"
                         % len(remaining))
                break
            if all(self._permanently_unavailable(a) for a in remaining):
                self.log("剩余账号均无签到接口/不可领取，停止重试")
                break
            wait = self.retry_backoff * attempted
            self.log("等待 %.0f 秒后重试 %d 个账号..." % (wait, len(remaining)))
            if not self._sleep(wait):
                break
            current = remaining
        return len(pending), earned

    def _run_cycle(self, trigger_reason="周期巡检", cycle_kind=CYCLE_HOUR):
        self.last_run_time = self._fmt8()
        self.last_cycle_date = self._calendar_day()
        self.last_cycle_window_day = self._window_day()
        self.log("开始执行任务 (%s)..." % trigger_reason)
        if not self.pool or not self.pool.accounts:
            self.log("暂无可用的活跃账号，跳过本次巡检")
            self._save_state()
            return
        if self._stop_event.is_set():
            self.log("巡检已取消（调度器已停止）")
            return

        now8 = self._now8()
        window_day_now = self._window_day()
        manual = (cycle_kind == CYCLE_MANUAL)
        keepalive_due = now8.hour in self.keepalive_hours
        checkin_due = (cycle_kind == CYCLE_HOUR
                       and now8.hour in self.checkin_hours)
        allow_complement = self._allow_complement_checkin(cycle_kind)
        targets = [a for a in self.pool.accounts if a.enabled and a.access_token]

        # 1. Token 保活：整点保活窗口全量刷新；其余巡检只刷新临近过期的
        force = keepalive_due
        ka = run_keepalive(self.pool, force=force)
        self.log("Token 保活：刷新 %d 个，失败 %d 个%s"
                 % (ka["refreshed"], ka["failed"],
                    "（保活窗口集中刷新）" if force else "（临近过期）"))
        for line in ka["logs"]:
            stripped = str(line).lstrip()
            if stripped.startswith("!"):
                self.log(stripped)

        # 2. 每日签到：自动开关（手动触发始终执行）。各巡回来由都只针对
        #    「本窗口还没签」的账号——can_checkin()/显式窗口字段是权威，
        #    已领取的账号不再重复请求，也不白耗有界重试额度。
        checkin_count = 0
        earned = 0
        if not (self.auto_checkin or manual):
            self.log("自动签到已关闭（QD_AUTO_CHECKIN=0），跳过自动签到；"
                     "手动触发与看板「每日签到」不受影响")
        else:
            if allow_complement or manual:
                pending = [a for a in targets if self._needs_checkin(a)]
            else:
                pending = []
            if pending:
                checkin_count, earned = self._run_checkin_phase(
                    pending, window_day_now, manual)
            elif checkin_due:
                self.log("所有账号今日已签到")

        # 3. 刷新额度快照（看板积分卡片依赖；自动开关，手动始终执行）
        if self.auto_quota_refresh or manual:
            for acc in targets:
                if self._stop_event.is_set():
                    self.log("巡检已取消（调度器已停止）")
                    break
                try:
                    acc.fetch_credits()
                except Exception:
                    pass
                self._sleep(DEFAULT_QUOTA_REFRESH_GAP)
        else:
            self.log("自动额度刷新已关闭（QD_AUTO_QUOTA_REFRESH=0），跳过")

        self.log("巡检完成：Token 保活 %d 个，签到 %d 个，本次新增积分 +%d"
                 % (ka["refreshed"], checkin_count, earned))
        self._save_state()

    # -- 只读状态 -----------------------------------------------------------
    def status(self):
        """只读快照：不得触发签到/刷新等任何写操作。"""
        checkin_text = "/".join("%02d:00" % h for h in self.checkin_hours) or "关闭"
        keepalive_text = "/".join("%02d:00" % h for h in self.keepalive_hours) or "关闭"
        mode = "北京时间排程 (%s 签到 · %s Token 保活)" % (checkin_text, keepalive_text)
        return {
            "enabled": self.enabled,
            "effective_enabled": self.effective_enabled,
            "paused_by_env": self._enabled_override is False,
            "running": self.worker_running(),
            "restarting": self.restart_pending(),
            "auto_checkin": self.auto_checkin,
            "auto_quota_refresh": self.auto_quota_refresh,
            "checkin_hours": list(self.checkin_hours),
            "keepalive_hours": list(self.keepalive_hours),
            "checkin_max_attempts": self.checkin_max_attempts,
            "retry_backoff": self.retry_backoff,
            "timezone": "UTC+8",
            "window_day": self._window_day(),
            "mode": mode,
            "mode_cn": mode,
            "mode_intl": mode,
            "last_run_time": self.last_run_time or "尚未运行",
            "next_run_time": self.next_run_time or "待调度",
            "startup_claim_date": self.startup_claim_date or "",
            "startup_claim_window_day": self.startup_claim_window_day or "",
            "checkin_attempt_day": self.checkin_attempt_day or "",
            "checkin_attempts": self.checkin_attempts,
            "last_cycle_date": self.last_cycle_date or "",
            "last_cycle_window_day": self.last_cycle_window_day or "",
            "state_file": self._state_path(),
            "logs": self.logs[-20:],
        }
