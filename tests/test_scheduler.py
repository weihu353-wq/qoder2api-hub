"""Offline regression tests for qoder_scheduler (fake clock / temp state /
mock accounts + mock task layer). No network, no real accounts.

Runnable both ways:
    python tests/test_scheduler.py
    python tests/run_offline.py          (unittest discovery, isolation env)

tests/run_offline.py exports QD_AUTO_CHECKIN=0 / QD_AUTO_QUOTA_REFRESH=0 to keep
the whole offline run side-effect free; every case here patches the QD_* env it
needs explicitly, so the defaults under test are never inherited by accident.

窗口语义与真实 Account 对齐：last_checkin_window（持久化 lastCheckinWindow）是
权威，旧 lastCheckin 只是展示时间戳——调度器不得用它的日期前缀否决窗口判定。
"""
import datetime
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# 调度器 log() 会 import qoder_proxy 写看板日志；离线测试用空实现顶替，
# 避免把整个代理模块（及其环境/目录副作用）拉进测试进程。
if "qoder_proxy" not in sys.modules:
    _proxy_stub = types.ModuleType("qoder_proxy")
    _proxy_stub.add_log_entry = lambda *a, **k: None
    sys.modules["qoder_proxy"] = _proxy_stub

import qoder_accounts as A  # noqa: E402
import qoder_scheduler as SCH  # noqa: E402

BJ = SCH.BEIJING_TZ
QD_ENV_KEYS = ("QD_SCHEDULER_ENABLED", "QD_AUTO_CHECKIN", "QD_AUTO_QUOTA_REFRESH",
               "QD_CHECKIN_HOURS", "QD_KEEPALIVE_HOURS",
               "QD_CHECKIN_MAX_ATTEMPTS", "QD_CHECKIN_RETRY_BACKOFF")


def bj_epoch(year, month, day, hour, minute=0):
    """北京时间 (UTC+8) 某时刻的 epoch 秒。"""
    return datetime.datetime(year, month, day, hour, minute,
                             tzinfo=BJ).timestamp()


class Clock(object):
    def __init__(self, epoch):
        self.t = float(epoch)

    def __call__(self):
        return self.t

    def set(self, epoch):
        self.t = float(epoch)

    def advance(self, seconds):
        self.t += float(seconds)


def wait_until(predicate, timeout=5.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class FakeAccount(object):
    """窗口语义与真实 Account 对齐的账号桩（可注入假时钟）。"""

    def __init__(self, uid, last_checkin_window=None, last_checkin=None,
                 enabled=True, token="tok", capability=None, clock=None):
        self.uid = uid
        self.nickname = uid
        self.enabled = enabled
        self.access_token = token
        self.last_checkin_window = last_checkin_window
        self.last_checkin = last_checkin
        self.credits = {}
        self.plan = ""
        self._capability = capability
        self._clock = clock
        self.fetch_credits_calls = 0
        self.fetch_plan_calls = 0

    def _now(self):
        return self._clock() if self._clock else time.time()

    def can_checkin(self):
        if self._capability is False:
            return False
        if self.last_checkin_window:
            return self.last_checkin_window != SCH.window_day(self._now())
        return not self.last_checkin

    def checkin_capability(self):
        return self._capability, ""

    def fetch_credits(self):
        self.fetch_credits_calls += 1
        return {"ok": True}

    def fetch_plan(self):
        self.fetch_plan_calls += 1
        return {"ok": True}


class FakePool(object):
    def __init__(self, accounts, directory=None):
        self.accounts = list(accounts)
        self.dir = directory


class Stub(object):
    """替代 qoder_tasks.run_batch_checkin / run_keepalive 的可编程桩。"""

    def __init__(self, clock):
        self.clock = clock
        self.batch_calls = []
        self.keepalive_calls = []
        self.fail_counts = {}      # uid -> 还需失败几次
        self.always_fail = set()   # uid -> 永远失败
        self.already = set()       # uid -> 已领取（不落窗口戳）
        self.on_batch = None
        self.active_batches = 0    # 并发批次数（防双 worker 探针）
        self.max_active_batches = 0

    def batch(self, targets, gap=1.0, inter_gap=1.0):
        self.batch_calls.append([a.uid for a in targets])
        self.active_batches += 1
        self.max_active_batches = max(self.max_active_batches,
                                      self.active_batches)
        try:
            if self.on_batch:
                self.on_batch(self)
            logs = []
            total = 0
            any_ok = False
            for acc in targets:
                if acc.uid in self.always_fail:
                    logs.append("! [%s] 签到失败: simulated permanent error"
                                % acc.uid)
                    continue
                remaining = self.fail_counts.get(acc.uid, 0)
                if remaining > 0:
                    self.fail_counts[acc.uid] = remaining - 1
                    logs.append("! [%s] 签到失败: simulated transient error"
                                % acc.uid)
                    continue
                if acc.uid in self.already:
                    any_ok = True
                    logs.append("✓ [%s] 今日活动奖励已领取" % acc.uid)
                    continue
                # 与 Account._stamp_checkin 同构：展示时间戳 + 显式窗口日
                acc.last_checkin = time.strftime("%Y-%m-%d %H:%M:%S")
                acc.last_checkin_window = SCH.window_day(self.clock())
                total += 100
                any_ok = True
                logs.append("✓ [%s] 签到成功 +100 积分" % acc.uid)
        finally:
            self.active_batches -= 1
        # run_batch_checkin 会给每个账号的日志加两空格前缀；桩保持同样形状，
        # 顺带锁住「调度器必须先 lstrip 才能识别 ✓/!」这一行为。
        return {"ok": any_ok, "logs": ["  " + line for line in logs],
                "credit_added": total, "accounts_count": len(targets)}

    def keepalive(self, pool, force=False, threshold_seconds=4 * 3600):
        self.keepalive_calls.append(bool(force))
        return {"refreshed": 0, "failed": 0, "logs": []}


class Bridge(object):
    """调度器模块级函数的间接层：按用例切换当前桩。"""

    def __init__(self):
        self.stub = None

    def batch(self, targets, gap=1.0, inter_gap=1.0):
        return self.stub.batch(targets, gap, inter_gap)

    def keepalive(self, pool, force=False, threshold_seconds=4 * 3600):
        return self.stub.keepalive(pool, force, threshold_seconds)


BRIDGE = Bridge()
SCH.run_batch_checkin = BRIDGE.batch
SCH.run_keepalive = BRIDGE.keepalive
SCH.set_logger = lambda fn: None


class SchedulerTestCase(unittest.TestCase):
    def setUp(self):
        self._saved_env = {k: os.environ.get(k) for k in QD_ENV_KEYS}
        for key in QD_ENV_KEYS:
            os.environ.pop(key, None)
        self.tmp = tempfile.mkdtemp(prefix="qd-sched-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.addCleanup(self._restore_env)
        self.clock = Clock(bj_epoch(2026, 10, 6, 10, 30))
        self.stub = Stub(self.clock)
        BRIDGE.stub = self.stub
        self.addCleanup(lambda: setattr(BRIDGE, "stub", None))

    def _restore_env(self):
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def make(self, accounts, **kwargs):
        for acc in accounts:
            if isinstance(acc, FakeAccount) and acc._clock is None:
                acc._clock = self.clock
        pool = FakePool(accounts, self.tmp)
        sched = SCH.Scheduler(pool, state_dir=self.tmp, now_fn=self.clock,
                              **kwargs)
        self.addCleanup(self._shutdown_scheduler, sched)
        return sched

    @staticmethod
    def _shutdown_scheduler(sched):
        """用例收尾：停 worker、join 交接线程，避免线程跨用例污染。"""
        sched.stop()
        if sched._restart_thread:
            sched._restart_thread.join(timeout=5)
        if sched._thread:
            sched._thread.join(timeout=5)

    def state(self):
        path = os.path.join(self.tmp, "scheduler", "state.json")
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)

    def cycle(self, sched, kind=SCH.CYCLE_HOUR, reason="test"):
        sched._execute_cycle(reason, kind)

    def use_account_clock(self):
        """让真实 Account.can_checkin() 跟随注入的假时钟（而非真实时间）。"""
        original = A.checkin_window_day

        def fake_window_day(now=None):
            return original(self.clock() if now is None else now)

        A.checkin_window_day = fake_window_day
        self.addCleanup(lambda: setattr(A, "checkin_window_day", original))


class WindowDayTests(SchedulerTestCase):
    def test_window_day_boundary_is_beijing_10am(self):
        self.clock.set(bj_epoch(2026, 10, 7, 9, 59))
        self.assertEqual(SCH.window_day(self.clock()), "2026-10-06")
        self.clock.set(bj_epoch(2026, 10, 7, 10, 0))
        self.assertEqual(SCH.window_day(self.clock()), "2026-10-07")
        self.clock.set(bj_epoch(2026, 10, 7, 23, 59))
        self.assertEqual(SCH.window_day(self.clock()), "2026-10-07")
        self.clock.set(bj_epoch(2026, 10, 8, 9, 59))
        self.assertEqual(SCH.window_day(self.clock()), "2026-10-07")
        self.clock.set(bj_epoch(2026, 10, 8, 10, 0))
        self.assertEqual(SCH.window_day(self.clock()), "2026-10-08")

    def test_window_day_reuses_accounts_helper(self):
        ts = bj_epoch(2026, 10, 7, 9, 30)
        self.assertIs(SCH.CHECKIN_WINDOW_HOUR, A.CHECKIN_WINDOW_HOUR_UTC8)
        self.assertEqual(SCH.window_day(ts), A.checkin_window_day(ts))

    def test_window_day_ignores_host_timezone(self):
        ts = bj_epoch(2026, 10, 7, 9, 30)          # 北京时间 09:30 -> 10-06
        code = ("import sys; sys.path.insert(0, %r); "
                "import qoder_scheduler as S; print(S.window_day(%r))"
                % (ROOT, ts))
        outs = {}
        for tz in ("UTC", "America/New_York", "Asia/Shanghai"):
            env = dict(os.environ)
            env["TZ"] = tz
            env["PYTHONIOENCODING"] = "utf-8"
            env["PYTHONUTF8"] = "1"
            res = subprocess.run([sys.executable, "-c", code],
                                 capture_output=True, text=True,
                                 encoding="utf-8", env=env, timeout=120)
            self.assertEqual(res.returncode, 0, res.stderr)
            outs[tz] = (res.stdout or "").strip()
        self.assertEqual(set(outs.values()), {"2026-10-06"}, outs)

    def test_next_fire_is_beijing(self):
        sched = self.make([])
        self.clock.set(bj_epoch(2026, 10, 7, 9, 0))
        sched._calc_next_fire()
        self.assertEqual(sched.next_run_time, "2026-10-07 10:00:00")
        self.clock.set(bj_epoch(2026, 10, 7, 23, 0))
        sched._calc_next_fire()
        self.assertEqual(sched.next_run_time, "2026-10-08 10:00:00")

    def test_default_hours_follow_beijing_window(self):
        sched = self.make([])
        self.assertEqual(sched.checkin_hours, [10])
        self.assertEqual(sched.keepalive_hours, [22])
        self.assertEqual(sched.all_hours, [10, 22])


class ConfigTests(SchedulerTestCase):
    def test_defaults_preserve_old_behaviour(self):
        sched = self.make([])
        self.assertTrue(sched.auto_checkin)
        self.assertTrue(sched.auto_quota_refresh)
        self.assertTrue(sched.effective_enabled)

    def test_env_overrides_state_and_defaults(self):
        os.environ["QD_AUTO_CHECKIN"] = "0"
        os.environ["QD_AUTO_QUOTA_REFRESH"] = "0"
        os.environ["QD_CHECKIN_HOURS"] = "9, 21"
        os.environ["QD_KEEPALIVE_HOURS"] = "23"
        sched = self.make([])
        self.assertFalse(sched.auto_checkin)
        self.assertFalse(sched.auto_quota_refresh)
        self.assertEqual(sched.checkin_hours, [9, 21])
        self.assertEqual(sched.keepalive_hours, [23])
        self.assertEqual(sched.all_hours, [9, 21, 23])

    def test_invalid_hours_fall_back_to_default(self):
        os.environ["QD_CHECKIN_HOURS"] = "10,not-an-hour"
        sched = self.make([])
        self.assertEqual(sched.checkin_hours, [10])

    def test_env_pause_does_not_overwrite_persisted_enabled(self):
        first = self.make([])
        first.log("seed state")                         # 落盘一次，产生持久化状态
        self.assertTrue(first.enabled)
        self.assertTrue(self.state()["enabled"])
        os.environ["QD_SCHEDULER_ENABLED"] = "0"
        paused = self.make([])
        self.assertTrue(paused.enabled)                 # 持久化状态没被覆盖
        self.assertFalse(paused.effective_enabled)      # 但本次进程完全暂停
        self.assertTrue(paused.status()["paused_by_env"])
        paused.log("touch state")                       # 触发落盘
        self.assertTrue(self.state()["enabled"])

    def test_configure_updates_and_persists(self):
        sched = self.make([])
        status = sched.configure(auto_checkin=False, checkin_hours=[11, 21])
        self.assertFalse(status["auto_checkin"])
        self.assertEqual(status["checkin_hours"], [11, 21])
        reloaded = self.make([])
        self.assertFalse(reloaded.auto_checkin)
        self.assertEqual(reloaded.checkin_hours, [11, 21])

    def test_set_enabled_keeps_legacy_interface(self):
        sched = self.make([])
        status = sched.set_enabled(False)
        self.assertFalse(status["enabled"])
        self.assertFalse(self.make([]).enabled)

    def test_legacy_state_file_still_loads(self):
        path = os.path.join(self.tmp, "scheduler", "state.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump({"enabled": False, "last_run_time": "2026-01-01 00:00:00",
                       "startup_claim_date": "2026-01-01",
                       "last_cycle_date": "2026-01-01", "logs": ["old"]}, fh)
        sched = self.make([])
        self.assertFalse(sched.enabled)
        self.assertEqual(sched.last_run_time, "2026-01-01 00:00:00")
        self.assertTrue(sched.auto_checkin)             # 新字段缺失 -> 默认
        self.assertEqual(sched.startup_claim_window_day, "")
        self.assertTrue(sched._allow_complement_checkin(SCH.CYCLE_STARTUP))


class GateTests(SchedulerTestCase):
    def test_startup_gate_uses_window_day(self):
        self.clock.set(bj_epoch(2026, 10, 7, 8, 0))     # 10:00 前 -> 窗口日 10-06
        sched = self.make([])
        self.assertTrue(sched._allow_complement_checkin(SCH.CYCLE_STARTUP))
        self.assertEqual(sched.startup_claim_window_day, "2026-10-06")
        self.assertFalse(sched._allow_complement_checkin(SCH.CYCLE_STARTUP))
        # 到达 10:00 后进入新窗口日：不能再认为「今天已经补过」
        self.clock.set(bj_epoch(2026, 10, 7, 10, 0))
        self.assertTrue(sched._allow_complement_checkin(SCH.CYCLE_STARTUP))
        self.assertEqual(sched.startup_claim_window_day, "2026-10-07")
        self.assertFalse(sched._allow_complement_checkin(SCH.CYCLE_STARTUP))

    def test_startup_gate_mark_is_persisted_before_acting(self):
        self.clock.set(bj_epoch(2026, 10, 7, 8, 0))
        sched = self.make([])
        sched._allow_complement_checkin(SCH.CYCLE_STARTUP)
        state = self.state()
        self.assertEqual(state["startup_claim_window_day"], "2026-10-06")
        self.assertEqual(state["startup_claim_date"], time.strftime("%Y-%m-%d"))

    def test_mark_before_act_blocks_repeat_without_attempts(self):
        # 池里仍有待签账号，但从未跑过巡检（attempts 未产生）-> 重复 startup 仍拒绝
        acc = FakeAccount("a1")
        sched = self.make([acc])
        self.assertTrue(sched._allow_complement_checkin(SCH.CYCLE_STARTUP))
        self.assertFalse(sched._allow_complement_checkin(SCH.CYCLE_STARTUP))
        restarted = self.make([acc])
        self.assertFalse(restarted._allow_complement_checkin(SCH.CYCLE_STARTUP))

    def test_hour_cycle_bypasses_startup_gate(self):
        sched = self.make([])
        self.assertTrue(sched._allow_complement_checkin(SCH.CYCLE_HOUR))
        self.assertTrue(sched._allow_complement_checkin(SCH.CYCLE_MANUAL))


class RecoveryTests(SchedulerTestCase):
    def test_crash_after_first_attempt_resumes_remaining(self):
        os.environ["QD_CHECKIN_MAX_ATTEMPTS"] = "4"
        os.environ["QD_CHECKIN_RETRY_BACKOFF"] = "0"
        acc = FakeAccount("a1")
        self.stub.fail_counts["a1"] = 1
        sched = self.make([acc])
        self.stub.on_batch = lambda stub: sched.stop()   # 第 1 次后模拟进程崩溃
        self.cycle(sched, SCH.CYCLE_STARTUP)
        self.assertEqual(len(self.stub.batch_calls), 1)
        state = self.state()
        self.assertEqual(state["checkin_attempts"], 1)
        self.assertEqual(state["checkin_attempt_day"], "2026-10-06")

        self.stub.on_batch = None
        restarted = self.make([acc])                     # 新进程，同一 state.json
        self.assertTrue(restarted._allow_complement_checkin(SCH.CYCLE_STARTUP))
        self.cycle(restarted, SCH.CYCLE_STARTUP)
        self.assertEqual(len(self.stub.batch_calls), 2)  # 用掉剩余额度续跑
        self.assertEqual(self.state()["checkin_attempts"], 2)
        self.assertEqual(acc.last_checkin_window, "2026-10-06")

    def test_resume_refused_when_budget_exhausted(self):
        os.environ["QD_CHECKIN_MAX_ATTEMPTS"] = "2"
        os.environ["QD_CHECKIN_RETRY_BACKOFF"] = "0"
        acc = FakeAccount("a1")
        self.stub.always_fail = {"a1"}
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_STARTUP)
        self.assertEqual(len(self.stub.batch_calls), 2)  # 单次巡检内用尽
        restarted = self.make([acc])
        self.assertFalse(restarted._allow_complement_checkin(SCH.CYCLE_STARTUP))
        self.cycle(restarted, SCH.CYCLE_STARTUP)
        self.assertEqual(len(self.stub.batch_calls), 2)  # 绝不重复

    def test_resume_refused_when_window_complete(self):
        acc = FakeAccount("a1")
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_STARTUP)
        self.assertEqual(len(self.stub.batch_calls), 1)
        restarted = self.make([acc])
        self.assertFalse(restarted._allow_complement_checkin(SCH.CYCLE_STARTUP))
        self.cycle(restarted, SCH.CYCLE_STARTUP)
        self.assertEqual(len(self.stub.batch_calls), 1)


class CheckinFlowTests(SchedulerTestCase):
    def test_no_duplicate_claim_within_window(self):
        acc = FakeAccount("a1", last_checkin_window="2026-10-06")
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_STARTUP)
        self.assertEqual(self.stub.batch_calls, [])       # 本窗口已签 -> 不发请求

    def test_due_hour_only_targets_unclaimed(self):
        claimed = FakeAccount("a1", last_checkin_window="2026-10-06")
        pending = FakeAccount("a2")
        self.clock.set(bj_epoch(2026, 10, 6, 10, 0))
        sched = self.make([claimed, pending])
        self.cycle(sched, SCH.CYCLE_HOUR)
        self.assertEqual(self.stub.batch_calls, [["a2"]])

    def test_before_10am_uses_previous_window_then_new_window_after(self):
        acc = FakeAccount("a1", last_checkin_window="2026-10-06",
                          last_checkin="2026-10-07 09:00:00")
        self.clock.set(bj_epoch(2026, 10, 7, 8, 0))
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_STARTUP)
        self.assertEqual(self.stub.batch_calls, [])       # 仍属 10-06 窗口，已签
        self.clock.set(bj_epoch(2026, 10, 7, 10, 0))
        self.cycle(sched, SCH.CYCLE_HOUR)                 # 新窗口开启
        self.assertEqual(self.stub.batch_calls, [["a1"]])
        self.assertEqual(acc.last_checkin_window, "2026-10-07")

    def test_disabled_account_is_skipped(self):
        acc = FakeAccount("a1", enabled=False)
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_HOUR)
        self.assertEqual(self.stub.batch_calls, [])

    def test_account_without_capability_is_skipped(self):
        acc = FakeAccount("a1", capability=False)
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_HOUR)
        self.assertEqual(self.stub.batch_calls, [])


class RealAccountTests(SchedulerTestCase):
    """真实 qoder_accounts.Account：9 点领上个窗口、10 点必须还能领。"""

    def _real_account(self):
        self.use_account_clock()
        return A.Account({"uid": "u1", "accessToken": "tok",
                          "lastCheckin": "2026-10-07 09:00:00",
                          "lastCheckinWindow": "2026-10-06"})

    def test_can_checkin_crosses_10am_boundary(self):
        acc = self._real_account()
        self.clock.set(bj_epoch(2026, 10, 7, 9, 0))
        self.assertFalse(acc.can_checkin())             # 09:00 仍属 10-06 窗口
        self.clock.set(bj_epoch(2026, 10, 7, 10, 0))
        self.assertTrue(acc.can_checkin())              # 10:00 新窗口开启

    def test_scheduler_does_not_veto_with_display_timestamp(self):
        # lastCheckin 显示的是「今天 09:00」，旧实现会拿日期前缀拦掉 10 点补签
        acc = self._real_account()
        self.clock.set(bj_epoch(2026, 10, 7, 10, 0))
        sched = self.make([acc])
        self.assertTrue(sched._needs_checkin(acc))

    def test_startup_cycle_claims_new_window_after_9am_claim(self):
        acc = self._real_account()
        self.clock.set(bj_epoch(2026, 10, 7, 9, 0))
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_STARTUP)
        self.assertEqual(self.stub.batch_calls, [])     # 上个窗口已领，不重复
        self.clock.set(bj_epoch(2026, 10, 7, 10, 0))
        self.cycle(sched, SCH.CYCLE_STARTUP)
        self.assertEqual(self.stub.batch_calls, [["u1"]])  # 新窗口补签
        self.assertEqual(acc.last_checkin_window, "2026-10-07")

    def test_legacy_timestamp_only_account_still_works(self):
        # 老账号只有 lastCheckin（无窗口字段）：按旧宿主时区推导活动窗口
        self.use_account_clock()
        legacy_stamp = datetime.datetime.fromtimestamp(
            bj_epoch(2026, 10, 7, 9, 0)).strftime("%Y-%m-%d %H:%M:%S")
        acc = A.Account({"uid": "u2", "accessToken": "tok",
                         "lastCheckin": legacy_stamp})
        self.clock.set(bj_epoch(2026, 10, 7, 9, 30))
        self.assertFalse(acc.can_checkin())
        self.clock.set(bj_epoch(2026, 10, 7, 10, 30))
        self.assertTrue(acc.can_checkin())


class SwitchTests(SchedulerTestCase):
    def test_auto_checkin_off_keeps_quota_refresh(self):
        os.environ["QD_AUTO_CHECKIN"] = "0"
        acc = FakeAccount("a1")
        self.clock.set(bj_epoch(2026, 10, 6, 10, 0))
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_HOUR)
        self.assertEqual(self.stub.batch_calls, [])
        self.assertEqual(acc.fetch_credits_calls, 1)
        self.assertTrue(self.stub.keepalive_calls)
        self.assertTrue(any("自动签到已关闭" in line for line in sched.logs))

    def test_auto_quota_off_keeps_checkin(self):
        os.environ["QD_AUTO_QUOTA_REFRESH"] = "0"
        acc = FakeAccount("a1")
        self.clock.set(bj_epoch(2026, 10, 6, 10, 0))
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_HOUR)
        self.assertEqual(self.stub.batch_calls, [["a1"]])
        self.assertEqual(acc.fetch_credits_calls, 0)

    def test_manual_still_runs_when_both_switches_off(self):
        os.environ["QD_AUTO_CHECKIN"] = "0"
        os.environ["QD_AUTO_QUOTA_REFRESH"] = "0"
        acc = FakeAccount("a1")
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_MANUAL)
        self.assertEqual(self.stub.batch_calls, [["a1"]])
        self.assertEqual(acc.fetch_credits_calls, 1)
        # trigger_now 走后台线程；等它真正跑完再退出用例，避免线程与 teardown 抢状态文件
        self.assertEqual(sched.trigger_now()["ok"], True)
        self.assertTrue(wait_until(lambda: acc.fetch_credits_calls >= 2
                                   and not sched._run_lock.locked()), sched.logs)
        self.assertEqual(acc.fetch_credits_calls, 2)

    def test_status_is_read_only(self):
        sched = self.make([FakeAccount("a1")])
        sched.status()
        sched.status()
        self.assertEqual(self.stub.batch_calls, [])
        self.assertEqual(self.stub.keepalive_calls, [])
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "scheduler",
                                                     "state.json")))


class RetryTests(SchedulerTestCase):
    def test_transient_failure_recovers_within_bound(self):
        os.environ["QD_CHECKIN_MAX_ATTEMPTS"] = "3"
        os.environ["QD_CHECKIN_RETRY_BACKOFF"] = "0"
        acc = FakeAccount("a1")
        self.stub.fail_counts["a1"] = 1
        self.clock.set(bj_epoch(2026, 10, 6, 10, 0))
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_HOUR)
        self.assertEqual(self.stub.batch_calls, [["a1"], ["a1"]])
        self.assertEqual(acc.last_checkin_window, "2026-10-06")

    def test_permanent_failure_is_bounded_and_not_replayed(self):
        os.environ["QD_CHECKIN_MAX_ATTEMPTS"] = "2"
        os.environ["QD_CHECKIN_RETRY_BACKOFF"] = "0"
        acc = FakeAccount("a1")
        self.stub.always_fail = {"a1"}
        self.clock.set(bj_epoch(2026, 10, 6, 10, 0))
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_HOUR)
        self.assertEqual(len(self.stub.batch_calls), 2)     # 有界，不无限重放
        self.cycle(sched, SCH.CYCLE_HOUR)                   # 同窗口日再跑
        self.assertEqual(len(self.stub.batch_calls), 2)     # 当日额度已用尽
        self.assertTrue(any("已达上限" in line for line in sched.logs))

    def test_unavailable_result_is_not_retried(self):
        os.environ["QD_CHECKIN_RETRY_BACKOFF"] = "0"
        acc = FakeAccount("a1")
        self.stub.already = {"a1"}          # 已领取：无失败信号
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_MANUAL)
        self.assertEqual(len(self.stub.batch_calls), 1)
        self.assertTrue(any("无失败信号" in line for line in sched.logs))

    def test_stop_cancels_remaining_retries(self):
        os.environ["QD_CHECKIN_MAX_ATTEMPTS"] = "3"
        os.environ["QD_CHECKIN_RETRY_BACKOFF"] = "0"
        acc = FakeAccount("a1")
        self.stub.always_fail = {"a1"}
        sched = self.make([acc])
        self.stub.on_batch = lambda stub: sched.stop()
        self.cycle(sched, SCH.CYCLE_MANUAL)
        self.assertEqual(len(self.stub.batch_calls), 1)


class LoopTests(SchedulerTestCase):
    def setUp(self):
        super(LoopTests, self).setUp()
        self._saved_loop = (SCH.STARTUP_DELAY_SECONDS, SCH.POLL_SECONDS,
                            SCH.HOUR_DEDUP_SECONDS)
        SCH.STARTUP_DELAY_SECONDS = 0.01
        SCH.POLL_SECONDS = 0.02
        SCH.HOUR_DEDUP_SECONDS = 0.02
        self.addCleanup(self._restore_loop)

    def _restore_loop(self):
        (SCH.STARTUP_DELAY_SECONDS, SCH.POLL_SECONDS,
         SCH.HOUR_DEDUP_SECONDS) = self._saved_loop

    def _run_briefly(self, sched, predicate, timeout=3.0):
        sched.start()
        try:
            return wait_until(predicate, timeout=timeout)
        finally:
            sched.stop()
            if sched._thread:
                sched._thread.join(timeout=3.0)

    def test_loop_runs_startup_cycle_when_enabled(self):
        acc = FakeAccount("a1")
        sched = self.make([acc])
        ran = self._run_briefly(sched, lambda: bool(self.stub.batch_calls))
        self.assertTrue(ran, sched.logs)
        self.assertTrue(self.stub.keepalive_calls)

    def test_env_pause_stops_all_automatic_work(self):
        os.environ["QD_SCHEDULER_ENABLED"] = "0"
        acc = FakeAccount("a1")
        sched = self.make([acc])
        self.assertFalse(sched.effective_enabled)
        self._run_briefly(sched, lambda: False, timeout=0.3)
        self.assertEqual(self.stub.batch_calls, [])
        self.assertEqual(self.stub.keepalive_calls, [])
        self.assertTrue(any("处于暂停状态" in line for line in sched.logs))


class LifecycleTests(SchedulerTestCase):
    """stop() -> start() 交接：proxy restart_scheduler 的 P1 回归。

    旧实现：stop() 只置位，start() 见旧 worker 还活着就 return；旧 worker 随后
    退出，于是没有任何 worker 接手——调度器静默死亡。这里锁住正确语义：旧
    worker 未退出前绝不起新 worker、也不清共享 stop 事件；退出后再交接。
    """

    def setUp(self):
        super(LifecycleTests, self).setUp()
        self._saved_loop = (SCH.STARTUP_DELAY_SECONDS, SCH.POLL_SECONDS,
                            SCH.HOUR_DEDUP_SECONDS)
        SCH.STARTUP_DELAY_SECONDS = 0.01
        SCH.POLL_SECONDS = 0.02
        SCH.HOUR_DEDUP_SECONDS = 0.02
        self.addCleanup(self._restore_loop)
        self.entered = threading.Event()
        self.release = threading.Event()

    def _restore_loop(self):
        (SCH.STARTUP_DELAY_SECONDS, SCH.POLL_SECONDS,
         SCH.HOUR_DEDUP_SECONDS) = self._saved_loop

    def _start_and_block_in_batch(self):
        """起 worker 并让它卡在批内：模拟 stop() 到达时旧 worker 还没退出。"""
        def slow(stub):
            self.entered.set()
            self.release.wait(5)
        self.stub.on_batch = slow
        sched = self.make([FakeAccount("a1")])
        sched.start()
        self.assertTrue(self.entered.wait(5), sched.logs)
        return sched

    def _release_batch(self):
        self.release.set()
        self.stub.on_batch = None

    @staticmethod
    def _live_worker_count():
        """当前存活的调度 worker 数（worker 线程固定命名，便于识别双 worker）。"""
        return len([t for t in threading.enumerate()
                    if t.name == "qd-scheduler" and t.is_alive()])

    def test_restart_after_stop_starts_a_fresh_worker(self):
        sched = self._start_and_block_in_batch()
        first = sched._thread
        sched.stop()
        sched.start()                       # 旧 worker 仍在批内
        self.assertTrue(sched.status()["restarting"])
        self.assertIs(sched._thread, first)  # 旧 worker 未退，绝不提前起新的
        self.assertTrue(first.is_alive())
        self._release_batch()
        self.assertTrue(wait_until(
            lambda: sched._thread is not first and sched.worker_running(), 5),
            sched.logs)
        self.assertFalse(first.is_alive())   # 先退出旧 worker 再交接
        self.assertFalse(sched.status()["restarting"])
        self.assertTrue(sched.status()["running"])

    def test_restart_never_leaves_scheduler_dead(self):
        sched = self._start_and_block_in_batch()
        first = sched._thread
        sched.stop()
        sched.start()
        self._release_batch()
        # 等交接真正完成：旧 worker 退出、新 worker 顶上来（不是「旧的还活着」）
        self.assertTrue(wait_until(
            lambda: sched._thread is not first and sched.worker_running(), 5),
            sched.logs)
        self.assertIsNot(sched._thread, first)
        self.assertFalse(first.is_alive())

    def test_stop_cancels_queued_restart(self):
        sched = self._start_and_block_in_batch()
        first = sched._thread
        sched.stop()
        sched.start()                       # 排队重启
        sched.stop()                        # 排队期间再次 stop -> 取消
        self._release_batch()
        first.join(timeout=5)
        self.assertTrue(wait_until(lambda: not sched.restart_pending(), 5))
        self.assertFalse(sched.worker_running())     # 没有起新 worker
        self.assertIs(sched._thread, first)
        self.assertTrue(any("已被 stop() 取消" in line for line in sched.logs))

    def test_repeated_restarts_coalesce_into_one_worker(self):
        self.stub.already = {"a1"}          # 一直待签，便于观察 worker 行为
        sched = self._start_and_block_in_batch()
        first = sched._thread
        for _ in range(3):
            sched.stop()
            sched.start()
        self.assertTrue(sched.status()["restarting"])
        self.assertIs(sched._thread, first)
        self._release_batch()
        self.assertTrue(wait_until(
            lambda: sched._thread is not first and sched.worker_running(), 5),
            sched.logs)
        second = sched._thread
        time.sleep(0.3)
        self.assertIs(sched._thread, second)         # 不会冒出第二个 worker
        self.assertFalse(sched.status()["restarting"])
        self.assertLessEqual(self.stub.max_active_batches, 1)  # 无双 worker 并发
        self.assertEqual(self._live_worker_count(), 1)

    def test_second_start_during_handoff_does_not_double_spawn(self):
        """旧 worker 已退出、交接线程尚未拿到生命周期锁时的第二次 start()。"""
        self.stub.already = {"a1"}          # 保持待签，便于观察 worker 数量
        sched = self._start_and_block_in_batch()
        first = sched._thread
        sched.stop()
        sched.start()                       # 排队交接：helper 正在 join 旧 worker
        # 抢在 helper 之前握住生命周期锁：旧 worker 退出后 helper 会卡在锁上，
        # 精确复现「old 已退、helper 尚未接管」的窗口。
        sched._lifecycle_lock.acquire()
        try:
            self._release_batch()
            first.join(timeout=5)
            time.sleep(0.2)
            self.assertTrue(sched.status()["restarting"])
            sched.start()                   # 关键：helper 尚未接管时的第二次 start()
            self.assertIs(sched._thread, first)   # 绝不在 helper 之前另起 worker
        finally:
            sched._lifecycle_lock.release()
        self.assertTrue(wait_until(
            lambda: sched._thread is not first and sched.worker_running(), 5),
            sched.logs)
        second = sched._thread
        time.sleep(0.3)
        self.assertIs(sched._thread, second)          # 只有一个新 worker
        self.assertEqual(self._live_worker_count(), 1)  # 不制造双 worker

    def test_start_while_running_is_noop(self):
        sched = self.make([FakeAccount("a1")])
        sched.start()
        self.assertTrue(wait_until(sched.worker_running, 5))
        first = sched._thread
        sched.start()
        self.assertIs(sched._thread, first)
        self.assertFalse(sched.status()["restarting"])

    def test_stop_then_start_without_live_worker_spawns_immediately(self):
        sched = self.make([FakeAccount("a1")])
        sched.stop()
        sched.start()
        self.assertTrue(sched.worker_running())
        self.assertFalse(sched.restart_pending())

    def test_stop_after_restart_pauses_the_new_worker(self):
        sched = self._start_and_block_in_batch()
        first = sched._thread
        sched.stop()
        sched.start()
        self._release_batch()
        self.assertTrue(wait_until(
            lambda: sched._thread is not first and sched.worker_running(), 5),
            sched.logs)
        second = sched._thread
        sched.stop()
        self.assertTrue(wait_until(lambda: not second.is_alive(), 5))
        self.assertFalse(sched.worker_running())


class LogSurfaceTests(SchedulerTestCase):
    """run_batch_checkin 的账号行带两空格前缀，调度器必须先 lstrip 再识别。"""

    def test_indented_success_line_is_surfaced(self):
        acc = FakeAccount("a1")
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_MANUAL)
        self.assertTrue(any("签到成功" in line for line in sched.logs),
                        sched.logs)
        self.assertTrue(any("✓" in line for line in sched.logs), sched.logs)

    def test_indented_failure_line_is_surfaced(self):
        os.environ["QD_CHECKIN_MAX_ATTEMPTS"] = "1"
        os.environ["QD_CHECKIN_RETRY_BACKOFF"] = "0"
        acc = FakeAccount("a1")
        self.stub.always_fail = {"a1"}
        sched = self.make([acc])
        self.cycle(sched, SCH.CYCLE_MANUAL)
        self.assertTrue(any("签到失败" in line for line in sched.logs),
                        sched.logs)


if __name__ == "__main__":
    unittest.main(verbosity=2)
