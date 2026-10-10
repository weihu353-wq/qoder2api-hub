"""跑 tests/ 下的所有套件，每个套件打印一行汇总（P0-2 测试工程化 · 骨架）。

    python tests/run_all.py            # 全部
    python tests/run_all.py guards     # 只跑名字里含 guards 的套件

编排照抄 wb 的 tests/run_all.py：并发执行、每套件独立日志文件、失败打印 tail 25 行、
强制 UTF-8（子进程 stdout 编码 + 日志写入）。套件名规范 _test_*.py（.js 需要 node）。
"""
import concurrent.futures as _cf
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TAIL_LINES = 25
JOBS = max(1, int(os.environ.get("QD_TESTS_JOBS") or 4))


def suites(pattern=""):
    names = sorted(n for n in os.listdir(HERE)
                   if n.startswith("_test_") and n.endswith((".py", ".js")))
    return [n for n in names if pattern in n]


def command(name):
    path = os.path.join(HERE, name)
    if name.endswith(".js"):
        return ["node", path]
    return [sys.executable, path]


def tail(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            lines = [line.rstrip() for line in fh if line.strip()]
    except OSError:
        return []
    return lines[-TAIL_LINES:]


def total_of(path):
    """从套件日志里取 `SUMMARY: TOTAL N checks` 的 N（迁移期间用它保证总数不下降）。"""
    import re
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            m = None
            for line in fh:
                hit = re.search(r"SUMMARY: TOTAL (\d+) checks", line)
                if hit:
                    m = int(hit.group(1))
            return m
    except OSError:
        return None


def run_one(name, logdir, env):
    log = os.path.join(logdir, name + ".log")
    with open(log, "wb") as fh:
        rc = subprocess.call(command(name), stdout=fh,
                             stderr=subprocess.STDOUT, env=env, cwd=HERE)
    return name, rc, log


def main(argv):
    if len(argv) > 1 and argv[1] in ("-h", "--help"):
        print(__doc__)
        return 0
    pattern = argv[1] if len(argv) > 1 else ""
    selected = suites(pattern)
    if not selected:
        print("  no suite matches %r in %s" % (pattern, HERE))
        return 2
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [ROOT] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else []))
    env["PYTHONIOENCODING"] = "utf-8"
    if not shutil.which("node"):
        env.pop("NODE_OK", None)
    logdir = tempfile.mkdtemp(prefix="qd-suites-")
    with _cf.ThreadPoolExecutor(max_workers=JOBS) as ex:
        results = list(ex.map(lambda n: run_one(n, logdir, env), selected))
    failed = []
    grand = 0
    unknown = []
    for name, rc, log in results:
        n = total_of(log)
        if n is None:
            unknown.append(name)
        else:
            grand += n
        print("  %-34s %-10s checks=%s" % (name, "PASS" if rc == 0 else "FAIL(rc=%d)" % rc,
                                            n if n is not None else "?"))
        if rc != 0:
            failed.append((name, log))
    for name, log in failed:
        print("")
        print("---- %s tail (%d lines) ----" % (name, TAIL_LINES))
        for line in tail(log):
            print("    " + line)
    print("")
    print("SUMMARY: %d suites, %d passed, %d failed, TOTAL %d checks%s  (logs: %s)"
          % (len(results), len(results) - len(failed), len(failed), grand,
             ("" if not unknown else " (+%d suites without SUMMARY: %s)"
              % (len(unknown), ", ".join(unknown))), logdir))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
