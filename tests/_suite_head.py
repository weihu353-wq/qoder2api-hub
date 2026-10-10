# -*- coding: utf-8 -*-
"""套件共享头部：import、计数器、check/skip、结果输出（P0-2 迁移专用）。

套件文件只要两行：

    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from _suite_head import *        # noqa: F401,F403

    P, A = bootstrap()               # 返回 (qoder_proxy, qoder_accounts)
    check("...", cond, extra)
    finish()                         # 打印 SUMMARY/RESULT 并按 FAIL 退出

为什么要显式抽出：第一次迁移 [35]/[39] 时每个套件各自抄一遍头部，结果两份都漏了
import（一份漏 json、一份漏 P）→ 两个套件直接 NameError 全红。共享模块从根上消掉这类抄漏。
"""
import base64
import contextlib
import hashlib
import io
import json
import os
import re
import shutil
import struct
import sys
import tarfile
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASS = FAIL = SKIP = 0

__all__ = ["base64", "contextlib", "hashlib", "io", "json", "os", "re", "shutil",
           "struct", "sys", "tarfile", "tempfile", "time", "ROOT",
           "bootstrap", "check", "skip", "finish", "PASS", "FAIL", "SKIP"]


def bootstrap(env=None):
    """把 ROOT 放进 sys.path、设好 ACCOUNTS_DIR，导入 P / A 并返回。"""
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    os.environ.setdefault("ACCOUNTS_DIR", os.path.join(ROOT, "_acc"))
    for k, v in (env or {}).items():
        os.environ.setdefault(k, str(v))
    import qoder_accounts as A
    import qoder_proxy as P
    return P, A


def check(label, cond, extra=None):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  [PASS] " + label)
    else:
        FAIL += 1
        print("  [FAIL] %s  %r" % (label, extra))


def skip(label, reason=""):
    global SKIP
    SKIP += 1
    print("  [SKIP] %s  -- %s" % (label, reason))


def finish():
    print("")
    print("SUMMARY: TOTAL %d checks, %d passed, %d failed, %d skipped"
          % (PASS + FAIL + SKIP, PASS, FAIL, SKIP))
    print("RESULT: %s (exit %d)" % ("RED" if FAIL else "GREEN", 1 if FAIL else 0))
    sys.exit(1 if FAIL else 0)
