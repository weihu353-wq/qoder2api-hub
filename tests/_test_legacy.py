"""兼容套件：把仓根那份 _test_qoder.py 整体当一支套件跑。

P0-2 是「先骨架、再逐段迁移」：在迁移完成前，689 条既有断言一条都不能丢，
所以这里用最薄的一层转发把它们接进新编排；两种入口都有效：

    python _test_qoder.py        # 老习惯（保留）
    python tests/run_all.py      # 新编排（并发 + 一行一套件 + 失败 tail）
"""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    target = os.path.join(ROOT, "_test_qoder.py")
    if not os.path.isfile(target):
        print("legacy suite missing: %s" % target)
        return 2
    return subprocess.call([sys.executable, target], cwd=ROOT)


if __name__ == "__main__":
    sys.exit(main())
