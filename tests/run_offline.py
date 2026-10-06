"""Run regressions without production data or external networking.

Usage: python tests/run_offline.py [--verbose]
Each suite runs in a fresh process with a temporary data directory. Unknown
network calls fail immediately; external protocol fixtures are opt-in outside
this runner. Success with fixture skips is reported explicitly.
"""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = """
import ipaddress, runpy, socket, sys, unittest
real_connect = socket.socket.connect
real_connect_ex = socket.socket.connect_ex
real_create_connection = socket.create_connection
def require_loopback(address):
    host = address[0] if isinstance(address, tuple) else ''
    if host == 'localhost':
        return
    try:
        if ipaddress.ip_address(host).is_loopback:
            return
    except ValueError:
        pass
    raise RuntimeError('External network access is disabled in offline regression tests')
def offline_connect(sock, address):
    require_loopback(address)
    return real_connect(sock, address)
def offline_connect_ex(sock, address):
    require_loopback(address)
    return real_connect_ex(sock, address)
def offline_create_connection(address, *args, **kwargs):
    require_loopback(address)
    return real_create_connection(address, *args, **kwargs)
socket.create_connection = offline_create_connection
socket.socket.connect = offline_connect
socket.socket.connect_ex = offline_connect_ex
sys.path.insert(0, sys.argv[1])
if sys.argv[2] == 'unittest':
    suite = unittest.defaultTestLoader.discover(sys.argv[1] + '/tests', pattern='test_*.py')
    if not suite.countTestCases():
        raise RuntimeError('No maintained regression tests were discovered')
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
runpy.run_path(sys.argv[1] + '/' + sys.argv[2], run_name='__main__')
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    failed = False
    for suite in ('_test_qoder.py', '_test_leak_guard.py',
                  'tests/_test_responses_protocol.py', 'unittest'):
        with tempfile.TemporaryDirectory(prefix='qoder-offline-') as temp:
            env = os.environ.copy()
            env.update({
                'ACCOUNTS_DIR': str(Path(temp) / 'accounts'),
                'USAGE_DIR': str(Path(temp) / 'usage'),
                'QD_TEST_FIXTURE_DIR': str(Path(temp) / 'absent-synthetic-fixtures'),
                'QD_NATIVE_IDENTITY': '0',
                'QD_AUTO_CHECKIN': '0',
                'QD_AUTO_QUOTA_REFRESH': '0',
                'QD_SCHEDULER_ENABLED': '0',
                'QD_DESKTOP_DISCOVERY': '0',
                'PYTHONDONTWRITEBYTECODE': '1',
                'PYTHONIOENCODING': 'utf-8',
                'PYTHONUTF8': '1',
            })
            try:
                result = subprocess.run(
                    [sys.executable, '-B', '-c', BOOTSTRAP, str(ROOT), suite],
                    cwd=temp, env=env, capture_output=True, text=True,
                    encoding='utf-8', errors='replace', timeout=180,
                )
            except subprocess.TimeoutExpired:
                print('FAIL: {} exceeded the 180-second offline limit'.format(suite))
                failed = True
                continue
            combined = result.stdout + result.stderr
            print('{}: exit={}'.format(suite, result.returncode))
            if args.verbose or result.returncode:
                print(combined.rstrip())
            else:
                summaries = [line for line in combined.splitlines()
                             if '[SKIP]' in line or line.startswith('SUMMARY:')
                             or line.startswith('responses protocol:')
                             or line.startswith('Ran ') or line.strip() == 'OK']
                for line in summaries:
                    print('  ' + line)
            failed = failed or result.returncode != 0
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
