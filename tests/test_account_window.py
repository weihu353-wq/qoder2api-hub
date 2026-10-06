"""Activity windows use UTC+8 and survive process/account reconstruction."""
import datetime
import unittest
from unittest import mock

import qoder_accounts as accounts


def epoch(hour, minute=0):
    return datetime.datetime(2026, 10, 6, hour, minute,
                             tzinfo=accounts._UTC8).timestamp()


class AccountWindowTests(unittest.TestCase):
    def account(self, **fields):
        account = accounts.Account(dict(uid='synthetic-window-account', **fields))
        account.checkin_capability = lambda: (None, 'not probed')
        return account

    def test_window_boundary(self):
        self.assertEqual(accounts.checkin_window_day(epoch(9, 59)), '2026-10-05')
        self.assertEqual(accounts.checkin_window_day(epoch(10)), '2026-10-06')

    def test_claim_before_ten_does_not_block_new_window(self):
        account = self.account()
        with mock.patch.object(accounts.time, 'time', return_value=epoch(9)):
            account._stamp_checkin()
            self.assertFalse(account.can_checkin())
        self.assertEqual(account.last_checkin_window, '2026-10-05')
        with mock.patch.object(accounts.time, 'time', return_value=epoch(10)):
            self.assertTrue(account.can_checkin())

    def test_same_window_suppressed_after_reload(self):
        account = self.account()
        with mock.patch.object(accounts.time, 'time', return_value=epoch(10)):
            account._stamp_checkin()
        reloaded = self.account(**{k: v for k, v in account.to_dict().items()
                                   if k != 'uid'})
        with mock.patch.object(accounts.time, 'time', return_value=epoch(23)):
            self.assertFalse(reloaded.can_checkin())

    def test_explicit_window_overrides_legacy_calendar_display(self):
        account = self.account(lastCheckin='2026-10-06 09:00:00',
                               lastCheckinWindow='2026-10-05')
        with mock.patch.object(accounts.time, 'time', return_value=epoch(10)):
            self.assertTrue(account.can_checkin())

    def test_legacy_timestamp_conversion_uses_its_host_timezone(self):
        recorded = datetime.datetime.fromtimestamp(epoch(9)).strftime('%Y-%m-%d %H:%M:%S')
        account = self.account(lastCheckin=recorded)
        with mock.patch.object(accounts.time, 'time', return_value=epoch(10)):
            self.assertTrue(account.can_checkin())
        with mock.patch.object(accounts.time, 'time', return_value=epoch(9, 30)):
            self.assertFalse(account.can_checkin())

    def test_disabled_capability_still_blocks_claim(self):
        account = self.account()
        account.checkin_capability = lambda: (False, 'unsupported')
        self.assertFalse(account.can_checkin())


if __name__ == '__main__':
    unittest.main()
