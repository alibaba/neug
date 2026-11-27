"""Regressions for audit failures hidden by driver exit zero."""

import unittest

from check_schedule_audit import check_schedule_audit


def audit(result: str) -> str:
    return f"ExecuteWorkloadMode  {result} SCHEDULE AUDIT -- operations\n"


class ScheduleAuditTest(unittest.TestCase):
    def test_both_phases_pass(self):
        check_schedule_audit([audit("PASSED"), "noise", audit("PASSED")])

    def test_failure_in_either_phase(self):
        for results in [
            ("FAILED", "PASSED"),
            ("PASSED", "FAILED"),
            ("FAILED", "FAILED"),
        ]:
            with self.subTest(results=results), self.assertRaises(ValueError):
                check_schedule_audit(map(audit, results))

    def test_incomplete_or_duplicate_audits(self):
        for results in [[], ["PASSED"], ["PASSED"] * 3]:
            with self.subTest(results=results), self.assertRaises(ValueError):
                check_schedule_audit(map(audit, results))

    def test_actions_timestamps(self):
        check_schedule_audit([f"2026-10-03T21:05:54Z {audit('PASSED')}"] * 2)

    def test_unrelated_pass_mentions_do_not_supply_audit_results(self):
        with self.assertRaises(ValueError):
            check_schedule_audit(["PASSED SCHEDULE AUDIT"] * 2)


if __name__ == "__main__":
    unittest.main()
