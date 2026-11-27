#!/usr/bin/env python3
"""Require successful warmup and measurement audits in the LDBC driver log."""

import argparse
import re
from pathlib import Path
from typing import Iterable

AUDIT = re.compile(r"ExecuteWorkloadMode\s+(PASSED|FAILED) SCHEDULE AUDIT\b")


def check_schedule_audit(lines: Iterable[str]) -> None:
    results = []
    for line in lines:
        match = AUDIT.search(line)
        if match:
            results.append(match.group(1))
    if results != ["PASSED", "PASSED"]:
        raise ValueError(
            "Expected passing warmup and measurement schedule audits; "
            f"found {results}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    args = parser.parse_args()
    try:
        with args.log.open(encoding="utf-8", errors="replace") as log:
            check_schedule_audit(log)
    except (OSError, ValueError) as error:
        parser.exit(1, f"LDBC schedule audit failed: {error}\n")


if __name__ == "__main__":
    main()
