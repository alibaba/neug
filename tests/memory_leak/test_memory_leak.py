#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import gc
import os
import platform
import shutil
import sys
import tempfile
from contextlib import contextmanager
from typing import Callable, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Platform guard — script is meaningful only on Linux/glibc.
# ---------------------------------------------------------------------------
if not sys.platform.startswith("linux"):
    sys.stderr.write(
        f"[test_memory_leak] platform '{sys.platform}' is not supported; "
        "this script requires Linux/glibc.\n"
    )
    sys.exit(2)


# ---------------------------------------------------------------------------
# RSS sampling and malloc_trim helpers.
# ---------------------------------------------------------------------------
_LIBC = None


def _libc():
    """Lazily resolve libc and bind ``malloc_trim``."""
    global _LIBC
    if _LIBC is None:
        path = ctypes.util.find_library("c") or "libc.so.6"
        _LIBC = ctypes.CDLL(path, use_errno=True)
        _LIBC.malloc_trim.argtypes = [ctypes.c_size_t]
        _LIBC.malloc_trim.restype = ctypes.c_int
    return _LIBC


def malloc_trim(pad: int = 0) -> int:
    """Call ``malloc_trim(pad)``; return libc's return value (1 if released)."""
    return _libc().malloc_trim(pad)


def get_rss_mb() -> float:
    """Return the current process VmRSS in MB (read from /proc/self/status)."""
    with open("/proc/self/status", "r") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                # Format: "VmRSS:\t  123456 kB"
                kb = int(line.split()[1])
                return kb / 1024.0
    raise RuntimeError("VmRSS not found in /proc/self/status")


def linear_slope_per_1000(samples: List[Tuple[int, float]]) -> float:
    """Compute slope (MB per 1000 cycles) via least-squares.

    ``samples`` is a list of (cycle_index, rss_mb).  Returns 0.0 when there
    are fewer than two distinct points.
    """
    n = len(samples)
    if n < 2:
        return 0.0
    sx = sum(p[0] for p in samples)
    sy = sum(p[1] for p in samples)
    sxx = sum(p[0] * p[0] for p in samples)
    sxy = sum(p[0] * p[1] for p in samples)
    denom = n * sxx - sx * sx
    if denom == 0:
        return 0.0
    slope_per_cycle = (n * sxy - sx * sy) / denom
    return slope_per_cycle * 1000.0


# ---------------------------------------------------------------------------
# Scenario harness.
# ---------------------------------------------------------------------------
class ScenarioResult:
    __slots__ = (
        "name",
        "iters",
        "sample",
        "base",
        "before_close",
        "after_close",
        "after_trim",
        "slope",
    )

    def __init__(self, name: str, iters: int, sample: int):
        self.name = name
        self.iters = iters
        self.sample = sample
        self.base = 0.0
        self.before_close = 0.0
        self.after_close = 0.0
        self.after_trim = 0.0
        self.slope = 0.0

    @property
    def delta(self) -> float:
        return self.after_trim - self.base

    def is_leak(self, slope_threshold: float, delta_threshold: float) -> bool:
        return self.slope > slope_threshold or self.delta > delta_threshold


@contextmanager
def isolated_dbdir(prefix: str):
    path = tempfile.mkdtemp(prefix=f"neug_rss_{prefix}_")
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


# ---------------------------------------------------------------------------
# Scenarios — each takes (iters, sample) and returns ScenarioResult.
# ---------------------------------------------------------------------------
def scenario_steady(iters: int, sample: int) -> ScenarioResult:
    """A. Long-running fixed query on a stable Database/Connection."""
    from neug import Database  # imported lazily so platform guard runs first

    res = ScenarioResult("steady", iters, sample)
    with isolated_dbdir("steady") as db_path:
        db = Database(db_path=db_path, mode="w")
        conn = db.connect()
        conn.execute("CREATE NODE TABLE n(id INT64 PRIMARY KEY, name STRING);")
        conn.execute("CREATE (a:n {id: 1, name: 'a'});", access_mode="i")

        gc.collect()
        res.base = get_rss_mb()
        samples: List[Tuple[int, float]] = [(0, res.base)]

        for i in range(1, iters + 1):
            conn.execute("MATCH (a:n) RETURN a.id, a.name;", access_mode="r")
            if i % sample == 0:
                samples.append((i, get_rss_mb()))

        res.before_close = get_rss_mb()
        conn.close()
        db.close()
        del conn, db
        gc.collect()
        res.after_close = get_rss_mb()
        malloc_trim(0)
        res.after_trim = get_rss_mb()
        res.slope = linear_slope_per_1000(samples)
    return res


def scenario_lifecycle(iters: int, sample: int) -> ScenarioResult:
    """B. Repeated Database open/close cycles."""
    from neug import Database

    res = ScenarioResult("lifecycle", iters, sample)
    with isolated_dbdir("lifecycle") as db_path:
        # Pre-create the schema once so each open does not perform DDL.
        seed = Database(db_path=db_path, mode="w")
        seed_conn = seed.connect()
        seed_conn.execute("CREATE NODE TABLE n(id INT64 PRIMARY KEY, name STRING);")
        seed_conn.close()
        seed.close()
        del seed_conn, seed
        gc.collect()

        res.base = get_rss_mb()
        samples: List[Tuple[int, float]] = [(0, res.base)]

        for i in range(1, iters + 1):
            db = Database(db_path=db_path, mode="w")
            conn = db.connect()
            conn.execute("MATCH (a:n) RETURN count(a);", access_mode="r")
            conn.close()
            db.close()
            del conn, db
            if i % sample == 0:
                gc.collect()
                samples.append((i, get_rss_mb()))

        gc.collect()
        res.before_close = get_rss_mb()
        # nothing left to close in this scenario
        res.after_close = res.before_close
        malloc_trim(0)
        res.after_trim = get_rss_mb()
        res.slope = linear_slope_per_1000(samples)
    return res


def scenario_ddl(iters: int, sample: int) -> ScenarioResult:
    """C. CREATE NODE TABLE / DROP TABLE alternating cycles."""
    from neug import Database

    res = ScenarioResult("ddl", iters, sample)
    with isolated_dbdir("ddl") as db_path:
        db = Database(db_path=db_path, mode="w")
        conn = db.connect()

        gc.collect()
        res.base = get_rss_mb()
        samples: List[Tuple[int, float]] = [(0, res.base)]

        for i in range(1, iters + 1):
            tname = f"t_{i}"
            conn.execute(
                f"CREATE NODE TABLE {tname}(id INT64 PRIMARY KEY, name STRING);",
                access_mode="s",
            )
            conn.execute(f"DROP TABLE {tname};", access_mode="s")
            if i % sample == 0:
                samples.append((i, get_rss_mb()))

        res.before_close = get_rss_mb()
        conn.close()
        db.close()
        del conn, db
        gc.collect()
        res.after_close = get_rss_mb()
        malloc_trim(0)
        res.after_trim = get_rss_mb()
        res.slope = linear_slope_per_1000(samples)
    return res


def scenario_cache(iters: int, sample: int) -> ScenarioResult:
    """D. Distinct query texts to exercise planner / query cache."""
    from neug import Database

    res = ScenarioResult("cache", iters, sample)
    with isolated_dbdir("cache") as db_path:
        db = Database(db_path=db_path, mode="w")
        conn = db.connect()
        conn.execute("CREATE NODE TABLE n(id INT64 PRIMARY KEY, name STRING);")

        gc.collect()
        res.base = get_rss_mb()
        samples: List[Tuple[int, float]] = [(0, res.base)]

        for i in range(1, iters + 1):
            # Inject the loop counter into the query text so each call has a
            # distinct hash; this stresses any cache keyed by query text.
            conn.execute(
                f"MATCH (a:n) WHERE a.id = {i} RETURN a.id, a.name;",
                access_mode="r",
            )
            if i % sample == 0:
                samples.append((i, get_rss_mb()))

        res.before_close = get_rss_mb()
        conn.close()
        db.close()
        del conn, db
        gc.collect()
        res.after_close = get_rss_mb()
        malloc_trim(0)
        res.after_trim = get_rss_mb()
        res.slope = linear_slope_per_1000(samples)
    return res


SCENARIOS: Dict[str, Callable[[int, int], ScenarioResult]] = {
    "steady": scenario_steady,
    "lifecycle": scenario_lifecycle,
    "ddl": scenario_ddl,
    "cache": scenario_cache,
}


# ---------------------------------------------------------------------------
# CLI / reporting.
# ---------------------------------------------------------------------------
def format_summary(
    results: List[ScenarioResult],
    slope_threshold: float,
    delta_threshold: float,
) -> str:
    header = (
        f"{'scenario':<11} {'iters':>7} {'base':>9} {'before':>9} "
        f"{'after_close':>12} {'after_trim':>11} {'delta':>8} "
        f"{'slope/1k':>10}  verdict"
    )
    lines = [header, "-" * len(header)]
    for r in results:
        verdict = (
            "LEAK?" if r.is_leak(slope_threshold, delta_threshold) else "ok"
        )
        lines.append(
            f"{r.name:<11} {r.iters:>7d} "
            f"{r.base:>9.2f} {r.before_close:>9.2f} "
            f"{r.after_close:>12.2f} {r.after_trim:>11.2f} "
            f"{r.delta:>+8.2f} {r.slope:>+10.3f}  {verdict}"
        )
    lines.append("")
    lines.append(
        f"thresholds: slope > {slope_threshold:.2f} MB/1k cycles "
        f"OR after_trim-base > {delta_threshold:.2f} MB  =>  LEAK?"
    )
    return "\n".join(lines)


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="test_memory_leak",
        description=(
            "Run RSS-based memory-leak smoke tests against the NeuG Python "
            "API. Linux/glibc only. Build NeuG in RELEASE with "
            "WITH_MIMALLOC=OFF for meaningful results."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--iters", type=int, default=2000,
        help="Number of loop cycles per scenario.",
    )
    p.add_argument(
        "--sample", type=int, default=100,
        help="Sampling interval — collect RSS every N cycles.",
    )
    p.add_argument(
        "--scenarios", nargs="+", default=list(SCENARIOS.keys()),
        choices=list(SCENARIOS.keys()),
        help="Subset of scenarios to run.",
    )
    p.add_argument(
        "--slope-threshold", type=float, default=0.5,
        help="Slope (MB per 1000 cycles) above which the scenario is flagged.",
    )
    p.add_argument(
        "--delta-threshold", type=float, default=5.0,
        help="after_trim - base (MB) above which the scenario is flagged.",
    )
    p.add_argument(
        "--exit-on-leak", action="store_true",
        help="Exit with code 1 when any scenario is flagged as LEAK?.",
    )
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    if args.sample <= 0 or args.iters <= 0:
        sys.stderr.write("[test_memory_leak] --iters and --sample must be > 0\n")
        return 2

    print(
        f"[test_memory_leak] platform={platform.platform()} "
        f"pid={os.getpid()} iters={args.iters} sample={args.sample}"
    )
    print(f"[test_memory_leak] scenarios={args.scenarios}")

    results: List[ScenarioResult] = []
    for name in args.scenarios:
        fn = SCENARIOS[name]
        print(f"[test_memory_leak] running scenario: {name} ...", flush=True)
        results.append(fn(args.iters, args.sample))

    print()
    print(format_summary(results, args.slope_threshold, args.delta_threshold))

    if args.exit_on_leak:
        leaked = any(
            r.is_leak(args.slope_threshold, args.delta_threshold)
            for r in results
        )
        if leaked:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
