"""Compare ABI-compatible baseline/fixed dylibs on macOS.

Build the insert_capacity_benchmark target. Before rebuilding the optimized
library, copy the baseline libneug.dylib into BUILD/capacity-validation/baseline/;
copy the optimized library into BUILD/capacity-validation/fixed/. Then run this
script with --build BUILD --iterations 24000 --repeats 5.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=6000)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("this comparison runner requires macOS dylibs")
    if args.iterations < 1 or args.repeats < 1:
        parser.error("iterations and repeats must be positive")
    build = args.build.resolve()
    output = build / "capacity-validation"
    for variant in ("baseline", "fixed"):
        library = output / variant / "libneug.dylib"
        if not library.is_file():
            parser.error(f"missing {library}; prepare both library snapshots first")
    output.mkdir(exist_ok=True)
    rows = []
    binary = build / "tests/transaction/insert_capacity_benchmark"
    for mode in ("reserved", "growth"):
        for threads in (1, 2, 4, 8, 12):
            for repeat in range(args.repeats):
                variants = ("baseline", "fixed")
                if repeat % 2:
                    variants = tuple(reversed(variants))
                for variant in variants:
                    name = f"{mode}-{threads}-{repeat}-{variant}"
                    env = dict(os.environ)
                    env["DYLD_LIBRARY_PATH"] = str(output / variant)
                    env["GLOG_minloglevel"] = "2"
                    proc = subprocess.run(
                        [
                            str(binary),
                            str(threads),
                            str(args.iterations),
                            mode,
                            str(output / (name + ".db")),
                        ],
                        env=env,
                        text=True,
                        capture_output=True,
                        timeout=180,
                    )
                    (output / (name + ".log")).write_text(proc.stdout + proc.stderr)
                    if proc.returncode:
                        raise RuntimeError(
                            f"{name} failed: see {output / (name + '.log')}"
                        )
                    result = next(
                        line[7:]
                        for line in proc.stdout.splitlines()
                        if line.startswith("RESULT ")
                    )
                    row = dict(json.loads(result), variant=variant, repeat=repeat)
                    assert row["errors"] == 0
                    rows.append(row)
                    (output / "results.json").write_text(json.dumps(rows, indent=2))
                    print(json.dumps(row), flush=True)
    summary = []
    for mode in ("reserved", "growth"):
        for threads in (1, 2, 4, 8, 12):
            medians = {}
            for variant in ("baseline", "fixed"):
                samples = [
                    r["ops_per_second"]
                    for r in rows
                    if (r["mode"], r["threads"], r["variant"])
                    == (mode, threads, variant)
                ]
                medians[variant] = statistics.median(samples)
            summary.append(
                dict(
                    mode=mode,
                    threads=threads,
                    **medians,
                    change_pct=100 * (medians["fixed"] / medians["baseline"] - 1),
                )
            )
    (output / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
