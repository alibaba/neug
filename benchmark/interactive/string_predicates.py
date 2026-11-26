# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""End-to-end embedded string-predicate benchmark for issue #1259.

Run in separate processes against each build, using the same arguments:
  PYTHONPATH=tools/python_bind python benchmark/interactive/string_predicates.py \
      --label fixed --output /tmp/string-predicates-fixed.json

Includes query execution and result consumption; excludes CSV generation/load,
PROFILE and warmup. ASCII data has no line terminators so both implementations
must return identical counts. This is a manual benchmark, not a timing CI test.
"""

import argparse
import csv
import hashlib
import json
import platform
import random
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

import neug


def quote(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def run(args):
    cases = []
    datasets = [
        (7, args.rows),
        (80, args.rows),
        (1100, args.long_rows),
        (4600, args.long_rows),
    ]
    with tempfile.TemporaryDirectory(prefix="neug-string-benchmark-") as directory:
        root = Path(directory)
        db = neug.Database(str(root / "db"), mode="w")
        conn = db.connect()
        try:
            for length, rows in datasets:
                table = f"Text{length}"
                text = (
                    "rhprobe"
                    if length == 7
                    else "rhprobe " + "a" * (length - 16) + " rhprobe"
                )
                assert len(text) == length
                path = root / f"{table}.csv"
                with path.open("w", newline="") as output:
                    writer = csv.writer(output)
                    writer.writerow(["id", "num", "text", "pattern"])
                    writer.writerows((i, 42, text, "rhprobe") for i in range(rows))
                conn.execute(
                    f"CREATE NODE TABLE {table}(id INT64 PRIMARY KEY, "
                    f"num INT64, text VARCHAR({length}), pattern STRING);"
                )
                conn.execute(
                    f"COPY {table} FROM {quote(path)} (header=true, delimiter=',');"
                )
                assert list(
                    conn.execute(f"MATCH (n:{table} {{id: 0}}) RETURN n.text;")
                ) == [[text]]
                for operator in ("CONTAINS", "STARTS WITH", "ENDS WITH"):
                    patterns = [
                        ("rhprobe", rows, "hit"),
                        ("absent", 0, "miss"),
                    ]
                    if operator == "CONTAINS":
                        patterns.extend(
                            [
                                (
                                    "probe" if length == 7 else " rhprobe",
                                    rows,
                                    "late hit",
                                ),
                                ("missing", 0, "fast miss"),
                            ]
                        )
                    for pattern, expected, kind in patterns:
                        cases.append(
                            {
                                "name": f"{length}/{operator}/{kind}",
                                "rows": rows,
                                "expected": expected,
                                "query": (
                                    f"MATCH (n:{table}) WHERE n.text {operator} "
                                    f"{quote(pattern)} RETURN count(*);"
                                ),
                                "parameters": {},
                            }
                        )
            for name, predicate, parameters in [
                ("INT64 equality", "n.num = 42", {}),
                ("STRING equality", "n.text = 'rhprobe'", {}),
                ("column CONTAINS", "n.text CONTAINS n.pattern", {}),
                (
                    "parameter CONTAINS",
                    "n.text CONTAINS $pattern",
                    {"pattern": "rhprobe"},
                ),
            ]:
                table = "Text7" if "equality" in name else "Text80"
                cases.append(
                    {
                        "name": name,
                        "rows": args.rows,
                        "expected": args.rows,
                        "query": (
                            f"MATCH (n:{table}) WHERE {predicate} RETURN count(*);"
                        ),
                        "parameters": parameters,
                    }
                )
            for case in cases:
                profile = conn.execute(
                    "PROFILE " + case["query"],
                    access_mode="read",
                    parameters=case["parameters"],
                )
                assert list(profile) == [[case["expected"]]], case["name"]
                case["operators"] = [
                    op["operator_name"]
                    for op in profile.get_profile_metrics()["operators"]
                ]
                case["samples_ms"] = []
            rng = random.Random(1259)
            for iteration in range(args.warmup + args.repeats):
                order = list(cases)
                rng.shuffle(order)
                for case in order:
                    start = time.perf_counter_ns()
                    result = list(
                        conn.execute(
                            case["query"],
                            access_mode="read",
                            parameters=case["parameters"],
                        )
                    )
                    elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
                    assert result == [[case["expected"]]], case["name"]
                    if iteration >= args.warmup:
                        case["samples_ms"].append(elapsed_ms)
                print(f"{args.label}: completed sweep {iteration + 1}", flush=True)
            for case in cases:
                case["median_ms"] = statistics.median(case["samples_ms"])
                case["min_ms"] = min(case["samples_ms"])
                case["max_ms"] = max(case["samples_ms"])
        finally:
            conn.close()
            db.close()
    report = {
        "label": args.label,
        "revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "predicate_source_sha256": hashlib.sha256(
            Path("src/compiler/function/vector_string_functions.cpp").read_bytes()
        ).hexdigest(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "warmup": args.warmup,
        "repeats": args.repeats,
        "cases": cases,
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=170000)
    parser.add_argument("--long-rows", type=int, default=10000)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    options = parser.parse_args()
    if min(options.rows, options.long_rows, options.repeats) < 1 or options.warmup < 0:
        parser.error(
            "row counts and repeats must be positive; warmup must be nonnegative"
        )
    run(options)
