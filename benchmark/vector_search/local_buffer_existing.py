"""Measure individual writes on independent copies of a legacy Context database."""

import argparse
import json
import logging
import random
import shutil
import statistics
import time

from local_buffer import vector


def run(args):
    import neug
    from neug import Database

    logging.disable(logging.CRITICAL)
    trial = args.root / f"{args.label}-26000-{args.trial}"
    trial.mkdir()
    report = {
        "label": args.label,
        "package_version": neug.__version__,
        "package_path": neug.__file__,
        "source_db": str(args.source),
        "count": 26000,
        "dimension": 1024,
        "threads": 4,
        "metric": "cosine",
        "m": 50,
        "ef_construction": 500,
        "trial": args.trial,
        "warmup_writes": 3,
        "measured_writes": 30,
    }
    for operation in ("insert", "merge"):
        destination = trial / operation
        shutil.copytree(args.source, destination)
        db = Database(
            str(destination), mode="w", max_thread_num=4, checkpoint_on_close=False
        )
        conn = db.connect()

        def execute(query, params=None):
            return list(conn.execute(query, parameters=params or {}))

        try:
            execute("LOAD vector_search;")
            existing = [
                r[0] for r in execute("MATCH (n:Context) RETURN n.id ORDER BY n.id;")
            ]
            assert len(existing) == 26000
            ids = (
                list(range(max(existing) + 1, max(existing) + 34))
                if operation == "insert"
                else random.Random(1901).sample(existing, 33)
            )
            payloads = [
                {"id": identity, "v": vector(26000 + identity)} for identity in ids
            ]
            query = (
                "CREATE (:Context {id:$id,vector:CAST($v,'FLOAT[1024]')});"
                if operation == "insert"
                else "MERGE (n:Context {id:$id}) SET n.vector=CAST($v,'FLOAT[1024]');"
            )
            samples = []
            for position, payload in enumerate(payloads):
                started = time.perf_counter_ns()
                execute(query, payload)
                elapsed = (time.perf_counter_ns() - started) / 1e6
                if position >= 3:
                    samples.append(elapsed)
            expected = 26000 + (33 if operation == "insert" else 0)
            assert execute("MATCH (n:Context) RETURN count(n);") == [[expected]]
            result = execute(
                "MATCH (n:Context) RETURN n.id,vector_distance_cosine(n.vector,$v) "
                "AS score ORDER BY score LIMIT 1;",
                {"v": payloads[-1]["v"]},
            )
            assert result[0][0] == ids[-1] and abs(result[0][1]) < 1e-5, result
            report[operation] = {
                "samples_ms": samples,
                "median_ms": statistics.median(samples),
                "verified": True,
            }
            (args.root / f"{trial.name}.json").write_text(
                json.dumps(report, indent=2) + "\n"
            )
            print(
                args.label,
                args.trial,
                operation,
                statistics.median(samples),
                flush=True,
            )
        finally:
            conn.close()
            db.close()


if __name__ == "__main__":
    from pathlib import Path

    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--trial", type=int, required=True)
    run(parser.parse_args())
