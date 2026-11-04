"""Compare Local Buffer write latency using the ontology reproduction workload.

Run each version in a separate process with its own PYTHONPATH and extension
home. Dataset generation, database setup, and validation are not timed.
"""

import argparse
import hashlib
import json
import logging
import platform
import random
import shutil
import statistics
import time
from pathlib import Path

DIMENSION = 1024


def vector(identity):
    rng = random.Random(811 + identity)
    return [round(rng.uniform(-1, 1), 5) for _ in range(DIMENSION)]


def prepare(root):
    root.mkdir(parents=True, exist_ok=True)
    for count in (1000, 10000):
        path = root / f"vectors-{count}.csv"
        if path.exists():
            raise FileExistsError(path)
        with path.open("w") as output:
            output.write("id|name|value|embedding\n")
            for identity in range(count):
                payload = json.dumps(vector(identity), separators=(",", ":"))
                output.write(f"{identity}|entity_{identity}|0|{payload}\n")


def run(args):
    import neug
    from neug import Database

    logging.disable(logging.CRITICAL)
    root = args.root
    csv = root / f"vectors-{args.count}.csv"
    trial = root / f"{args.label}-{args.count}-{args.trial}"
    trial.mkdir()
    report = {
        "label": args.label,
        "package_version": neug.__version__,
        "commit": getattr(neug, "__commit__", "unknown"),
        "package_path": neug.__file__,
        "platform": platform.platform(),
        "count": args.count,
        "dimension": DIMENSION,
        "threads": 4,
        "metric": "cosine",
        "m": 50,
        "ef_construction": 500,
        "trial": args.trial,
        "csv_sha256": hashlib.sha256(csv.read_bytes()).hexdigest(),
        "warmup_writes": 3,
        "measured_writes": 30,
    }
    result_path = root / f"{trial.name}.json"

    def save():
        result_path.write_text(json.dumps(report, indent=2) + "\n")

    def open_db(path):
        db = Database(str(path), mode="w", max_thread_num=4, checkpoint_on_close=False)
        conn = db.connect()
        conn.execute("LOAD vector_search;")
        return db, conn

    def execute(conn, query, params=None):
        return list(conn.execute(query, parameters=params or {}))

    base = trial / "base"
    db, conn = open_db(base)
    try:
        execute(
            conn,
            "CREATE NODE TABLE Entity(id INT64 PRIMARY KEY, name STRING, "
            "value INT64, embedding FLOAT[1024]);",
        )
        execute(
            conn,
            "CREATE INDEX entity_embedding ON Entity USING HNSW (embedding) "
            "WITH (metric='cosine', m=50, ef_construction=500);",
        )
        query = (
            f'COPY Entity FROM (LOAD FROM "{csv}" '
            "RETURN id,name,value,CAST(embedding, 'FLOAT[1024]') AS embedding);"
        )
        started = time.perf_counter_ns()
        execute(conn, query)
        report["copy_ms"] = (time.perf_counter_ns() - started) / 1e6
        assert execute(conn, "MATCH (n:Entity) RETURN count(n);") == [[args.count]]
        execute(conn, "CHECKPOINT;")
        save()
        print(args.label, args.count, args.trial, "COPY", report["copy_ms"], flush=True)
    finally:
        conn.close()
        db.close()

    for operation in ("insert", "merge"):
        destination = trial / operation
        shutil.copytree(base, destination)
        db, conn = open_db(destination)
        samples = []
        ids = (
            list(range(args.count, args.count + 33))
            if operation == "insert"
            else random.Random(1901).sample(range(args.count), 33)
        )
        payloads = [
            {
                "id": identity,
                "name": f"written_{identity}",
                "value": 1,
                "embedding": vector(args.count + identity),
            }
            for identity in ids
        ]
        query = (
            "CREATE (:Entity {id:$id,name:$name,value:$value,"
            "embedding:CAST($embedding, 'FLOAT[1024]')});"
            if operation == "insert"
            else "MERGE (n:Entity {id:$id}) SET n.name=$name,n.value=$value,"
            "n.embedding=CAST($embedding, 'FLOAT[1024]');"
        )
        try:
            for position, payload in enumerate(payloads):
                started = time.perf_counter_ns()
                execute(conn, query, payload)
                elapsed = (time.perf_counter_ns() - started) / 1e6
                if position >= 3:
                    samples.append(elapsed)
            expected = args.count + (33 if operation == "insert" else 0)
            assert execute(conn, "MATCH (n:Entity) RETURN count(n);") == [[expected]]
            assert execute(
                conn, "MATCH (n:Entity) WHERE n.value=1 RETURN count(n);"
            ) == [[33]]
            search = execute(
                conn,
                "MATCH (n:Entity) RETURN n.id,vector_distance_cosine(n.embedding,$v) "
                "AS score ORDER BY score LIMIT 1;",
                {"v": payloads[-1]["embedding"]},
            )
            assert search[0][0] == ids[-1] and abs(search[0][1]) < 1e-5, search
            report[operation] = {
                "samples_ms": samples,
                "median_ms": statistics.median(samples),
                "verified": True,
            }
            save()
            print(
                args.label,
                args.count,
                args.trial,
                operation,
                report[operation]["median_ms"],
                flush=True,
            )
        finally:
            conn.close()
            db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--label")
    parser.add_argument("--count", type=int, choices=(1000, 10000))
    parser.add_argument("--trial", type=int)
    args = parser.parse_args()
    if args.prepare:
        prepare(args.root)
    else:
        run(args)
