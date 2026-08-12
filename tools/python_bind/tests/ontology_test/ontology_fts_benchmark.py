#!/usr/bin/env python3
"""Mixed read-only ontology full-text-search throughput benchmark."""

import argparse
import json
import logging
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from neug import Session
from neug.database import Database

CLIENTS = 4
DEFAULT_WEIGHTS = {
    "fts_only": 100,
    "fts_then_graph": 0,
    "scalar_then_fts": 0,
    "graph_then_fts": 0,
}


@dataclass
class WorkloadStats:
    ok: int
    errors: int
    latencies_ms: list


def sql_string_literal(value):
    return "'" + value.replace("'", "''") + "'"


def percentile(values, percentage):
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * percentage) - 1))
    return ordered[index]


def load_database(data_dir, db_path):
    entity_path = data_dir / "entity_fts.jsonl"
    product_path = data_dir / "product.jsonl"
    rel_ep_path = data_dir / "rel_ep.jsonl"
    for path in (entity_path, product_path, rel_ep_path):
        if not path.is_file():
            raise FileNotFoundError(f"ontology data file is unavailable: {path}")

    db = Database(db_path=str(db_path), mode="w")
    conn = db.connect()
    conn.execute("LOAD fts;")
    conn.execute(
        "CREATE NODE TABLE Entity("
        "uid STRING PRIMARY KEY, name STRING, description STRING, "
        "entity_type STRING, product STRING, authority INT64, kg_id STRING);"
    )
    conn.execute(
        "CREATE NODE TABLE Product("
        "name STRING PRIMARY KEY, uid STRING, description STRING);"
    )
    conn.execute(
        "CREATE REL TABLE rel_ep("
        "FROM Entity TO Product, rel_type STRING, content STRING);"
    )
    conn.execute(
        f'COPY Entity FROM (LOAD FROM "{entity_path}" '
        "RETURN uid, name, description, entity_type, product, authority, kg_id);"
    )
    conn.execute(
        f'COPY Product FROM (LOAD FROM "{product_path}" '
        "RETURN name, uid, description);"
    )
    if rel_ep_path.stat().st_size:
        conn.execute(
            f'COPY rel_ep FROM (LOAD FROM "{rel_ep_path}" '
            "RETURN source, target, rel_type, content) "
            '(from="Entity", to="Product");'
        )
    conn.execute(
        "CREATE INDEX entity_description_fts "
        "ON Entity USING FTS (description);"
    )
    conn.close()
    db.close()


def load_query_terms(data_dir, pool_size, seed):
    terms = []
    with (data_dir / "product.jsonl").open(encoding="utf-8") as stream:
        for line in stream:
            name = json.loads(line)["name"]
            if name:
                # Quoting the product name makes punctuation such as the
                # hyphen in DLF-Legacy part of an FTS phrase.
                terms.append(f'"{name}"')
    if not terms:
        raise ValueError("product.jsonl contains no FTS query terms")
    rng = random.Random(seed)
    return rng.sample(terms, min(pool_size, len(terms)))


def build_query_pool(query_terms, topk):
    queries = {name: [] for name in DEFAULT_WEIGHTS}
    for term in query_terms:
        query_literal = sql_string_literal(term)
        queries["fts_only"].append(
            "MATCH (n:Entity) "
            "RETURN n.uid, n.name, n.description, n.entity_type, "
            "n.product, n.authority, "
            f"bm25(n.description, {query_literal}) AS score "
            f"ORDER BY score ASC LIMIT {topk};"
        )
        queries["fts_then_graph"].append(
            "MATCH (n:Entity) "
            f"WITH n, bm25(n.description, {query_literal}) AS score "
            f"ORDER BY score ASC LIMIT {topk} "
            "MATCH (n)-[r:rel_ep]->(p:Product) "
            "RETURN n.uid, n.name, score, p.name, r.rel_type, r.content "
            "ORDER BY score ASC;"
        )

    queries["scalar_then_fts"].append(
        "MATCH (n:Entity) "
        "WHERE n.product = 'Flink' AND n.authority >= 2 "
        "RETURN n.uid, n.name, n.description, n.authority, "
        "bm25(n.description, 'MaxCompute') AS score "
        f"ORDER BY score ASC LIMIT {topk};"
    )
    queries["graph_then_fts"].append(
        "MATCH (n:Entity)-[r:rel_ep]->"
        "(p:Product {name: 'MaxCompute'}) "
        "RETURN n.uid, n.name, n.description, r.rel_type, r.content, "
        "bm25(n.description, 'Flink') AS score "
        f"ORDER BY score ASC LIMIT {topk};"
    )
    return queries


def parse_weights(value):
    weights = {}
    for item in value.split(","):
        name, weight = item.split("=", 1)
        if name not in DEFAULT_WEIGHTS:
            raise ValueError(f"unknown workload: {name}")
        weights[name] = int(weight)
    if (
        set(weights) != set(DEFAULT_WEIGHTS)
        or any(weight < 0 for weight in weights.values())
        or not any(weights.values())
    ):
        raise ValueError(
            "weights must contain every workload, be non-negative, "
            "and include at least one positive value"
        )
    return weights


def validate_warmup_result(name, rows, topk):
    if not rows:
        raise AssertionError(f"{name} returned no rows during warmup")

    score_column = {
        "fts_only": 6,
        "fts_then_graph": 2,
        "scalar_then_fts": 4,
        "graph_then_fts": 5,
    }[name]
    scores = [row[score_column] for row in rows]
    if any(left > right for left, right in zip(scores, scores[1:])):
        raise AssertionError(f"{name} results are not ordered by score")

    if name == "fts_only":
        if len(rows) > topk:
            raise AssertionError(
                f"{name} returned {len(rows)} rows, expected <= {topk}"
            )
        uids = [row[0] for row in rows]
        if len(uids) != len(set(uids)):
            raise AssertionError(f"{name} returned duplicate entity UIDs")
    elif name in {"scalar_then_fts", "graph_then_fts"} and len(rows) > topk:
        raise AssertionError(f"{name} returned {len(rows)} rows, expected <= {topk}")


def run_benchmark(
    db_path, queries, weights, duration, warmup, server_threads, port, seed, topk
):
    db = Database(db_path=str(db_path))
    connection = db.connect()
    connection.execute("LOAD fts;")
    connection.close()
    uri = db.serve(
        host="localhost",
        port=port,
        blocking=False,
        thread_num=server_threads,
    )
    barrier = threading.Barrier(CLIENTS + 1)
    timing = {}
    stats = {name: WorkloadStats(ok=0, errors=0, latencies_ms=[]) for name in weights}
    error_samples = []
    lock = threading.Lock()
    names = list(weights)
    selection_weights = [weights[name] for name in names]

    def worker(worker_id):
        rng = random.Random(seed + worker_id)
        session = Session(endpoint=uri, timeout="30s", num_threads=1)
        barrier.wait()
        while time.perf_counter() < timing["stop_at"]:
            name = rng.choices(names, weights=selection_weights, k=1)[0]
            query = rng.choice(queries[name])
            started = time.perf_counter()
            try:
                rows = list(session.execute(query, access_mode="read"))
                elapsed_ms = (time.perf_counter() - started) * 1000
                if started < timing["measure_at"]:
                    validate_warmup_result(name, rows, topk)
                else:
                    with lock:
                        stats[name].ok += 1
                        stats[name].latencies_ms.append(elapsed_ms)
            except Exception as error:
                if started < timing["measure_at"]:
                    raise RuntimeError(
                        f"warmup correctness validation failed for {name}"
                    ) from error
                with lock:
                    stats[name].errors += 1
                    if len(error_samples) < 10:
                        error_samples.append(f"{name}: {error}")
        session.close()

    try:
        with ThreadPoolExecutor(max_workers=CLIENTS) as executor:
            futures = [
                executor.submit(worker, worker_id) for worker_id in range(CLIENTS)
            ]
            now = time.perf_counter()
            timing["measure_at"] = now + warmup
            timing["stop_at"] = timing["measure_at"] + duration
            barrier.wait()
            for future in futures:
                future.result()
    finally:
        db.stop_serving()
        db.close()

    return stats, error_samples


def print_results(stats, duration, weights):
    all_latencies = []
    total_ok = 0
    total_errors = 0
    print("\nMixed workload results")
    print("| Workload | Weight | OK | Errors | QPS | P50 ms | P95 ms | P99 ms |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for name in weights:
        item = stats[name]
        total_ok += item.ok
        total_errors += item.errors
        all_latencies.extend(item.latencies_ms)
        p50 = (
            f"{statistics.median(item.latencies_ms):.2f}" if item.latencies_ms else "-"
        )
        p95 = f"{percentile(item.latencies_ms, 0.95):.2f}" if item.latencies_ms else "-"
        p99 = f"{percentile(item.latencies_ms, 0.99):.2f}" if item.latencies_ms else "-"
        print(
            f"| {name} | {weights[name]} | {item.ok} | {item.errors} | "
            f"{item.ok / duration:.2f} | {p50} | {p95} | {p99} |"
        )
    print(
        f"\nclients={CLIENTS}, total_qps={total_ok / duration:.2f}, "
        f"ok={total_ok}, errors={total_errors}, "
        f"p50={statistics.median(all_latencies):.2f}ms, "
        f"p95={percentile(all_latencies, 0.95):.2f}ms, "
        f"p99={percentile(all_latencies, 0.99):.2f}ms"
    )


def main():
    logging.getLogger("neug.session").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--skip-load", action="store_true")
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--server-threads", type=int, default=4)
    parser.add_argument("--port", type=int, default=10000)
    parser.add_argument("--topk", type=int, default=20)
    parser.add_argument("--query-pool-size", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--weights",
        default=",".join(
            f"{name}={weight}" for name, weight in DEFAULT_WEIGHTS.items()
        ),
    )
    args = parser.parse_args()

    if not args.skip_load:
        load_database(args.data_dir, args.db_path)
    elif not args.db_path.exists():
        raise FileNotFoundError(f"database is unavailable: {args.db_path}")

    weights = parse_weights(args.weights)
    query_terms = load_query_terms(
        args.data_dir, args.query_pool_size, args.seed
    )
    queries = build_query_pool(query_terms, args.topk)
    stats, errors = run_benchmark(
        args.db_path,
        queries,
        weights,
        args.duration,
        args.warmup,
        args.server_threads,
        args.port,
        args.seed,
        args.topk,
    )
    print_results(stats, args.duration, weights)
    if errors:
        print("\nError samples:")
        for error in errors:
            print(f"  {error}")
    return 1 if any(item.errors for item in stats.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
