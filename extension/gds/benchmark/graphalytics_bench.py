#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# LDBC Graphalytics kernel benchmark driver for NeuG GDS.
#
# Each algorithm is timed via a Cypher CALL with ``LIMIT 1 RETURN count(*)`` so
# the query still executes the full kernel but does not export per-vertex
# results (fair comparison with other systems that time compute only).

import argparse
import os
import shutil
import statistics
import time
from pathlib import Path
from typing import Dict, List, Tuple

import neug


def parse_properties(path: Path) -> Dict[str, str]:
    props: Dict[str, str] = {}
    with path.open() as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            props[k.strip()] = v.strip()
    return props


def find_prop(props, suffix):
    for k, v in props.items():
        if k.endswith(suffix):
            return v
    return None


def sniff_edge_format(edge_file: Path) -> Tuple[int, str]:
    with edge_file.open() as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "," in line:
                return len([x for x in line.split(",") if x.strip()]), ","
            return len(line.split()), " "
    raise RuntimeError("empty edge file")


def ensure_csv_alias(src: Path) -> Path:
    if src.suffix.lower() == ".csv":
        return src
    alias = src.with_name(src.name + ".csv")
    if not alias.exists():
        try:
            os.symlink(src, alias)
        except OSError:
            shutil.copyfile(src, alias)
    return alias


def timed_runs(conn, title: str, query: str, runs: int, results: Dict[str, float]) -> None:
    times: List[float] = []
    for i in range(runs):
        t0 = time.time()
        rows = list(conn.execute(query))
        dt = time.time() - t0
        times.append(dt)
        print(f"[run] {title:<9} #{i + 1} {dt:.3f}s -> {rows}", flush=True)
    median = statistics.median(times)
    results[title.lower()] = median
    run_str = ", ".join(f"{t:.3f}" for t in times)
    print(f"[run] {title:<9} median={median:.3f}s runs=[{run_str}]",
          flush=True)


def main():
    ap = argparse.ArgumentParser(
        description="Time NeuG GDS kernels on an LDBC Graphalytics dataset.")
    default_data = os.environ.get("GRAPHALYTICS_DATA_ROOT")
    default_db = os.environ.get("NEUG_BENCH_DB_ROOT", "./neug_bench_db")
    ap.add_argument("--data-root", default=default_data,
                    help="Directory with <dataset>.properties and graph files "
                         "(or set GRAPHALYTICS_DATA_ROOT)")
    ap.add_argument("--db-root", default=default_db,
                    help="Directory for per-dataset .db checkpoints")
    ap.add_argument("--dataset", default="graph500-22")
    ap.add_argument("--concurrency", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--pr-iterations", type=int, default=10)
    ap.add_argument("--pr-damping", type=float, default=0.85)
    ap.add_argument("--cdlp-iterations", type=int, default=10)
    ap.add_argument("--runs", type=int, default=5,
                    help="Repeat each algorithm query; report median time")
    ap.add_argument("--skip-load", action="store_true",
                    help="Reuse existing .db checkpoint; skip COPY FROM")
    args = ap.parse_args()

    if not args.data_root:
        raise SystemExit("--data-root is required (or set GRAPHALYTICS_DATA_ROOT)")

    data_root = Path(args.data_root)
    db_root = Path(args.db_root)
    db_root.mkdir(parents=True, exist_ok=True)
    dataset = args.dataset

    props = parse_properties(data_root / f"{dataset}.properties")
    directed = str(find_prop(props, ".directed")).lower() == "true"
    bfs_source = int(find_prop(props, ".bfs.source-vertex") or 0)
    sssp_source = int(find_prop(props, ".sssp.source-vertex") or bfs_source)
    e_raw = data_root / find_prop(props, ".edge-file")
    edge_cols, edge_delim = sniff_edge_format(e_raw)
    has_weight = edge_cols >= 3

    db_path = db_root / f"{dataset}.db"
    if args.skip_load:
        if not db_path.exists():
            raise SystemExit(f"--skip-load: missing db {db_path}")
        db_mode = "w"
        checkpoint_on_close = False
    else:
        if db_path.exists():
            shutil.rmtree(db_path) if db_path.is_dir() else db_path.unlink()
        db_mode = "w"
        checkpoint_on_close = True

    print(f"[dataset] {dataset} directed={directed} bfs_source={bfs_source} "
          f"edge_cols={edge_cols} has_weight={has_weight} conc={args.concurrency} "
          f"runs={args.runs} skip_load={args.skip_load}",
          flush=True)

    db = neug.Database(str(db_path), mode=db_mode, buffer_strategy="M_FULL",
                       checkpoint_on_close=checkpoint_on_close)
    conn = db.connect()
    g = f"g_{dataset.replace('-', '_')}"
    bool_str = "true" if directed else "false"
    c = args.concurrency
    results: Dict[str, float] = {}
    try:
        conn.execute("LOAD gds;")

        if not args.skip_load:
            v_file = ensure_csv_alias(data_root / find_prop(props, ".vertex-file"))
            e_file = ensure_csv_alias(e_raw)
            conn.execute("CREATE NODE TABLE IF NOT EXISTS node (id INT64 PRIMARY KEY);")
            if has_weight:
                conn.execute(
                    "CREATE REL TABLE IF NOT EXISTS rel (FROM node TO node, weight DOUBLE);")
            else:
                conn.execute("CREATE REL TABLE IF NOT EXISTS rel (FROM node TO node);")

            t0 = time.time()
            conn.execute(f'COPY node FROM "{v_file}" (HEADER=false);')
            results["load_node"] = time.time() - t0
            print(f"[load] node {results['load_node']:.3f}s", flush=True)

            t0 = time.time()
            if edge_delim == " ":
                conn.execute(f'COPY rel FROM "{e_file}" (HEADER=false, DELIMITER=" ");')
            else:
                conn.execute(f'COPY rel FROM "{e_file}" (HEADER=false);')
            results["load_rel"] = time.time() - t0
            print(f"[load] rel  {results['load_rel']:.3f}s", flush=True)

        conn.execute(f"CALL project_graph('{g}', ['node'], {{'[node, rel, node]':'' }});")

        timed_runs(
            conn, "BFS",
            f"CALL BFS('{g}', {{source: {bfs_source}, directed: {bool_str}, concurrency: {c}}}) "
            "WITH node, distance LIMIT 1 RETURN count(*) AS n;",
            args.runs, results)
        timed_runs(
            conn, "WCC",
            f"CALL WCC('{g}', {{concurrency: {c}}}) WITH node, comp LIMIT 1 RETURN count(*) AS n;",
            args.runs, results)
        timed_runs(
            conn, "PAGERANK",
            f"CALL page_rank('{g}', {{damping_factor: {args.pr_damping}, "
            f"max_iterations: {args.pr_iterations}, directed: {bool_str}, concurrency: {c}}}) "
            "WITH node, rank LIMIT 1 RETURN count(*) AS n;",
            args.runs, results)
        timed_runs(
            conn, "CDLP",
            f"CALL cdlp('{g}', {{max_iterations: {args.cdlp_iterations}, concurrency: {c}}}) "
            "WITH node, label LIMIT 1 RETURN count(*) AS n;",
            args.runs, results)
        timed_runs(
            conn, "LCC",
            f"CALL LCC('{g}', {{directed: {bool_str}, concurrency: {c}}}) "
            "WITH node, lcc LIMIT 1 RETURN count(*) AS n;",
            args.runs, results)
        sssp_opts = f"source: {sssp_source}, directed: {bool_str}, concurrency: {c}"
        if has_weight:
            sssp_opts += ", weight: 'weight'"
        timed_runs(
            conn, "SSSP",
            f"CALL SSSP('{g}', {{{sssp_opts}}}) WITH node, distance LIMIT 1 RETURN count(*) AS n;",
            args.runs, results)
    finally:
        conn.close()
        db.close()

    print("[RESULTS] " + " ".join(f"{k}={v:.3f}" for k, v in results.items()), flush=True)


if __name__ == "__main__":
    main()
