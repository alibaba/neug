#!/usr/bin/env python3
"""
LSQB (Labelled Subgraph Query Benchmark) Benchmark Script
==========================================================

This script runs LSQB-style queries on NeuG to reproduce the performance
results from the blog article. Uses LDBC SNB SF1 dataset.

Dataset: https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz

Usage:
    python run_benchmark.py --data-dir /path/to/dataset
"""

import argparse
import csv
import json
import os
import shutil
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ──────────────────────────────────────────────────────────────────────
# LSQB-style queries adapted for LDBC SNB SF1 schema
# ──────────────────────────────────────────────────────────────────────

LSQB_QUERIES = {
    # Q1: Long path traversal (9-hop chain)
    1: """
    MATCH (:PLACE {type: 'country'})<-[:ISPARTOF]-(:PLACE {type: 'city'})
          <-[:ISLOCATEDIN]-(:PERSON)<-[:HASMEMBER]-(:FORUM)
          -[:CONTAINEROF]->(:POST)<-[:REPLYOF]-(:COMMENT)
          -[:HASTAG]->(:TAG)-[:HASTYPE]->(:TAGCLASS)
    RETURN count(*) AS count
    """,

    # Q2: 2-hop with comment-post pattern
    2: """
    MATCH (person1:PERSON)-[:KNOWS]->(person2:PERSON),
          (person1)<-[:HASCREATOR]-(comment:COMMENT)
          -[:REPLYOF]->(post:POST)-[:HASCREATOR]->(person2)
    RETURN count(*) AS count
    """,

    # Q3: Triangle pattern in same country
    3: """
    MATCH (country:PLACE {type: 'country'})
    MATCH (person1:PERSON)-[:ISLOCATEDIN]->(city1:PLACE)-[:ISPARTOF]->(country)
    MATCH (person2:PERSON)-[:ISLOCATEDIN]->(city2:PLACE)-[:ISPARTOF]->(country)
    MATCH (person3:PERSON)-[:ISLOCATEDIN]->(city3:PLACE)-[:ISPARTOF]->(country)
    MATCH (person1)-[:KNOWS]->(person2)-[:KNOWS]->(person3)-[:KNOWS]->(person1)
    RETURN count(*) AS count
    """,

    # Q4: Multi-label with likes/replies
    4: """
    MATCH (:TAG)<-[:HASTAG]-(message:POST:COMMENT)-[:HASCREATOR]->(:PERSON),
          (message)<-[:LIKES]-(:PERSON),
          (message)<-[:REPLYOF]-(comment:COMMENT)
    RETURN count(*) AS count
    """,

    # Q5: Tag co-occurrence via comments
    5: """
    MATCH (tag1:TAG)<-[:HASTAG]-(message:POST:COMMENT)
          <-[:REPLYOF]-(comment:COMMENT)-[:HASTAG]->(tag2:TAG)
    WHERE id(tag1) <> id(tag2)
    RETURN count(*) AS count
    """,

    # Q6: 2-hop with interest tags
    6: """
    MATCH (person1:PERSON)-[:KNOWS]->(person2:PERSON)
          -[:KNOWS]->(person3:PERSON)-[:HASINTEREST]->(:TAG)
    WHERE id(person1) <> id(person3)
    RETURN count(*) AS count
    """,

    # Q7: Optional matches for likes/replies
    7: """
    MATCH (:TAG)<-[:HASTAG]-(message:POST:COMMENT)-[:HASCREATOR]->(:PERSON)
    OPTIONAL MATCH (message)<-[:LIKES]-(:PERSON)
    OPTIONAL MATCH (message)<-[:REPLYOF]-(:COMMENT)
    RETURN count(*) AS count
    """,

    # Q8: Tag pattern with NOT EXISTS
    8: """
    MATCH (tag1:TAG)<-[:HASTAG]-(message:POST:COMMENT)
          <-[:REPLYOF]-(comment:COMMENT)-[:HASTAG]->(tag2:TAG)
    WHERE NOT (comment)-[:HASTAG]->(tag1) AND id(tag1) <> id(tag2)
    RETURN count(*) AS count
    """,

    # Q9: 2-hop with NOT EXISTS
    9: """
    MATCH (person1:PERSON)-[:KNOWS]->(person2:PERSON)
          -[:KNOWS]->(person3:PERSON)-[:HASINTEREST]->(:TAG)
    WHERE NOT (person1)-[:KNOWS]->(person3) AND id(person1) <> id(person3)
    RETURN count(*) AS count
    """,
}

# Schema for LDBC SNB SF1
SCHEMA_DDL = [
    "CREATE NODE TABLE PLACE (id INT64, name STRING, url STRING, type STRING, PRIMARY KEY(id))",
    "CREATE NODE TABLE PERSON (id INT64, firstName STRING, lastName STRING, gender STRING, birthday DATE, creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, language STRING, email STRING, PRIMARY KEY(id))",
    "CREATE NODE TABLE COMMENT (id INT64, creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, content STRING, length INT32, PRIMARY KEY(id))",
    "CREATE NODE TABLE POST (id INT64, imageFile STRING, creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, language STRING, content STRING, length INT32, PRIMARY KEY(id))",
    "CREATE NODE TABLE FORUM (id INT64, title STRING, creationDate TIMESTAMP, PRIMARY KEY(id))",
    "CREATE NODE TABLE ORGANISATION (id INT64, type STRING, name STRING, url STRING, PRIMARY KEY(id))",
    "CREATE NODE TABLE TAGCLASS (id INT64, name STRING, url STRING, PRIMARY KEY(id))",
    "CREATE NODE TABLE TAG (id INT64, name STRING, url STRING, PRIMARY KEY(id))",
    "CREATE REL TABLE HASCREATOR (FROM COMMENT TO PERSON, FROM POST TO PERSON, creationDate TIMESTAMP)",
    "CREATE REL TABLE HASTAG (FROM POST TO TAG, FROM FORUM TO TAG, FROM COMMENT TO TAG)",
    "CREATE REL TABLE REPLYOF (FROM COMMENT TO COMMENT, FROM COMMENT TO POST)",
    "CREATE REL TABLE CONTAINEROF (FROM FORUM TO POST)",
    "CREATE REL TABLE HASMEMBER (FROM FORUM TO PERSON, joinDate TIMESTAMP)",
    "CREATE REL TABLE HASMODERATOR (FROM FORUM TO PERSON)",
    "CREATE REL TABLE HASINTEREST (FROM PERSON TO TAG)",
    "CREATE REL TABLE ISLOCATEDIN (FROM COMMENT TO PLACE, FROM PERSON TO PLACE, FROM POST TO PLACE, FROM ORGANISATION TO PLACE)",
    "CREATE REL TABLE KNOWS (FROM PERSON TO PERSON, creationDate TIMESTAMP)",
    "CREATE REL TABLE LIKES (FROM PERSON TO COMMENT, FROM PERSON TO POST, creationDate TIMESTAMP)",
    "CREATE REL TABLE WORKAT (FROM PERSON TO ORGANISATION, workFrom INT32)",
    "CREATE REL TABLE ISPARTOF (FROM PLACE TO PLACE)",
    "CREATE REL TABLE HASTYPE (FROM TAG TO TAGCLASS)",
    "CREATE REL TABLE ISSUBCLASSOF (FROM TAGCLASS TO TAGCLASS)",
    "CREATE REL TABLE STUDYAT (FROM PERSON TO ORGANISATION, classYear INT32)",
]

# CSV preprocessing configurations
DEDUP_CSVS = {
    "static/place_isPartOf_place_0_0.csv": ["from", "to"],
    "dynamic/comment_replyOf_comment_0_0.csv": ["from", "to"],
    "dynamic/person_knows_person_0_0.csv": None,  # keep extra cols
    "static/tagclass_isSubclassOf_tagclass_0_0.csv": ["from", "to"],
}

# Benchmark settings
WARMUP_RUNS = 2
MEASURED_RUNS = 5


@dataclass
class QueryResult:
    query_id: int
    ok: bool
    result: Optional[int]
    elapsed_ms: float
    error: str = ""


def preprocess_csvs(data_dir: Path, derived_dir: Path) -> Dict[str, Path]:
    """Create derived CSVs with unique headers."""
    derived_dir.mkdir(parents=True, exist_ok=True)
    mapping: Dict[str, Path] = {}

    for relpath, new_header in DEDUP_CSVS.items():
        out = derived_dir / Path(relpath).name
        src = data_dir / relpath

        # Only add to mapping if source file exists
        if not src.exists():
            print(f"  Warning: Source CSV not found: {relpath}")
            continue

        # Add to mapping (either use existing derived file or create new one)
        if out.exists():
            mapping[relpath] = out
            continue

        with src.open("r", encoding="utf-8") as fi, \
             out.open("w", encoding="utf-8", newline="") as fo:
            reader = csv.reader(fi, delimiter="|")
            writer = csv.writer(fo, delimiter="|")
            old_header = next(reader)
            if new_header is None:
                new_header = ["from", "to"] + old_header[2:]
            writer.writerow(new_header)
            for row in reader:
                writer.writerow(row)
        mapping[relpath] = out

    return mapping


def create_hascreator_with_date(data_dir: Path, derived_dir: Path) -> Tuple[Path, Path]:
    """Create HASCREATOR CSVs with creationDate from message tables."""
    out_comment = derived_dir / "comment_hasCreator_person_with_date.csv"
    out_post = derived_dir / "post_hasCreator_person_with_date.csv"

    if out_comment.exists() and out_post.exists():
        return out_comment, out_post

    # Load creation dates
    comment_time: Dict[str, str] = {}
    post_time: Dict[str, str] = {}

    with (data_dir / "dynamic" / "comment_0_0.csv").open("r", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="|"):
            comment_time[r["id"]] = r["creationDate"]

    with (data_dir / "dynamic" / "post_0_0.csv").open("r", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="|"):
            post_time[r["id"]] = r["creationDate"]

    # Create derived files
    for rel_csv, time_map, out in [
        ("dynamic/comment_hasCreator_person_0_0.csv", comment_time, out_comment),
        ("dynamic/post_hasCreator_person_0_0.csv", post_time, out_post),
    ]:
        with (data_dir / rel_csv).open("r", encoding="utf-8") as fi, \
             out.open("w", encoding="utf-8", newline="") as fo:
            reader = csv.reader(fi, delimiter="|")
            writer = csv.writer(fo, delimiter="|")
            next(reader, None)
            writer.writerow(["from", "to", "creationDate"])
            for row in reader:
                if len(row) >= 2:
                    writer.writerow([row[0], row[1], time_map.get(row[0], "")])

    return out_comment, out_post


def get_copy_statements(data_dir: Path, dedup_map: Dict[str, Path],
                        hc_comment: Path, hc_post: Path) -> List[str]:
    """Generate COPY statements for data loading."""

    def f(relpath: str) -> str:
        if relpath in dedup_map:
            return str(dedup_map[relpath]).replace("'", "\\'")
        return str(data_dir / relpath).replace("'", "\\'")

    return [
        f"COPY PLACE FROM '{f('static/place_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY PERSON FROM '{f('dynamic/person_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY COMMENT FROM '{f('dynamic/comment_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY POST FROM '{f('dynamic/post_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY FORUM FROM '{f('dynamic/forum_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY ORGANISATION FROM '{f('static/organisation_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY TAGCLASS FROM '{f('static/tagclass_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY TAG FROM '{f('static/tag_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY HASCREATOR FROM '{hc_comment}' (FROM='COMMENT', TO='PERSON', DELIMITER='|', HEADER=true)",
        f"COPY HASCREATOR FROM '{hc_post}' (FROM='POST', TO='PERSON', DELIMITER='|', HEADER=true)",
        f"COPY HASTAG FROM '{f('dynamic/post_hasTag_tag_0_0.csv')}' (FROM='POST', TO='TAG', DELIMITER='|', HEADER=true)",
        f"COPY HASTAG FROM '{f('dynamic/comment_hasTag_tag_0_0.csv')}' (FROM='COMMENT', TO='TAG', DELIMITER='|', HEADER=true)",
        f"COPY HASTAG FROM '{f('dynamic/forum_hasTag_tag_0_0.csv')}' (FROM='FORUM', TO='TAG', DELIMITER='|', HEADER=true)",
        f"COPY REPLYOF FROM '{f('dynamic/comment_replyOf_comment_0_0.csv')}' (FROM='COMMENT', TO='COMMENT', DELIMITER='|', HEADER=true)",
        f"COPY REPLYOF FROM '{f('dynamic/comment_replyOf_post_0_0.csv')}' (FROM='COMMENT', TO='POST', DELIMITER='|', HEADER=true)",
        f"COPY CONTAINEROF FROM '{f('dynamic/forum_containerOf_post_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY HASMEMBER FROM '{f('dynamic/forum_hasMember_person_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY HASMODERATOR FROM '{f('dynamic/forum_hasModerator_person_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY HASINTEREST FROM '{f('dynamic/person_hasInterest_tag_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY ISLOCATEDIN FROM '{f('dynamic/comment_isLocatedIn_place_0_0.csv')}' (FROM='COMMENT', TO='PLACE', DELIMITER='|', HEADER=true)",
        f"COPY ISLOCATEDIN FROM '{f('dynamic/person_isLocatedIn_place_0_0.csv')}' (FROM='PERSON', TO='PLACE', DELIMITER='|', HEADER=true)",
        f"COPY ISLOCATEDIN FROM '{f('dynamic/post_isLocatedIn_place_0_0.csv')}' (FROM='POST', TO='PLACE', DELIMITER='|', HEADER=true)",
        f"COPY ISLOCATEDIN FROM '{f('static/organisation_isLocatedIn_place_0_0.csv')}' (FROM='ORGANISATION', TO='PLACE', DELIMITER='|', HEADER=true)",
        f"COPY KNOWS FROM '{f('dynamic/person_knows_person_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY LIKES FROM '{f('dynamic/person_likes_comment_0_0.csv')}' (FROM='PERSON', TO='COMMENT', DELIMITER='|', HEADER=true)",
        f"COPY LIKES FROM '{f('dynamic/person_likes_post_0_0.csv')}' (FROM='PERSON', TO='POST', DELIMITER='|', HEADER=true)",
        f"COPY WORKAT FROM '{f('dynamic/person_workAt_organisation_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY ISPARTOF FROM '{f('static/place_isPartOf_place_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY HASTYPE FROM '{f('static/tag_hasType_tagclass_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY ISSUBCLASSOF FROM '{f('static/tagclass_isSubclassOf_tagclass_0_0.csv')}' (DELIMITER='|', HEADER=true)",
        f"COPY STUDYAT FROM '{f('dynamic/person_studyAt_organisation_0_0.csv')}' (DELIMITER='|', HEADER=true)",
    ]


def load_data(data_dir: Path, db_path: Path, derived_dir: Path, force: bool = False):
    """Load LDBC SNB SF1 data into NeuG."""
    from neug.database import Database

    print(f"Loading data into NeuG...")
    print(f"  Data directory: {data_dir}")
    print(f"  Database path: {db_path}")

    # Clean up existing database with safety checks
    if db_path.exists():
        # Safety check: refuse to delete dangerous paths
        abs_path = db_path.resolve()
        dangerous_paths = [
            Path.home(),
            Path.home() / "Documents",
            Path("/"),
            Path.cwd(),
        ]
        for dangerous in dangerous_paths:
            if abs_path == dangerous or str(abs_path).startswith(str(dangerous) + "/") and abs_path.parent == dangerous:
                print(f"Error: Refusing to delete path: {db_path}")
                print("       This path appears to be a system or user directory.")
                print("       Please specify a different --db-path.")
                raise SystemExit(1)

        # Check if it looks like a NeuG database
        if not (db_path.is_dir() and (db_path / "graph.yaml").exists()):
            print(f"Warning: {db_path} exists but does not look like a NeuG database.")
            if not force:
                print("       Use --force to overwrite.")
                raise SystemExit(1)

        if not force:
            print(f"Error: Database already exists: {db_path}")
            print("       Use --force to overwrite.")
            raise SystemExit(1)

        print(f"  Removing existing database...")
        shutil.rmtree(db_path)

    # Preprocess CSVs
    print("  Preprocessing CSV files...")
    dedup_map = preprocess_csvs(data_dir, derived_dir)
    hc_comment, hc_post = create_hascreator_with_date(data_dir, derived_dir)

    # Get COPY statements
    copy_stmts = get_copy_statements(data_dir, dedup_map, hc_comment, hc_post)

    # Create database and load data
    db = Database(str(db_path))
    conn = db.connect()

    # Create schema
    print("  Creating schema...")
    for stmt in SCHEMA_DDL:
        conn.execute(stmt)

    # Load data
    print("  Loading data...")
    for i, stmt in enumerate(copy_stmts):
        table = stmt.split("FROM")[0].replace("COPY ", "").strip()
        print(f"    [{i+1}/{len(copy_stmts)}] {table}")
        conn.execute(stmt)

    conn.close()
    db.close()

    print("  Data loading complete.")


def run_query(conn, query: str) -> Tuple[Optional[int], float]:
    """Run a query and return (result, elapsed_ms)."""
    start = time.perf_counter()
    result = conn.execute(query)
    elapsed = (time.perf_counter() - start) * 1000

    data = json.loads(result.get_bolt_response())
    table = data.get("table", [])
    if table and len(table) > 0:
        first_row = table[0]
        if isinstance(first_row, dict):
            for key, value in first_row.items():
                return int(value) if value is not None else 0, elapsed
    return None, elapsed


def run_benchmark(db_path: Path) -> List[QueryResult]:
    """Run LSQB benchmark queries."""
    from neug.database import Database

    print(f"\n{'='*60}")
    print("Running LSQB SF1 Benchmark")
    print(f"{'='*60}")

    results = []

    db = Database(str(db_path))
    conn = db.connect()

    for qid in range(1, 10):
        query = LSQB_QUERIES[qid]
        print(f"\nQuery {qid}:")

        # Warmup
        for _ in range(WARMUP_RUNS):
            try:
                run_query(conn, query)
            except Exception as e:
                print(f"  Warmup error: {e}")

        # Measure
        times = []
        result = None
        ok = True

        for i in range(MEASURED_RUNS):
            try:
                r, elapsed = run_query(conn, query)
                times.append(elapsed)
                if r is not None:
                    result = r
                print(f"  Run {i+1}: {elapsed:.2f}ms, result={r}")
            except Exception as e:
                ok = False
                print(f"  Run {i+1}: ERROR - {e}")
                break

        if ok and times:
            p50 = statistics.median(times)
            avg = statistics.mean(times)
            print(f"  P50: {p50:.2f}ms, Avg: {avg:.2f}ms")
        else:
            times = []

        results.append(QueryResult(
            query_id=qid,
            ok=ok and result is not None,
            result=result,
            elapsed_ms=statistics.median(times) if times else 0,
            error="" if ok else "Query failed"
        ))

    conn.close()
    db.close()

    return results


def print_results(results: List[QueryResult]):
    """Print benchmark results summary."""
    print(f"\n{'='*60}")
    print("Results Summary")
    print(f"{'='*60}")
    print(f"| Query | P50 (ms) | Result | Status |")
    print(f"|-------|----------|--------|--------|")
    for r in results:
        status = "OK" if r.ok else "FAIL"
        result_str = "N/A" if r.result is None else str(r.result)
        print(f"| Q{r.query_id}    | {r.elapsed_ms:8.2f} | {result_str:>12} | {status} |")


def save_results(results: List[QueryResult], output_dir: Path):
    """Save benchmark results to JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)

    data = {
        "engine": "neug",
        "benchmark": "lsqb_sf1",
        "threads": 1,
        "warmup_runs": WARMUP_RUNS,
        "measured_runs": MEASURED_RUNS,
        "results": [
            {
                "query_id": r.query_id,
                "ok": r.ok,
                "result": r.result,
                "elapsed_ms": r.elapsed_ms,
                "error": r.error
            }
            for r in results
        ]
    }

    output_file = output_dir / "neug_lsqb_results.json"
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nResults saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="LSQB Benchmark for NeuG")
    parser.add_argument("--data-dir", type=str, required=True,
                        help="Path to LDBC SNB SF1 dataset directory")
    parser.add_argument("--db-path", type=str, default="./lsqb_sf1.db",
                        help="Path to NeuG database (default: ./lsqb_sf1.db)")
    parser.add_argument("--derived-dir", type=str, default="./derived_csvs",
                        help="Path to derived CSV directory")
    parser.add_argument("--output-dir", type=str, default="./results",
                        help="Path to output directory")
    parser.add_argument("--skip-load", action="store_true",
                        help="Skip data loading (use existing database)")
    parser.add_argument("--force", action="store_true",
                        help="Force overwrite existing database")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path)
    derived_dir = Path(args.derived_dir)
    output_dir = Path(args.output_dir)

    # Check data directory
    if not args.skip_load:
        if not data_dir.exists():
            print(f"Error: Data directory not found: {data_dir}")
            print("\nDownload the dataset from:")
            print("  https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz")
            return 1

        # Load data
        load_data(data_dir, db_path, derived_dir, force=args.force)
    else:
        if not db_path.exists():
            print(f"Error: Database not found: {db_path}")
            return 1

    # Run benchmark
    results = run_benchmark(db_path)

    # Print and save results
    print_results(results)
    save_results(results, output_dir)

    return 0


if __name__ == "__main__":
    exit(main())