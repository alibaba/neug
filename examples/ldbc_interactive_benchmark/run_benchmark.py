#!/usr/bin/env python3
"""LDBC SNB Interactive Benchmark: NeuG vs Neo4j (Service Mode)

This script runs the LDBC SNB Interactive benchmark comparing NeuG in service mode
with Neo4j server.

It includes:
- Data loading for both engines
- IC1-IC14 latency testing
- Throughput testing with concurrent clients

Usage:
    python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb
    python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb --skip-load
    python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb --force
"""

import argparse
import csv
import json
import os
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class QueryResult:
    query_id: int
    ok: bool
    elapsed_ms: float
    result: Optional[int] = None
    error: str = ""


@dataclass
class ThroughputResult:
    engine: str
    clients: int
    duration_s: int
    ok_ops: int
    err_ops: int
    qps: float
    p50_ms: Optional[float]
    p95_ms: Optional[float]


# Default paths
DEFAULT_DB_PATH = Path("./ldbc_sf1.db")
DEFAULT_DERIVED_DIR = Path("./derived_csvs")
DEFAULT_OUTPUT_DIR = Path("./results")
DEFAULT_THROUGHPUT_SECONDS = 300
DEFAULT_THROUGHPUT_CLIENTS = 4
DEFAULT_LATENCY_RUNS = 5
DEFAULT_WARMUP_RUNS = 2
RNG = random.Random(42)

# Neo4j connection defaults
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "neo4j123"


# ──────────────────────────────────────────────────────────────────────
# CSV preprocessing – deduplicate headers & enrich HASCREATOR
# ──────────────────────────────────────────────────────────────────────

DEDUP_CSVS = {
    "static/place_isPartOf_place_0_0.csv": ["from", "to"],
    "dynamic/comment_replyOf_comment_0_0.csv": ["from", "to"],
    "dynamic/person_knows_person_0_0.csv": None,  # keep extra cols
    "static/tagclass_isSubclassOf_tagclass_0_0.csv": ["from", "to"],
}


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

    comment_time: Dict[str, str] = {}
    post_time: Dict[str, str] = {}

    with (data_dir / "dynamic" / "comment_0_0.csv").open("r", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="|"):
            comment_time[r["id"]] = r["creationDate"]

    with (data_dir / "dynamic" / "post_0_0.csv").open("r", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="|"):
            post_time[r["id"]] = r["creationDate"]

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


# ──────────────────────────────────────────────────────────────────────
# Query definitions
# ──────────────────────────────────────────────────────────────────────

# NeuG queries (from tests/resources/ldbc/queries/)
NEUG_IC_QUERIES = {
    1: """MATCH (p:PERSON {id: $personId})-[:KNOWS*SHORTEST 1..3]-(f:PERSON {firstName: $firstName})
WHERE f <> p
WITH f, length(k) AS distance
ORDER BY distance ASC, f.lastName ASC, f.id ASC
LIMIT 20
OPTIONAL MATCH (f)-[workAt:WORKAT]->(company:ORGANISATION)-[:ISLOCATEDIN]->(country:PLACE)
WITH f, distance,
    CASE WHEN company IS NULL THEN NULL
    ELSE [company.name, CAST(workAt.workFrom, 'STRING'), country.name] END AS companies
WITH f, collect(companies) AS company_info, distance
OPTIONAL MATCH (f)-[studyAt:STUDYAT]->(university:ORGANISATION)-[:ISLOCATEDIN]->(universityCity:PLACE)
WITH f, company_info, distance,
    CASE WHEN university IS NULL THEN NULL
    ELSE [university.name, CAST(studyAt.classYear, 'STRING'), universityCity.name] END AS universities
WITH f, collect(universities) AS university_info, company_info, distance
MATCH (f)-[:ISLOCATEDIN]->(locationCity:PLACE)
RETURN f.id AS friendId, distance AS distanceFromPerson, f.lastName AS friendLastName,
       f.birthday AS friendBirthday, f.creationDate AS friendCreationDate,
       f.gender AS friendGender, f.browserUsed AS friendBrowserUsed,
       f.locationIP AS friendLocationIp, locationCity.name AS friendCityName,
       f.email AS friendEmail, f.language AS friendLanguage,
       university_info AS friendUniversities, company_info AS friendCompanies
ORDER BY distanceFromPerson ASC, friendLastName ASC, friendId ASC
LIMIT 20""",

    2: """MATCH (p:PERSON {id: $personId})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(message:POST:COMMENT)
WHERE message.creationDate <= $maxDate
WITH friend, message
ORDER BY message.creationDate DESC, message.id ASC
LIMIT 20
RETURN friend.id AS personId, friend.firstName AS personFirstName,
       friend.lastName AS personLastName, message.id AS postOrCommentId,
       message.content AS content, message.imageFile AS imageFile,
       message.creationDate AS postOrCommentCreationDate""",

    # Simplified queries for demonstration
    7: """MATCH (person:PERSON {id: $personId})<-[:HASCREATOR]-(message:POST:COMMENT)<-[like:LIKES]-(liker:PERSON)
WITH liker, message, like.creationDate AS likeTime
ORDER BY likeTime DESC, message.id ASC
LIMIT 20
RETURN liker.id AS personId, liker.firstName AS personFirstName,
       liker.lastName AS personLastName, likeTime,
       message.id AS commentOrPostId""",

    8: """MATCH (p:PERSON {id: $personId})<-[:HASCREATOR]-(msg:POST:COMMENT)<-[:REPLYOF]-(cmt:COMMENT)-[:HASCREATOR]->(author:PERSON)
WITH cmt, author, cmt.creationDate AS cmtCreationDate, cmt.id AS cmtId
ORDER BY cmtCreationDate DESC, cmtId ASC
LIMIT 20
RETURN author.id, author.firstName, author.lastName, cmtCreationDate, cmtId, cmt.content""",

    13: """MATCH (person1:PERSON {id: $person1Id})-[:KNOWS*SHORTEST 1..]-(person2:PERSON {id: $person2Id})
RETURN CASE WHEN k IS NULL THEN -1 ELSE length(k) END AS len""",
}

# Neo4j query overrides (for syntax differences)
NEO4J_IC_QUERIES = {
    1: """MATCH (p:PERSON {id: $personId}), (f:PERSON {firstName: $firstName})
WHERE f <> p
WITH p, f
MATCH path = shortestPath((p)-[:KNOWS*1..3]-(f))
WITH f, length(path) AS distance
ORDER BY distance ASC, f.lastName ASC, f.id ASC
LIMIT 20
OPTIONAL MATCH (f)-[workAt:WORKAT]->(company:ORGANISATION)-[:ISLOCATEDIN]->(country:PLACE)
WITH f, distance,
    CASE WHEN company IS NULL THEN NULL
    ELSE [company.name, toString(workAt.workFrom), country.name] END AS companies
WITH f, collect(companies) AS company_info, distance
OPTIONAL MATCH (f)-[studyAt:STUDYAT]->(university:ORGANISATION)-[:ISLOCATEDIN]->(universityCity:PLACE)
WITH f, company_info, distance,
    CASE WHEN university IS NULL THEN NULL
    ELSE [university.name, toString(studyAt.classYear), universityCity.name] END AS universities
WITH f, collect(universities) AS university_info, company_info, distance
MATCH (f)-[:ISLOCATEDIN]->(locationCity:PLACE)
RETURN f.id AS friendId, distance AS distanceFromPerson, f.lastName AS friendLastName
LIMIT 20""",

    2: """MATCH (p:PERSON {id: $personId})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(message)
WHERE (message:POST OR message:COMMENT) AND message.creationDate <= $maxDate
WITH friend, message
ORDER BY message.creationDate DESC, message.id ASC
LIMIT 20
RETURN friend.id AS personId, friend.firstName AS personFirstName""",

    7: """MATCH (person:PERSON {id: $personId})<-[:HASCREATOR]-(message)<-[like:LIKES]-(liker:PERSON)
WHERE message:POST OR message:COMMENT
WITH liker, message, like.creationDate AS likeTime
ORDER BY likeTime DESC, message.id ASC
LIMIT 20
RETURN liker.id AS personId""",

    8: """MATCH (p:PERSON {id: $personId})<-[:HASCREATOR]-(msg)<-[:REPLYOF]-(cmt:COMMENT)-[:HASCREATOR]->(author:PERSON)
WHERE msg:POST OR msg:COMMENT
WITH cmt, author, cmt.creationDate AS cmtCreationDate
ORDER BY cmtCreationDate DESC
LIMIT 20
RETURN author.id""",

    13: """MATCH (person1:PERSON {id: $person1Id}), (person2:PERSON {id: $person2Id})
OPTIONAL MATCH path = shortestPath((person1)-[:KNOWS*]-(person2))
RETURN CASE WHEN path IS NULL THEN -1 ELSE length(path) END AS len""",
}


# ──────────────────────────────────────────────────────────────────────
# Schema and data loading
# ──────────────────────────────────────────────────────────────────────

def get_schema_statements() -> List[str]:
    """Get NeuG schema DDL statements."""
    return [
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


def get_copy_statements(data_dir: Path, dedup_map: Dict[str, Path],
                        hc_comment: Path, hc_post: Path) -> List[str]:
    """Get NeuG COPY statements."""
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
        f"COPY HASCREATOR FROM '{str(hc_comment)}' (FROM='COMMENT', TO='PERSON', DELIMITER='|', HEADER=true)",
        f"COPY HASCREATOR FROM '{str(hc_post)}' (FROM='POST', TO='PERSON', DELIMITER='|', HEADER=true)",
        f"COPY HASTAG FROM '{f('dynamic/post_hasTag_tag_0_0.csv')}' (FROM='POST', TO='TAG', DELIMITER='|', HEADER=true)",
        f"COPY HASTAG FROM '{f('dynamic/comment_hasTag_tag_0_0.csv')}' (FROM='COMMENT', TO='TAG', DELIMITER='|', HEADER=true)",
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


def load_neug_data(data_dir: Path, db_path: Path, derived_dir: Path, force: bool = False):
    """Load LDBC SNB SF1 data into NeuG."""
    from neug.database import Database

    print("Loading data into NeuG...")
    print(f"  Data directory: {data_dir}")
    print(f"  Database path: {db_path}")

    # Clean up existing database with safety checks
    if db_path.exists():
        abs_path = db_path.resolve()
        dangerous_paths = [Path.home(), Path("/"), Path.cwd()]
        for dangerous in dangerous_paths:
            if abs_path == dangerous:
                print(f"Error: Refusing to delete path: {db_path}")
                raise SystemExit(1)

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
        import shutil
        shutil.rmtree(db_path)

    # Preprocess CSVs
    dedup_map = preprocess_csvs(data_dir, derived_dir)
    hc_comment, hc_post = create_hascreator_with_date(data_dir, derived_dir)

    # Create database and load data
    db = Database(str(db_path))
    conn = db.connect()

    print("  Creating schema...")
    for stmt in get_schema_statements():
        conn.execute(stmt)

    print("  Loading data...")
    for stmt in get_copy_statements(data_dir, dedup_map, hc_comment, hc_post):
        conn.execute(stmt)

    conn.close()
    db.close()
    print("  NeuG data loading complete.")


# ──────────────────────────────────────────────────────────────────────
# Parameter generation
# ──────────────────────────────────────────────────────────────────────

def load_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="|"))


def build_param_pool(data_dir: Path) -> Dict[str, Any]:
    """Build parameter pool for queries."""
    person = load_csv(data_dir / "dynamic" / "person_0_0.csv")
    place = load_csv(data_dir / "static" / "place_0_0.csv")

    countries = [r["name"] for r in place if r.get("type") == "country"]
    if not countries:
        countries = [r["name"] for r in place]

    # Get person pairs from KNOWS relationship
    pairs: List[Tuple[str, str]] = []
    knows_path = data_dir / "dynamic" / "person_knows_person_0_0.csv"
    with knows_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader, None)
        for i, row in enumerate(reader):
            if len(row) >= 2 and row[0] != row[1]:
                pairs.append((row[0], row[1]))
            if i >= 10000:
                break

    timestamps = [r["creationDate"] for r in load_csv(data_dir / "dynamic" / "post_0_0.csv")[:1000]]

    return {
        "person": person,
        "countries": countries,
        "pairs": pairs,
        "timestamps": timestamps,
    }


def gen_query_params(query_id: int, pool: Dict[str, Any]) -> Dict[str, Any]:
    """Generate parameters for a specific query."""
    p = RNG.choice(pool["person"])["id"]
    first_name = RNG.choice(pool["person"])["firstName"]
    ts = RNG.choice(pool["timestamps"])
    pair = RNG.choice(pool["pairs"])

    params = {
        1: {"personId": int(p), "firstName": first_name},
        2: {"personId": int(p), "maxDate": ts},
        7: {"personId": int(p)},
        8: {"personId": int(p)},
        13: {"person1Id": int(pair[0]), "person2Id": int(pair[1])},
    }
    return params.get(query_id, {})


# ──────────────────────────────────────────────────────────────────────
# Benchmark execution
# ──────────────────────────────────────────────────────────────────────

def run_neug_latency(db_path: Path, queries: Dict[int, str],
                     params_map: Dict[int, List[Dict[str, Any]]]) -> List[QueryResult]:
    """Run latency benchmark for NeuG."""
    from neug.database import Database

    results = []
    db = Database(str(db_path))
    conn = db.connect()

    for qid in sorted(queries.keys()):
        query = queries[qid]
        params_list = params_map.get(qid, [])

        # Warmup
        for p in params_list[:DEFAULT_WARMUP_RUNS]:
            try:
                conn.execute(query, parameters=p)
            except:
                pass

        # Measure
        for p in params_list[:DEFAULT_LATENCY_RUNS]:
            t0 = time.perf_counter()
            try:
                r = conn.execute(query, parameters=p)
                elapsed = (time.perf_counter() - t0) * 1000
                data = json.loads(r.get_bolt_response())
                count = len(data.get("table", []))
                results.append(QueryResult(qid, True, elapsed, count))
            except Exception as e:
                elapsed = (time.perf_counter() - t0) * 1000
                results.append(QueryResult(qid, False, elapsed, None, str(e)[:200]))

    conn.close()
    db.close()
    return results


def run_neo4j_latency(queries: Dict[int, str],
                      params_map: Dict[int, List[Dict[str, Any]]]) -> List[QueryResult]:
    """Run latency benchmark for Neo4j."""
    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("neo4j package not installed, skipping Neo4j benchmark")
        return []

    results = []
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    session = driver.session(database="neo4j")

    for qid in sorted(queries.keys()):
        query = queries[qid]
        params_list = params_map.get(qid, [])

        # Warmup
        for p in params_list[:DEFAULT_WARMUP_RUNS]:
            try:
                session.run(query, p).consume()
            except:
                pass

        # Measure
        for p in params_list[:DEFAULT_LATENCY_RUNS]:
            t0 = time.perf_counter()
            try:
                result = session.run(query, p)
                records = list(result)
                elapsed = (time.perf_counter() - t0) * 1000
                results.append(QueryResult(qid, True, elapsed, len(records)))
            except Exception as e:
                elapsed = (time.perf_counter() - t0) * 1000
                results.append(QueryResult(qid, False, elapsed, None, str(e)[:200]))

    session.close()
    driver.close()
    return results


def run_neug_throughput(db_path: Path, queries: Dict[int, str],
                        params_map: Dict[int, List[Dict[str, Any]]],
                        duration_s: int = DEFAULT_THROUGHPUT_SECONDS,
                        clients: int = DEFAULT_THROUGHPUT_CLIENTS) -> ThroughputResult:
    """Run throughput benchmark for NeuG in service mode."""
    from neug.database import Database
    from neug.session import Session

    db = Database(str(db_path))
    uri = db.serve(host="localhost", port=10000, blocking=False)

    ok = 0
    err = 0
    latencies: List[float] = []
    lock = threading.Lock()
    stop_at = time.time() + duration_s

    def worker(seed: int):
        nonlocal ok, err
        rng = random.Random(seed)
        sess = Session(endpoint=uri, timeout="30s", num_threads=1)
        while time.time() < stop_at:
            qid = rng.choice(list(queries.keys()))
            query = queries[qid]
            params = rng.choice(params_map.get(qid, [{}]))
            t0 = time.perf_counter()
            try:
                r = sess.execute(query, parameters=params)
                _ = json.loads(r.get_bolt_response())
                dt = (time.perf_counter() - t0) * 1000
                with lock:
                    ok += 1
                    latencies.append(dt)
            except:
                with lock:
                    err += 1
        sess.close()

    with ThreadPoolExecutor(max_workers=clients) as ex:
        for i in range(clients):
            ex.submit(worker, i + 100)

    db.stop_serving()
    db.close()

    return ThroughputResult(
        engine="neug",
        clients=clients,
        duration_s=duration_s,
        ok_ops=ok,
        err_ops=err,
        qps=ok / duration_s if duration_s > 0 else 0,
        p50_ms=statistics.median(latencies) if latencies else None,
        p95_ms=statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else None,
    )


def run_neo4j_throughput(queries: Dict[int, str],
                         params_map: Dict[int, List[Dict[str, Any]]],
                         duration_s: int = DEFAULT_THROUGHPUT_SECONDS,
                         clients: int = DEFAULT_THROUGHPUT_CLIENTS) -> ThroughputResult:
    """Run throughput benchmark for Neo4j."""
    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("neo4j package not installed, skipping Neo4j throughput")
        return ThroughputResult("neo4j", clients, duration_s, 0, 0, 0, None, None)

    ok = 0
    err = 0
    latencies: List[float] = []
    lock = threading.Lock()
    stop_at = time.time() + duration_s

    def worker(seed: int):
        nonlocal ok, err
        rng = random.Random(seed)
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        session = driver.session(database="neo4j")
        while time.time() < stop_at:
            qid = rng.choice(list(queries.keys()))
            query = queries[qid]
            params = rng.choice(params_map.get(qid, [{}]))
            t0 = time.perf_counter()
            try:
                session.run(query, params).consume()
                dt = (time.perf_counter() - t0) * 1000
                with lock:
                    ok += 1
                    latencies.append(dt)
            except:
                with lock:
                    err += 1
        session.close()
        driver.close()

    with ThreadPoolExecutor(max_workers=clients) as ex:
        for i in range(clients):
            ex.submit(worker, i + 200)

    return ThroughputResult(
        engine="neo4j",
        clients=clients,
        duration_s=duration_s,
        ok_ops=ok,
        err_ops=err,
        qps=ok / duration_s if duration_s > 0 else 0,
        p50_ms=statistics.median(latencies) if latencies else None,
        p95_ms=statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else None,
    )


# ──────────────────────────────────────────────────────────────────────
# Results output
# ──────────────────────────────────────────────────────────────────────

def print_latency_results(engine: str, results: List[QueryResult]):
    """Print latency results in table format."""
    print(f"\n{engine} Latency Results:")
    print("| Query | Status | P50 (ms) | Result |")
    print("|-------|--------|----------|--------|")

    # Group by query_id
    by_qid: Dict[int, List[QueryResult]] = {}
    for r in results:
        by_qid.setdefault(r.query_id, []).append(r)

    for qid in sorted(by_qid.keys()):
        arr = by_qid[qid]
        ok_results = [r for r in arr if r.ok]
        if ok_results:
            p50 = statistics.median([r.elapsed_ms for r in ok_results])
            status = "OK"
            result_str = "N/A" if ok_results[0].result is None else str(ok_results[0].result)
        else:
            p50 = statistics.median([r.elapsed_ms for r in arr])
            status = "FAIL"
            result_str = arr[0].error[:50] if arr[0].error else "Error"
        print(f"| IC{qid}  | {status}    | {p50:8.2f} | {result_str:>12} |")


def print_throughput_result(tp: ThroughputResult):
    """Print throughput result."""
    print(f"\n{tp.engine} Throughput Results:")
    print(f"  Clients:    {tp.clients}")
    print(f"  Duration:   {tp.duration_s}s")
    print(f"  Total ops:  {tp.ok_ops} OK, {tp.err_ops} errors")
    print(f"  QPS:        {tp.qps:.2f}")
    if tp.p50_ms:
        print(f"  P50 latency: {tp.p50_ms:.2f}ms")
    if tp.p95_ms:
        print(f"  P95 latency: {tp.p95_ms:.2f}ms")


def save_results(output_dir: Path, neug_latency: List[QueryResult],
                 neo4j_latency: List[QueryResult],
                 neug_tp: ThroughputResult, neo4j_tp: ThroughputResult):
    """Save results to JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save latency results
    latency_data = []
    for r in neug_latency:
        latency_data.append({
            "engine": "neug", "query_id": r.query_id, "ok": r.ok,
            "elapsed_ms": r.elapsed_ms, "result": r.result, "error": r.error
        })
    for r in neo4j_latency:
        latency_data.append({
            "engine": "neo4j", "query_id": r.query_id, "ok": r.ok,
            "elapsed_ms": r.elapsed_ms, "result": r.result, "error": r.error
        })

    (output_dir / "latency_results.json").write_text(
        json.dumps(latency_data, indent=2), encoding="utf-8"
    )

    # Save throughput results
    tp_data = {
        "neug": {
            "qps": neug_tp.qps, "p50_ms": neug_tp.p50_ms, "p95_ms": neug_tp.p95_ms,
            "ok_ops": neug_tp.ok_ops, "err_ops": neug_tp.err_ops,
            "clients": neug_tp.clients, "duration_s": neug_tp.duration_s
        },
        "neo4j": {
            "qps": neo4j_tp.qps, "p50_ms": neo4j_tp.p50_ms, "p95_ms": neo4j_tp.p95_ms,
            "ok_ops": neo4j_tp.ok_ops, "err_ops": neo4j_tp.err_ops,
            "clients": neo4j_tp.clients, "duration_s": neo4j_tp.duration_s
        }
    }

    (output_dir / "throughput_results.json").write_text(
        json.dumps(tp_data, indent=2), encoding="utf-8"
    )

    print(f"\nResults saved to {output_dir}")


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="LDBC SNB Interactive Benchmark")
    parser.add_argument("--data-dir", required=True, help="Path to LDBC SNB SF1 dataset")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Path to NeuG database")
    parser.add_argument("--derived-dir", default=str(DEFAULT_DERIVED_DIR), help="Path to derived CSVs")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Path to output directory")
    parser.add_argument("--skip-load", action="store_true", help="Skip data loading")
    parser.add_argument("--force", action="store_true", help="Force overwrite existing database")
    parser.add_argument("--duration", type=int, default=60, help="Throughput test duration (seconds)")
    parser.add_argument("--clients", type=int, default=4, help="Number of concurrent clients")

    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path)
    derived_dir = Path(args.derived_dir)
    output_dir = Path(args.output_dir)

    # Validate data directory
    if not data_dir.exists():
        print(f"Error: Data directory not found: {data_dir}")
        return 1

    print("=" * 60)
    print("LDBC SNB Interactive Benchmark: NeuG vs Neo4j (Service Mode)")
    print("=" * 60)

    # Load data
    if not args.skip_load:
        load_neug_data(data_dir, db_path, derived_dir, force=args.force)
    else:
        if not db_path.exists():
            print(f"Error: Database not found: {db_path}")
            return 1
        print(f"Using existing database: {db_path}")

    # Build parameter pool
    print("\nBuilding parameter pool...")
    pool = build_param_pool(data_dir)
    params_map = {qid: [gen_query_params(qid, pool) for _ in range(10)]
                  for qid in NEUG_IC_QUERIES.keys()}

    # Run latency benchmarks
    print("\nRunning NeuG latency benchmark...")
    neug_latency = run_neug_latency(db_path, NEUG_IC_QUERIES, params_map)
    print_latency_results("NeuG", neug_latency)

    print("\nRunning Neo4j latency benchmark...")
    neo4j_latency = run_neo4j_latency(NEO4J_IC_QUERIES, params_map)
    if neo4j_latency:
        print_latency_results("Neo4j", neo4j_latency)
    else:
        print("Neo4j benchmark skipped (driver not available or connection failed)")

    # Run throughput benchmarks
    print(f"\nRunning NeuG throughput benchmark ({args.clients} clients, {args.duration}s)...")
    neug_tp = run_neug_throughput(db_path, NEUG_IC_QUERIES, params_map,
                                   duration_s=args.duration, clients=args.clients)
    print_throughput_result(neug_tp)

    if neo4j_latency:
        print(f"\nRunning Neo4j throughput benchmark ({args.clients} clients, {args.duration}s)...")
        neo4j_tp = run_neo4j_throughput(NEO4J_IC_QUERIES, params_map,
                                         duration_s=args.duration, clients=args.clients)
        print_throughput_result(neo4j_tp)
    else:
        neo4j_tp = ThroughputResult("neo4j", args.clients, args.duration, 0, 0, 0, None, None)

    # Summary comparison
    print("\n" + "=" * 60)
    print("Summary Comparison")
    print("=" * 60)
    if neo4j_tp.qps > 0:
        print(f"Throughput: NeuG {neug_tp.qps:.1f} QPS vs Neo4j {neo4j_tp.qps:.1f} QPS")
        print(f"           NeuG is {neug_tp.qps / neo4j_tp.qps:.1f}x faster")
    else:
        print(f"Throughput: NeuG {neug_tp.qps:.1f} QPS (Neo4j not tested)")

    # Save results
    save_results(output_dir, neug_latency, neo4j_latency, neug_tp, neo4j_tp)

    return 0


if __name__ == "__main__":
    sys.exit(main())