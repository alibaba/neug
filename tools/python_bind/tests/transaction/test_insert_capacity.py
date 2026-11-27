# Copyright 2026 Alibaba Group Holding Limited.
# Licensed under the Apache License, Version 2.0.

"""Concurrent reservation correctness; no timing thresholds in CI."""

import concurrent.futures
import socket
import threading
from contextlib import closing

import pytest

from neug import Database
from neug import Session


@pytest.mark.parametrize("batch_size", [1, 64], ids=["small", "growth"])
def test_mixed_insert_capacity_and_wal_recovery(tmp_path, batch_size):
    workers, batches, seed_count = 4, 96, 32
    payload = "comment-" + "x" * 120
    path = str(tmp_path / "mixed_capacity")
    with closing(Database(path, checkpoint_on_close=True)) as db:
        with closing(db.connect()) as conn:
            for label in ("Person", "Forum"):
                list(conn.execute(f"CREATE NODE TABLE {label}(id INT64 PRIMARY KEY)"))
            list(
                conn.execute(
                    "CREATE NODE TABLE Comment(id INT64 PRIMARY KEY, content STRING)"
                )
            )
            for ddl in (
                "CREATE REL TABLE Likes(FROM Person TO Comment, seq INT64)",
                "CREATE REL TABLE Member(FROM Person TO Forum, seq INT64)",
                "CREATE REL TABLE Creator(FROM Comment TO Person)",
            ):
                list(conn.execute(ddl))
            for label in ("Person", "Forum", "Comment"):
                properties = "id:id,content:'seed'" if label == "Comment" else "id:id"
                list(
                    conn.execute(
                        "UNWIND CAST($ids, 'INT64[]') AS id "
                        f"CREATE (:{label} {{{properties}}})",
                        parameters={"ids": list(range(seed_count))},
                    )
                )

    unwind = "UNWIND CAST($ids, 'INT64[]') AS id "
    writes = (
        "MATCH (p:Person {id:$endpoint}) "
        + unwind
        + "CREATE (:Comment {id:id,content:$text})-[:Creator]->(p)",
        "MATCH (p:Person {id:$endpoint}), (c:Comment {id:$endpoint}) "
        + unwind
        + "CREATE (p)-[:Likes {seq:id}]->(c)",
        "MATCH (p:Person {id:$endpoint}), (f:Forum {id:$endpoint}) "
        + unwind
        + "CREATE (p)-[:Member {seq:id}]->(f)",
    )
    expected = {"Comment": [[i, "seed"] for i in range(seed_count)]}
    expected.update({label: [] for label in ("Creator", "Likes", "Member")})

    def batch_ids(worker, batch):
        first = seed_count + (worker * batches + batch) * batch_size
        return list(range(first, first + batch_size))

    for worker in range(workers):
        for batch in range(batches):
            operation = (worker + batch) % 3
            for row in batch_ids(worker, batch):
                endpoint = (worker + batch) % seed_count
                if operation == 0:
                    expected["Comment"].append([row, payload])
                    expected["Creator"].append([row, endpoint])
                else:
                    label = "Likes" if operation == 1 else "Member"
                    expected[label].append([row, endpoint, endpoint])
    for rows in expected.values():
        rows.sort()

    reads = {
        "Comment": "MATCH (c:Comment) RETURN c.id,c.content ORDER BY c.id",
        "Creator": "MATCH (c:Comment)-[:Creator]->(p:Person) "
        "RETURN c.id,p.id ORDER BY c.id",
        "Likes": "MATCH (p:Person)-[r:Likes]->(c:Comment) "
        "RETURN r.seq,p.id,c.id ORDER BY r.seq",
        "Member": "MATCH (p:Person)-[r:Member]->(f:Forum) "
        "RETURN r.seq,p.id,f.id ORDER BY r.seq",
    }

    # The larger batches insert 8,192 comments and each relationship type,
    # exceeding the initial row capacity and exercising shared growth/retries.
    with closing(
        Database(path, max_thread_num=workers, checkpoint_on_close=False)
    ) as db:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        endpoint = db.serve(
            port, "127.0.0.1", False, thread_num=workers, auto_compaction=False
        )
        barrier = threading.Barrier(workers)

        def write(worker):
            with closing(Session(endpoint, timeout="60s")) as session:
                session._http_session.trust_env = False
                barrier.wait(timeout=30)
                for batch in range(batches):
                    list(
                        session.execute(
                            writes[(worker + batch) % 3],
                            access_mode="insert",
                            parameters={
                                "ids": batch_ids(worker, batch),
                                "text": payload,
                                "endpoint": (worker + batch) % seed_count,
                            },
                        )
                    )

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(write, range(workers)))
            with closing(Session(endpoint, timeout="60s")) as session:
                session._http_session.trust_env = False
                for label, query in reads.items():
                    assert list(session.execute(query)) == expected[label], label
        finally:
            db.stop_serving()

    # No shutdown checkpoint: replay must retain every ID, payload and endpoint
    # exactly once, including inserts that were retried after capacity growth.
    with closing(Database(path, checkpoint_on_close=False)) as db:
        with closing(db.connect()) as conn:
            for label, query in reads.items():
                assert list(conn.execute(query)) == expected[label], label
