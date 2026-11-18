"""Indexed insert-only writes, snapshot filtering, growth and WAL recovery."""

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from conftest import wait_for_server_ready

from neug.database import Database
from neug.session import Session

pytestmark = pytest.mark.skipif(
    os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() not in ("1", "true"),
    reason="Set NEUG_RUN_EXTENSION_TESTS=1",
)


def test_indexed_insert_snapshots_growth_and_reopen(
    tmp_path, unused_tcp_port, monkeypatch
):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    path = str(tmp_path / "indexed-inserts")
    db = Database(path, mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute("LOAD vector_search;")
    conn.execute("LOAD fts;")
    conn.execute(
        "CREATE NODE TABLE Item(id INT64 PRIMARY KEY, text STRING, embedding FLOAT[2]);"
    )
    conn.execute("CREATE (:Item {id: 0, text: 'common base', embedding: [0.0, 0.0]});")
    conn.execute("CREATE INDEX vec ON Item USING HNSW (embedding);")
    conn.execute("CREATE INDEX text_idx ON Item USING FTS (text);")
    conn.execute("CHECKPOINT;")
    conn.close()
    endpoint = db.serve(unused_tcp_port, "127.0.0.1", False, thread_num=8)
    wait_for_server_ready(endpoint)

    def vector_rows(session, value, limit=1):
        return list(
            session.execute(
                "MATCH (n:Item) RETURN n.id, "
                f"vector_distance_l2(n.embedding, [{float(value)}, 0.0]) AS score "
                f"ORDER BY score LIMIT {limit};",
                access_mode="read",
            )
        )

    def text_rows(session):
        return list(
            session.execute(
                "MATCH (n:Item) RETURN n.id, bm25(n.text, 'common') AS score "
                "ORDER BY score LIMIT 6000;",
                access_mode="read",
            )
        )

    writer = Session.open(endpoint, timeout="60s")
    reader = Session.open(endpoint, timeout="60s")
    try:
        # Prepare capacity before pinning a long reader: growth intentionally
        # drains all readers, whereas ordinary inserts must not block it.
        writer.execute(
            "CREATE (:Item {id: 1, text: 'common one', embedding: [1.0, 0.0]});",
            access_mode="insert",
        )
        reader.begin_transaction(read_only=True)
        assert vector_rows(reader, 2)[0][0] == 1
        writer.execute(
            "CREATE (:Item {id: 2, text: 'common two', embedding: [2.0, 0.0]});",
            access_mode="insert",
        )
        assert vector_rows(reader, 2)[0][0] == 1
        assert {r[0] for r in text_rows(reader)} == {0, 1}
        reader.rollback()
        assert vector_rows(reader, 2)[0] == [2, 0.0]
        assert {r[0] for r in text_rows(reader)} == {0, 1, 2}

        barrier = Barrier(4, timeout=30)

        def append(worker):
            session = Session.open(endpoint, timeout="120s")
            try:
                barrier.wait()
                for batch in range(25):
                    start = 3 + worker * 1250 + batch * 50
                    end = start + 49
                    session.execute(
                        f"UNWIND {list(range(start, end + 1))} AS i "
                        "CREATE (:Item {id: i, text: 'common appended', "
                        "embedding: CAST([CAST(i, 'FLOAT'), 0.0], 'FLOAT[2]')});",
                        access_mode="insert",
                    )
                    # Concurrent lower timestamps may still hold back the
                    # published read watermark. Check completeness after all
                    # writers finish; searches here exercise read/write overlap.
                    vector_rows(session, end)
            finally:
                session.close()

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(append, range(4)))
        for value in (52, 1302, 2552, 3802, 5002):
            assert vector_rows(reader, value)[0] == [value, 0.0]
        assert list(reader.execute("MATCH (n:Item) RETURN count(n);")) == [[5003]]
        assert {r[0] for r in text_rows(reader)} == set(range(5003))
    finally:
        reader.close()
        writer.close()
        db.stop_serving()
        db.close()

    # No checkpoint after the inserts: both indexes must include the WAL tail.
    reopened = Database(path, mode="w", checkpoint_on_close=False)
    conn = reopened.connect()
    try:
        conn.execute("LOAD vector_search;")
        conn.execute("LOAD fts;")
        assert list(conn.execute("MATCH (n:Item) RETURN count(n);")) == [[5003]]
        rows = list(
            conn.execute(
                "MATCH (n:Item) RETURN n.id, vector_distance_l2(n.embedding, "
                "[5002.0, 0.0]) AS score ORDER BY score LIMIT 1;"
            )
        )
        assert rows == [[5002, 0.0]]
        assert (
            len(
                list(
                    conn.execute(
                        "MATCH (n:Item) RETURN n.id, bm25(n.text, 'common') AS score "
                        "ORDER BY score LIMIT 6000;"
                    )
                )
            )
            == 5003
        )
    finally:
        conn.close()
        reopened.close()
