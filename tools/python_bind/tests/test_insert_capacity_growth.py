"""Insert capacity growth occurs before WAL application and drains old readers."""

import concurrent.futures
import socket
import threading
import time

from neug import Database
from neug.session import Session


def start(db):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return db.serve(port, "127.0.0.1", False, thread_num=4, auto_compaction=False)


def test_batch_growth_and_wal_recovery(tmp_path):
    path = str(tmp_path / "capacity")
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    list(c.execute("CREATE NODE TABLE Item(id INT64, value STRING, PRIMARY KEY(id))"))
    c.close()
    endpoint = start(db)
    s = Session(endpoint, timeout="60s")
    try:
        payload = "x" * 200
        # Both row capacity and the independent string append region grow.
        list(
            s.execute(
                "UNWIND CAST($ids, 'INT64[]') AS i CREATE (:Item {id:i,value:$v})",
                access_mode="insert",
                parameters={"v": payload, "ids": list(range(5000))},
            )
        )
        assert list(s.execute("MATCH (n:Item) RETURN count(n)")) == [[5000]]
        assert list(s.execute("MATCH (n:Item {id:4999}) RETURN n.value")) == [[payload]]
    finally:
        s.close()
        db.stop_serving()
        db.close()
    # No checkpoint-on-close: the successful batch must replay against the
    # original small checkpoint without replaying the discarded attempt.
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    try:
        assert list(c.execute("MATCH (n:Item) RETURN count(n)")) == [[5000]]
        assert list(c.execute("MATCH (n:Item {id:4999}) RETURN n.value")) == [[payload]]
    finally:
        c.close()
        db.close()


def test_growth_waits_for_read_transaction(tmp_path):
    db = Database(str(tmp_path / "drain"), max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    list(c.execute("CREATE NODE TABLE Item(id INT64, value INT64, PRIMARY KEY(id))"))
    list(c.execute("CREATE (:Item {id:-1,value:42})"))
    c.close()
    endpoint = start(db)
    reader, writer = Session(endpoint, timeout="60s"), Session(endpoint, timeout="60s")
    try:
        reader.begin_transaction(read_only=True)
        assert list(reader.execute("MATCH (n:Item {id:-1}) RETURN n.value")) == [[42]]
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(
                lambda: list(
                    writer.execute(
                        "UNWIND CAST($ids, 'INT64[]') AS i CREATE (:Item {id:i,value:i})",
                        access_mode="insert",
                        parameters={"ids": list(range(5000))},
                    )
                )
            )
            time.sleep(0.3)
            assert not pending.done()
            reader.commit()
            pending.result(timeout=60)
        assert list(reader.execute("MATCH (n:Item) RETURN count(n)")) == [[5001]]
    finally:
        reader.close()
        writer.close()
        db.stop_serving()
        db.close()


def test_string_payload_growth_without_row_growth(tmp_path):
    path = str(tmp_path / "string_payload")
    db = Database(path, max_thread_num=4, checkpoint_on_close=True)
    c = db.connect()
    list(c.execute("CREATE NODE TABLE Item(id INT64, value STRING, PRIMARY KEY(id))"))
    list(c.execute("CREATE (:Item {id:-1,value:'seed'})"))
    c.close()
    db.close()
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    c.close()
    endpoint = start(db)
    s = Session(endpoint, timeout="60s")
    try:
        payload = "z" * 200
        list(
            s.execute(
                "UNWIND CAST($ids, 'INT64[]') AS i CREATE (:Item {id:i,value:$v})",
                access_mode="insert",
                parameters={"ids": list(range(1000)), "v": payload},
            )
        )
        assert list(s.execute("MATCH (n:Item) RETURN count(n)")) == [[1001]]
        assert list(s.execute("MATCH (n:Item {id:999}) RETURN n.value")) == [[payload]]
        # The existing update path remains able to grow string storage.
        list(
            s.execute(
                "MATCH (n:Item) SET n.value=$v",
                access_mode="update",
                parameters={"v": "q" * 250},
            )
        )
        assert list(s.execute("MATCH (n:Item {id:999}) RETURN n.value")) == [
            ["q" * 250]
        ]
    finally:
        s.close()
        db.stop_serving()
        db.close()


def test_concurrent_batches_with_string_primary_keys(tmp_path):
    db = Database(
        str(tmp_path / "string_keys"), max_thread_num=4, checkpoint_on_close=False
    )
    c = db.connect()
    list(c.execute("CREATE NODE TABLE Item(id STRING, value STRING, PRIMARY KEY(id))"))
    c.close()
    endpoint = start(db)

    def insert_batch(worker):
        s = Session(endpoint, timeout="60s")
        keys = [f"{worker}-{i}-" + "k" * 100 for i in range(1500)]
        try:
            list(
                s.execute(
                    "UNWIND CAST($ids, 'STRING[]') AS id CREATE (:Item {id:id,value:$v})",
                    access_mode="insert",
                    parameters={"ids": keys, "v": "x" * 200},
                )
            )
        finally:
            s.close()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(insert_batch, range(4)))
        s = Session(endpoint, timeout="60s")
        try:
            assert list(s.execute("MATCH (n:Item) RETURN count(n)")) == [[6000]]
            for worker in range(4):
                assert list(
                    s.execute(
                        "MATCH (n:Item {id:$id}) RETURN n.value",
                        parameters={"id": f"{worker}-1499-" + "k" * 100},
                    )
                ) == [["x" * 200]]
        finally:
            s.close()
    finally:
        db.stop_serving()
        db.close()


def test_fixed_vector_array_growth(tmp_path):
    db = Database(str(tmp_path / "arrays"), max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    list(
        c.execute(
            "CREATE NODE TABLE Item(id INT64, embedding FLOAT[4], PRIMARY KEY(id))"
        )
    )
    c.close()
    endpoint = start(db)
    s = Session(endpoint, timeout="60s")
    try:
        query = (
            "UNWIND CAST($ids, 'INT64[]') AS id "
            "CREATE (:Item {id:id,embedding:CAST($v, 'FLOAT[4]')})"
        )
        params = {"ids": list(range(5000)), "v": [1, 2, 3, 4]}
        list(s.execute(query, access_mode="insert", parameters=params))
        assert list(s.execute("MATCH (n:Item) RETURN count(n)")) == [[5000]]
        assert list(s.execute("MATCH (n:Item {id:4999}) RETURN n.embedding")) == [
            [[1, 2, 3, 4]]
        ]
    finally:
        s.close()
        db.stop_serving()
        db.close()


def test_atomic_growth_four_readers_four_writers(tmp_path):
    db = Database(
        str(tmp_path / "atomic_mixed"), max_thread_num=8, checkpoint_on_close=False
    )
    c = db.connect()
    list(c.execute("CREATE NODE TABLE Item(id INT64, value STRING, PRIMARY KEY(id))"))
    list(c.execute("CREATE REL TABLE Link(FROM Item TO Item, note STRING)"))
    list(c.execute("CREATE (:Item {id:-1,value:'anchor'})"))
    c.close()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = db.serve(port, "127.0.0.1", False, thread_num=8, auto_compaction=False)
    barrier = threading.Barrier(8)

    def worker(index):
        session = Session(endpoint, timeout="60s")
        try:
            barrier.wait(timeout=30)
            if index < 4:
                for _ in range(40):
                    assert list(
                        session.execute("MATCH (n:Item {id:-1}) RETURN n.value")
                    ) == [["anchor"]]
                    count = list(session.execute("MATCH (n:Item) RETURN count(n)"))[0][
                        0
                    ]
                    assert 1 <= count <= 10001
            else:
                base = (index - 4) * 2500
                for batch in range(10):
                    list(
                        session.execute(
                            "UNWIND CAST($ids, 'INT64[]') AS id CREATE (:Item {id:id,value:$v})",
                            access_mode="insert",
                            parameters={
                                "ids": list(
                                    range(base + batch * 250, base + (batch + 1) * 250)
                                ),
                                "v": "x" * 200,
                            },
                        )
                    )
        finally:
            session.close()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(worker, range(8)))
        session = Session(endpoint, timeout="60s")
        try:
            assert list(session.execute("MATCH (n:Item) RETURN count(n)")) == [[10001]]
            list(
                session.execute(
                    "MATCH (n:Item {id:9999}) SET n.value=$v",
                    access_mode="update",
                    parameters={"v": "y" * 250},
                )
            )
            assert list(session.execute("MATCH (n:Item {id:9999}) RETURN n.value")) == [
                ["y" * 250]
            ]
            list(
                session.execute(
                    "MATCH (a:Item {id:-1}), (b:Item {id:9999}) CREATE (a)-[:Link {note:'after growth'}]->(b)",
                    access_mode="insert",
                )
            )
            assert list(
                session.execute(
                    "MATCH (:Item {id:-1})-[e:Link]->(:Item {id:9999}) RETURN e.note"
                )
            ) == [["after growth"]]
        finally:
            session.close()
    finally:
        db.stop_serving()
        db.close()
