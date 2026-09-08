"""Operator streams retain one COPY pipeline and per-execution reader state."""

import pytest


def operator_names(result):
    return [op["operator_name"] for op in result.get_profile_metrics()["operators"]]


@pytest.mark.parametrize("file_format", ["csv", "jsonl", "json"])
def test_copy_stream_uses_regular_operators(empty_db, tmp_path, file_format):
    _, conn = empty_db
    path = tmp_path / f"nodes.{file_format}"
    content = {
        "csv": "id|name\n1|one\n2|two\n3|three\n",
        "jsonl": '{"id":1,"name":"one"}\n{"id":2,"name":"two"}\n'
        '{"id":3,"name":"three"}\n',
        "json": '[{"id":1,"name":"one"},{"id":2,"name":"two"},'
        '{"id":3,"name":"three"}]',
    }
    path.write_text(content[file_format], encoding="utf-8")
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id))")
    result = conn.execute(f'PROFILE COPY person FROM "{path}" (batch_size=1)')
    assert len(result) == 3
    names = operator_names(result)
    assert "DataSourceOpr" in names
    assert "BatchInsertVertexOpr" in names
    assert not any("FusedCSV" in name for name in names)
    assert list(conn.execute("MATCH (p:person) RETURN p.id, p.name ORDER BY p.id")) == [
        [1, "one"],
        [2, "two"],
        [3, "three"],
    ]


def test_copy_stream_subquery_projection(empty_db, tmp_path):
    _, conn = empty_db
    path = tmp_path / "projection.csv"
    path.write_text("id|score|name\n1|10|one\n2|20|two\n3|30|three\n")
    conn.execute(
        "CREATE NODE TABLE selected(id INT64, name STRING, value INT64, "
        "PRIMARY KEY(id))"
    )
    result = conn.execute(
        f'PROFILE COPY selected FROM (LOAD FROM "{path}" '
        "(header=true, batch_size=1) WHERE score > 10 "
        "RETURN id, name, score + 1 AS value)"
    )
    names = operator_names(result)
    assert "DataSourceOpr" in names
    assert "ProjectOpr" in names
    assert "BatchInsertVertexOpr" in names
    assert list(
        conn.execute("MATCH (n:selected) RETURN n.id, n.name, n.value ORDER BY n.id")
    ) == [[2, "two", 21], [3, "three", 31]]


def test_copy_stream_late_parse_error_rolls_back(empty_db, tmp_path):
    _, conn = empty_db
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id))")
    conn.execute("CREATE (:person {id: 0, name: 'original'})")
    path = tmp_path / "bad.csv"
    path.write_text("id|name\n1|first\nnot-an-integer|bad\n")
    with pytest.raises(RuntimeError):
        conn.execute(f'COPY person FROM "{path}" (batch_size=1)')
    assert list(conn.execute("MATCH (p:person) RETURN p.id, p.name")) == [
        [0, "original"]
    ]
    path.write_text("id|name\n1|first\n2|second\n")
    conn.execute(f'COPY person FROM "{path}" (batch_size=1)')
    assert list(conn.execute("MATCH (p:person) RETURN p.id ORDER BY p.id")) == [
        [0],
        [1],
        [2],
    ]


def test_load_stream_reexpands_glob_each_execution(empty_db, tmp_path):
    _, conn = empty_db
    (tmp_path / "part1.csv").write_text("id\n1\n")
    query = (
        f'LOAD FROM "{tmp_path}/part*.csv" '
        "(header=true, batch_size=1) RETURN id ORDER BY id"
    )
    assert list(conn.execute(query)) == [[1]]
    (tmp_path / "part2.csv").write_text("id\n2\n")
    assert list(conn.execute(query)) == [[1], [2]]
    assert list(conn.execute(query)) == [[1], [2]]


def test_stream_preserves_literal_heads_and_global_aggregation(empty_db, tmp_path):
    _, conn = empty_db
    assert list(conn.execute("RETURN 42")) == [[42]]
    assert list(conn.execute("UNWIND [1, 2, 3] AS x RETURN x + 1")) == [[2], [3], [4]]
    path = tmp_path / "values.csv"
    path.write_text("id\n3\n1\n2\n")
    source = f'LOAD FROM "{path}" (header=true, batch_size=1)'
    assert list(conn.execute(f"{source} RETURN count(*), sum(id)")) == [[3, 6]]
    assert list(conn.execute(f"{source} RETURN id ORDER BY id LIMIT 2")) == [[1], [2]]


def test_failed_stream_copy_does_not_persist_partial_batch(tmp_path):
    from neug import Database

    db_path = str(tmp_path / "durable")
    csv_path = tmp_path / "late_error.csv"
    csv_path.write_text("id|name\n1|valid\ninvalid|bad\n")
    db = Database(db_path=db_path, mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id))")
        conn.execute("CREATE (:person {id: 0, name: 'original'})")
        with pytest.raises(RuntimeError):
            conn.execute(f'COPY person FROM "{csv_path}" (batch_size=1)')
    finally:
        conn.close()
        db.close()

    db = Database(db_path=db_path, mode="w")
    conn = db.connect()
    try:
        assert list(conn.execute("MATCH (p:person) RETURN p.id, p.name")) == [
            [0, "original"]
        ]
    finally:
        conn.close()
        db.close()
