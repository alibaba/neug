from pathlib import Path

import pytest
from conftest import cypher_path


@pytest.mark.parametrize(
    "path", [r"C:\data\people.csv", "O'Brien/data.csv", Path("a b.csv")]
)
def test_cypher_path_round_trip(empty_db, path):
    _, conn = empty_db
    result = conn.execute(f"RETURN {cypher_path(path)}")
    assert list(result)[0][0] == str(path)


def test_copy_from_path_with_quote(empty_db, tmp_path):
    source = tmp_path / "O'Brien data.csv"
    source.write_text("id|name\n1|Alice\n", encoding="utf-8")
    _, conn = empty_db
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id))")
    conn.execute(f'COPY person FROM {cypher_path(source)} (header=true, delim="|")')
    assert list(conn.execute("MATCH (p:person) RETURN p.name")) == [["Alice"]]
