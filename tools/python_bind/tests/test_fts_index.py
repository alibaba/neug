#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import subprocess
import sys
import textwrap

import pytest

from neug.database import Database

EXTENSION_TESTS_ENABLED = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
pytestmark = pytest.mark.skipif(
    not EXTENSION_TESTS_ENABLED,
    reason="Set NEUG_RUN_EXTENSION_TESTS=1 to run FTS extension tests.",
)


def load_fts(connection, skip_if_unavailable=False):
    try:
        connection.execute("LOAD fts;")
    except RuntimeError as error:
        if skip_if_unavailable:
            pytest.skip(f"FTS extension not available: {error}")
        raise


def create_item_table(connection):
    connection.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, text STRING);")


def search(connection, query, limit=10):
    return list(
        connection.execute(
            "MATCH (n:Item) "
            f"RETURN n.id, bm25(n.text, '{query}') AS score "
            f"ORDER BY score ASC LIMIT {limit};"
        )
    )


@pytest.fixture()
def fts_database(tmp_path):
    db = Database(db_path=str(tmp_path / "fts_db"), mode="w")
    connection = db.connect()
    load_fts(connection, skip_if_unavailable=True)
    create_item_table(connection)
    connection.execute(
        "CREATE (:Item {id: 1, text: 'search text alpha'}), "
        "(:Item {id: 2, text: 'search text beta'}), "
        "(:Item {id: 3, text: 'gamma'});"
    )
    connection.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
    try:
        yield connection
    finally:
        connection.close()
        db.close()


@pytest.fixture()
def fts_hybrid_database(tmp_path):
    db = Database(db_path=str(tmp_path / "fts_hybrid_db"), mode="w")
    connection = db.connect()
    load_fts(connection, skip_if_unavailable=True)
    connection.execute("CREATE NODE TABLE Author(name STRING PRIMARY KEY);")
    connection.execute(
        "CREATE NODE TABLE Article("
        "id INT64 PRIMARY KEY, title STRING, category STRING);"
    )
    connection.execute("CREATE REL TABLE WROTE(FROM Author TO Article);")
    connection.execute("CREATE REL TABLE CITES(FROM Article TO Article);")
    connection.execute("CREATE (:Author {name: 'Ada'}), (:Author {name: 'Bob'});")
    connection.execute(
        "CREATE (:Article {id: 1, title: 'database database database', "
        "category: 'general'}), "
        "(:Article {id: 2, title: 'database indexing', category: 'database'}), "
        "(:Article {id: 3, title: 'graph database systems', "
        "category: 'database'}), "
        "(:Article {id: 4, title: 'database storage internals', "
        "category: 'storage'}), "
        "(:Article {id: 5, title: 'distributed database design', "
        "category: 'database'}), "
        "(:Article {id: 6, title: 'reference material', "
        "category: 'reference'});"
    )
    connection.execute(
        "MATCH (ada:Author {name: 'Ada'}), (article:Article) "
        "WHERE article.id IN [2, 3, 5] CREATE (ada)-[:WROTE]->(article);"
    )
    connection.execute(
        "MATCH (bob:Author {name: 'Bob'}), (article:Article) "
        "WHERE article.id IN [1, 4] CREATE (bob)-[:WROTE]->(article);"
    )
    connection.execute(
        "MATCH (article:Article), (cited:Article {id: 6}) "
        "WHERE article.id IN [1, 2, 3, 4, 5] "
        "CREATE (article)-[:CITES]->(cited);"
    )
    connection.execute("CREATE INDEX article_title_fts ON Article USING FTS (title);")
    try:
        yield connection
    finally:
        connection.close()
        db.close()


def test_fts_topk_search(fts_database):
    rows = search(fts_database, "search text", limit=2)
    assert {row[0] for row in rows} == {1, 2}
    assert all(row[1] <= 0.0 for row in rows)


@pytest.mark.parametrize(
    ("order_by", "direction", "limit"),
    [
        pytest.param(None, None, None, id="default-order-without-limit"),
        pytest.param(None, None, 2, id="default-order-with-limit"),
        pytest.param("score", "ASC", None, id="score-asc-without-limit"),
        pytest.param("score", "ASC", 2, id="score-asc-with-limit"),
        pytest.param("score", "DESC", None, id="score-desc-without-limit"),
        pytest.param("score", "DESC", 2, id="score-desc-with-limit"),
        pytest.param("n.id", "ASC", None, id="column-asc-without-limit"),
        pytest.param("n.id", "ASC", 2, id="column-asc-with-limit"),
        pytest.param("n.id", "DESC", None, id="column-desc-without-limit"),
        pytest.param("n.id", "DESC", 2, id="column-desc-with-limit"),
        pytest.param(None, None, 0, id="zero-limit"),
        pytest.param(None, None, 4294967295, id="uint32-max-limit"),
        pytest.param(None, None, 4294967296, id="uint32-overflow-limit"),
        pytest.param(None, None, 9223372036854775807, id="int64-max-limit"),
    ],
)
def test_fts_order_by_limit(fts_database, order_by, direction, limit):
    fts_database.execute(
        "CREATE (:Item {id: 41, text: 'orderlimit orderlimit orderlimit'}), "
        "(:Item {id: 13, text: 'orderlimit orderlimit filler'}), "
        "(:Item {id: 37, text: 'orderlimit filler filler'}), "
        "(:Item {id: 22, text: 'orderlimit filler filler filler filler'});"
    )

    query_prefix = "MATCH (n:Item) " "RETURN n.id, bm25(n.text, 'orderlimit') AS score"
    score_ascending = list(fts_database.execute(query_prefix + " ORDER BY score ASC;"))
    assert len(score_ascending) == 4
    assert [row[1] for row in score_ascending] == sorted(
        row[1] for row in score_ascending
    )
    assert len({row[1] for row in score_ascending}) == len(score_ascending)

    if order_by is None:
        expected = score_ascending
    elif order_by == "score":
        expected = sorted(
            score_ascending, key=lambda row: row[1], reverse=direction == "DESC"
        )
    else:
        expected = sorted(
            score_ascending, key=lambda row: row[0], reverse=direction == "DESC"
        )
    if limit is not None:
        expected = expected[:limit]

    query = query_prefix
    if order_by is not None:
        query += f" ORDER BY {order_by} {direction}"
    if limit is not None:
        query += f" LIMIT {limit}"

    actual = list(fts_database.execute(query + ";"))
    assert [row[0] for row in actual] == [row[0] for row in expected]
    assert [row[1] for row in actual] == pytest.approx([row[1] for row in expected])


@pytest.mark.parametrize(
    ("query", "expected_ids"),
    [
        ("database", {4, 5, 6}),
        ("graph database", {4, 5, 6}),
        ('"graph database"', {4}),
        ("data*", {4, 5, 6, 7}),
    ],
)
def test_fts_query_string_forms(fts_database, query, expected_ids):
    fts_database.execute(
        "CREATE (:Item {id: 4, text: 'graph database'}), "
        "(:Item {id: 5, text: 'graph scalable database'}), "
        "(:Item {id: 6, text: 'database graph'}), "
        "(:Item {id: 7, text: 'dataset catalog'}), "
        "(:Item {id: 8, text: 'metadata catalog'});"
    )

    assert {row[0] for row in search(fts_database, query)} == expected_ids


def test_fts_scalar_filter_returns_exact_topk(fts_hybrid_database):
    exhaustive = list(
        fts_hybrid_database.execute(
            "MATCH (article:Article) "
            "RETURN article.id, bm25(article.title, 'database') AS score "
            "ORDER BY score ASC LIMIT 100;"
        )
    )
    eligible_ids = {2, 3, 5}
    expected = [row for row in exhaustive if row[0] in eligible_ids][:2]
    assert exhaustive[0][0] not in eligible_ids

    actual = list(
        fts_hybrid_database.execute(
            "MATCH (article:Article) "
            "WHERE article.category = 'database' "
            "RETURN article.id, bm25(article.title, 'database') AS score "
            "ORDER BY score ASC LIMIT 2;"
        )
    )
    assert [row[0] for row in actual] == [row[0] for row in expected]
    assert [row[1] for row in actual] == pytest.approx([row[1] for row in expected])


def test_fts_topk_can_feed_graph_traversal(fts_hybrid_database):
    exhaustive_topk = list(
        fts_hybrid_database.execute(
            "MATCH (article:Article) "
            "RETURN article.id, bm25(article.title, 'database') AS score "
            "ORDER BY score ASC LIMIT 2;"
        )
    )

    actual = list(
        fts_hybrid_database.execute(
            "MATCH (article:Article) "
            "WITH article, bm25(article.title, 'database') AS score "
            "ORDER BY score ASC LIMIT 2 "
            "MATCH (article)-[:CITES]->(cited:Article) "
            "RETURN article.id, cited.id, score ORDER BY score ASC;"
        )
    )
    assert [(row[0], row[1]) for row in actual] == [
        (row[0], 6) for row in exhaustive_topk
    ]
    assert [row[2] for row in actual] == pytest.approx(
        [row[1] for row in exhaustive_topk]
    )


def test_graph_candidates_receive_exact_fts_topk(fts_hybrid_database):
    exhaustive = list(
        fts_hybrid_database.execute(
            "MATCH (article:Article) "
            "RETURN article.id, bm25(article.title, 'database') AS score "
            "ORDER BY score ASC LIMIT 100;"
        )
    )
    eligible_ids = {2, 3, 5}
    expected = [row for row in exhaustive if row[0] in eligible_ids][:2]
    assert exhaustive[0][0] not in eligible_ids

    actual = list(
        fts_hybrid_database.execute(
            "MATCH (:Author {name: 'Ada'})-[:WROTE]->(article:Article) "
            "RETURN article.id, bm25(article.title, 'database') AS score "
            "ORDER BY score ASC LIMIT 2;"
        )
    )
    assert [row[0] for row in actual] == [row[0] for row in expected]
    assert [row[1] for row in actual] == pytest.approx([row[1] for row in expected])


def test_show_and_drop_fts_index(fts_database):
    assert list(fts_database.execute("CALL SHOW_INDEXES() RETURN *;")) == [
        ["item_text_fts", "fts", "Item", "text", "{}", "active"]
    ]

    fts_database.execute("DROP INDEX item_text_fts IF EXISTS;")
    assert list(fts_database.execute("CALL SHOW_INDEXES() RETURN *;")) == []
    fts_database.execute("DROP INDEX item_text_fts IF EXISTS;")


@pytest.mark.parametrize(
    "drop_statement",
    ["ALTER TABLE Item DROP text;", "DROP TABLE Item;"],
)
def test_dropping_indexed_schema_removes_fts_index(fts_database, drop_statement):
    fts_database.execute(drop_statement)
    assert list(fts_database.execute("CALL SHOW_INDEXES() RETURN *;")) == []


def test_fts_tracks_inserts_updates_and_deletes(fts_database):
    fts_database.execute("CREATE (:Item {id: 4, text: 'durable fox'});")
    assert [row[0] for row in search(fts_database, "durable")] == [4]

    fts_database.execute("MATCH (n:Item) WHERE n.id = 4 SET n.text = 'current fox';")
    assert search(fts_database, "durable") == []
    assert [row[0] for row in search(fts_database, "current")] == [4]

    fts_database.execute("MATCH (n:Item) WHERE n.id = 4 DELETE n;")
    assert search(fts_database, "current") == []


def test_fts_index_survives_database_reopen(tmp_path):
    database_path = str(tmp_path / "persistent_fts_db")

    db = Database(db_path=database_path, mode="w")
    connection = db.connect()
    load_fts(connection, skip_if_unavailable=True)
    create_item_table(connection)
    connection.execute("CREATE (:Item {id: 1, text: 'durable fox'});")
    connection.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
    connection.close()
    db.close()

    reopened_db = Database(db_path=database_path, mode="w")
    reopened_connection = reopened_db.connect()
    try:
        load_fts(reopened_connection)
        assert [row[0] for row in search(reopened_connection, "durable")] == [1]

        reopened_connection.execute("CREATE (:Item {id: 2, text: 'durable hare'});")
        assert {row[0] for row in search(reopened_connection, "durable")} == {
            1,
            2,
        }
    finally:
        reopened_connection.close()
        reopened_db.close()


def test_explicit_checkpoint_discards_later_uncheckpointed_fts_data(tmp_path):
    database_path = str(tmp_path / "explicit_checkpoint_fts_db")
    db = Database(db_path=database_path, mode="w", checkpoint_on_close=False)
    connection = db.connect()
    load_fts(connection, skip_if_unavailable=True)
    create_item_table(connection)
    connection.execute("CREATE (:Item {id: 1, text: 'durable token'});")
    connection.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
    connection.execute("CHECKPOINT;")

    connection.execute("CREATE (:Item {id: 2, text: 'volatile token'});")
    assert [row[0] for row in search(connection, "volatile")] == [2]
    connection.close()
    db.close()

    reopened_db = Database(db_path=database_path, mode="w", checkpoint_on_close=False)
    reopened_connection = reopened_db.connect()
    try:
        load_fts(reopened_connection)
        rows = list(
            reopened_connection.execute("MATCH (n:Item) RETURN n.id ORDER BY n.id;")
        )
        assert rows == [[1]]
        assert [row[0] for row in search(reopened_connection, "durable")] == [1]
        assert search(reopened_connection, "volatile") == []
    finally:
        reopened_connection.close()
        reopened_db.close()


def test_fts_dynamic_query_parameter_is_bound_per_execution(fts_database):
    statement = (
        "MATCH (n:Item) RETURN n.id, bm25(n.text, $query) AS score "
        "ORDER BY score ASC;"
    )
    assert {
        row[0]
        for row in fts_database.execute(statement, parameters={"query": "search"})
    } == {1, 2}
    assert [
        row[0] for row in fts_database.execute(statement, parameters={"query": "gamma"})
    ] == [3]


def test_fts_dynamic_query_parameter_rejects_invalid_values(fts_database):
    statement = (
        "MATCH (n:Item) RETURN n.id, bm25(n.text, $query) AS score "
        "ORDER BY score ASC;"
    )
    with pytest.raises(Exception):
        list(fts_database.execute(statement, parameters={}))
    with pytest.raises(Exception):
        list(fts_database.execute(statement, parameters={"query": None}))


def test_fts_null_values_are_not_indexed_and_transitions_are_maintained(tmp_path):
    database_path = str(tmp_path / "fts_null_db")
    db = Database(db_path=database_path, mode="w")
    connection = db.connect()
    load_fts(connection, skip_if_unavailable=True)
    create_item_table(connection)
    connection.execute(
        "CREATE (:Item {id: 1, text: NULL}), (:Item {id: 2}), "
        "(:Item {id: 3, text: 'visible token'});"
    )
    connection.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
    assert [row[0] for row in search(connection, "visible")] == [3]

    connection.execute("CREATE (:Item {id: 4, text: NULL}), (:Item {id: 5});")
    connection.execute("MATCH (n:Item) WHERE n.id = 3 SET n.text = NULL;")
    assert search(connection, "visible") == []
    connection.execute("MATCH (n:Item) WHERE n.id = 1 SET n.text = 'added token';")
    assert [row[0] for row in search(connection, "added")] == [1]
    connection.execute("MATCH (n:Item) WHERE n.id = 2 SET n.text = NULL;")
    connection.execute("CHECKPOINT;")
    connection.close()
    db.close()

    reopened = Database(db_path=database_path, mode="w")
    reopened_connection = reopened.connect()
    try:
        load_fts(reopened_connection)
        assert [row[0] for row in search(reopened_connection, "added")] == [1]
        assert search(reopened_connection, "visible") == []
    finally:
        reopened_connection.close()
        reopened.close()


def test_multiple_fts_indexes_for_same_property_are_ambiguous(fts_database):
    fts_database.execute(
        "CREATE INDEX item_text_porter_fts ON Item USING FTS (text) "
        "WITH (tokenizer = 'porter unicode61');"
    )
    with pytest.raises(RuntimeError, match="Multiple FTS indexes"):
        search(fts_database, "search")


@pytest.mark.parametrize(
    ("tokenizer", "jieba_mode", "expected_ids"),
    [
        pytest.param("unicode61", None, ([], []), id="unicode61"),
        pytest.param("ascii", None, ([], []), id="ascii"),
        pytest.param("porter", None, ([], []), id="porter"),
        pytest.param("trigram", None, ([], [2]), id="trigram"),
        pytest.param("jieba", None, ([1], [2]), id="jieba-default"),
        pytest.param("jieba", "mp", ([1], [2]), id="jieba-mp"),
        pytest.param("jieba", "hmm", ([1], [2]), id="jieba-hmm"),
        pytest.param("jieba", "mix", ([1], [2]), id="jieba-mix"),
    ],
)
def test_jieba_tokenizer_modes_segment_chinese(
    tmp_path, tokenizer, jieba_mode, expected_ids
):
    tokenizer_name = tokenizer
    mode_name = jieba_mode or "default"
    database_name = f"{tokenizer_name}_{mode_name}_fts_db"
    db = Database(db_path=str(tmp_path / database_name), mode="w")
    connection = db.connect()
    try:
        load_fts(connection, skip_if_unavailable=True)
        create_item_table(connection)
        connection.execute(
            "CREATE (:Item {id: 1, text: '他来到了网易杭研大厦'}), "
            "(:Item {id: 2, text: '我来到北京清华大学'});"
        )
        options = f"tokenizer = '{tokenizer}'"
        if jieba_mode is not None:
            options += f", jieba_mode = '{jieba_mode}'"
        connection.execute(
            "CREATE INDEX item_text_fts ON Item USING FTS (text) " f"WITH ({options});"
        )

        assert [row[0] for row in search(connection, "网易")] == expected_ids[0]
        assert [row[0] for row in search(connection, "清华大学")] == expected_ids[1]
    finally:
        connection.close()
        db.close()


def test_jieba_user_dict_extends_builtin_dictionary(tmp_path):
    user_dict = tmp_path / "user.dict.utf8"
    user_dict.write_text("万圣节\n", encoding="utf-8")

    db = Database(db_path=str(tmp_path / "jieba_builtin_dict_fts_db"), mode="w")
    connection = db.connect()
    try:
        load_fts(connection, skip_if_unavailable=True)
        create_item_table(connection)
        connection.execute("CREATE (:Item {id: 1, text: '万圣节后举行派对'});")
        connection.execute(
            "CREATE INDEX item_text_fts ON Item USING FTS (text) "
            "WITH (tokenizer = 'jieba', jieba_mode = 'mp');"
        )
        # Text: 万圣 / 节后 / 举行 / 派对; query: 万圣 / 节.
        assert [row[0] for row in search(connection, "万圣节")] == []
    finally:
        connection.close()
        db.close()

    db = Database(db_path=str(tmp_path / "jieba_user_dict_fts_db"), mode="w")
    connection = db.connect()
    try:
        load_fts(connection, skip_if_unavailable=True)
        create_item_table(connection)
        connection.execute("CREATE (:Item {id: 1, text: '万圣节后举行派对'});")
        connection.execute(
            "CREATE INDEX item_text_fts ON Item USING FTS (text) "
            "WITH (tokenizer = 'jieba', jieba_mode = 'mp', "
            f"jieba_dict = '{user_dict}');"
        )
        # Text: 万圣节 / 后 / 举行 / 派对; query: 万圣节.
        assert [row[0] for row in search(connection, "万圣节")] == [1]
    finally:
        connection.close()
        db.close()


@pytest.mark.parametrize(
    ("option", "value", "error_pattern"),
    [
        ("tokenizer", "unknown", "tokenizer"),
        ("jieba_mode", "mix", "jieba_mode"),
        ("prefix", "2 bad", "prefix"),
        ("detail", "invalid", "detail"),
    ],
)
def test_create_fts_index_rejects_invalid_options(
    tmp_path, option, value, error_pattern
):
    db = Database(db_path=str(tmp_path / f"invalid_{option}_fts_db"), mode="w")
    connection = db.connect()
    try:
        load_fts(connection, skip_if_unavailable=True)
        create_item_table(connection)
        with pytest.raises(RuntimeError, match=error_pattern):
            connection.execute(
                "CREATE INDEX item_text_fts ON Item USING FTS (text) "
                f"WITH ({option} = '{value}');"
            )
    finally:
        connection.close()
        db.close()


def test_create_jieba_fts_index_rejects_invalid_mode(tmp_path):
    db = Database(db_path=str(tmp_path / "invalid_jieba_mode_fts_db"), mode="w")
    connection = db.connect()
    try:
        load_fts(connection, skip_if_unavailable=True)
        create_item_table(connection)
        with pytest.raises(RuntimeError, match="jieba_mode"):
            connection.execute(
                "CREATE INDEX item_text_fts ON Item USING FTS (text) "
                "WITH (tokenizer = 'jieba', jieba_mode = 'invalid');"
            )
    finally:
        connection.close()
        db.close()


def test_fts_index_persistence_after_process_restart(tmp_path):
    database_path = str(tmp_path / "fts_process_restart_db")
    create_script = textwrap.dedent(
        f"""
        from neug.database import Database
        db = Database(db_path={database_path!r}, mode="w")
        connection = db.connect()
        connection.execute("LOAD fts;")
        connection.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, text STRING);")
        connection.execute("CREATE (:Item {{id: 1, text: 'durable fox'}});")
        connection.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
        connection.close()
        db.close()
        """
    )
    reopen_script = textwrap.dedent(
        f"""
        from neug.database import Database
        db = Database(db_path={database_path!r}, mode="w")
        connection = db.connect()
        connection.execute("LOAD fts;")
        query = "MATCH (n:Item) RETURN n.id, bm25(n.text, 'durable') AS score ORDER BY score ASC;"
        assert [row[0] for row in connection.execute(query)] == [1]
        connection.execute("CREATE (:Item {{id: 2, text: 'durable hare'}});")
        assert {{row[0] for row in connection.execute(query)}} == {{1, 2}}
        connection.close()
        db.close()
        """
    )
    subprocess.run([sys.executable, "-c", create_script], check=True)
    subprocess.run([sys.executable, "-c", reopen_script], check=True)


@pytest.mark.parametrize("query", ["", "   ", 'unterminated"', "quick.brown"])
def test_invalid_fts_query_raises_runtime_error(fts_database, query):
    with pytest.raises(RuntimeError, match="FTS query failed"):
        search(fts_database, query)
