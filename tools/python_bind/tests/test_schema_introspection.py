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

"""End-to-end coverage for the Schema Introspection CALL functions."""

import json

import pytest

from neug.database import Database


@pytest.fixture
def schema_connection(tmp_path):
    db = Database(db_path=str(tmp_path / "schema_introspection"), mode="w")
    conn = db.connect()
    company_ddl = "CREATE NODE TABLE Company("
    company_ddl += "id INT64 PRIMARY KEY, name STRING);"
    conn.execute(company_ddl)
    conn.execute(
        "CREATE NODE TABLE Person("
        "id INT64 PRIMARY KEY, name STRING DEFAULT 'anonymous', "
        "age INT32, active BOOL DEFAULT true);"
    )
    conn.execute(
        "CREATE REL TABLE WorksAt("
        "FROM Person TO Company, since INT32 DEFAULT 2000, role STRING, "
        "MANY_TO_ONE) WITH (sort_key_for_nbr='since');"
    )
    yield conn, tmp_path
    conn.close()
    db.close()


def test_show_tables_for_empty_graph(tmp_path):
    db = Database(db_path=str(tmp_path / "empty_graph"), mode="w")
    conn = db.connect()

    assert list(conn.execute("CALL SHOW_NODE_TABLES() RETURN *;")) == []
    assert list(conn.execute("CALL SHOW_REL_TABLE() RETURN *;")) == []

    conn.close()
    db.close()


def test_show_node_tables(schema_connection):
    conn, tmp_path = schema_connection
    people_csv = tmp_path / "temporary_people.csv"
    people_csv.write_text("id|nickname\n1|alice\n")
    copy_query = f'COPY TEMP TempPerson FROM "{people_csv}" '
    copy_query += "(primary_key='id', header=true);"
    conn.execute(copy_query)

    rows = list(
        conn.execute(
            "CALL SHOW_NODE_TABLES() "
            "RETURN vertex_label_name, primary_key, temporary;"
        )
    )
    assert rows == [
        ["Company", "id", False],
        ["Person", "id", False],
        ["TempPerson", "id", True],
    ]


def test_show_rel_table(schema_connection):
    conn, tmp_path = schema_connection
    edges_csv = tmp_path / "temporary_edges.csv"
    edges_csv.write_text("src|dst|weight\n1|1|0.5\n")
    conn.execute("CREATE (:Company {id: 1, name: 'NeuG'});")
    conn.execute(
        f'COPY TEMP TempPartner FROM "{edges_csv}" '
        "(header=true, from='Company', to='Company');"
    )

    rows = list(
        conn.execute(
            "CALL SHOW_REL_TABLE() RETURN edge_label_name, src_label_name, "
            "dst_label_name, multiplicity, temporary, extra_options;"
        )
    )
    assert rows[0][:5] == [
        "TempPartner",
        "Company",
        "Company",
        "MANY_TO_MANY",
        True,
    ]
    assert json.loads(rows[0][5]) == {}
    assert rows[1][:5] == [
        "WorksAt",
        "Person",
        "Company",
        "MANY_TO_ONE",
        False,
    ]
    assert json.loads(rows[1][5]) == {"sort_key_for_nbr": "since"}


def test_show_table_info_for_node(schema_connection):
    conn, _ = schema_connection
    rows = list(
        conn.execute(
            "CALL SHOW_TABLE_INFO('Person') RETURN property_name, "
            "property_type, default_value, primary_key;"
        )
    )
    assert rows == [
        ["id", "INT64", "0", True],
        ["name", "VARCHAR", "anonymous", False],
        ["age", "INT32", "0", False],
        ["active", "BOOLEAN", "true", False],
    ]


def test_show_table_info_for_edge_triplet(schema_connection):
    conn, _ = schema_connection
    rows = list(
        conn.execute(
            "CALL SHOW_TABLE_INFO(' [ Person , WorksAt , Company ] ') "
            "RETURN property_name, property_type, default_value, primary_key;"
        )
    )
    assert rows == [
        ["since", "INT32", "2000", False],
        ["role", "VARCHAR", "", False],
    ]


@pytest.mark.parametrize(
    "query, message",
    [
        (
            "CALL SHOW_TABLE_INFO('Missing');",
            "Node table 'Missing' does not exist",
        ),
        (
            "CALL SHOW_TABLE_INFO('[Person, WorksAt]');",
            "Invalid edge triplet",
        ),
        (
            "CALL SHOW_TABLE_INFO('[Company, WorksAt, Person]');",
            "Edge table .* does not exist",
        ),
    ],
)
def test_invalid_table_info_targets(schema_connection, query, message):
    conn, _ = schema_connection
    with pytest.raises(Exception, match=message):
        list(conn.execute(query))
