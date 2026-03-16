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

import csv
import os
import shutil
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug.database import Database


def _count_query(conn, cypher):
    """Execute query and return number of result rows."""
    return len(list(conn.execute(cypher)))


def _parse_csv(path, delimiter="|", has_header=True):
    """Parse CSV; returns (header or None, list of data rows)."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=delimiter)
        rows = list(reader)
    if not rows:
        return (None, [])
    if has_header:
        return (rows[0], rows[1:])
    return (None, rows)


class TestExport:
    """COPY TO CSV tests using tinysnb. Assert header and data row count only."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.db_dir = "/tmp/tinysnb"
        if not os.path.exists(self.db_dir):
            pytest.fail(f"Database not found at {self.db_dir}")
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        self.tmp_path = tmp_path
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_export_person_with_header(self):
        out_path = self.tmp_path / "person.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:person) RETURN v")
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v) TO '{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None and len(header) == 1
        assert len(rows) == expected

    def test_export_person_without_header(self):
        out_path = self.tmp_path / "person_no_header.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:person) RETURN v")
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v) TO '{out_path}' (HEADER = false);"
        )
        assert out_path.exists()
        _, rows = _parse_csv(out_path, "|", has_header=False)
        assert len(rows) == expected

    def test_export_knows(self):
        out_path = self.tmp_path / "knows.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn, "MATCH (v:person)-[e:knows]->(v2:person) RETURN e"
        )
        self.conn.execute(
            f"COPY (MATCH (v:person)-[e:knows]->(v2:person) RETURN e) TO '{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None and len(header) >= 1
        assert len(rows) == expected

    def test_export_path(self):
        out_path = self.tmp_path / "path.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn, "MATCH (v:person)-[e:knows*0..1]->(v2:person) RETURN e"
        )
        self.conn.execute(
            f"COPY (MATCH (v:person)-[e:knows*0..1]->(v2:person) RETURN e) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None
        assert len(rows) == expected

    def test_export_delimiter_comma(self):
        out_path = self.tmp_path / "delim_comma.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:person) RETURN v.ID, v.fName")
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName) TO "
            f"'{out_path}' (HEADER = true, DELIMITER = ',');"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, ",", has_header=True)
        assert len(header) == 2
        assert len(rows) == expected

    def test_export_selected_columns(self):
        out_path = self.tmp_path / "selected.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn, "MATCH (v:person) RETURN v.ID, v.fName, v.age"
        )
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName, v.age) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert len(header) == 3
        assert len(rows) == expected

    def test_export_where(self):
        out_path = self.tmp_path / "filtered.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn, "MATCH (v:person) WHERE v.age > 20 RETURN v.*"
        )
        self.conn.execute(
            f"COPY (MATCH (v:person) WHERE v.age > 20 RETURN v.*) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None
        assert len(rows) == expected

    def test_export_organisation(self):
        out_path = self.tmp_path / "org.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:organisation) RETURN v.*")
        self.conn.execute(
            f"COPY (MATCH (v:organisation) RETURN v.*) TO '{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None and len(header) >= 1
        assert len(rows) == expected

    def test_export_movies(self):
        out_path = self.tmp_path / "movies.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:movies) RETURN v.*")
        self.conn.execute(
            f"COPY (MATCH (v:movies) RETURN v.*) TO '{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None and len(header) >= 1
        assert len(rows) == expected

    def test_export_studyAt(self):
        out_path = self.tmp_path / "studyAt.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn,
            "MATCH (v:person)-[e:studyAt]->(v2:organisation) RETURN e",
        )
        self.conn.execute(
            f"COPY (MATCH (v:person)-[e:studyAt]->(v2:organisation) RETURN e) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None
        assert len(rows) == expected

    def test_export_workAt(self):
        out_path = self.tmp_path / "workAt.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn,
            "MATCH (v:person)-[e:workAt]->(v2:organisation) RETURN e",
        )
        self.conn.execute(
            f"COPY (MATCH (v:person)-[e:workAt]->(v2:organisation) RETURN e) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None
        assert len(rows) == expected

    def test_export_verify_count(self):
        out_path = self.tmp_path / "verify.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:person) RETURN v.ID, v.fName")
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert len(header) == 2
        assert len(rows) == expected

    def test_export_property_types_scalar(self):
        out_path = self.tmp_path / "scalar.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn,
            "MATCH (v:person) RETURN v.ID, v.fName, v.age, v.eyeSight, v.isStudent",
        )
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName, v.age, v.eyeSight, v.isStudent) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert len(header) == 5
        assert len(rows) == expected

    def test_export_property_types_string_dates(self):
        out_path = self.tmp_path / "string_date.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn,
            "MATCH (v:person) RETURN v.ID, v.fName, v.birthdate, v.registerTime",
        )
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName, v.birthdate, v.registerTime) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert len(header) == 4
        assert len(rows) == expected

    def test_export_node_person_whole(self):
        out_path = self.tmp_path / "node_person.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:person) RETURN v")
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v) TO '{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert len(header) == 1
        assert len(rows) == expected

    def test_export_node_organisation_whole(self):
        out_path = self.tmp_path / "node_org.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:organisation) RETURN v")
        self.conn.execute(
            f"COPY (MATCH (v:organisation) RETURN v) TO '{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert len(header) == 1
        assert len(rows) == expected

    def test_export_node_movies_whole(self):
        out_path = self.tmp_path / "node_movies.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:movies) RETURN v")
        self.conn.execute(
            f"COPY (MATCH (v:movies) RETURN v) TO '{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert len(header) == 1
        assert len(rows) == expected

    def test_export_edge_knows(self):
        out_path = self.tmp_path / "edge_knows.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn, "MATCH (:person)-[e:knows]->(:person) RETURN e"
        )
        self.conn.execute(
            f"COPY (MATCH (:person)-[e:knows]->(:person) RETURN e) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None and len(header) >= 1
        assert len(rows) == expected

    def test_export_edge_studyAt_workAt(self):
        for label, fname in [("studyAt", "e_studyAt"), ("workAt", "e_workAt")]:
            out_path = self.tmp_path / f"{fname}.csv"
            out_path.unlink(missing_ok=True)
            expected = _count_query(
                self.conn,
                f"MATCH (:person)-[e:{label}]->(:organisation) RETURN e",
            )
            self.conn.execute(
                f"COPY (MATCH (:person)-[e:{label}]->(:organisation) RETURN e) TO "
                f"'{out_path}' (HEADER = true);"
            )
            assert out_path.exists()
            header, rows = _parse_csv(out_path, "|", has_header=True)
            assert header is not None and len(header) >= 1
            assert len(rows) == expected

    def test_export_path_var_len(self):
        out_path = self.tmp_path / "path_var.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn, "MATCH (v:person)-[e:knows*0..1]->(v2:person) RETURN e"
        )
        self.conn.execute(
            f"COPY (MATCH (v:person)-[e:knows*0..1]->(v2:person) RETURN e) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None
        assert len(rows) == expected

    def test_export_path_multi_hop(self):
        out_path = self.tmp_path / "path_multi.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn,
            "MATCH (v:person)-[k:knows*1..3]->(v2:person) RETURN v, k, v2",
        )
        self.conn.execute(
            f"COPY (MATCH (v:person)-[k:knows*1..3]->(v2:person) RETURN v, k, v2) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None and len(header) == 3
        assert len(rows) == expected

    def test_export_delimiter_pipe(self):
        out_path = self.tmp_path / "delim_pipe.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:person) RETURN v.ID, v.fName")
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName) TO '{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert len(header) == 2
        assert len(rows) == expected

    def test_export_delimiter_semicolon(self):
        out_path = self.tmp_path / "delim_semi.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:person) RETURN v.ID, v.fName")
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName) TO "
            f"'{out_path}' (HEADER = true, DELIM = ';');"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, ";", has_header=True)
        assert len(header) == 2
        assert len(rows) == expected

    def test_export_header_true(self):
        out_path = self.tmp_path / "header_true.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:person) RETURN v.ID, v.fName")
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName) TO "
            f"'{out_path}' (HEADER = true);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert header is not None and len(header) == 2
        assert len(rows) == expected

    def test_export_header_false(self):
        out_path = self.tmp_path / "header_false.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:person) RETURN v.ID, v.fName")
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName) TO "
            f"'{out_path}' (HEADER = false);"
        )
        assert out_path.exists()
        _, rows = _parse_csv(out_path, "|", has_header=False)
        assert len(rows) == expected

    def test_export_batch_size(self):
        out_path = self.tmp_path / "batch.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(self.conn, "MATCH (v:person) RETURN v.ID, v.fName")
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName) TO "
            f"'{out_path}' (HEADER = true, BATCH_SIZE = 1024);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert len(header) == 2
        assert len(rows) == expected

    def test_export_combined_options(self):
        out_path = self.tmp_path / "combined.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn, "MATCH (v:person) RETURN v.ID, v.fName, v.age"
        )
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, v.fName, v.age) TO "
            f"'{out_path}' (HEADER = true, DELIMITER = ',', BATCH_SIZE = 2048);"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, ",", has_header=True)
        assert len(header) == 3
        assert len(rows) == expected

    def test_export_collect_names(self):
        out_path = self.tmp_path / "collect_names.csv"
        out_path.unlink(missing_ok=True)
        expected = _count_query(
            self.conn, "MATCH (v:person) RETURN v.ID, collect(v.fName)"
        )
        self.conn.execute(
            f"COPY (MATCH (v:person) RETURN v.ID, collect(v.fName)) TO "
            f"'{out_path}' (HEADER = true, QUOTE = '\\'');"
        )
        assert out_path.exists()
        header, rows = _parse_csv(out_path, "|", has_header=True)
        assert len(header) == 2
        assert len(rows) == expected

    # Verify that the 'QUOTE' option correctly changes the wrapping character for string values.
    # Here, we explicitly set QUOTE = "'" (single quote).
    # Expected behavior: The output string "Alice" should be wrapped in single quotes instead of the default double quotes.
    def test_export_with_single_quote(self):
        out_path = self.tmp_path / "single_quote.csv"
        out_path.unlink(missing_ok=True)
        self.conn.execute(
            f"COPY (MATCH (v:person {{ID: 0}}) RETURN v.fName) TO '{out_path}' (HEADER = false, QUOTE = '\\'');"
        )
        assert out_path.exists()
        with open(out_path, "r", encoding="utf-8") as f:
            content = f.read()
            assert content == "'Alice'\n"

    # Verify default escaping behavior when data contains the quote character itself.
    # Scenario: The data contains a double quote (John"s).
    # Since the default QUOTE character is double quote ("), the internal quote must be escaped.
    # Expected behavior: The field is wrapped in double quotes, and the internal double quote is escaped with a backslash (\).
    def test_export_with_escape_char(self):
        out_path = self.tmp_path / "escape_char.csv"
        out_path.unlink(missing_ok=True)
        self.conn.execute("CREATE (:person {ID: 1006, fName: 'John\"s'})")
        try:
            self.conn.execute(
                f"COPY (MATCH (v:person {{ID: 1006}}) RETURN v.fName) TO "
                f"'{out_path}' (HEADER = false);"
            )
            assert out_path.exists()
            with open(out_path, "r", encoding="utf-8") as f:
                content = f.read()
                assert content == '"John\\"s"\n'
        finally:
            self.conn.execute("MATCH (v:person {ID: 1006}) DELETE v")
