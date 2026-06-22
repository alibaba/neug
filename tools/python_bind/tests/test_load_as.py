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

"""
End-to-end tests for LOAD NODE TABLE / LOAD REL TABLE AS (temporary graph).

These tests exercise the full compiler + execution stack via the Python
binding, covering:
  - Basic LOAD NODE TABLE / LOAD REL TABLE
  - WHERE filter pushdown
  - RETURN projection pushdown
  - WHERE + RETURN combined
  - Error cases (missing options, label conflicts, bad RETURN columns)
  - Automatic cleanup when the connection is closed
"""

import os
import shutil
import sys

import pytest

from neug import Database

# Extension tests require parquet extension to be compiled and
# NEUG_RUN_EXTENSION_TESTS=1 to be set.
EXTENSION_TESTS_ENABLED = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
extension_test = pytest.mark.skipif(
    not EXTENSION_TESTS_ENABLED,
    reason="Extension tests disabled by default; set NEUG_RUN_EXTENSION_TESTS=1 to enable.",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_csv(directory: str, filename: str, content: str) -> str:
    """Write *content* to ``directory/filename`` and return the full path.

    NeuG's CSV reader defaults to ``|`` as delimiter and ``HEADER=false``.
    All test CSV files therefore use ``|`` as the separator and always pair
    with ``header = true`` in the LOAD statement.
    """
    path = os.path.join(directory, filename)
    with open(path, "w") as f:
        f.write(content)
    return path


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestLoadAs:
    """End-to-end tests for LOAD NODE/REL TABLE AS (temporary graph)."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.db_dir = str(tmp_path / "test_load_as_db")
        self.csv_dir = str(tmp_path / "csv")
        shutil.rmtree(self.db_dir, ignore_errors=True)
        os.makedirs(self.csv_dir, exist_ok=True)

        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()

        # ----- shared CSV fixtures -----
        self.people_csv = _write_csv(
            self.csv_dir,
            "people.csv",
            "id|name|age\n" "1|Alice|30\n" "2|Bob|25\n" "3|Carol|35\n" "4|Dave|20\n",
        )

        self.items_csv = _write_csv(
            self.csv_dir,
            "items.csv",
            "item_id|title|price\n"
            "101|Widget|9.99\n"
            "102|Gadget|19.99\n"
            "103|Doohickey|4.99\n",
        )

        self.edges_csv = _write_csv(
            self.csv_dir,
            "edges.csv",
            "src_id|dst_id|weight\n" "1|2|0.5\n" "2|3|1.0\n" "3|4|0.8\n",
        )

        self.dangling_edges_csv = _write_csv(
            self.csv_dir,
            "dangling_edges.csv",
            "src_id|dst_id|weight\n" "1|2|0.5\n" "99|3|1.0\n",
        )

        # Edge CSV where key columns are NOT at positions [0] and [1].
        # Columns: weight, src_id, dst_id — keys are at positions [1] and [2].
        self.edges_shuffled_csv = _write_csv(
            self.csv_dir,
            "edges_shuffled.csv",
            "weight|src_id|dst_id\n"
            "0.5|1|2\n"
            "1.0|2|3\n"
            "0.8|3|4\n",
        )

        yield

        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_persistent_person_table(self):
        """Create the persistent Person node table and insert four rows.

        Required before LOAD REL TABLE tests so that edge endpoint look-ups
        succeed.
        """
        self.conn.execute(
            "CREATE NODE TABLE Person(id INT64, name STRING, age INT64, "
            "PRIMARY KEY(id));"
        )
        for vid, name, age in [
            (1, "Alice", 30),
            (2, "Bob", 25),
            (3, "Carol", 35),
            (4, "Dave", 20),
        ]:
            self.conn.execute(
                f"CREATE (p:Person {{id: {vid}, name: '{name}', age: {age}}});"
            )

    # ------------------------------------------------------------------
    # LOAD NODE TABLE — basic
    # ------------------------------------------------------------------

    def test_load_node_table_basic(self):
        """LOAD NODE TABLE creates a temporary vertex table that is
        queryable via MATCH."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempPeople;"
        )

        result = self.conn.execute(
            "MATCH (n:TempPeople) RETURN n.id, n.name, n.age ORDER BY n.id;"
        )
        rows = list(result)
        assert len(rows) == 4, f"Expected 4 vertices, got {len(rows)}"
        assert rows[0][0] == 1
        assert rows[0][1] == "Alice"
        assert rows[3][0] == 4
        assert rows[3][1] == "Dave"

    def test_load_node_table_default_primary_key(self):
        """When ``primary_key`` is omitted, the first column is used."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(header = true) AS TempDefault;"
        )

        result = self.conn.execute("MATCH (n:TempDefault) RETURN n.id ORDER BY n.id;")
        rows = list(result)
        assert len(rows) == 4

    # ------------------------------------------------------------------
    # LOAD NODE TABLE — WHERE filter pushdown
    # ------------------------------------------------------------------

    def test_load_node_table_where_filter(self):
        """WHERE predicate is pushed down to the DataSource; only matching
        rows are inserted."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"WHERE age > 25 AS TempFiltered;"
        )

        result = self.conn.execute(
            "MATCH (n:TempFiltered) RETURN n.id, n.name, n.age " "ORDER BY n.id;"
        )
        rows = list(result)
        # Alice(30) and Carol(35) pass the filter.
        assert len(rows) == 2, f"Expected 2 vertices, got {len(rows)}"
        assert rows[0][1] == "Alice"
        assert rows[1][1] == "Carol"

    # ------------------------------------------------------------------
    # LOAD NODE TABLE — RETURN projection pushdown
    # ------------------------------------------------------------------

    def test_load_node_table_return_projection(self):
        """RETURN restricts which source columns become vertex properties."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"RETURN id, name AS TempSlim;"
        )

        # Only id and name should be accessible as properties.
        result = self.conn.execute(
            "MATCH (n:TempSlim) RETURN n.id, n.name ORDER BY n.id;"
        )
        rows = list(result)
        assert len(rows) == 4
        assert rows[0][1] == "Alice"

        # age was not in RETURN — referencing it should raise an error.
        with pytest.raises(Exception):
            list(self.conn.execute("MATCH (n:TempSlim) RETURN n.age;"))

    # ------------------------------------------------------------------
    # LOAD NODE TABLE — WHERE + RETURN combined
    # ------------------------------------------------------------------

    def test_load_node_table_where_and_return(self):
        """WHERE filters rows; RETURN selects which columns become
        properties.  The WHERE-only column (age) must NOT appear as a
        property."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"WHERE age >= 25 "
            f"RETURN id, name AS TempWR;"
        )

        result = self.conn.execute(
            "MATCH (n:TempWR) RETURN n.id, n.name ORDER BY n.id;"
        )
        rows = list(result)
        # Bob(25), Alice(30), Carol(35) satisfy age >= 25.
        assert len(rows) == 3, f"Expected 3 vertices, got {len(rows)}"

        # age must not be a property even though it was used in WHERE.
        with pytest.raises(Exception):
            list(self.conn.execute("MATCH (n:TempWR) RETURN n.age;"))

    # ------------------------------------------------------------------
    # LOAD REL TABLE — basic
    # ------------------------------------------------------------------

    def test_load_rel_table_basic(self):
        """LOAD REL TABLE creates a temporary edge table on top of
        existing (persistent or temporary) vertex tables."""
        # Load temporary vertex tables first.
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempPerson;"
        )
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'TempPerson', to = 'TempPerson', "
            f"from_col = 'src_id', to_col = 'dst_id') AS TempKnows;"
        )

        result = self.conn.execute(
            "MATCH (a:TempPerson)-[r:TempKnows]->(b:TempPerson) "
            "RETURN a.id, b.id, r.weight ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3, f"Expected 3 edges, got {len(rows)}"
        # First edge: src=1 -> dst=2
        assert rows[0][0] == 1
        assert rows[0][1] == 2

    def test_load_rel_table_with_persistent_vertices(self):
        """Edges can reference a persistent vertex table."""
        self._load_persistent_person_table()

        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'Person', to = 'Person', "
            f"from_col = 'src_id', to_col = 'dst_id') AS TempKnowsP;"
        )

        result = self.conn.execute(
            "MATCH (a:Person)-[r:TempKnowsP]->(b:Person) "
            "RETURN a.id, b.id ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3

    def test_load_rel_table_no_from_col_to_col(self):
        """Without from_col/to_col, columns[0] and [1] are used as keys
        (consistent with COPY FROM default behavior)."""
        self._load_persistent_person_table()
        # edges.csv columns: src_id(0) | dst_id(1) | weight(2)
        # Without from_col/to_col, src_id and dst_id are keys by position.
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'Person', to = 'Person') AS TempDefaultKey;"
        )
        result = self.conn.execute(
            "MATCH (a:Person)-[r:TempDefaultKey]->(b:Person) "
            "RETURN a.id, b.id, r.weight ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3
        assert rows[0] == [1, 2, 0.5]

    def test_load_rel_table_from_col_to_col_at_position_0_1(self):
        """from_col/to_col match file columns[0/1] — no reordering needed."""
        self._load_persistent_person_table()
        # edges.csv columns: src_id(0) | dst_id(1) | weight(2)
        # from_col/to_col match positions [0/1] — works without subquery.
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'Person', to = 'Person', "
            f"from_col = 'src_id', to_col = 'dst_id') AS TempEdge01;"
        )
        result = self.conn.execute(
            "MATCH (a:Person)-[r:TempEdge01]->(b:Person) "
            "RETURN a.id, b.id, r.weight ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3
        assert rows[0] == [1, 2, 0.5]

    def test_load_rel_table_from_col_to_col_not_at_position_0_1(self):
        """When from_col/to_col point to non-[0/1] columns, the binder
        automatically switches to a subquery path to reorder columns so
        that ddlColumns[0/1] are the src/dst keys.

        File columns: weight(0) | src_id(1) | dst_id(2)
        from_col='src_id', to_col='dst_id' → ddlColumns=[src_id, dst_id, weight]
        Subquery projection ensures data order matches ddlColumns.
        """
        self._load_persistent_person_table()
        # edges_shuffled.csv: weight | src_id | dst_id
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_shuffled_csv}" '
            f"(header = true, "
            f"from = 'Person', to = 'Person', "
            f"from_col = 'src_id', to_col = 'dst_id') AS TempEdgeShuffled;"
        )
        result = self.conn.execute(
            "MATCH (a:Person)-[r:TempEdgeShuffled]->(b:Person) "
            "RETURN a.id, b.id, r.weight ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3
        assert rows[0] == [1, 2, 0.5]
        assert rows[1] == [2, 3, 1.0]
        assert rows[2] == [3, 4, 0.8]

    def test_load_rel_table_return_without_from_col_to_col(self):
        """RETURN without from_col/to_col: RETURN columns in source order,
        first two become keys by position."""
        self._load_persistent_person_table()
        # edges.csv columns: src_id(0) | dst_id(1) | weight(2)
        # RETURN selects all three; src_id and dst_id are keys by position.
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'Person', to = 'Person') "
            f"RETURN src_id, dst_id, weight AS TempReturnNoKey;"
        )
        result = self.conn.execute(
            "MATCH (a:Person)-[r:TempReturnNoKey]->(b:Person) "
            "RETURN a.id, b.id, r.weight ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3
        assert rows[0] == [1, 2, 0.5]

    def test_load_rel_table_return_reorders_keys_to_front(self):
        """When file has keys NOT at [0/1], user uses RETURN to place keys
        first. 

        File columns: weight(0) | src_id(1) | dst_id(2)
        RETURN: src_id, dst_id, weight → keys at [0/1] in ddlColumns.
        """
        self._load_persistent_person_table()
        # edges_shuffled.csv: weight | src_id | dst_id
        # Without RETURN, weight would be treated as src key (wrong).
        # RETURN reorders columns so src_id and dst_id are at [0/1].
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_shuffled_csv}" '
            f"(header = true, "
            f"from = 'Person', to = 'Person') "
            f"RETURN src_id, dst_id, weight AS TempReorder;"
        )
        result = self.conn.execute(
            "MATCH (a:Person)-[r:TempReorder]->(b:Person) "
            "RETURN a.id, b.id, r.weight ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3
        assert rows[0] == [1, 2, 0.5]
        assert rows[1] == [2, 3, 1.0]
        assert rows[2] == [3, 4, 0.8]

    def test_load_rel_table_return_drops_columns(self):
        """RETURN can select a subset of columns, dropping unnecessary ones.
        Only the returned columns become edge properties.

        File columns: src_id(0) | dst_id(1) | weight(2)
        RETURN: src_id, dst_id → only key columns, no edge properties.
        """
        self._load_persistent_person_table()
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'Person', to = 'Person') "
            f"RETURN src_id, dst_id AS TempNoProps;"
        )
        # Edge exists but has no 'weight' property.
        result = self.conn.execute(
            "MATCH (a:Person)-[r:TempNoProps]->(b:Person) "
            "RETURN a.id, b.id ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3
        assert rows[0] == [1, 2]
        # Accessing weight should fail since it wasn't in RETURN.
        with pytest.raises(Exception):
            self.conn.execute(
                "MATCH ()-[r:TempNoProps]->() RETURN r.weight;"
            )

    # ------------------------------------------------------------------
    # Error cases
    # ------------------------------------------------------------------

    def test_load_rel_table_missing_from_to(self):
        """LOAD REL TABLE without ``from`` / ``to`` options must fail."""
        with pytest.raises(Exception):
            self.conn.execute(
                f'LOAD REL TABLE FROM "{self.edges_csv}" '
                f"(header = true) AS TempEdge;"
            )

    def test_load_node_table_return_missing_primary_key(self):
        """RETURN that omits the primary key column must fail."""
        with pytest.raises(Exception):
            self.conn.execute(
                f'LOAD NODE TABLE FROM "{self.people_csv}" '
                f"(primary_key = 'id', header = true) "
                f"RETURN name, age AS TempBad;"
            )

    def test_load_rel_table_return_missing_from_to_cols(self):
        """RETURN that omits from_col / to_col for an edge table must
        fail."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempPerson2;"
        )
        with pytest.raises(Exception):
            self.conn.execute(
                f'LOAD REL TABLE FROM "{self.edges_csv}" '
                f"(header = true, "
                f"from = 'TempPerson2', to = 'TempPerson2', "
                f"from_col = 'src_id', to_col = 'dst_id') "
                f"RETURN weight AS TempBadEdge;"
            )

    def test_load_node_table_return_nonexistent_column(self):
        """RETURN with a column name not present in the source must
        fail."""
        with pytest.raises(Exception):
            self.conn.execute(
                f'LOAD NODE TABLE FROM "{self.people_csv}" '
                f"(primary_key = 'id', header = true) "
                f"RETURN id, no_such_column AS TempBad2;"
            )

    def test_load_node_table_label_conflict(self):
        """LOAD AS with a label that already exists as a persistent table
        must fail."""
        self.conn.execute("CREATE NODE TABLE Conflict(id INT64, PRIMARY KEY(id));")
        with pytest.raises(Exception):
            self.conn.execute(
                f'LOAD NODE TABLE FROM "{self.people_csv}" '
                f"(primary_key = 'id', header = true) AS Conflict;"
            )

    # ------------------------------------------------------------------
    # Cleanup on connection close
    # ------------------------------------------------------------------

    def test_cleanup_on_close(self):
        """Temporary labels must disappear after the connection is closed.

        ``Connection::Close()`` removes all temporary labels from the
        shared PropertyGraph, so a fresh connection on the same Database
        should no longer see them.
        """
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempEphemeral;"
        )
        # Verify the temp table is visible on the current connection.
        result = self.conn.execute("MATCH (n:TempEphemeral) RETURN count(n);")
        rows = list(result)
        assert rows[0][0] == 4

        # Closing the connection triggers temp-label cleanup.
        self.conn.close()

        # A new connection on the same Database must not see the label.
        conn2 = self.db.connect()
        try:
            with pytest.raises(Exception):
                list(conn2.execute("MATCH (n:TempEphemeral) RETURN n.id;"))
        finally:
            conn2.close()

        # Re-bind self.conn so the fixture teardown does not double-close.
        self.conn = self.db.connect()

    # ------------------------------------------------------------------
    # Mixed persistent + temporary MATCH queries
    # ------------------------------------------------------------------

    def test_mixed_persistent_and_temporary_match(self):
        """MATCH can span persistent and temporary tables in the same
        query."""
        self._load_persistent_person_table()

        # Load a temporary edge table connecting persistent Person vertices.
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'Person', to = 'Person', "
            f"from_col = 'src_id', to_col = 'dst_id') AS TempLink;"
        )

        result = self.conn.execute(
            "MATCH (a:Person)-[r:TempLink]->(b:Person) "
            "RETURN a.name, b.name ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3
        # src_id=1 (Alice) -> dst_id=2 (Bob)
        assert rows[0][0] == "Alice"
        assert rows[0][1] == "Bob"

    # ------------------------------------------------------------------
    # LOAD AS specific corner cases
    # ------------------------------------------------------------------

    def test_duplicate_load_as_same_label(self):
        """LOAD AS the same label twice on the same connection must fail."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempDup;"
        )
        with pytest.raises(Exception):
            self.conn.execute(
                f'LOAD NODE TABLE FROM "{self.people_csv}" '
                f"(primary_key = 'id', header = true) AS TempDup;"
            )

    def test_reload_same_label_after_close(self):
        """After close() cleans up the temp label, reloading the same
        label on a fresh connection must succeed."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempReusable;"
        )
        result = self.conn.execute("MATCH (n:TempReusable) RETURN count(n);")
        assert list(result)[0][0] == 4

        # Close triggers cleanup.
        self.conn.close()

        # Reconnect and reload the same label — should succeed.
        conn2 = self.db.connect()
        try:
            conn2.execute(
                f'LOAD NODE TABLE FROM "{self.people_csv}" '
                f"(primary_key = 'id', header = true) AS TempReusable;"
            )
            result2 = conn2.execute("MATCH (n:TempReusable) RETURN count(n);")
            assert list(result2)[0][0] == 4
        finally:
            conn2.close()

        self.conn = self.db.connect()

    def test_temp_not_persisted_after_db_reopen(self):
        """Temp labels must not appear in the checkpoint file on disk.

        Close the DB (triggers checkpoint), reopen from the same directory,
        and verify the temp label is absent.
        """
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempGhost;"
        )
        result = self.conn.execute("MATCH (n:TempGhost) RETURN count(n);")
        assert list(result)[0][0] == 4

        # Tear down fixture resources so we can reopen the DB directory.
        self.conn.close()
        self.db.close()

        db2 = Database(db_path=self.db_dir, mode="w")
        conn2 = db2.connect()
        try:
            with pytest.raises(Exception):
                list(conn2.execute("MATCH (n:TempGhost) RETURN n.id;"))
        finally:
            conn2.close()
            db2.close()

        # Re-open so the fixture teardown can close cleanly.
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()

    def test_load_rel_table_nonexistent_vertex_label(self):
        """LOAD REL TABLE referencing a vertex label that does not exist
        must fail."""
        with pytest.raises(Exception):
            self.conn.execute(
                f'LOAD REL TABLE FROM "{self.edges_csv}" '
                f"(header = true, "
                f"from = 'Nope', to = 'Nope', "
                f"from_col = 'src_id', to_col = 'dst_id') AS TempBadEdge;"
            )

    def test_load_rel_table_temp_src_persistent_dst(self):
        """LOAD REL TABLE with ``from`` pointing to a temp vertex table
        and ``to`` pointing to a persistent vertex table must succeed."""
        self._load_persistent_person_table()

        # Load src vertices as a temp table.
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempSrc;"
        )

        # Edge from TempSrc (temp) → Person (persistent).
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'TempSrc', to = 'Person', "
            f"from_col = 'src_id', to_col = 'dst_id') AS TempMixedEdge;"
        )

        result = self.conn.execute(
            "MATCH (a:TempSrc)-[r:TempMixedEdge]->(b:Person) "
            "RETURN a.id, b.id ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3, f"Expected 3 edges, got {len(rows)}"

    # ------------------------------------------------------------------
    # Additional LOAD AS corner cases
    # ------------------------------------------------------------------

    def test_load_rel_table_dangling_reference(self):
        """Edge referencing a vertex key that does not exist is silently
        skipped (consistent with the persist-graph COPY FROM path)."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempPersonD;"
        )
        # dangling_edges.csv has src_id=99 which does not exist.
        # Storage silently skips the dangling edge; only (1→2) is inserted.
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.dangling_edges_csv}" '
            f"(header = true, "
            f"from = 'TempPersonD', to = 'TempPersonD', "
            f"from_col = 'src_id', to_col = 'dst_id') AS TempDanglingEdge;"
        )
        result = self.conn.execute(
            "MATCH (a:TempPersonD)-[r:TempDanglingEdge]->(b:TempPersonD) "
            "RETURN a.id, b.id;"
        )
        rows = list(result)
        assert len(rows) == 1, f"Expected 1 valid edge, got {len(rows)}"

    def test_multiple_temp_tables_all_cleaned(self):
        """Multiple temp tables created on the same connection must all
        disappear after close."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempA;"
        )
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.items_csv}" '
            f"(primary_key = 'item_id', header = true) AS TempB;"
        )
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempC;"
        )

        # Verify all three exist.
        for label in ("TempA", "TempB", "TempC"):
            result = self.conn.execute(f"MATCH (n:{label}) RETURN count(n);")
            assert list(result)[0][0] > 0

        # Close triggers cleanup.
        self.conn.close()
        conn2 = self.db.connect()
        try:
            for label in ("TempA", "TempB", "TempC"):
                with pytest.raises(Exception):
                    list(conn2.execute(f"MATCH (n:{label}) RETURN n.id;"))
        finally:
            conn2.close()
        self.conn = self.db.connect()

    def test_persistent_survives_temp_cleanup(self):
        """Persistent tables must survive Connection::Close() while
        temporary tables are cleaned up."""
        self.conn.execute(
            "CREATE NODE TABLE Persistent(id INT64, name STRING, " "PRIMARY KEY(id));"
        )
        self.conn.execute("CREATE (p:Persistent {id: 1, name: 'Alice'});")
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempGone;"
        )

        # Close and reopen.
        self.conn.close()
        conn2 = self.db.connect()
        try:
            # Persistent should still exist.
            result = conn2.execute("MATCH (n:Persistent) RETURN n.id, n.name;")
            rows = list(result)
            assert len(rows) == 1
            assert rows[0][1] == "Alice"

            # TempGone should not exist.
            with pytest.raises(Exception):
                list(conn2.execute("MATCH (n:TempGone) RETURN n.id;"))
        finally:
            conn2.close()
        self.conn = self.db.connect()

    def test_property_type_inference(self):
        """Verify that CSV column types are correctly inferred.

        ``age`` is INT64 in the CSV, so ``n.age + 1`` should work as
        integer arithmetic.
        """
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempTyped;"
        )
        result = self.conn.execute(
            "MATCH (n:TempTyped) RETURN n.age + 1 ORDER BY n.id;"
        )
        rows = list(result)
        assert len(rows) == 4
        # Alice age=30, 30+1=31.
        assert rows[0][0] == 31

    def test_return_column_ordering(self):
        """RETURN declares columns in a specific order; the schema
        should respect that order."""
        # RETURN name, id (reversed from source order).
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"RETURN name, id AS TempOrder;"
        )
        result = self.conn.execute(
            "MATCH (n:TempOrder) RETURN n.name, n.id ORDER BY n.id;"
        )
        rows = list(result)
        assert len(rows) == 4
        # First row: Alice, id=1.
        assert rows[0][0] == "Alice"
        assert rows[0][1] == 1

    def test_load_rel_table_with_where(self):
        """WHERE filter pushed down to edge LOAD REL TABLE."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempPersonW;"
        )
        # Only edges with weight > 0.6: 1.0 and 0.8 (skip 0.5).
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'TempPersonW', to = 'TempPersonW', "
            f"from_col = 'src_id', to_col = 'dst_id') "
            f"WHERE weight > 0.6 AS TempFilteredEdge;"
        )
        result = self.conn.execute(
            "MATCH (a:TempPersonW)-[r:TempFilteredEdge]->(b:TempPersonW) "
            "RETURN a.id, b.id, r.weight ORDER BY r.weight;"
        )
        rows = list(result)
        assert len(rows) == 2, f"Expected 2 edges, got {len(rows)}"

    def test_load_rel_table_with_return(self):
        """RETURN projection on edge LOAD REL TABLE."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempPersonR;"
        )
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'TempPersonR', to = 'TempPersonR', "
            f"from_col = 'src_id', to_col = 'dst_id') "
            f"RETURN src_id, dst_id, weight AS TempSlimEdge;"
        )
        result = self.conn.execute(
            "MATCH (a:TempPersonR)-[r:TempSlimEdge]->(b:TempPersonR) "
            "RETURN a.id, b.id, r.weight ORDER BY a.id;"
        )
        rows = list(result)
        assert len(rows) == 3

    def test_load_rel_table_where_and_return(self):
        """WHERE + RETURN combined on edge LOAD REL TABLE."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempPersonWR;"
        )
        # Filter weight >= 0.8, project src_id, dst_id, weight.
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'TempPersonWR', to = 'TempPersonWR', "
            f"from_col = 'src_id', to_col = 'dst_id') "
            f"WHERE weight >= 0.8 "
            f"RETURN src_id, dst_id, weight AS TempWREdge;"
        )
        result = self.conn.execute(
            "MATCH (a:TempPersonWR)-[r:TempWREdge]->(b:TempPersonWR) "
            "RETURN a.id, b.id, r.weight ORDER BY r.weight;"
        )
        rows = list(result)
        # Only edges with weight >= 0.8: (2→3, 1.0) and (3→4, 0.8).
        assert len(rows) == 2, f"Expected 2 edges, got {len(rows)}"

    # ------------------------------------------------------------------
    # Additional corner cases
    # ------------------------------------------------------------------

    def test_empty_csv_file(self):
        """LOAD NODE TABLE from a CSV with header only (no data rows)
        should succeed and create an empty table."""
        empty_csv = _write_csv(self.csv_dir, "empty.csv", "id|name|age\n")
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{empty_csv}" '
            f"(primary_key = 'id', header = true) AS TempEmpty;"
        )
        result = self.conn.execute("MATCH (n:TempEmpty) RETURN count(n);")
        assert list(result)[0][0] == 0

    def test_where_filters_out_all_rows(self):
        """WHERE that matches no rows should create an empty temp table."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"WHERE age > 1000 AS TempNone;"
        )
        result = self.conn.execute("MATCH (n:TempNone) RETURN count(n);")
        assert list(result)[0][0] == 0

    def test_where_references_only_return_columns(self):
        """WHERE and RETURN share the same columns — no WHERE-only
        columns, so no extra projection logic is needed."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"WHERE age >= 30 RETURN id, age AS TempOverlap;"
        )
        result = self.conn.execute(
            "MATCH (n:TempOverlap) RETURN n.id, n.age ORDER BY n.id;"
        )
        rows = list(result)
        # Alice(1,30) and Carol(3,35) pass age >= 30
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 3

    def test_multiple_where_only_columns(self):
        """Multiple WHERE-only columns (not in RETURN) should all be
        filtered correctly without becoming properties."""
        score_csv = _write_csv(
            self.csv_dir,
            "people_score.csv",
            "id|name|age|score\n"
            "1|Alice|30|85\n"
            "2|Bob|25|90\n"
            "3|Carol|35|75\n"
            "4|Dave|20|95\n",
        )
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{score_csv}" '
            f"(primary_key = 'id', header = true) "
            f"WHERE age > 25 AND score > 80 "
            f"RETURN id, name AS TempMultiWhere;"
        )
        result = self.conn.execute(
            "MATCH (n:TempMultiWhere) RETURN n.id, n.name ORDER BY n.id;"
        )
        rows = list(result)
        # Only Alice(1, age=30, score=85) passes both conditions
        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == "Alice"

        # age and score must not be properties
        with pytest.raises(Exception):
            list(self.conn.execute("MATCH (n:TempMultiWhere) RETURN n.age;"))
        with pytest.raises(Exception):
            list(self.conn.execute("MATCH (n:TempMultiWhere) RETURN n.score;"))

    def test_where_with_arithmetic_expression(self):
        """WHERE with arithmetic expression (age * 2 > 60)."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"WHERE age * 2 > 60 AS TempArith;"
        )
        result = self.conn.execute("MATCH (n:TempArith) RETURN n.id ORDER BY n.id;")
        rows = list(result)
        # Only Carol(3, age=35) passes age * 2 > 60
        assert len(rows) == 1
        assert rows[0][0] == 3

    def test_return_only_primary_key(self):
        """RETURN with only the primary key column should succeed."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"RETURN id AS TempIdOnly;"
        )
        result = self.conn.execute("MATCH (n:TempIdOnly) RETURN n.id ORDER BY n.id;")
        rows = list(result)
        assert len(rows) == 4

        # name and age must not be properties
        with pytest.raises(Exception):
            list(self.conn.execute("MATCH (n:TempIdOnly) RETURN n.name;"))

    def test_temp_and_persistent_join(self):
        """MATCH query joining temp and persistent tables."""
        self._load_persistent_person_table()

        extra_csv = _write_csv(
            self.csv_dir,
            "extra_people.csv",
            "id|nickname\n" "1|Ally\n" "2|Bobby\n" "5|Eve\n",
        )
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{extra_csv}" '
            f"(primary_key = 'id', header = true) AS TempExtra;"
        )

        result = self.conn.execute(
            "MATCH (p:Person), (t:TempExtra) "
            "WHERE p.id = t.id "
            "RETURN p.name, t.nickname ORDER BY p.id;"
        )
        rows = list(result)
        # IDs 1 and 2 exist in both tables
        assert len(rows) == 2
        assert rows[0][0] == "Alice"
        assert rows[0][1] == "Ally"
        assert rows[1][0] == "Bob"
        assert rows[1][1] == "Bobby"

    def test_multiple_queries_on_same_temp_table(self):
        """Multiple MATCH queries on the same temp table should all
        work correctly."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempStable;"
        )

        # Query 1: count
        result = self.conn.execute("MATCH (n:TempStable) RETURN count(n);")
        assert list(result)[0][0] == 4

        # Query 2: filter
        result = self.conn.execute(
            "MATCH (n:TempStable) WHERE n.age > 25 RETURN n.id ORDER BY n.id;"
        )
        rows = list(result)
        assert len(rows) == 2

        # Query 3: aggregation
        result = self.conn.execute("MATCH (n:TempStable) RETURN sum(n.age);")
        assert list(result)[0][0] == 110  # 30+25+35+20

        # Query 4: order + limit
        result = self.conn.execute(
            "MATCH (n:TempStable) RETURN n.name ORDER BY n.name LIMIT 2;"
        )
        rows = list(result)
        assert len(rows) == 2
        assert rows[0][0] == "Alice"
        assert rows[1][0] == "Bob"

    def test_aggregation_on_temp_table(self):
        """SUM, AVG, MIN, MAX on temp table properties."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempAgg;"
        )
        result = self.conn.execute(
            "MATCH (n:TempAgg) " "RETURN sum(n.age), min(n.age), max(n.age);"
        )
        rows = list(result)
        assert len(rows) == 1
        # ages: 30+25+35+20 = 110, min = 20, max = 35
        assert rows[0][0] == 110
        assert rows[0][1] == 20
        assert rows[0][2] == 35

    def test_where_with_string_comparison(self):
        """WHERE with string equality comparison."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"WHERE name = 'Alice' AS TempAlice;"
        )
        result = self.conn.execute("MATCH (n:TempAlice) RETURN n.id, n.name;")
        rows = list(result)
        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == "Alice"

    def test_load_rel_table_with_where_only_columns(self):
        """LOAD REL TABLE with WHERE-only columns (category not in
        RETURN) should filter correctly without category becoming a
        property."""
        edges_extra_csv = _write_csv(
            self.csv_dir,
            "edges_extra.csv",
            "src_id|dst_id|weight|category\n" "1|2|0.5|A\n" "2|3|1.0|B\n" "3|4|0.8|A\n",
        )
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempPersonEWC;"
        )
        self.conn.execute(
            f'LOAD REL TABLE FROM "{edges_extra_csv}" '
            f"(header = true, "
            f"from = 'TempPersonEWC', to = 'TempPersonEWC', "
            f"from_col = 'src_id', to_col = 'dst_id') "
            f"WHERE category = 'B' "
            f"RETURN src_id, dst_id, weight AS TempWOC;"
        )
        result = self.conn.execute(
            "MATCH (a:TempPersonEWC)-[r:TempWOC]->(b:TempPersonEWC) "
            "RETURN a.id, b.id, r.weight;"
        )
        rows = list(result)
        # Only edge (2→3, 1.0) has category='B'
        assert len(rows) == 1
        assert rows[0][0] == 2
        assert rows[0][1] == 3

        # category must not be a property
        with pytest.raises(Exception):
            list(
                self.conn.execute(
                    "MATCH (a:TempPersonEWC)-[r:TempWOC]->(b:TempPersonEWC) "
                    "RETURN r.category;"
                )
            )

    def test_where_with_or_expression(self):
        """WHERE with OR expression."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"WHERE age < 22 OR age > 32 AS TempOr;"
        )
        result = self.conn.execute("MATCH (n:TempOr) RETURN n.id ORDER BY n.id;")
        rows = list(result)
        # Dave(4, age=20) and Carol(3, age=35)
        assert len(rows) == 2
        assert rows[0][0] == 3
        assert rows[1][0] == 4

    def test_where_with_range_condition(self):
        """WHERE with AND of two comparisons (range condition)."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) "
            f"WHERE age >= 25 AND age <= 30 AS TempRange;"
        )
        result = self.conn.execute("MATCH (n:TempRange) RETURN n.id ORDER BY n.id;")
        rows = list(result)
        # Bob(2, age=25) and Alice(1, age=30)
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2

    # ------------------------------------------------------------------
    # DROP temporary table tests
    # ------------------------------------------------------------------

    def test_drop_temporary_node_table(self):
        """DROP TABLE removes a temporary node table; MATCH on it
        must fail afterwards, and Close() must still succeed."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempToDrop;"
        )
        # Exists before DROP.
        result = self.conn.execute("MATCH (n:TempToDrop) RETURN count(n);")
        assert list(result)[0][0] == 4

        self.conn.execute("DROP TABLE TempToDrop;")

        with pytest.raises(Exception):
            self.conn.execute("MATCH (n:TempToDrop) RETURN n.id;")

    def test_drop_temporary_edge_table(self):
        """DROP TABLE removes a temporary edge table; the node table
        must remain accessible afterwards."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempNodeKeep;"
        )
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'TempNodeKeep', to = 'TempNodeKeep', "
            f"from_col = 'src_id', to_col = 'dst_id') AS TempEdgeToDrop;"
        )
        # Edge query works before DROP.
        result = self.conn.execute(
            "MATCH (a:TempNodeKeep)-[r:TempEdgeToDrop]->(b:TempNodeKeep) "
            "RETURN count(r);"
        )
        assert list(result)[0][0] == 3

        self.conn.execute("DROP TABLE TempEdgeToDrop;")

        # Edge query must now fail.
        with pytest.raises(Exception):
            self.conn.execute(
                "MATCH (a:TempNodeKeep)-[r:TempEdgeToDrop]->(b:TempNodeKeep) "
                "RETURN a.id;"
            )

        # Node table must still be accessible.
        result = self.conn.execute("MATCH (n:TempNodeKeep) RETURN count(n);")
        assert list(result)[0][0] == 4

    def test_drop_temporary_node_then_recreate(self):
        """After DROPping a temporary node table, re-LOADing the same
        label name must succeed."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempRecycle;"
        )
        self.conn.execute("DROP TABLE TempRecycle;")

        # Re-LOAD same label name.
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempRecycle;"
        )
        result = self.conn.execute("MATCH (n:TempRecycle) RETURN count(n);")
        assert list(result)[0][0] == 4

    def test_drop_temporary_edge_then_recreate(self):
        """After DROPping a temporary edge table, re-LOADing the same
        edge label name must succeed."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempReEdgeNode;"
        )
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'TempReEdgeNode', to = 'TempReEdgeNode', "
            f"from_col = 'src_id', to_col = 'dst_id') AS TempEdgeRecycle;"
        )
        self.conn.execute("DROP TABLE TempEdgeRecycle;")

        # Re-LOAD same edge label name.
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.edges_csv}" '
            f"(header = true, "
            f"from = 'TempReEdgeNode', to = 'TempReEdgeNode', "
            f"from_col = 'src_id', to_col = 'dst_id') AS TempEdgeRecycle;"
        )
        result = self.conn.execute(
            "MATCH (a:TempReEdgeNode)-[r:TempEdgeRecycle]->(b:TempReEdgeNode) "
            "RETURN count(r);"
        )
        assert list(result)[0][0] == 3


# ---------------------------------------------------------------------------
# LOAD AS from JSONL / Parquet (using tinysnb dataset)
# ---------------------------------------------------------------------------


def _get_tinysnb_path():
    """Resolve the tinysnb dataset path from the workspace root."""
    current_file = os.path.abspath(__file__)
    tests_dir = os.path.dirname(current_file)
    python_bind_dir = os.path.dirname(tests_dir)
    tools_dir = os.path.dirname(python_bind_dir)
    workspace_root = os.path.dirname(tools_dir)
    path = os.path.join(workspace_root, "example_dataset", "tinysnb")
    return path if os.path.exists(path) else None


class TestLoadAsJsonl:
    """LOAD NODE TABLE AS from JSONL files."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        tinysnb = _get_tinysnb_path()
        if not tinysnb:
            pytest.skip("tinysnb dataset not found")
        self.jsonl_path = os.path.join(tinysnb, "json", "vPerson.jsonl")
        if not os.path.exists(self.jsonl_path):
            pytest.skip(f"JSONL file not found: {self.jsonl_path}")

        self.db_dir = str(tmp_path / "test_load_as_jsonl_db")
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_load_node_table_from_jsonl(self):
        """Basic LOAD NODE TABLE from JSONL."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.jsonl_path}" '
            f"(primary_key = 'ID') AS TempJsonPerson;"
        )
        result = self.conn.execute(
            "MATCH (n:TempJsonPerson) RETURN n.fName ORDER BY n.fName;"
        )
        rows = list(result)
        assert len(rows) == 8
        assert rows[0][0] == "Alice"

    def test_load_node_table_from_jsonl_with_where(self):
        """LOAD NODE TABLE from JSONL with WHERE filter."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.jsonl_path}" '
            f"(primary_key = 'ID') WHERE age >= 35 AS TempJsonOld;"
        )
        result = self.conn.execute(
            "MATCH (n:TempJsonOld) RETURN n.fName, n.age ORDER BY n.age;"
        )
        rows = list(result)
        assert len(rows) >= 3
        for row in rows:
            assert row[1] >= 35

    def test_load_node_table_from_jsonl_with_return(self):
        """LOAD NODE TABLE from JSONL with RETURN projection."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.jsonl_path}" '
            f"(primary_key = 'ID') RETURN ID, fName, age AS TempJsonSlim;"
        )
        result = self.conn.execute(
            "MATCH (n:TempJsonSlim) RETURN n.ID, n.fName, n.age " "ORDER BY n.ID;"
        )
        rows = list(result)
        assert len(rows) == 8
        assert rows[0][1] == "Alice"
        with pytest.raises(RuntimeError):
            self.conn.execute("MATCH (n:TempJsonSlim) RETURN n.eyeSight;")

    def test_load_node_table_from_jsonl_where_and_return(self):
        """WHERE + RETURN combined on JSONL: WHERE col not in RETURN."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.jsonl_path}" '
            f"(primary_key = 'ID') "
            f"WHERE age >= 35 "
            f"RETURN ID, fName, gender AS TempJsonWR;"
        )
        result = self.conn.execute(
            "MATCH (n:TempJsonWR) RETURN n.fName, n.gender ORDER BY n.fName;"
        )
        rows = list(result)
        assert len(rows) >= 3
        with pytest.raises(RuntimeError):
            self.conn.execute("MATCH (n:TempJsonWR) RETURN n.age;")


class TestLoadAsJson:
    """LOAD NODE TABLE AS from JSON (array-of-objects) files."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        tinysnb = _get_tinysnb_path()
        if not tinysnb:
            pytest.skip("tinysnb dataset not found")
        self.json_path = os.path.join(tinysnb, "json", "vPerson.json")
        if not os.path.exists(self.json_path):
            pytest.skip(f"JSON file not found: {self.json_path}")

        self.db_dir = str(tmp_path / "test_load_as_json_db")
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_load_node_table_from_json(self):
        """Basic LOAD NODE TABLE from JSON."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.json_path}" '
            f"(primary_key = 'ID') AS TempJsonArrPerson;"
        )
        result = self.conn.execute(
            "MATCH (n:TempJsonArrPerson) RETURN n.fName ORDER BY n.fName;"
        )
        rows = list(result)
        assert len(rows) == 8
        assert rows[0][0] == "Alice"

    def test_load_node_table_from_json_with_where(self):
        """LOAD NODE TABLE from JSON with WHERE filter."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.json_path}" '
            f"(primary_key = 'ID') WHERE age >= 35 AS TempJsonArrOld;"
        )
        result = self.conn.execute(
            "MATCH (n:TempJsonArrOld) RETURN n.fName, n.age ORDER BY n.age;"
        )
        rows = list(result)
        assert len(rows) >= 3
        for row in rows:
            assert row[1] >= 35

    def test_load_node_table_from_json_with_return(self):
        """LOAD NODE TABLE from JSON with RETURN projection."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.json_path}" '
            f"(primary_key = 'ID') RETURN ID, fName, age AS TempJsonArrSlim;"
        )
        result = self.conn.execute(
            "MATCH (n:TempJsonArrSlim) RETURN n.ID, n.fName, n.age " "ORDER BY n.ID;"
        )
        rows = list(result)
        assert len(rows) == 8
        assert rows[0][1] == "Alice"
        with pytest.raises(RuntimeError):
            self.conn.execute("MATCH (n:TempJsonArrSlim) RETURN n.eyeSight;")

    def test_load_node_table_from_json_where_and_return(self):
        """WHERE + RETURN combined on JSON: WHERE col not in RETURN."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.json_path}" '
            f"(primary_key = 'ID') "
            f"WHERE age >= 35 "
            f"RETURN ID, fName, gender AS TempJsonArrWR;"
        )
        result = self.conn.execute(
            "MATCH (n:TempJsonArrWR) RETURN n.fName, n.gender ORDER BY n.fName;"
        )
        rows = list(result)
        assert len(rows) >= 3
        with pytest.raises(RuntimeError):
            self.conn.execute("MATCH (n:TempJsonArrWR) RETURN n.age;")


@extension_test
class TestLoadAsParquet:
    """LOAD NODE TABLE AS from Parquet files (requires parquet extension)."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        tinysnb = _get_tinysnb_path()
        if not tinysnb:
            pytest.skip("tinysnb dataset not found")
        self.parquet_path = os.path.join(tinysnb, "parquet", "vPerson.parquet")
        if not os.path.exists(self.parquet_path):
            pytest.skip(f"Parquet file not found: {self.parquet_path}")

        self.db_dir = str(tmp_path / "test_load_as_parquet_db")
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        self.conn.execute("load parquet")

        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_load_node_table_from_parquet(self):
        """Basic LOAD NODE TABLE from Parquet."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.parquet_path}" '
            f"(primary_key = 'ID') AS TempPqPerson;"
        )
        result = self.conn.execute(
            "MATCH (n:TempPqPerson) RETURN n.fName ORDER BY n.fName;"
        )
        rows = list(result)
        assert len(rows) == 8
        assert rows[0][0] == "Alice"

    def test_load_node_table_from_parquet_with_where(self):
        """LOAD NODE TABLE from Parquet with WHERE filter."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.parquet_path}" '
            f"(primary_key = 'ID') WHERE age >= 40 AS TempPqOld;"
        )
        result = self.conn.execute(
            "MATCH (n:TempPqOld) RETURN n.fName, n.age ORDER BY n.age;"
        )
        rows = list(result)
        assert len(rows) >= 2
        for row in rows:
            assert row[1] >= 40

    def test_load_node_table_from_parquet_with_return(self):
        """LOAD NODE TABLE from Parquet with RETURN projection."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.parquet_path}" '
            f"(primary_key = 'ID') RETURN ID, fName, height AS TempPqSlim;"
        )
        result = self.conn.execute(
            "MATCH (n:TempPqSlim) RETURN n.ID, n.fName, n.height " "ORDER BY n.ID;"
        )
        rows = list(result)
        assert len(rows) == 8
        with pytest.raises(RuntimeError):
            self.conn.execute("MATCH (n:TempPqSlim) RETURN n.age;")

    def test_load_node_table_from_parquet_where_and_return(self):
        """WHERE + RETURN combined on Parquet: WHERE col not in RETURN."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.parquet_path}" '
            f"(primary_key = 'ID') "
            f"WHERE age >= 40 "
            f"RETURN ID, fName, gender AS TempPqWR;"
        )
        result = self.conn.execute(
            "MATCH (n:TempPqWR) RETURN n.fName, n.gender ORDER BY n.fName;"
        )
        rows = list(result)
        assert len(rows) >= 2
        with pytest.raises(RuntimeError):
            self.conn.execute("MATCH (n:TempPqWR) RETURN n.age;")

    def test_load_rel_table_from_parquet(self):
        """LOAD REL TABLE from Parquet: keys at positions [0] and [1]."""
        # eMeets.parquet columns: from(0) | to(1) | location | times | data
        # Keys are at positions [0] and [1] by convention.
        meets_path = os.path.join(
            os.path.dirname(self.parquet_path), "eMeets.parquet"
        )
        if not os.path.exists(meets_path):
            pytest.skip(f"eMeets.parquet not found: {meets_path}")

        # Load vertices first.
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.parquet_path}" '
            f"(primary_key = 'ID') AS PqPerson;"
        )
        # Load edges — no from_col/to_col needed since keys are at [0/1].
        self.conn.execute(
            f'LOAD REL TABLE FROM "{meets_path}" '
            f"(from = 'PqPerson', to = 'PqPerson') AS PqMeets;"
        )
        result = self.conn.execute(
            "MATCH (a:PqPerson)-[r:PqMeets]->(b:PqPerson) "
            "RETURN a.ID, b.ID ORDER BY a.ID, b.ID;"
        )
        rows = list(result)
        assert len(rows) >= 5  # eMeets has 7 edges


@extension_test
class TestLoadAsRemoteHttpfs:
    """LOAD NODE/REL TABLE AS from remote Parquet files via httpfs.

    Uses public OSS-hosted tinysnb dataset:
      - vPerson.parquet (8 rows, 16 columns)
      - eMeets.parquet  (7 rows: from, to, location, times, data)
    """

    VERTEX_URL = "http://graphscope.oss-cn-beijing.aliyuncs.com/neug/vPerson.parquet"
    EDGE_URL = "http://graphscope.oss-cn-beijing.aliyuncs.com/neug/eMeets.parquet"

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.db_dir = str(tmp_path / "test_load_as_remote_db")
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        self.conn.execute("load httpfs")
        self.conn.execute("load parquet")
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_load_node_table_from_httpfs_parquet(self):
        """Basic LOAD NODE TABLE from remote Parquet via HTTP."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.VERTEX_URL}" '
            f"(primary_key = 'ID') AS TempRemotePerson;"
        )
        result = self.conn.execute(
            "MATCH (n:TempRemotePerson) RETURN n.fName ORDER BY n.fName;"
        )
        rows = list(result)
        assert len(rows) == 8
        assert rows[0][0] == "Alice"

    def test_load_node_table_from_httpfs_with_where(self):
        """LOAD NODE TABLE from remote with WHERE filter."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.VERTEX_URL}" '
            f"(primary_key = 'ID') WHERE age >= 40 AS TempRemoteOld;"
        )
        result = self.conn.execute(
            "MATCH (n:TempRemoteOld) RETURN n.fName, n.age ORDER BY n.age;"
        )
        rows = list(result)
        assert len(rows) >= 2
        for row in rows:
            assert row[1] >= 40

    def test_load_node_table_from_httpfs_with_return(self):
        """LOAD NODE TABLE from remote with RETURN projection."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.VERTEX_URL}" '
            f"(primary_key = 'ID') RETURN ID, fName, height AS TempRemoteSlim;"
        )
        result = self.conn.execute(
            "MATCH (n:TempRemoteSlim) RETURN n.fName, n.height ORDER BY n.fName;"
        )
        rows = list(result)
        assert len(rows) == 8
        # age should NOT be a property.
        with pytest.raises(RuntimeError):
            self.conn.execute("MATCH (n:TempRemoteSlim) RETURN n.age;")

    def test_load_node_table_from_httpfs_where_and_return(self):
        """WHERE + RETURN combined on remote: WHERE col not in RETURN."""
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.VERTEX_URL}" '
            f"(primary_key = 'ID') "
            f"WHERE age >= 40 "
            f"RETURN ID, fName, gender AS TempRemoteWR;"
        )
        result = self.conn.execute(
            "MATCH (n:TempRemoteWR) RETURN n.fName, n.gender ORDER BY n.fName;"
        )
        rows = list(result)
        assert len(rows) >= 2
        with pytest.raises(RuntimeError):
            self.conn.execute("MATCH (n:TempRemoteWR) RETURN n.age;")

    def test_load_rel_table_from_httpfs_parquet(self):
        """LOAD REL TABLE from remote Parquet (eMeets)."""
        # First load vertex table.
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.VERTEX_URL}" '
            f"(primary_key = 'ID') AS TempRemoteSrc;"
        )
        # Load edge table: eMeets has columns 'from', 'to', ...
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.EDGE_URL}" '
            f"(from = 'TempRemoteSrc', to = 'TempRemoteSrc', "
            f"from_col = 'from', to_col = 'to') AS TempRemoteMeets;"
        )
        result = self.conn.execute(
            "MATCH (a:TempRemoteSrc)-[r:TempRemoteMeets]->(b:TempRemoteSrc) "
            "RETURN a.fName, b.fName, r.location ORDER BY a.ID;"
        )
        rows = list(result)
        assert len(rows) >= 1

    def test_mixed_persistent_and_httpfs_temp_query(self):
        """Persistent graph + remote temp graph joint query.

        Creates a persistent Person table with local data, then loads a
        temporary edge table from remote Parquet (eMeets), and performs a
        cross-table MATCH spanning both.
        """
        # Create persistent vertex table with the same IDs as vPerson.parquet.
        self.conn.execute(
            "CREATE NODE TABLE Person(ID INT64, name STRING, PRIMARY KEY(ID));"
        )
        for vid, name in [
            (0, "Alice"),
            (2, "Bob"),
            (3, "Carol"),
            (5, "Dan"),
            (7, "Elizabeth"),
            (8, "Farooq"),
            (9, "Greg"),
            (10, "Hubert"),
        ]:
            self.conn.execute(f"CREATE (p:Person {{ID: {vid}, name: '{name}'}});")

        # Load remote edge table referencing persistent Person vertices.
        self.conn.execute(
            f'LOAD REL TABLE FROM "{self.EDGE_URL}" '
            f"(from = 'Person', to = 'Person', "
            f"from_col = 'from', to_col = 'to') AS TempRemoteEdge;"
        )

        # Joint query: persistent vertices + temporary remote edges.
        result = self.conn.execute(
            "MATCH (a:Person)-[r:TempRemoteEdge]->(b:Person) "
            "RETURN a.name, b.name, r.location ORDER BY a.ID;"
        )
        rows = list(result)
        # eMeets.parquet has 7 edges among the 8 person IDs.
        assert len(rows) >= 1
        # Verify we get actual names from the persistent table.
        assert all(isinstance(row[0], str) for row in rows)
        assert all(isinstance(row[1], str) for row in rows)


# ---------------------------------------------------------------------------
# LOAD AS rejected in read-only mode
# ---------------------------------------------------------------------------


class TestLoadAsReadOnlyRejection:
    """Verify that LOAD AS is rejected when the database is in read-only mode."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.db_dir = str(tmp_path / "test_load_as_readonly_db")
        self.csv_dir = str(tmp_path / "csv_ro")
        os.makedirs(self.csv_dir, exist_ok=True)

        self.people_csv = _write_csv(
            self.csv_dir,
            "people.csv",
            "id|name|age\n" "1|Alice|30\n" "2|Bob|25\n" "3|Carol|35\n",
        )
        self.edges_csv = _write_csv(
            self.csv_dir,
            "edges.csv",
            "src_id|dst_id|weight\n" "1|2|0.5\n" "2|3|1.0\n",
        )

        # Phase 1: create a persistent schema in read-write mode so the DB
        # directory is valid and LOAD REL TABLE can resolve vertex references.
        db_rw = Database(db_path=self.db_dir, mode="w")
        conn_rw = db_rw.connect()
        conn_rw.execute(
            "CREATE NODE TABLE Person(id INT64, name STRING, age INT64, "
            "PRIMARY KEY(id));"
        )
        for vid, name, age in [(1, "Alice", 30), (2, "Bob", 25), (3, "Carol", 35)]:
            conn_rw.execute(
                f"CREATE (p:Person {{id: {vid}, name: '{name}', age: {age}}});"
            )
        conn_rw.close()
        db_rw.close()

        # Phase 2: reopen in read-only mode for testing.
        self.db = Database(db_path=self.db_dir, mode="r")
        self.conn = self.db.connect()

        yield

        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_load_node_table_rejected_in_read_only(self):
        """LOAD NODE TABLE must fail in read-only mode."""
        with pytest.raises(Exception, match="read-only mode"):
            self.conn.execute(
                f'LOAD NODE TABLE FROM "{self.people_csv}" '
                f"(primary_key = 'id', header = true) AS TempFail;"
            )

    def test_load_rel_table_rejected_in_read_only(self):
        """LOAD REL TABLE must fail in read-only mode."""
        with pytest.raises(Exception, match="read-only mode"):
            self.conn.execute(
                f'LOAD REL TABLE FROM "{self.edges_csv}" '
                f"(header = true, from = 'Person', to = 'Person', "
                f"from_col = 'src_id', to_col = 'dst_id') AS TempEdgeFail;"
            )
