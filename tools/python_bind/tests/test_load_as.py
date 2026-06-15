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
            "id|name|age\n"
            "1|Alice|30\n"
            "2|Bob|25\n"
            "3|Carol|35\n"
            "4|Dave|20\n",
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
            "src_id|dst_id|weight\n"
            "1|2|0.5\n"
            "2|3|1.0\n"
            "3|4|0.8\n",
        )

        self.dangling_edges_csv = _write_csv(
            self.csv_dir,
            "dangling_edges.csv",
            "src_id|dst_id|weight\n"
            "1|2|0.5\n"
            "99|3|1.0\n",
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
        for vid, name, age in [(1, "Alice", 30), (2, "Bob", 25),
                               (3, "Carol", 35), (4, "Dave", 20)]:
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

        result = self.conn.execute(
            "MATCH (n:TempDefault) RETURN n.id ORDER BY n.id;"
        )
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
            "MATCH (n:TempFiltered) RETURN n.id, n.name, n.age "
            "ORDER BY n.id;"
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
            list(self.conn.execute(
                "MATCH (n:TempSlim) RETURN n.age;"
            ))

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
            list(self.conn.execute(
                "MATCH (n:TempWR) RETURN n.age;"
            ))

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
        self.conn.execute(
            "CREATE NODE TABLE Conflict(id INT64, PRIMARY KEY(id));"
        )
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
        result = self.conn.execute(
            "MATCH (n:TempEphemeral) RETURN count(n);"
        )
        rows = list(result)
        assert rows[0][0] == 4

        # Closing the connection triggers temp-label cleanup.
        self.conn.close()

        # A new connection on the same Database must not see the label.
        conn2 = self.db.connect()
        try:
            with pytest.raises(Exception):
                list(conn2.execute(
                    "MATCH (n:TempEphemeral) RETURN n.id;"
                ))
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
        result = self.conn.execute(
            "MATCH (n:TempReusable) RETURN count(n);"
        )
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
            result2 = conn2.execute(
                "MATCH (n:TempReusable) RETURN count(n);"
            )
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
        result = self.conn.execute(
            "MATCH (n:TempGhost) RETURN count(n);"
        )
        assert list(result)[0][0] == 4

        # Tear down fixture resources so we can reopen the DB directory.
        self.conn.close()
        self.db.close()

        db2 = Database(db_path=self.db_dir, mode="w")
        conn2 = db2.connect()
        try:
            with pytest.raises(Exception):
                list(conn2.execute(
                    "MATCH (n:TempGhost) RETURN n.id;"
                ))
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
            result = self.conn.execute(
                f"MATCH (n:{label}) RETURN count(n);"
            )
            assert list(result)[0][0] > 0

        # Close triggers cleanup.
        self.conn.close()
        conn2 = self.db.connect()
        try:
            for label in ("TempA", "TempB", "TempC"):
                with pytest.raises(Exception):
                    list(conn2.execute(
                        f"MATCH (n:{label}) RETURN n.id;"
                    ))
        finally:
            conn2.close()
        self.conn = self.db.connect()

    def test_persistent_survives_temp_cleanup(self):
        """Persistent tables must survive Connection::Close() while
        temporary tables are cleaned up."""
        self.conn.execute(
            "CREATE NODE TABLE Persistent(id INT64, name STRING, "
            "PRIMARY KEY(id));"
        )
        self.conn.execute(
            "CREATE (p:Persistent {id: 1, name: 'Alice'});"
        )
        self.conn.execute(
            f'LOAD NODE TABLE FROM "{self.people_csv}" '
            f"(primary_key = 'id', header = true) AS TempGone;"
        )

        # Close and reopen.
        self.conn.close()
        conn2 = self.db.connect()
        try:
            # Persistent should still exist.
            result = conn2.execute(
                "MATCH (n:Persistent) RETURN n.id, n.name;"
            )
            rows = list(result)
            assert len(rows) == 1
            assert rows[0][1] == "Alice"

            # TempGone should not exist.
            with pytest.raises(Exception):
                list(conn2.execute(
                    "MATCH (n:TempGone) RETURN n.id;"
                ))
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
