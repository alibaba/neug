#
# Copyright 2020 Alibaba Group Holding Limited.
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

"""End-to-end tests for COPY TEMP (temporary graph)."""

import os
import shutil
import time

import pytest

from neug import Database


def _write_csv(directory, filename, content):
    path = os.path.join(directory, filename)
    with open(path, "w") as f:
        f.write(content)
    return path


class TestCopyTemp:
    """End-to-end tests for COPY TEMP (temporary graph)."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.db_dir = str(tmp_path / "test_copy_temp_db")
        self.csv_dir = str(tmp_path / "csv")
        shutil.rmtree(self.db_dir, ignore_errors=True)
        os.makedirs(self.csv_dir, exist_ok=True)
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()

        self.people_csv = _write_csv(
            self.csv_dir,
            "people.csv",
            "id|name|age\n1|Alice|30\n2|Bob|25\n3|Carol|35\n4|Dave|20\n",
        )
        self.edges_csv = _write_csv(
            self.csv_dir,
            "edges.csv",
            "src_id|dst_id|weight\n1|2|0.5\n2|3|1.0\n3|4|0.8\n",
        )
        self.edges_shuffled_csv = _write_csv(
            self.csv_dir,
            "edges_shuffled.csv",
            "weight|src_id|dst_id\n0.5|1|2\n1.0|2|3\n0.8|3|4\n",
        )

        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def _load_persistent_person_table(self):
        self.conn.execute(
            "CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
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
    # Basic NODE tests
    # ------------------------------------------------------------------

    def test_copy_temp_node_basic(self):
        self.conn.execute(
            f'COPY TEMP TempPeople FROM "{self.people_csv}" (header = true)'
        )
        rows = list(
            self.conn.execute("MATCH (n:TempPeople) RETURN n.id, n.name ORDER BY n.id;")
        )
        assert len(rows) == 4 and rows[0][1] == "Alice"

    def test_copy_temp_node_default_pk(self):
        self.conn.execute(
            f'COPY TEMP TempDefault FROM "{self.people_csv}" (header = true)'
        )
        assert len(list(self.conn.execute("MATCH (n:TempDefault) RETURN n.id;"))) == 4

    def test_copy_temp_node_where(self):
        self.conn.execute(
            f'COPY TEMP TempFiltered FROM (LOAD FROM "{self.people_csv}" '
            f"(header = true) WHERE age > 25 RETURN *)"
        )
        rows = list(
            self.conn.execute("MATCH (n:TempFiltered) RETURN n.name ORDER BY n.id;")
        )
        assert len(rows) == 2 and rows[0][0] == "Alice"

    def test_copy_temp_node_return(self):
        self.conn.execute(
            f'COPY TEMP TempSlim FROM (LOAD FROM "{self.people_csv}" '
            f"(header = true) RETURN id, name)"
        )
        assert len(list(self.conn.execute("MATCH (n:TempSlim) RETURN n.id;"))) == 4
        with pytest.raises(Exception):
            list(self.conn.execute("MATCH (n:TempSlim) RETURN n.age;"))

    def test_copy_temp_node_where_return(self):
        self.conn.execute(
            f'COPY TEMP TempWR FROM (LOAD FROM "{self.people_csv}" '
            f"(header = true) WHERE age >= 25 RETURN id, name)"
        )
        rows = list(self.conn.execute("MATCH (n:TempWR) RETURN n.name ORDER BY n.id;"))
        assert len(rows) == 3

    # ------------------------------------------------------------------
    # Basic REL tests
    # ------------------------------------------------------------------

    def test_copy_temp_rel_basic(self):
        """edges.csv: src_id|dst_id|weight - first two cols are keys."""
        self.conn.execute(
            f'COPY TEMP TempPerson FROM "{self.people_csv}" (header = true)'
        )
        self.conn.execute(
            f'COPY TEMP TempKnows FROM "{self.edges_csv}" '
            f"(header = true, from = 'TempPerson', to = 'TempPerson')"
        )
        rows = list(
            self.conn.execute(
                "MATCH (a:TempPerson)-[r:TempKnows]->(b:TempPerson) RETURN a.id, b.id ORDER BY a.id;"
            )
        )
        assert len(rows) == 3 and rows[0] == [1, 2]

    def test_copy_temp_rel_persistent(self):
        """REL with persistent vertices, first two cols are keys."""
        self._load_persistent_person_table()
        self.conn.execute(
            f'COPY TEMP TempKnowsP FROM "{self.edges_csv}" '
            f"(header = true, from = 'Person', to = 'Person')"
        )
        assert (
            len(list(self.conn.execute("MATCH ()-[r:TempKnowsP]->() RETURN r;"))) == 3
        )

    def test_copy_temp_rel_shuffled_needs_return(self):
        """edges_shuffled.csv: weight|src_id|dst_id - keys NOT at [0/1].
        Must use subquery RETURN to reorder columns.
        from/to are COPY TEMP options (graph semantics), not LOAD FROM options."""
        self._load_persistent_person_table()
        self.conn.execute(
            f'COPY TEMP TempShuf FROM (LOAD FROM "{self.edges_shuffled_csv}" '
            f"(header = true) "
            f"RETURN src_id, dst_id, weight) "
            f"(from = 'Person', to = 'Person')"
        )
        rows = list(
            self.conn.execute(
                "MATCH (a:Person)-[r:TempShuf]->(b:Person) RETURN a.id, b.id ORDER BY a.id;"
            )
        )
        assert len(rows) == 3 and rows[0] == [1, 2]

    def test_copy_temp_rel_return_no_extra_props(self):
        """RETURN only src/dst keys - edge has no extra properties.
        from/to are COPY TEMP options, not LOAD FROM options."""
        self._load_persistent_person_table()
        self.conn.execute(
            f'COPY TEMP TempNoProps FROM (LOAD FROM "{self.edges_csv}" '
            f"(header = true) "
            f"RETURN src_id, dst_id) "
            f"(from = 'Person', to = 'Person')"
        )
        rows = list(
            self.conn.execute(
                "MATCH (a:Person)-[r:TempNoProps]->(b:Person) RETURN a.id, b.id ORDER BY a.id;"
            )
        )
        assert len(rows) == 3 and rows[0] == [1, 2]

    def test_copy_temp_rel_missing_from_to(self):
        """COPY TEMP without from/to creates a NODE table, not REL."""
        self.conn.execute(f'COPY TEMP TempEdge FROM "{self.edges_csv}" (header = true)')
        rows = list(
            self.conn.execute("MATCH (n:TempEdge) RETURN n.src_id ORDER BY n.src_id;")
        )
        assert len(rows) == 3

    # ------------------------------------------------------------------
    # Cleanup & lifecycle
    # ------------------------------------------------------------------

    def test_copy_temp_cleanup(self):
        self.conn.execute(f'COPY TEMP TempE FROM "{self.people_csv}" (header = true)')
        assert list(self.conn.execute("MATCH (n:TempE) RETURN count(n);"))[0][0] == 4
        self.conn.close()
        conn2 = self.db.connect()
        try:
            with pytest.raises(Exception):
                conn2.execute("MATCH (n:TempE) RETURN count(n);")
        finally:
            conn2.close()

    def test_duplicate_label_same_connection(self):
        """COPY TEMP same label twice must fail."""
        self.conn.execute(f'COPY TEMP TempDup FROM "{self.people_csv}" (header = true)')
        with pytest.raises(Exception):
            self.conn.execute(
                f'COPY TEMP TempDup FROM "{self.people_csv}" (header = true)'
            )

    def test_reload_same_label_after_close(self):
        """After close() cleanup, reloading same label succeeds."""
        self.conn.execute(
            f'COPY TEMP TempReuse FROM "{self.people_csv}" (header = true)'
        )
        assert (
            list(self.conn.execute("MATCH (n:TempReuse) RETURN count(n);"))[0][0] == 4
        )
        self.conn.close()
        conn2 = self.db.connect()
        try:
            conn2.execute(
                f'COPY TEMP TempReuse FROM "{self.people_csv}" (header = true)'
            )
            assert (
                list(conn2.execute("MATCH (n:TempReuse) RETURN count(n);"))[0][0] == 4
            )
        finally:
            conn2.close()

    def test_temp_not_persisted_after_db_reopen(self):
        """Temp labels not in checkpoint; absent after DB reopen."""
        self.conn.execute(
            f'COPY TEMP TempGhost FROM "{self.people_csv}" (header = true)'
        )
        assert (
            list(self.conn.execute("MATCH (n:TempGhost) RETURN count(n);"))[0][0] == 4
        )
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
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()

    def test_persistent_survives_temp_cleanup(self):
        """Persistent tables survive close(); temp tables don't."""
        self.conn.execute("CREATE NODE TABLE Persistent(id INT64, PRIMARY KEY(id));")
        self.conn.execute("CREATE (p:Persistent {id: 1});")
        self.conn.execute(
            f'COPY TEMP TempGone FROM "{self.people_csv}" (header = true)'
        )
        self.conn.close()
        conn2 = self.db.connect()
        try:
            assert (
                list(conn2.execute("MATCH (n:Persistent) RETURN count(n);"))[0][0] == 1
            )
            with pytest.raises(Exception):
                list(conn2.execute("MATCH (n:TempGone) RETURN n.id;"))
        finally:
            conn2.close()

    # ------------------------------------------------------------------
    # Mixed persistent + temporary queries
    # ------------------------------------------------------------------

    def test_mixed_persistent_and_temp_match(self):
        """MATCH spanning persistent vertices + temporary edges."""
        self._load_persistent_person_table()
        self.conn.execute(
            f'COPY TEMP TempLink FROM "{self.edges_csv}" '
            f"(header = true, from = 'Person', to = 'Person')"
        )
        rows = list(
            self.conn.execute(
                "MATCH (a:Person)-[r:TempLink]->(b:Person) RETURN a.name, b.name ORDER BY a.id;"
            )
        )
        assert len(rows) == 3
        assert rows[0][0] == "Alice" and rows[0][1] == "Bob"

    def test_temp_and_persistent_join(self):
        """Cartesian join between persistent and temp tables."""
        self._load_persistent_person_table()
        extra_csv = _write_csv(
            self.csv_dir, "extra.csv", "id|nickname\n1|Ally\n2|Bobby\n5|Eve\n"
        )
        self.conn.execute(f'COPY TEMP TempExtra FROM "{extra_csv}" (header = true)')
        rows = list(
            self.conn.execute(
                "MATCH (p:Person), (t:TempExtra) WHERE p.id = t.id "
                "RETURN p.name, t.nickname ORDER BY p.id;"
            )
        )
        assert len(rows) == 2
        assert rows[0] == ["Alice", "Ally"]

    def test_temp_src_persistent_dst(self):
        """REL from temp vertex to persistent vertex."""
        self._load_persistent_person_table()
        self.conn.execute(f'COPY TEMP TempSrc FROM "{self.people_csv}" (header = true)')
        self.conn.execute(
            f'COPY TEMP TempMixed FROM "{self.edges_csv}" '
            f"(header = true, from = 'TempSrc', to = 'Person')"
        )
        rows = list(
            self.conn.execute(
                "MATCH (a:TempSrc)-[r:TempMixed]->(b:Person) RETURN a.id, b.id ORDER BY a.id;"
            )
        )
        assert len(rows) == 3

    # ------------------------------------------------------------------
    # Corner cases
    # ------------------------------------------------------------------

    def test_empty_csv(self):
        """Header-only CSV creates an empty temp table."""
        empty_csv = _write_csv(self.csv_dir, "empty.csv", "id|name|age\n")
        self.conn.execute(f'COPY TEMP TempEmpty FROM "{empty_csv}" (header = true)')
        assert (
            list(self.conn.execute("MATCH (n:TempEmpty) RETURN count(n);"))[0][0] == 0
        )

    def test_where_filters_all_rows(self):
        """WHERE matching nothing creates empty table."""
        self.conn.execute(
            f'COPY TEMP TempNone FROM (LOAD FROM "{self.people_csv}" '
            f"(header = true) WHERE age > 1000 RETURN *)"
        )
        assert list(self.conn.execute("MATCH (n:TempNone) RETURN count(n);"))[0][0] == 0

    def test_property_type_inference(self):
        """CSV types inferred correctly (age is INT64)."""
        self.conn.execute(
            f'COPY TEMP TempTyped FROM "{self.people_csv}" (header = true)'
        )
        rows = list(
            self.conn.execute("MATCH (n:TempTyped) RETURN n.age + 1 ORDER BY n.id;")
        )
        assert rows[0][0] == 31  # Alice age=30

    def test_multiple_queries_on_same_temp(self):
        """Multiple queries on same temp table all work."""
        self.conn.execute(
            f'COPY TEMP TempStable FROM "{self.people_csv}" (header = true)'
        )
        assert (
            list(self.conn.execute("MATCH (n:TempStable) RETURN count(n);"))[0][0] == 4
        )
        rows = list(
            self.conn.execute(
                "MATCH (n:TempStable) WHERE n.age > 25 RETURN n.id ORDER BY n.id;"
            )
        )
        assert len(rows) == 2
        assert (
            list(self.conn.execute("MATCH (n:TempStable) RETURN sum(n.age);"))[0][0]
            == 110
        )

    def test_aggregation_on_temp(self):
        """SUM, MIN, MAX on temp table."""
        self.conn.execute(f'COPY TEMP TempAgg FROM "{self.people_csv}" (header = true)')
        rows = list(
            self.conn.execute(
                "MATCH (n:TempAgg) RETURN sum(n.age), min(n.age), max(n.age);"
            )
        )
        assert rows[0] == [110, 20, 35]

    def test_where_string_comparison(self):
        """WHERE with string equality."""
        self.conn.execute(
            f'COPY TEMP TempAlice FROM (LOAD FROM "{self.people_csv}" '
            f"(header = true) WHERE name = 'Alice' RETURN *)"
        )
        rows = list(self.conn.execute("MATCH (n:TempAlice) RETURN n.id, n.name;"))
        assert len(rows) == 1 and rows[0][1] == "Alice"

    def test_where_or_expression(self):
        """WHERE with OR."""
        self.conn.execute(
            f'COPY TEMP TempOr FROM (LOAD FROM "{self.people_csv}" '
            f"(header = true) WHERE age < 22 OR age > 32 RETURN *)"
        )
        rows = list(self.conn.execute("MATCH (n:TempOr) RETURN n.id ORDER BY n.id;"))
        assert len(rows) == 2  # Carol(35), Dave(20)

    def test_where_arithmetic(self):
        """WHERE with arithmetic expression."""
        self.conn.execute(
            f'COPY TEMP TempArith FROM (LOAD FROM "{self.people_csv}" '
            f"(header = true) WHERE age * 2 > 60 RETURN *)"
        )
        rows = list(self.conn.execute("MATCH (n:TempArith) RETURN n.id;"))
        assert len(rows) == 1  # Carol(35): 35*2=70>60

    def test_nonexistent_vertex_label(self):
        """REL referencing non-existent vertex label must fail."""
        with pytest.raises(Exception):
            self.conn.execute(
                f'COPY TEMP TempBadEdge FROM "{self.edges_csv}" '
                f"(header = true, from = 'Nope', to = 'Nope')"
            )

    def test_label_conflict_with_persistent(self):
        """COPY TEMP with existing persistent label must fail."""
        self.conn.execute("CREATE NODE TABLE Conflict(id INT64, PRIMARY KEY(id));")
        with pytest.raises(Exception):
            self.conn.execute(
                f'COPY TEMP Conflict FROM "{self.people_csv}" (header = true)'
            )

    def test_dangling_reference_silent_skip(self):
        """Edge with non-existent vertex key is silently skipped."""
        dangling_csv = _write_csv(
            self.csv_dir, "dangling.csv", "src_id|dst_id|weight\n1|2|0.5\n99|3|1.0\n"
        )
        self.conn.execute(f'COPY TEMP TempPD FROM "{self.people_csv}" (header = true)')
        self.conn.execute(
            f'COPY TEMP TempDangling FROM "{dangling_csv}" '
            f"(header = true, from = 'TempPD', to = 'TempPD')"
        )
        rows = list(
            self.conn.execute(
                "MATCH (a:TempPD)-[r:TempDangling]->(b:TempPD) RETURN a.id, b.id;"
            )
        )
        assert len(rows) == 1  # only (1->2) valid

    def test_rel_with_where(self):
        """REL COPY TEMP with WHERE filter via subquery."""
        self.conn.execute(f'COPY TEMP TempPW FROM "{self.people_csv}" (header = true)')
        self.conn.execute(
            f'COPY TEMP TempFilteredEdge FROM (LOAD FROM "{self.edges_csv}" '
            f"(header = true) WHERE weight > 0.6 RETURN *) "
            f"(from = 'TempPW', to = 'TempPW')"
        )
        rows = list(
            self.conn.execute(
                "MATCH (a:TempPW)-[r:TempFilteredEdge]->(b:TempPW) RETURN a.id, b.id ORDER BY a.id;"
            )
        )
        assert len(rows) == 2  # weight 1.0 and 0.8

    def test_rel_where_and_return(self):
        """REL with WHERE + RETURN via subquery."""
        self.conn.execute(f'COPY TEMP TempPWR FROM "{self.people_csv}" (header = true)')
        self.conn.execute(
            f'COPY TEMP TempWREdge FROM (LOAD FROM "{self.edges_csv}" '
            f"(header = true) WHERE weight >= 0.8 RETURN src_id, dst_id, weight) "
            f"(from = 'TempPWR', to = 'TempPWR')"
        )
        rows = list(
            self.conn.execute(
                "MATCH (a:TempPWR)-[r:TempWREdge]->(b:TempPWR) RETURN a.id, b.id ORDER BY a.id;"
            )
        )
        assert len(rows) == 2

    # ------------------------------------------------------------------
    # DROP temporary table
    # ------------------------------------------------------------------

    def test_drop_temp_node(self):
        """DROP TABLE removes temp node table."""
        self.conn.execute(
            f'COPY TEMP TempDrop FROM "{self.people_csv}" (header = true)'
        )
        assert list(self.conn.execute("MATCH (n:TempDrop) RETURN count(n);"))[0][0] == 4
        self.conn.execute("DROP TABLE TempDrop;")
        with pytest.raises(Exception):
            self.conn.execute("MATCH (n:TempDrop) RETURN n.id;")

    def test_drop_temp_edge(self):
        """DROP edge table; node table remains."""
        self.conn.execute(
            f'COPY TEMP TempNodeK FROM "{self.people_csv}" (header = true)'
        )
        self.conn.execute(
            f'COPY TEMP TempEdgeK FROM "{self.edges_csv}" '
            f"(header = true, from = 'TempNodeK', to = 'TempNodeK')"
        )
        self.conn.execute("DROP TABLE TempEdgeK;")
        with pytest.raises(Exception):
            self.conn.execute("MATCH ()-[r:TempEdgeK]->() RETURN r;")
        assert (
            list(self.conn.execute("MATCH (n:TempNodeK) RETURN count(n);"))[0][0] == 4
        )

    def test_drop_then_recreate(self):
        """After DROP, same label can be reused."""
        self.conn.execute(
            f'COPY TEMP TempRecycle FROM "{self.people_csv}" (header = true)'
        )
        self.conn.execute("DROP TABLE TempRecycle;")
        self.conn.execute(
            f'COPY TEMP TempRecycle FROM "{self.people_csv}" (header = true)'
        )
        assert (
            list(self.conn.execute("MATCH (n:TempRecycle) RETURN count(n);"))[0][0] == 4
        )


# ---------------------------------------------------------------------------
# JSONL format
# ---------------------------------------------------------------------------


def _get_tinysnb_path():
    current_file = os.path.abspath(__file__)
    workspace_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
    )
    path = os.path.join(workspace_root, "example_dataset", "tinysnb")
    return path if os.path.exists(path) else None


class TestCopyTempJsonl:
    """COPY TEMP from JSONL files."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        tinysnb = _get_tinysnb_path()
        if not tinysnb:
            pytest.skip("tinysnb dataset not found")
        self.jsonl_path = os.path.join(tinysnb, "json", "vPerson.jsonl")
        if not os.path.exists(self.jsonl_path):
            pytest.skip(f"JSONL file not found: {self.jsonl_path}")
        self.db_dir = str(tmp_path / "test_copy_temp_jsonl_db")
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_node_from_jsonl(self):
        self.conn.execute(
            f"COPY TEMP TempJP FROM \"{self.jsonl_path}\" (primary_key = 'ID')"
        )
        rows = list(
            self.conn.execute("MATCH (n:TempJP) RETURN n.fName ORDER BY n.fName;")
        )
        assert len(rows) == 8 and rows[0][0] == "Alice"

    def test_node_from_jsonl_with_where(self):
        self.conn.execute(
            f'COPY TEMP TempJOld FROM (LOAD FROM "{self.jsonl_path}" '
            f"(primary_key = 'ID') WHERE age >= 35 RETURN *)"
        )
        rows = list(
            self.conn.execute(
                "MATCH (n:TempJOld) RETURN n.fName, n.age ORDER BY n.age;"
            )
        )
        assert len(rows) >= 3
        for row in rows:
            assert row[1] >= 35

    def test_node_from_jsonl_with_return(self):
        self.conn.execute(
            f'COPY TEMP TempJSlim FROM (LOAD FROM "{self.jsonl_path}" '
            f"(primary_key = 'ID') RETURN ID, fName, age)"
        )
        rows = list(
            self.conn.execute("MATCH (n:TempJSlim) RETURN n.fName ORDER BY n.fName;")
        )
        assert len(rows) == 8
        with pytest.raises(Exception):
            self.conn.execute("MATCH (n:TempJSlim) RETURN n.eyeSight;")


class TestCopyTempJson:
    """COPY TEMP from JSON (array-of-objects) files."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        tinysnb = _get_tinysnb_path()
        if not tinysnb:
            pytest.skip("tinysnb dataset not found")
        self.json_path = os.path.join(tinysnb, "json", "vPerson.json")
        if not os.path.exists(self.json_path):
            pytest.skip(f"JSON file not found: {self.json_path}")
        self.db_dir = str(tmp_path / "test_copy_temp_json_db")
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_node_from_json(self):
        self.conn.execute(
            f"COPY TEMP TempJArr FROM \"{self.json_path}\" (primary_key = 'ID')"
        )
        rows = list(
            self.conn.execute("MATCH (n:TempJArr) RETURN n.fName ORDER BY n.fName;")
        )
        assert len(rows) == 8 and rows[0][0] == "Alice"

    def test_node_from_json_with_where(self):
        self.conn.execute(
            f'COPY TEMP TempJArrOld FROM (LOAD FROM "{self.json_path}" '
            f"(primary_key = 'ID') WHERE age >= 35 RETURN *)"
        )
        rows = list(
            self.conn.execute("MATCH (n:TempJArrOld) RETURN n.age ORDER BY n.age;")
        )
        assert all(row[0] >= 35 for row in rows)


# ---------------------------------------------------------------------------
# Parquet format (extension test)
# ---------------------------------------------------------------------------

EXTENSION_TESTS_ENABLED = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
extension_test = pytest.mark.skipif(
    not EXTENSION_TESTS_ENABLED,
    reason="Extension tests disabled; set NEUG_RUN_EXTENSION_TESTS=1 to enable.",
)


@extension_test
class TestCopyTempParquet:
    """COPY TEMP from Parquet files."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        tinysnb = _get_tinysnb_path()
        if not tinysnb:
            pytest.skip("tinysnb dataset not found")
        self.parquet_path = os.path.join(tinysnb, "parquet", "vPerson.parquet")
        if not os.path.exists(self.parquet_path):
            pytest.skip("Parquet file not found")
        self.db_dir = str(tmp_path / "test_copy_temp_parquet_db")
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        self.conn.execute("load parquet")
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_node_from_parquet(self):
        self.conn.execute(
            f"COPY TEMP TempPq FROM \"{self.parquet_path}\" (primary_key = 'ID')"
        )
        rows = list(
            self.conn.execute("MATCH (n:TempPq) RETURN n.fName ORDER BY n.fName;")
        )
        assert len(rows) == 8 and rows[0][0] == "Alice"

    def test_node_from_parquet_with_where(self):
        self.conn.execute(
            f'COPY TEMP TempPqOld FROM (LOAD FROM "{self.parquet_path}" '
            f"(primary_key = 'ID') WHERE age >= 40 RETURN *)"
        )
        rows = list(
            self.conn.execute("MATCH (n:TempPqOld) RETURN n.age ORDER BY n.age;")
        )
        assert all(row[0] >= 40 for row in rows)

    def test_rel_from_parquet(self):
        """REL from Parquet; keys at [0/1]."""
        meets_path = os.path.join(os.path.dirname(self.parquet_path), "eMeets.parquet")
        if not os.path.exists(meets_path):
            pytest.skip("eMeets.parquet not found")
        self.conn.execute(
            f"COPY TEMP PqPerson FROM \"{self.parquet_path}\" (primary_key = 'ID')"
        )
        self.conn.execute(
            f'COPY TEMP PqMeets FROM "{meets_path}" '
            f"(from = 'PqPerson', to = 'PqPerson')"
        )
        rows = list(
            self.conn.execute(
                "MATCH (a:PqPerson)-[r:PqMeets]->(b:PqPerson) RETURN a.ID, b.ID ORDER BY a.ID;"
            )
        )
        assert len(rows) >= 5


# ---------------------------------------------------------------------------
# Remote HTTPFS (extension test)
# ---------------------------------------------------------------------------


@extension_test
class TestCopyTempRemoteHttpfs:
    """COPY TEMP from remote Parquet via httpfs."""

    VERTEX_URL = "http://graphscope.oss-cn-beijing.aliyuncs.com/neug/vPerson.parquet"
    EDGE_URL = "http://graphscope.oss-cn-beijing.aliyuncs.com/neug/eMeets.parquet"

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.db_dir = str(tmp_path / "test_copy_temp_remote_db")
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        self.conn.execute("load httpfs")
        self.conn.execute("load parquet")
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_node_from_remote(self):
        self.conn.execute(
            f"COPY TEMP TempRemote FROM \"{self.VERTEX_URL}\" (primary_key = 'ID')"
        )
        rows = list(
            self.conn.execute("MATCH (n:TempRemote) RETURN n.fName ORDER BY n.fName;")
        )
        assert len(rows) == 8 and rows[0][0] == "Alice"

    def test_node_from_remote_with_where(self):
        self.conn.execute(
            f'COPY TEMP TempRemoteOld FROM (LOAD FROM "{self.VERTEX_URL}" '
            f"(primary_key = 'ID') WHERE age >= 40 RETURN *)"
        )
        rows = list(
            self.conn.execute("MATCH (n:TempRemoteOld) RETURN n.age ORDER BY n.age;")
        )
        assert all(row[0] >= 40 for row in rows)

    def test_rel_from_remote(self):
        self.conn.execute(
            f"COPY TEMP TempRSrc FROM \"{self.VERTEX_URL}\" (primary_key = 'ID')"
        )
        self.conn.execute(
            f'COPY TEMP TempRMeets FROM "{self.EDGE_URL}" '
            f"(from = 'TempRSrc', to = 'TempRSrc')"
        )
        rows = list(
            self.conn.execute(
                "MATCH (a:TempRSrc)-[r:TempRMeets]->(b:TempRSrc) RETURN a.ID, b.ID ORDER BY a.ID;"
            )
        )
        assert len(rows) >= 1


# ---------------------------------------------------------------------------
# Read-only mode rejection
# ---------------------------------------------------------------------------


class TestCopyTempReadOnlyRejection:
    """COPY TEMP rejected in read-only mode."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.db_dir = str(tmp_path / "test_copy_temp_ro_db")
        self.csv_dir = str(tmp_path / "csv_ro")
        os.makedirs(self.csv_dir, exist_ok=True)
        self.people_csv = _write_csv(
            self.csv_dir, "people.csv", "id|name|age\n1|Alice|30\n2|Bob|25\n"
        )
        # Create valid DB in write mode first
        db_rw = Database(db_path=self.db_dir, mode="w")
        conn_rw = db_rw.connect()
        conn_rw.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
        conn_rw.close()
        db_rw.close()
        # Reopen read-only
        self.db = Database(db_path=self.db_dir, mode="r")
        self.conn = self.db.connect()
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_copy_temp_node_rejected(self):
        with pytest.raises(Exception, match="read-only mode"):
            self.conn.execute(
                f'COPY TEMP TempFail FROM "{self.people_csv}" ' f"(header = true)"
            )

    def test_copy_temp_rel_rejected(self):
        with pytest.raises(Exception, match="read-only mode"):
            self.conn.execute(
                f'COPY TEMP TempEdgeFail FROM "{self.people_csv}" '
                f"(header = true, from = 'Person', to = 'Person')"
            )


# ======================================================================
# COPY TEMP rejected in Session (Service Mode)
# ======================================================================
def test_copy_temp_rejected_in_session(tmp_path):
    """COPY TEMP must be rejected when submitted via Session (Service Mode)."""
    from neug.session import Session

    db_dir = str(tmp_path / "test_copy_temp_session_db")
    csv_dir = str(tmp_path / "csv")
    os.makedirs(csv_dir, exist_ok=True)
    people_csv = os.path.join(csv_dir, "people.csv")
    with open(people_csv, "w") as f:
        f.write("id|name|age\n1|Alice|30\n2|Bob|25\n")

    db = Database(db_path=db_dir, mode="w")
    uri = db.serve(10086, "localhost", False)
    time.sleep(1)

    try:
        session = Session(uri, timeout="10s")
        with pytest.raises(Exception, match="not supported for TP service"):
            session.execute(
                f'COPY TEMP TempFail FROM "{people_csv}" ' f"(header = true)"
            )
    finally:
        db.stop_serving()
        db.close()
        shutil.rmtree(db_dir, ignore_errors=True)
