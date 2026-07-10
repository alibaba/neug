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
End-to-end tests for PROFILE and EXPLAIN modes in both AP (OLAP) and TP (OLTP) modes.

This test suite covers:
- AP mode (local embedded): QueryProcessor::execute_internal
- TP mode (remote HTTP service): NeugDBSession::Eval
- Both modes sharing the same underlying Pipeline/OprTimer machinery

Tests verify:
1. PROFILE mode: Executes query and collects per-operator metrics
2. EXPLAIN mode: Builds operator tree without executing, returns 0 rows
3. Dict interface: get_profile_metrics() returns structured metrics
4. Text interface: get_profile_text() returns ASCII-formatted output
"""

import logging
import os
import shutil
import sys
import tempfile
import time

import pytest
from conftest import wait_for_server_ready

from neug.database import Database
from neug.session import Session

logger = logging.getLogger(__name__)


def load_sample_data(conn):
    """
    Create and populate a simple graph with person and software nodes.

    Args:
        conn: A connection object (either AP or TP mode) to execute queries.
    """
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE software(id INT64, name STRING, lang STRING, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
    conn.execute("CREATE REL TABLE created(FROM person TO software, weight DOUBLE);")

    # Insert person data
    conn.execute("CREATE (p:person {id: 1, name: 'marko', age: 29});")
    conn.execute("CREATE (p:person {id: 2, name: 'vadas', age: 27});")
    conn.execute("CREATE (p:person {id: 3, name: 'josh', age: 32});")
    conn.execute("CREATE (p:person {id: 4, name: 'peter', age: 35});")

    # Insert software data
    conn.execute("CREATE (s:software {id: 10, name: 'lop', lang: 'java'});")
    conn.execute("CREATE (s:software {id: 11, name: 'ripple', lang: 'java'});")

    # Insert relationships
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 1 AND b.id = 2 "
        "CREATE (a)-[:knows {weight: 0.5}]->(b);"
    )
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 1 AND b.id = 3 "
        "CREATE (a)-[:knows {weight: 1.0}]->(b);"
    )
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 2 AND b.id = 3 "
        "CREATE (a)-[:knows {weight: 0.7}]->(b);"
    )
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 3 AND b.id = 4 "
        "CREATE (a)-[:knows {weight: 0.4}]->(b);"
    )

    conn.execute(
        "MATCH (a:person), (s:software) WHERE a.id = 1 AND s.id = 10 "
        "CREATE (a)-[:created {weight: 0.4}]->(s);"
    )
    conn.execute(
        "MATCH (a:person), (s:software) WHERE a.id = 4 AND s.id = 10 "
        "CREATE (a)-[:created {weight: 0.6}]->(s);"
    )
    conn.execute(
        "MATCH (a:person), (s:software) WHERE a.id = 1 AND s.id = 11 "
        "CREATE (a)-[:created {weight: 1.0}]->(s);"
    )


@pytest.fixture(scope="module")
def ap_mode_db(tmp_path_factory):
    """
    Fixture for AP mode (embedded, local).

    Yields:
        conn: A connection object for executing queries in AP mode.
    """
    db_dir = str(tmp_path_factory.mktemp("test_explain_profile_ap"))
    db = Database(db_dir, "w")
    conn = db.connect()

    load_sample_data(conn)

    yield conn

    conn.close()
    db.close()


@pytest.fixture(scope="module")
def tp_mode_db(tmp_path_factory):
    """
    Fixture for TP mode (remote HTTP service).

    Yields:
        endpoint: The HTTP endpoint URI of the service.
    """
    db_dir = str(tmp_path_factory.mktemp("test_explain_profile_tp"))
    db = Database(db_dir, "w")
    conn = db.connect()
    load_sample_data(conn)
    conn.close()

    # Start the service
    endpoint = db.serve(10030, "localhost", False)
    time.sleep(1)

    yield endpoint

    db.stop_serving()
    db.close()


# ============================================================================
# AP Mode Tests
# ============================================================================


class TestAPModeProfile:
    """Tests for PROFILE mode in AP (embedded) mode."""

    def test_profile_single_table_scan(self, ap_mode_db):
        """Test PROFILE with single table scan."""
        conn = ap_mode_db
        query = "PROFILE MATCH (p:person) RETURN p.name, p.age"
        result = conn.execute(query)

        # Verify query results
        rows = list(result)
        assert len(rows) == 4, "Expected 4 person records"
        col_names = result.column_names()
        assert len(col_names) == 2
        assert "name" in col_names[0]
        assert "age" in col_names[1]

        # Verify profile result exists
        assert result.has_profile_result(), "PROFILE result should be present"

        # Verify metrics dict
        metrics = result.get_profile_metrics()
        assert "total_elapsed_ms" in metrics
        assert "total_output_rows" in metrics
        assert "operators" in metrics
        assert metrics["total_output_rows"] == 4
        assert len(metrics["operators"]) > 0

        # Verify text output
        text_output = result.get_profile_text()
        assert isinstance(text_output, str)
        assert len(text_output) > 0
        logger.info(f"Profile output:\n{text_output}")

    def test_profile_with_join(self, ap_mode_db):
        """Test PROFILE with join operation."""
        conn = ap_mode_db
        query = "PROFILE MATCH (p1:person), (p2:person) WHERE p1.id < p2.id RETURN p1.name, p2.name"
        result = conn.execute(query)

        # Verify query results
        rows = list(result)
        assert len(rows) == 6, "Expected 6 cross-product pairs (4 choose 2)"

        # Verify profile result
        assert result.has_profile_result()
        metrics = result.get_profile_metrics()
        assert metrics["total_output_rows"] == 6

        logger.info(
            f"Join profile: {metrics['total_elapsed_ms']:.3f} ms, {metrics['total_output_rows']} rows"
        )

    def test_profile_with_aggregation(self, ap_mode_db):
        """Test PROFILE with aggregation."""
        conn = ap_mode_db
        query = "PROFILE MATCH (p:person) RETURN COUNT(*) as person_count"
        result = conn.execute(query)

        # Verify query results
        rows = list(result)
        assert len(rows) == 1
        assert rows[0][0] == 4

        # Verify profile result
        assert result.has_profile_result()
        metrics = result.get_profile_metrics()
        assert metrics["total_output_rows"] == 1

        logger.info(f"Aggregation profile: {metrics['total_elapsed_ms']:.3f} ms")

    def test_profile_with_edge_traversal(self, ap_mode_db):
        """Test PROFILE with edge traversal."""
        conn = ap_mode_db
        query = "PROFILE MATCH (p:person)-[e:knows]->(q:person) RETURN p.name, q.name"
        result = conn.execute(query)

        # Verify query results
        rows = list(result)
        assert len(rows) == 4, "Expected 4 knows relationships"

        # Verify profile result
        assert result.has_profile_result()
        metrics = result.get_profile_metrics()
        assert metrics["total_output_rows"] == 4

        logger.info(f"Edge traversal profile: {metrics['total_elapsed_ms']:.3f} ms")

    def test_profile_metrics_structure(self, ap_mode_db):
        """Test the structure of profile metrics dict."""
        conn = ap_mode_db
        query = "PROFILE MATCH (p:person) RETURN p.id LIMIT 2"
        result = conn.execute(query)

        list(result)  # Consume result

        metrics = result.get_profile_metrics()

        # Check top-level keys
        assert "total_elapsed_ms" in metrics
        assert "total_output_rows" in metrics
        assert "operators" in metrics

        # Check operator structure
        assert len(metrics["operators"]) > 0
        for op in metrics["operators"]:
            assert "operator_id" in op
            assert "operator_name" in op
            assert "elapsed_ms" in op
            assert "output_rows" in op
            assert "parent_id" in op
            assert "child_ids" in op

            logger.info(
                f"Operator {op['operator_id']}: {op['operator_name']} "
                f"({op['elapsed_ms']:.3f} ms, {op['output_rows']} rows)"
            )


class TestAPModeExplain:
    """Tests for EXPLAIN mode in AP (embedded) mode."""

    def test_explain_basic(self, ap_mode_db):
        """Test EXPLAIN mode returns plan but no data rows."""
        conn = ap_mode_db
        query = "EXPLAIN MATCH (p:person)-[e:knows]->(q:person) RETURN p.name, q.name"
        result = conn.execute(query)

        # EXPLAIN should return 0 rows
        rows = list(result)
        assert len(rows) == 0, "EXPLAIN should return 0 data rows"

        # But should have profile result (the plan)
        assert result.has_profile_result()
        metrics = result.get_profile_metrics()
        assert metrics["total_output_rows"] == 0
        assert len(metrics["operators"]) > 0

        logger.info(f"Explain plan has {len(metrics['operators'])} operators")

    def test_explain_complex_query(self, ap_mode_db):
        """Test EXPLAIN with complex query."""
        conn = ap_mode_db
        query = (
            "EXPLAIN MATCH (p:person)-[e1:knows]->(q:person), "
            "(q)-[e2:created]->(s:software) "
            "RETURN p.name, s.name"
        )
        result = conn.execute(query)

        # EXPLAIN returns no rows
        rows = list(result)
        assert len(rows) == 0

        # But plan is visible
        assert result.has_profile_result()
        metrics = result.get_profile_metrics()
        assert len(metrics["operators"]) > 0

        text_output = result.get_profile_text()
        assert len(text_output) > 0
        logger.info(f"Complex plan:\n{text_output}")

    def test_explain_text_output_format(self, ap_mode_db):
        """Test EXPLAIN text output has proper formatting."""
        conn = ap_mode_db
        query = "EXPLAIN MATCH (p:person) RETURN p.id"
        result = conn.execute(query)

        list(result)  # Consume result

        text_output = result.get_profile_text()

        # Should be non-empty string
        assert isinstance(text_output, str)
        assert len(text_output) > 0

        # Should contain box-drawing characters or ASCII formatting
        assert any(char in text_output for char in ["─", "│", "┌", "└", "+", "|", "-"])

        logger.info(f"Text output:\n{text_output}")


class TestAPModeNormalExecution:
    """Tests for normal execution (no PROFILE/EXPLAIN) in AP mode."""

    def test_normal_execution_no_profile(self, ap_mode_db):
        """Test normal query execution without PROFILE/EXPLAIN."""
        conn = ap_mode_db
        query = "MATCH (p:person) RETURN p.name"
        result = conn.execute(query)

        rows = list(result)
        assert len(rows) == 4

        # Normal execution should not have profile result
        assert not result.has_profile_result()


# ============================================================================
# TP Mode Tests (HTTP Remote Service)
# ============================================================================


class TestTPModeProfile:
    """Tests for PROFILE mode in TP (remote service) mode."""

    def test_profile_single_table_scan(self, tp_mode_db):
        """Test PROFILE with single table scan in TP mode."""
        endpoint = tp_mode_db
        session = Session.open(endpoint)
        query = "PROFILE MATCH (p:person) RETURN p.name, p.age"
        result = session.execute(query)

        # Verify query results
        rows = list(result)
        assert len(rows) == 4, "Expected 4 person records"
        col_names = result.column_names()
        assert len(col_names) == 2
        assert "name" in col_names[0]
        assert "age" in col_names[1]

        # Verify profile result exists
        assert result.has_profile_result(), "PROFILE result should be present"

        # Verify metrics dict
        metrics = result.get_profile_metrics()
        assert "total_elapsed_ms" in metrics
        assert "total_output_rows" in metrics
        assert "operators" in metrics
        assert metrics["total_output_rows"] == 4
        assert len(metrics["operators"]) > 0

        # Verify text output
        text_output = result.get_profile_text()
        assert isinstance(text_output, str)
        assert len(text_output) > 0
        logger.info(f"TP Profile output:\n{text_output}")

        session.close()

    def test_profile_with_join(self, tp_mode_db):
        """Test PROFILE with join operation in TP mode."""
        endpoint = tp_mode_db
        session = Session.open(endpoint)
        query = "PROFILE MATCH (p1:person), (p2:person) WHERE p1.id < p2.id RETURN p1.name, p2.name"
        result = session.execute(query)

        rows = list(result)
        assert len(rows) == 6

        assert result.has_profile_result()
        metrics = result.get_profile_metrics()
        assert metrics["total_output_rows"] == 6

        logger.info(f"TP Join profile: {metrics['total_elapsed_ms']:.3f} ms")

        session.close()

    def test_profile_with_edge_traversal(self, tp_mode_db):
        """Test PROFILE with edge traversal in TP mode."""
        endpoint = tp_mode_db
        session = Session.open(endpoint)
        query = "PROFILE MATCH (p:person)-[e:knows]->(q:person) RETURN p.name, q.name"
        result = session.execute(query)

        rows = list(result)
        assert len(rows) == 4

        assert result.has_profile_result()
        metrics = result.get_profile_metrics()
        assert metrics["total_output_rows"] == 4

        logger.info(f"TP Edge traversal profile: {metrics['total_elapsed_ms']:.3f} ms")

        session.close()


class TestTPModeExplain:
    """Tests for EXPLAIN mode in TP (remote service) mode."""

    def test_explain_basic(self, tp_mode_db):
        """Test EXPLAIN mode in TP returns plan but no data rows."""
        endpoint = tp_mode_db
        session = Session.open(endpoint)
        query = "EXPLAIN MATCH (p:person)-[e:knows]->(q:person) RETURN p.name, q.name"
        result = session.execute(query)

        # EXPLAIN should return 0 rows
        rows = list(result)
        assert len(rows) == 0, "EXPLAIN should return 0 data rows"

        # But should have profile result (the plan)
        assert result.has_profile_result()
        metrics = result.get_profile_metrics()
        assert metrics["total_output_rows"] == 0
        assert len(metrics["operators"]) > 0

        logger.info(f"TP Explain plan has {len(metrics['operators'])} operators")

        session.close()

    def test_explain_complex_query(self, tp_mode_db):
        """Test EXPLAIN with complex query in TP mode."""
        endpoint = tp_mode_db
        session = Session.open(endpoint)
        query = (
            "EXPLAIN MATCH (p:person)-[e1:knows]->(q:person), "
            "(q)-[e2:created]->(s:software) "
            "RETURN p.name, s.name"
        )
        result = session.execute(query)

        rows = list(result)
        assert len(rows) == 0

        assert result.has_profile_result()
        metrics = result.get_profile_metrics()
        assert len(metrics["operators"]) > 0

        text_output = result.get_profile_text()
        assert len(text_output) > 0
        logger.info(f"TP Complex plan:\n{text_output}")

        session.close()


class TestTPModeNormalExecution:
    """Tests for normal execution (no PROFILE/EXPLAIN) in TP mode."""

    def test_normal_execution_no_profile(self, tp_mode_db):
        """Test normal query execution without PROFILE/EXPLAIN in TP mode."""
        endpoint = tp_mode_db
        session = Session.open(endpoint)
        query = "MATCH (p:person) RETURN p.name"
        result = session.execute(query)

        rows = list(result)
        assert len(rows) == 4

        # Normal execution should not have profile result
        assert not result.has_profile_result()

        session.close()


# ============================================================================
# Cross-Mode Consistency Tests
# ============================================================================


class TestAPTPConsistency:
    """Tests to verify AP and TP modes produce consistent results."""

    def test_profile_result_count_consistency(self, ap_mode_db, tp_mode_db):
        """Verify AP and TP produce same row count for PROFILE."""
        query = "PROFILE MATCH (p:person) RETURN p.id"

        # AP mode
        result_ap = ap_mode_db.execute(query)
        rows_ap = list(result_ap)
        metrics_ap = result_ap.get_profile_metrics()

        # TP mode
        endpoint = tp_mode_db
        session = Session.open(endpoint)
        result_tp = session.execute(query)
        rows_tp = list(result_tp)
        metrics_tp = result_tp.get_profile_metrics()
        session.close()

        # Both should return 4 rows
        assert len(rows_ap) == len(rows_tp) == 4
        assert metrics_ap["total_output_rows"] == metrics_tp["total_output_rows"] == 4

        logger.info(
            f"AP: {metrics_ap['total_output_rows']} rows, "
            f"TP: {metrics_tp['total_output_rows']} rows - CONSISTENT"
        )

    def test_explain_plan_consistency(self, ap_mode_db, tp_mode_db):
        """Verify AP and TP produce same plan structure for EXPLAIN."""
        query = "EXPLAIN MATCH (p:person)-[e:knows]->(q:person) RETURN p.name"

        # AP mode
        result_ap = ap_mode_db.execute(query)
        list(result_ap)
        metrics_ap = result_ap.get_profile_metrics()

        # TP mode
        endpoint = tp_mode_db
        session = Session.open(endpoint)
        result_tp = session.execute(query)
        list(result_tp)
        metrics_tp = result_tp.get_profile_metrics()
        session.close()

        # Both should have same number of operators in plan
        assert len(metrics_ap["operators"]) == len(metrics_tp["operators"])
        assert metrics_ap["total_output_rows"] == metrics_tp["total_output_rows"] == 0

        logger.info(
            f"AP plan: {len(metrics_ap['operators'])} operators, "
            f"TP plan: {len(metrics_tp['operators'])} operators - CONSISTENT"
        )

    def test_profile_operator_names_consistency(self, ap_mode_db, tp_mode_db):
        """Verify AP and TP produce same operator names."""
        query = "PROFILE MATCH (p:person) RETURN p.name"

        # AP mode
        result_ap = ap_mode_db.execute(query)
        list(result_ap)
        metrics_ap = result_ap.get_profile_metrics()
        ops_ap = [op["operator_name"] for op in metrics_ap["operators"]]

        # TP mode
        endpoint = tp_mode_db
        session = Session.open(endpoint)
        result_tp = session.execute(query)
        list(result_tp)
        metrics_tp = result_tp.get_profile_metrics()
        ops_tp = [op["operator_name"] for op in metrics_tp["operators"]]
        session.close()

        # Operator sequence should match
        assert ops_ap == ops_tp

        logger.info(f"Operators: {' -> '.join(ops_ap)} - CONSISTENT")
