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
Test cases for the TinySNB Tutorial (doc/source/tutorials/tinysnb_tutorial.md)

This module tests all the code examples from the TinySNB tutorial to ensure
they work correctly and the documentation stays in sync with the actual behavior.
"""

import os
import shutil
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

import neug
from neug.database import Database


@pytest.fixture(scope="module")
def tinysnb_db(tmp_path_factory):
    """
    Fixture to create a TinySNB database for testing.
    This fixture is shared across all tests in this module.
    """
    db_path = tmp_path_factory.mktemp("tinysnb_tutorial")
    db = Database(str(db_path))
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    yield db, conn
    conn.close()
    db.close()


class TestTutorialDatasetLoading:
    """Tests for the 'Getting Started' section of the tutorial."""

    def test_load_tinysnb_dataset(self, tmp_path):
        """Test loading the TinySNB dataset (Tutorial: Loading the Dataset)."""
        db_path = str(tmp_path / "tinysnb_test")

        # First, let's load the builtin TinySNB dataset into a new database
        db = neug.Database(db_path)
        db.load_builtin_dataset("tinysnb")
        conn = db.connect()

        # Verify dataset loaded successfully
        result = list(conn.execute("MATCH (n) RETURN count(n) as total_nodes"))
        assert result[0][0] > 0, "TinySNB dataset should have nodes"

        conn.close()
        db.close()

    def test_reopen_existing_database(self, tmp_path):
        """Test reopening an existing database without reloading."""
        db_path = str(tmp_path / "tinysnb_reopen")

        # Create and load dataset
        db = neug.Database(db_path)
        db.load_builtin_dataset("tinysnb")
        db.close()

        # Reopen the existing database
        db2 = neug.Database(db_path)
        conn = db2.connect()

        result = list(conn.execute("MATCH (n) RETURN count(n)"))
        assert result[0][0] > 0, "Database should retain data after reopen"

        conn.close()
        db2.close()


class TestTutorialSchemaExploration:
    """Tests for the 'Exploring the Schema' section of the tutorial."""

    def test_count_total_nodes(self, tinysnb_db):
        """Test counting total nodes in the graph."""
        db, conn = tinysnb_db

        result = list(conn.execute("MATCH (n) RETURN count(n) as total_nodes"))
        total_nodes = result[0][0]

        # TinySNB should have exactly 14 nodes
        assert total_nodes == 14, f"Expected 14 total nodes, got {total_nodes}"

    def test_count_people(self, tinysnb_db):
        """Test counting person nodes."""
        db, conn = tinysnb_db

        result = list(conn.execute("MATCH (p:person) RETURN count(p) as people_count"))
        people_count = result[0][0]

        assert people_count == 8, f"Expected 8 people, got {people_count}"

    def test_count_organisations(self, tinysnb_db):
        """Test counting organisation nodes."""
        db, conn = tinysnb_db

        result = list(
            conn.execute("MATCH (o:organisation) RETURN count(o) as org_count")
        )
        org_count = result[0][0]

        assert org_count == 3, f"Expected 3 organisations, got {org_count}"

    def test_count_movies(self, tinysnb_db):
        """Test counting movie nodes."""
        db, conn = tinysnb_db

        result = list(conn.execute("MATCH (m:movies) RETURN count(m) as movie_count"))
        movie_count = result[0][0]

        assert movie_count == 3, f"Expected 3 movies, got {movie_count}"


class TestTutorialPeopleQueries:
    """Tests for the 'Exploring People in Our Social Network' section."""

    def test_get_all_people_with_info(self, tinysnb_db):
        """Test querying all people with their basic information."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p:person)
            RETURN p.fName, p.age, p.isStudent, p.isWorker
            ORDER BY p.age
        """
            )
        )

        assert len(result) == 8, "Should return 8 people"
        # Verify result structure
        for record in result:
            assert len(record) == 4, "Each record should have 4 fields"
            assert record[0] is not None, "Name should not be null"
            assert isinstance(record[1], int), "Age should be an integer"
            assert isinstance(record[2], bool), "isStudent should be a boolean"
            assert isinstance(record[3], bool), "isWorker should be a boolean"

    def test_find_students(self, tinysnb_db):
        """Test finding all students."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p:person)
            WHERE p.isStudent = true
            RETURN p.fName, p.age
            ORDER BY p.age
        """
            )
        )

        assert len(result) > 0, "Should find at least one student"
        # All returned records should be students
        for record in result:
            assert record[0] is not None, "Student name should not be null"

    def test_find_working_adults(self, tinysnb_db):
        """Test finding working adults (workers who are not students)."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p:person)
            WHERE p.isWorker = true AND p.isStudent = false
            RETURN p.fName, p.age
            ORDER BY p.age DESC
        """
            )
        )

        assert len(result) >= 0, "Query should execute successfully"

    def test_find_people_in_thirties(self, tinysnb_db):
        """Test finding people in their thirties."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p:person)
            WHERE p.age >= 30 AND p.age < 40
            RETURN p.fName, p.age
            ORDER BY p.age
        """
            )
        )

        # Verify all returned people are in their thirties
        for record in result:
            age = record[1]
            assert 30 <= age < 40, f"Age {age} is not in thirties"


class TestTutorialRelationships:
    """Tests for the 'Social Network Analysis: Relationships' section."""

    def test_who_knows_whom(self, tinysnb_db):
        """Test exploring 'knows' relationships."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p1:person)-[k:knows]->(p2:person)
            RETURN p1.fName, p2.fName, k.date
            ORDER BY p1.fName, p2.fName
        """
            )
        )

        # TinySNB should have knows relationships
        assert len(result) > 0, "Should have 'knows' relationships"

    def test_most_connected_people(self, tinysnb_db):
        """Test finding the most connected people."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p:person)-[k:knows]->(friend:person)
            RETURN p.fName, count(friend) as friend_count
            ORDER BY friend_count DESC
            LIMIT 5
        """
            )
        )

        assert len(result) > 0, "Should find connected people"
        # Results should be ordered by friend_count descending
        for i in range(len(result) - 1):
            assert (
                result[i][1] >= result[i + 1][1]
            ), "Results should be ordered by friend_count DESC"

    def test_most_popular_people(self, tinysnb_db):
        """Test finding people who are known by the most others."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p:person)<-[k:knows]-(friend:person)
            RETURN p.fName, count(friend) as known_by_count
            ORDER BY known_by_count DESC
            LIMIT 5
        """
            )
        )

        assert len(result) > 0, "Should find popular people"

    def test_mutual_friendships(self, tinysnb_db):
        """Test finding mutual friendships (bidirectional relationships)."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p1:person)-[k1:knows]->(p2:person),
                  (p2:person)-[k2:knows]->(p1:person)
            WHERE p1.id < p2.id
            RETURN p1.fName, p2.fName
            ORDER BY p1.fName
        """
            )
        )

        # Query should execute successfully (may or may not have mutual friendships)
        assert isinstance(result, list), "Query should return a list"

    def test_mutual_friendships_with_filter(self, tinysnb_db):
        """Test finding mutual friendships (bidirectional relationships)."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p1:person)-[k1:knows]->(p2:person),
                  (p2:person)-[k2:knows]->(p1:person)
            Where p1.fName = 'Alice' AND p2.fName = 'Bob' AND p1.id < p2.id
            RETURN p1.fName, p2.fName
            ORDER BY p1.fName
        """
            )
        )

        assert result == [["Alice", "Bob"]], "Query should return ['Alice', 'Bob']"


class TestTutorialProfessionalNetworks:
    """Tests for the 'Professional Networks: Work and Education' section."""

    def test_academic_affiliations(self, tinysnb_db):
        """Test querying academic affiliations."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p:person)-[s:studyAt]->(o:organisation)
            RETURN p.fName, o.name, s.year
            ORDER BY s.year DESC
        """
            )
        )

        # Should have some study relationships
        assert len(result) >= 0, "Query should execute successfully"

    def test_most_popular_institutions(self, tinysnb_db):
        """Test finding the most popular educational institutions."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p:person)-[s:studyAt]->(o:organisation)
            RETURN o.name, count(p) as student_count
            ORDER BY student_count DESC
        """
            )
        )

        assert len(result) >= 0, "Query should execute successfully"

    def test_professional_affiliations(self, tinysnb_db):
        """Test querying professional affiliations."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p:person)-[w:workAt]->(o:organisation)
            RETURN p.fName, o.name, w.year
            ORDER BY w.year DESC
        """
            )
        )

        assert len(result) >= 0, "Query should execute successfully"


class TestTutorialAdvancedPatterns:
    """Tests for the 'Advanced Pattern Matching' section."""

    def test_friends_of_friends(self, tinysnb_db):
        """Test finding friends of friends (2-degree connections)."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p1:person)-[:knows]->(mutual:person)-[:knows]->(p2:person)
            WHERE p1.id <> p2.id
            AND NOT (p1)-[:knows]->(p2)
            RETURN p1.fName, p2.fName, mutual.fName
            ORDER BY p1.fName
        """
            )
        )

        # Query should execute successfully (may or may not have results)
        assert isinstance(result, list), "Query should return a list"

    def test_colleagues(self, tinysnb_db):
        """Test finding colleagues (people working at the same organization)."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p1:person)-[:workAt]->(o:organisation)<-[:workAt]-(p2:person)
            WHERE p1.id < p2.id
            RETURN p1.fName, p2.fName, o.name
            ORDER BY o.name
        """
            )
        )

        assert isinstance(result, list), "Query should return a list"

    def test_classmates(self, tinysnb_db):
        """Test finding alumni/classmates."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p1:person)-[s1:studyAt]->(o:organisation)<-[s2:studyAt]-(p2:person)
            WHERE p1.id < p2.id
            RETURN p1.fName, p2.fName, o.name, s1.year, s2.year
            ORDER BY o.name
        """
            )
        )

        assert isinstance(result, list), "Query should return a list"


class TestTutorialNetworkAnalytics:
    """Tests for the 'Social Network Analytics' section."""

    def test_network_statistics(self, tinysnb_db):
        """Test calculating basic network metrics."""
        db, conn = tinysnb_db

        # Get person count
        result = list(conn.execute("MATCH (p:person) RETURN count(p) as person_count"))
        person_count = result[0][0]
        assert person_count == 8, f"Expected 8 people, got {person_count}"

        # Get connection count
        result = list(
            conn.execute("MATCH ()-[k:knows]->() RETURN count(k) as connections")
        )
        actual_connections = result[0][0]
        assert actual_connections > 0, "Should have some connections"

        # Calculate density
        max_possible = person_count * (person_count - 1)
        density = (actual_connections / max_possible) * 100 if max_possible > 0 else 0
        assert 0 <= density <= 100, "Density should be between 0 and 100"

    def test_network_hubs(self, tinysnb_db):
        """Test finding the most connected individuals (network hubs)."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p:person)
            OPTIONAL MATCH (p)-[out:knows]->()
            OPTIONAL MATCH (p)<-[i:knows]-()
            RETURN p.fName,
                   count(DISTINCT out) as outgoing,
                   count(DISTINCT i) as incoming,
                   count(DISTINCT out) + count(DISTINCT i) as total_connections
            ORDER BY total_connections DESC
            LIMIT 5
        """
            )
        )

        assert len(result) > 0, "Should find network hubs"
        # Verify structure
        for record in result:
            assert len(record) == 4, "Each record should have 4 fields"

    def test_age_based_social_analysis(self, tinysnb_db):
        """Test analyzing social connections by age groups."""
        db, conn = tinysnb_db

        result = list(
            conn.execute(
                """
            MATCH (p1:person)-[:knows]->(p2:person)
            WITH p1, p2,
                 CASE
                     WHEN p1.age < 25 THEN "Young (< 25)"
                     WHEN p1.age < 35 THEN "Adult (25-34)"
                     WHEN p1.age < 50 THEN "Middle-aged (35-49)"
                     ELSE "Senior (50+)"
                 END as age_group1,
                 CASE
                     WHEN p2.age < 25 THEN "Young (< 25)"
                     WHEN p2.age < 35 THEN "Adult (25-34)"
                     WHEN p2.age < 50 THEN "Middle-aged (35-49)"
                     ELSE "Senior (50+)"
                 END as age_group2
            RETURN age_group1, age_group2, count(*) as connection_count
            ORDER BY connection_count DESC
        """
            )
        )

        assert len(result) > 0, "Should have age group connections"


class TestTutorialCleanup:
    """Tests for proper resource cleanup."""

    def test_connection_cleanup(self, tmp_path):
        """Test that connections can be properly closed."""
        db_path = str(tmp_path / "cleanup_test")
        db = neug.Database(db_path)
        db.load_builtin_dataset("tinysnb")
        conn = db.connect()

        # Execute a query
        result = list(conn.execute("MATCH (n) RETURN count(n)"))
        assert result[0][0] > 0

        # Close connection and database
        conn.close()
        db.close()

        # Should be able to reopen
        db2 = neug.Database(db_path)
        conn2 = db2.connect()
        result2 = list(conn2.execute("MATCH (n) RETURN count(n)"))
        assert result2[0][0] > 0

        conn2.close()
        db2.close()
