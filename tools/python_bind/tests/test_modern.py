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

import logging
import os
import sys
import time
import unittest

from conftest import ensure_result_cnt_eq
from conftest import ensure_result_cnt_gt_zero
from conftest import submit_cypher_query

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug.database import Database

logger = logging.getLogger(__name__)


class TestModernBench(unittest.TestCase):
    """
    Test Running Modern Benchmark queries.
    """

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        db_dir = os.environ.get("MODERN_DATA_DIR")
        if db_dir is None:
            raise RuntimeError("MODERN_DATA_DIR environment variable is not set")
        self.db = Database(str(db_dir), "r")
        self.conn = self.db.connect()

    def tearDown(self):
        if self.conn:
            self.conn.close()
        if self.db:
            self.db.close()

    # def test_starts_with(self):
    #     result = self.conn.execute(
    #         "Match (n) Where n.name starts with 'mar' Return n.name"
    #     )
    #     for record in result:
    #         assert record[0] == "marko", f"Expected value 'marko', got {record[0]}"

    # def test_ends_with(self):
    #     result = self.conn.execute(
    #         "Match (n) Where n.name ends with 'das' Return n.name"
    #     )
    #     for record in result:
    #         assert record[0] == "vadas", f"Expected value 'vadas', got {record[0]}"

    def test_upper(self):
        """
        Test the UPPER() function on Person names.
        """
        result = self.conn.execute("MATCH (n:person) RETURN UPPER(n.name)")
        expected = {"MARKO", "VADAS", "JOSH", "PETER"}
        actual = {record[0] for record in result}
        assert actual == expected, f"Expected {expected}, got {actual}"

    def test_lower_constant(self):
        """
        Test the LOWER() function on constant strings.
        """
        result = self.conn.execute(
            "RETURN LOWER('MARKO'), LOWER('VaDaS'), LOWER('Josh'), LOWER('PETER')"
        )
        row = next(iter(result))
        expected = ("marko", "vadas", "josh", "peter")
        assert tuple(row) == expected, f"Expected {expected}, got {row}"

    def test_reverse(self):
        """
        Test the REVERSE() function on Person names.
        """
        result = self.conn.execute("MATCH (n:person) RETURN n.name, REVERSE(n.name)")
        expected_map = {
            "marko": "okram",
            "vadas": "sadav",
            "josh": "hsoj",
            "peter": "retep",
        }
        for record in result:
            original, reversed_str = record
            expected = expected_map[original]
            assert (
                reversed_str == expected
            ), f"Expected {expected} for {original}, got {reversed_str}"
