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


class TestICBench(unittest.TestCase):
    """
    Test Running LDBC Benchmark queries.
    """

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        db_dir = os.environ.get("LDBC_DATA_DIR")
        if db_dir is None:
            raise RuntimeError("LDBC_DATA_DIR environment variable is not set")
        self.db = Database(str(db_dir), "r")
        self.conn = self.db.connect()

    def tearDown(self):
        if self.conn:
            self.conn.close()
        if self.db:
            self.db.close()

    # Mini queries are designed to test individual functionalities within LDBC queries.
    # Once all functionalities are fully supported, these tests will be replaced by the actual IC benchmark queries.
    def test_min_queries(self):

        # test START_NODE and END_NODE
        # todo(engine): Engine Abort
        # submit_cypher_query(
        #     conn=self.conn,
        #     query="Match (n:PERSON {id: 933})-[k:KNOWS]->(m:PERSON {id: 2199023256077})"
        #     " Return START_NODE(k) as n1, END_NODE(k) as n2;",
        #     lambda_func=ensure_result_cnt_gt_zero,
        # )

        # test LENGTH
        result = self.conn.execute(
            "Match (n:PERSON {id: 933})-[k:KNOWS*1..3]->(m) Return LENGTH(k) as len Order by len Limit 1"
        )
        for record in result:
            assert record[0] == 1, f"Expected value 1, got {record[0]}"

        # test undirected and unweighted shortest path
        # todo(engine): Error thrown
        # result = self.conn.execute(
        #     "Match (n:PERSON {id: 933})-[k:KNOWS* SHORTEST  1..3]-(m:PERSON {id: 2199023256668}) Return LENGTH(k) Limit 1;"
        # )
        # for record in result:
        #     assert record[0] == 1, f"Expected value 1, got {record[0]}"

        # test nodes and rels
        # todo(engine): Error thrown
        submit_cypher_query(
            conn=self.conn,
            query="Match (n:PERSON {id: 933})-[k:KNOWS*1..3]-(m:PERSON {id: 2199023256668})"
            " Return nodes(k) as n1, rels(k) as n2 LIMIT 1;",
            lambda_func=ensure_result_cnt_gt_zero,
        )

        # test properties
        # todo(engine): Error thrown
        # submit_cypher_query(
        #     conn=self.conn,
        #     query="Match (n:PERSON {id: 933})-[k:KNOWS*1..3]-(m:PERSON {id: 2199023256668})"
        #     " Return properties(nodes(k), 'firstName') as n1, properties(rels(k),'creationDate') as n2 LIMIT 1;",
        #     lambda_func=ensure_result_cnt_gt_zero,
        # )

        # test case expression
        result = self.conn.execute(
            "Match (n:PERSON {id: 933}) Return CASE WHEN n.id > 0 THEN n.id ELSE 0 END"
        )
        for record in result:
            assert record[0] == 933, f"Expected value 933, got {record[0]}"

        # test to_tuple function
        # todo(engine): VariableKeys is deprecated by ToTuple in PB.
        submit_cypher_query(
            conn=self.conn,
            query="Match (n:PERSON {id: 933})"
            " Return [n.firstName, n.gender, n.birthday] as n2 LIMIT 1;",
            lambda_func=ensure_result_cnt_gt_zero,
        )

        # test dummy scan before projection
        result = self.conn.execute("Return 1002")
        for record in result:
            assert record[0] == 1002, f"Expected value 1002, got {record[0]}"
