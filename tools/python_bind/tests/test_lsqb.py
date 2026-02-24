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


class TestLsqb(unittest.TestCase):
    """
    Test running query on a graph that is already created and loaded
    """

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        db_dir = os.environ.get("LSQB_DATA_DIR")
        if db_dir is None:
            raise RuntimeError("LSQB_DATA_DIR environment variable is not set")
        self.db = Database(str(db_dir), "r")
        self.conn = self.db.connect()

    def tearDown(self):
        if self.conn:
            self.conn.close()
        if self.db:
            self.db.close()

    def test_queries(self):

        submit_cypher_query(
            conn=self.conn,
            query="MATCH (n:Country) return n limit 10;",
            lambda_func=ensure_result_cnt_gt_zero,
        )

        submit_cypher_query(
            conn=self.conn,
            query="MATCH (n:Country)<-[:City_isPartOf_Country]-(:City) return count(n);",
            lambda_func=ensure_result_cnt_gt_zero,
        )

        submit_cypher_query(
            conn=self.conn,
            query="MATCH (n:Person) WHERE n.id = 772 return n;",
            lambda_func=lambda result: ensure_result_cnt_eq(result, 1),
        )

        submit_cypher_query(
            conn=self.conn,
            query="MATCH (n: Comment)-[e:Comment_hasCreator_Person]->(p:Person) WHERE p.id = 772 return n.id LIMIT 10;",
            lambda_func=ensure_result_cnt_gt_zero,
        )

        submit_cypher_query(
            conn=self.conn,
            query="MATCH (n:Person)-[e:Person_likes_Post]->(p:Post) WHERE n.id = 772 AND p.id <>"
            "206158439468 return n.id LIMIT 10;",
            lambda_func=lambda result: ensure_result_cnt_gt_zero(result)
            and ensure_result_cnt_eq(result, 10),
        )

        # TODO(xiaoli): plan not correct,
        # submit_cypher_query(
        #     conn=self.conn,
        #     query="" \
        #     "MATCH (:Country)<-[:City_isPartOf_Country]-(:City)<-[:Person_isLocatedIn_City]" \
        #     "-(:Person)<-[:Forum_hasMember_Person]-(:Forum)-[:Forum_containerOf_Post]->(:Post)" \
        #     "<-[Comment_replyOf_Post]-(:Comment)-[:Comment_hasTag_Tag]->(:Tag)-[:Tag_hasType_TagClass]->" \
        #     "(:TagClass) RETURN count(*) as count;",
        #     lambda_func=lambda result: ensure_result_cnt_eq(result, 8773828)
        # )
