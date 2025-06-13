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

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from nexg.database import Database

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
        pass

    def tearDown(self):
        pass

    def test_open_graph(self):
        db_dir = os.environ.get("LSQB_DATA_DIR")
        if db_dir is None:
            raise RuntimeError("LSQB_DATA_DIR environment variable is not set")
        db = Database(str(db_dir), "r")
        conn = db.connect()
        result = conn.execute("MATCH (n:Country) return n limit 10;")
        assert result is not None
        for record in result:
            print("Record:", record)
        assert len(result) > 0

        result = conn.execute(
            "MATCH (n:Country)<-[:City_isPartOf_Country]-(:City) return count(n);"
        )
        assert result is not None
        for record in result:
            print("Record:", record)
        assert len(result) > 0

        result = conn.execute("MATCH (n:Person) WHERE n.id = 772 return n;")
        assert result is not None
        res_count = 0
        for record in result:
            print("Record:", record)
            res_count += 1
        assert res_count == 1, "Expected exactly one record for Person with id 772"

        result = conn.execute(
            "MATCH (n: Comment)-[e:Comment_hasCreator_Person]->(p:Person) WHERE p.id = 772 return n.id LIMIT 10;"
        )
        assert result is not None
        res_count = 0
        for record in result:
            print("Record:", record)
            res_count += 1
        assert res_count > 0, "Expected at least one comment for Person with id 772"

        result = conn.execute(
            "MATCH (n:Person)-[e:Person_likes_Post]-(p:Post) WHERE n.id = 772 AND p.id <> 206158439468 return n.id LIMIT 10;"
        )
        assert result is not None
        res_count = 0
        for record in result:
            print("Record:", record)
            res_count += 1
        assert (
            res_count > 0 and res_count <= 10
        ), "Expected at least one post liked by Person with id 772"

        conn.close()
        db.close()
