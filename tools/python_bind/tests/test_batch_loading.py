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
import shutil
import sys
import time
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug.database import Database

logger = logging.getLogger(__name__)


class TestBachLoading(unittest.TestCase):
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

    def test_batch_loading_modern_graph(self):
        # create a tmp directory for the graph
        db_dir = "/tmp/test_batch_loading"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        os.makedirs(db_dir)

        # get env : FLEX_DATA_DIR
        flex_data_dir = os.environ.get("FLEX_DATA_DIR")
        if not flex_data_dir:
            raise Exception("FLEX_DATA_DIR is not set")
        person_csv = os.path.join(flex_data_dir, "person.csv")
        person_knows_person_csv_part1 = os.path.join(
            flex_data_dir, "person_knows_person.part1.csv"
        )
        person_knows_person_csv_part2 = os.path.join(
            flex_data_dir, "person_knows_person.part2.csv"
        )

        db = Database(db_dir, "w")
        conn = db.connect()
        # First create the graph schema
        conn.execute(
            "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")

        # Then load data.
        conn.execute(f'COPY person from "{person_csv}"')
        # TODO(zhanglei,xiaoli): support specifying the starting/ending label name
        conn.execute(
            f'COPY knows from "{person_knows_person_csv_part1}" (from="person", to="person")'
        )
        conn.execute(
            f'COPY knows from "{person_knows_person_csv_part2}" (from="person", to="person")'
        )

        # Then run a query
        res = conn.execute("MATCH (n) return n.id;")
        for record in res:
            print(record)

        res = conn.execute("MATCH (n) return count(*);")
        for record in res:
            print(record)

        conn.close()
        db.close()

        db2 = Database(db_dir, "r")
        conn2 = db2.connect()

        res = conn2.execute("MATCH (n) return count(n);")
        for record in res:
            print(record)

        # get the schema
        result_schema = res.get_result_schema()
        logger.info(f"result schema: {result_schema}")

        res = conn2.execute("MATCH (n)-[e:knows]->(m) return e;")
        for record in res:
            print(record)
            logger.info(f"record: {record}")

        # get the schema
        result_schema = res.get_result_schema()
        logger.info(f"result schema: {result_schema}")

    def test_open_close(self):
        tmp_path = os.environ.get("TMPDIR", "/tmp")
        db_dir = tmp_path + "/test_open_close"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
        db = Database(str(db_dir), "rw")
        db.close()
        db1 = Database(str(db_dir), "r")
        db2 = Database(str(db_dir), "r")

        conn = db2.connect()
        assert db1 is not None and db2 is not None
        db1.close()
        db2.close()

        # Expect runtime error if we try to execute a query on a closed connection
        with self.assertRaises(RuntimeError):
            conn.execute("MATCH (n) return count(n);")

    def test_copy_from(self):
        tmp_path = os.environ.get("TMPDIR", "/tmp")
        db_dir = tmp_path + "/test_copy_from"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        db = Database(str(db_dir), "w")
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE person(name STRING, age INT32, PRIMARY KEY(name));"
        )
        # write to file person.csv
        person_csv = os.path.join(db_dir, "person.csv")
        with open(person_csv, "w") as f:
            f.write("name|age\n")
            f.write("Alice|30\n")
            f.write("Bob|25\n")
            f.write("Charlie|35\n")

        conn.execute(f'COPY person from "{person_csv}"')
        res = conn.execute("MATCH (n) return n.name, n.age;")
        for record in res:
            print(record)

        # rm the file
        shutil.rmtree(db_dir)

        res = conn.execute("MATCH (n: person) WHERE n.age > 34 return n.name;")
        assert res.__next__()[0] == "Charlie"
