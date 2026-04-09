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
        shutil.rmtree(db_dir, ignore_errors=True)
        # get env : FLEX_DATA_DIR
        flex_data_dir = os.environ.get("FLEX_DATA_DIR")
        if not flex_data_dir:
            raise Exception("FLEX_DATA_DIR is not set")
        person_csv = os.path.join(flex_data_dir, "person.csv")
        person_knows_person_csv = os.path.join(
            flex_data_dir, "test_data/person_knows_person.part*.csv"
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
        conn.execute(
            f'COPY knows from "{person_knows_person_csv}" (from="person", to="person")'
        )

        # Then run a query
        res = list(conn.execute("MATCH (n) return n.id;"))
        assert res == [[1], [2], [4], [6]]

        res = list(conn.execute("MATCH (n) return count(*);"))
        assert res[0] == [4]

        conn.close()
        db.close()
        del db
        del conn

        db2 = Database(db_dir, "r")
        conn2 = db2.connect()

        res = conn2.execute("MATCH (n) return count(n);")
        res_data = list(res)
        assert res_data[0] == [4]

        # get the projected columns for debugging
        column_names = res.column_names()
        logger.info(f"columns: {column_names}")

        res = conn2.execute("MATCH (n)-[e:knows]->(m) return count(e);")
        assert list(res)[0] == [2]

        column_names = res.column_names()
        logger.info(f"columns: {column_names}")
        db2.close()

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

        res = conn.execute("MATCH (n: person) WHERE n.age > 34 return n.name;")
        assert res.__next__()[0] == "Charlie"

    def test_loading_with_invalid_vertices(self):
        db_dir = "/tmp/test_loading_with_invalid_vertices"
        shutil.rmtree(db_dir, ignore_errors=True)
        db = Database(db_dir, "w")
        conn = db.connect()
        # First create the graph schema
        conn.execute(
            "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        conn.execute(
            "CREATE NODE TABLE software(id INT64, name STRING, lang STRING, PRIMARY KEY(id));"
        )
        conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
        conn.execute(
            "CREATE REL TABLE created(FROM person TO software, weight DOUBLE);"
        )
        # Then load data.
        # write to file person.csv
        person_csv = os.path.join(db_dir, "person.csv")
        with open(person_csv, "w") as f:
            f.write("id|name|age\n")
            f.write("1|Alice|30\n")
            f.write("2|Bob|25\n")
            f.write("3|Charlie|35\n")
            f.write("4|David|40\n")
        conn.execute(f'COPY person from "{person_csv}"')
        # write to file software.csv
        software_csv = os.path.join(db_dir, "software.csv")
        with open(software_csv, "w") as f:
            f.write("id|name|lang\n")
            f.write("101|GraphX|Scala\n")
            f.write("102|Neo4j|Java\n")
        conn.execute(f'COPY software from "{software_csv}"')
        # write to file person_knows_person.csv
        person_knows_person_csv = os.path.join(db_dir, "person_knows_person.csv")
        with open(person_knows_person_csv, "w") as f:
            f.write("from|to|weight\n")
            f.write("1|2|0.5\n")  # valid
            f.write("2|3|0.6\n")  # valid
            f.write("3|4|0.7\n")  # valid
            f.write("4|1|0.8\n")  # valid
            f.write("5|1|0.9\n")  # invalid src
            f.write("1|6|0.4\n")  # invalid dst
            f.write("7|8|0.3\n")  # invalid src and dst
            f.write("2|4|0.2\n")  # valid
            f.write("3|1|0.1\n")  # valid
            f.write("4|2|0.05\n")  # valid
        conn.execute(
            f'COPY knows from "{person_knows_person_csv}" (from="person", to="person")'
        )
        # write to file person_created_software.csv
        person_created_software_csv = os.path.join(
            db_dir, "person_created_software.csv"
        )
        with open(person_created_software_csv, "w") as f:
            f.write("from|to|weight\n")
            f.write("1|101|0.9\n")  # valid
            f.write("2|102|0.8\n")  # valid
            f.write("3|103|0.7\n")  # invalid dst
            f.write("5|101|0.6\n")  # invalid src
            f.write("4|102|0.5\n")  # valid
        conn.execute(
            f'COPY created from "{person_created_software_csv}" (from="person", to="software")'
        )
        # Then run a query
        res = list(conn.execute("MATCH (n: person) return count(n);"))
        assert res[0] == [4]
        res = list(conn.execute("MATCH (n: software) return count(n);"))
        assert res[0] == [2]
        res = list(
            conn.execute("MATCH (n: person)-[e: knows]->(m: person) return count(e);")
        )
        assert res[0] == [7]
        res = list(
            conn.execute(
                "MATCH (n: person)-[e: created]->(m: software) return count(e);"
            )
        )
        assert res[