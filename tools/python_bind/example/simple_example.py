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

import os
import shutil
import subprocess
import sys

import neug
from neug.database import Database


def check_bad_top_level_connect_exits_cleanly():
    """Run a subprocess calling neug.connect() to ensure it fails gracefully."""
    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH", "")
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [repo_root, pythonpath]))
    script = (
        "import sys\n"
        "import neug\n"
        "try:\n"
        "    neug.connect()\n"
        "    sys.exit(1)  # Should not succeed\n"
        "except Exception:\n"
        "    sys.exit(0)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, env=env
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Unexpected return code {proc.returncode}; stderr: {proc.stderr}"
        )


if __name__ == "__main__":
    # expect 2 args, csv_data_dir and db_dir
    if len(sys.argv) != 3:
        print("Usage: python simple_example.py <csv_data_dir> <db_dir>")
        sys.exit(1)

    print(f"Neug version {neug.__version__}")
    data_dir = sys.argv[1]
    db_dir = sys.argv[2]
    shutil.rmtree(db_dir, ignore_errors=True)

    print(f"Loading data from {data_dir} into database {db_dir}")

    person_csv = os.path.join(data_dir, "person.csv")
    person_knows_person_csv = os.path.join(data_dir, "person_knows_person.csv")

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

    res = conn.execute("MATCH (n)-[e]-(m) return count(e);")
    for record in res:
        print(record)

    db.close()
    db2 = Database("")
    db2.load_builtin_dataset(dataset_name="tinysnb")
    conn2 = db2.connect()
    res2 = conn2.execute("MATCH(n) return count(n);")
    print(res2.__next__()[0])

    conn2.close()
    db2.close()

    print("Checking that bad top-level connect() exits cleanly...")
    check_bad_top_level_connect_exits_cleanly()
