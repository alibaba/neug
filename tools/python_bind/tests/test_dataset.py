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
import shutil

import pytest

from neug import Database
from neug.datasets.loader import DatasetLoader
from neug.datasets.loader import get_available_datasets


def test_get_available_datasets():
    """
    Test the get_available_datasets function to ensure it returns the expected dataset information.
    """
    datasets = get_available_datasets()

    assert len(datasets) > 0, "No datasets found"

    # Check the first dataset
    dataset = datasets[0]
    assert dataset.name == "modern_graph", "Dataset name mismatch"
    assert "person" in dataset.node_types, "Node type 'person' not found"
    assert "software" in dataset.node_types, "Node type 'software' not found"
    assert "knows" in dataset.edge_types, "Edge type 'knows' not found"
    assert "created" in dataset.edge_types, "Edge type 'created' not found"


def test_load_modern_graph_static():
    db = Database.from_builtin_dataset(
        "modern_graph"
    )  # should be loaded to a temporary directory
    assert db is not None, "Failed to load modern_graph dataset"
    conn = db.connect()
    res = conn.execute("MATCH (n) RETURN count(n) AS count")
    assert res.__next__()[0] == 6, "Expected 6 nodes in modern_graph dataset"


def test_load_modern_graph_on_existing_db():
    shutil.rmtree(
        "/tmp/test_modern_graph.db", ignore_errors=True
    )  # Clean up any existing database
    db = Database(db_path="/tmp/test_modern_graph.db", mode="w")
    db.load_builtin_dataset("modern_graph")
    assert db is not None, "Failed to load modern_graph dataset into existing database"
    conn = db.connect()
    res = conn.execute("MATCH (n) RETURN count(n) AS count")
    assert res.__next__()[0] == 6, "Expected 6 nodes in modern_graph dataset"
    db.close()


def test_load_non_existent_dataset():
    """
    Test loading a non-existent dataset to ensure it raises an error.
    """
    with pytest.raises(ValueError):
        Database.from_builtin_dataset("non_existent_dataset")


def test_load_tinysnb_dataset():
    db = Database.from_builtin_dataset(
        "tinysnb"
    )  # should be loaded to a temporary directory
    assert db is not None, "Failed to load tinysnb dataset"
    conn = db.connect()
    res = conn.execute("MATCH (n) RETURN count(n)")
    assert res.__next__()[0] == 14, "Expected 6 nodes in modern_graph dataset"


def test_load_builtin_dataset():
    db_dir = "/tmp/test_load_builtin_dataset"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_dir)
    db.load_builtin_dataset(dataset_name="tinysnb")

    conn = db.connect()
    res = conn.execute("MATCH(n) return count(n);")
    assert res.__next__()[0] == 14

    conn.close()
    db.close()

    db2 = Database(db_dir, mode="r")
    conn2 = db2.connect()
    res2 = conn2.execute("MATCH(n) return count(n);")
    assert res2.__next__()[0] == 14
