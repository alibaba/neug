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

import pytest
from conftest import wait_for_server_ready

from neug.database import Database
from neug.session import Session


@pytest.fixture(params=["embedded", "tp"])
def scan_connection(request, tmp_path):
    db = Database(
        str(tmp_path / "label_holes"), max_thread_num=1, checkpoint_on_close=False
    )
    conn = None
    try:
        if request.param == "embedded":
            conn = db.connect()
        else:
            endpoint = db.serve(
                host="127.0.0.1",
                port=request.getfixturevalue("unused_tcp_port"),
                blocking=False,
                thread_num=1,
                auto_compaction=False,
            )
            wait_for_server_ready(endpoint)
            conn = Session.open(endpoint, timeout="10s")
        yield conn
    finally:
        if conn is not None:
            conn.close()
        if request.param == "tp":
            db.stop_serving()
        db.close()


@pytest.mark.parametrize(
    "labels,dropped",
    [
        (("Dummy", "Domain"), ()),
        (("Dummy", "Domain"), ("Dummy",)),
        (("Dummy", "Domain", "Other"), ("Dummy",)),
        (("First", "Dummy", "Domain", "Other"), ("Dummy",)),
        (("Dummy", "Domain", "Middle", "Other"), ("Dummy", "Middle")),
    ],
    ids=["no-hole", "lower-hole", "multiple-survivors", "interior-hole", "two-holes"],
)
def test_property_scans_preserve_label_ids(scan_connection, labels, dropped):
    """Issue #1253: tombstones must not shift property columns to other labels."""
    conn = scan_connection
    for label in labels:
        conn.execute(
            f"CREATE NODE TABLE {label}(id STRING, name STRING, PRIMARY KEY(id));"
        )
    for label in dropped:
        conn.execute(f"DROP TABLE {label};")
    surviving = [label for label in labels if label not in dropped]
    for label in surviving:
        conn.execute(f"CREATE (:{label} {{id: 'one', name: '{label}-probe'}});")

    for label in surviving:
        assert list(conn.execute(f"MATCH (n:{label} {{id: 'one'}}) RETURN n.id;")) == [
            ["one"]
        ]
        assert list(
            conn.execute(
                f"MATCH (n:{label}) WHERE CONTAINS(n.name, '{label}-probe') "
                "RETURN count(n);"
            )
        ) == [[1]]
        # A function in the projection also exercises record property access.
        assert list(conn.execute(f"MATCH (n:{label}) RETURN upper(n.name);")) == [
            [f"{label.upper()}-PROBE"]
        ]

    assert list(
        conn.execute(
            "MATCH (n) WHERE CONTAINS(n.name, 'probe') RETURN n.name ORDER BY n.name;"
        )
    ) == [[f"{label}-probe"] for label in sorted(surviving)]
