#!/usr/bin/env python3

"""Parquet contracts through NeuG SQL and the active backend wiring.

Reuse example_dataset for regular reads. Immutable interoperability fixtures
cover additional boundaries without loading Arrow; older cases still use
PyArrow as an optional test-only producer, as in test_load_array. Expected query
results remain explicit and independent of the file producer. Most cases are
backend-neutral; focused assertions verify that the registered reader and writer
use Carquet after the production switch.
"""

import math
import os
import shutil
from datetime import date
from datetime import datetime
from pathlib import Path

import pytest

from neug import Database

EXTENSION_TESTS_ENABLED = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
pytestmark = pytest.mark.skipif(
    not EXTENSION_TESTS_ENABLED,
    reason="Extension tests disabled by default; set NEUG_RUN_EXTENSION_TESTS=1.",
)

DATASET_DIR = Path(__file__).resolve().parents[3] / "example_dataset"
FIXTURE_DIR = (
    Path(__file__).resolve().parents[3] / "extension" / "parquet" / "tests" / "fixtures"
)


@pytest.fixture
def connection(tmp_path):
    db = Database(db_path=str(tmp_path / "parquet_backend_contract"), mode="w")
    conn = db.connect()
    conn.execute("LOAD PARQUET")
    yield conn
    conn.close()
    db.close()


@pytest.fixture
def type_file(tmp_path):
    """Generate boundary cases absent from the repository's example datasets."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "reader_types.parquet"
    fixed3 = pa.list_(pa.float64(), 3)
    matrix2x2 = pa.list_(pa.list_(pa.int32(), 2), 2)
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int64()),
            "enabled": pa.array([True, False, None, True], pa.bool_()),
            "signed_value": pa.array([-(2**31), 0, 2**31 - 1, None], pa.int32()),
            "unsigned_value": pa.array([0, 2**32 - 1, None, 42], pa.uint32()),
            "score": pa.array([1.25, -2.5, None, 4.5], pa.float64()),
            "label": pa.array(["", "hello", None, "中文"], pa.string()),
            "event_date": pa.array(
                [date(1970, 1, 1), date(1969, 12, 31), None, date(2024, 2, 29)],
                pa.date32(),
            ),
            "timestamp_s": pa.array(
                [
                    datetime(1970, 1, 1),
                    datetime(1969, 12, 31, 23, 59, 59),
                    None,
                    datetime(2023, 6, 15, 12, 30, 45),
                ],
                pa.timestamp("s"),
            ),
            "timestamp_ms": pa.array(
                [
                    datetime(1970, 1, 1, 0, 0, 0, 123000),
                    datetime(1969, 12, 31, 23, 59, 59),
                    None,
                    datetime(2023, 6, 15, 12, 30, 45, 123000),
                ],
                pa.timestamp("ms"),
            ),
            "items": pa.array([[1, None, 3], [], None, [4, 5]], pa.list_(pa.int64())),
            "fixed3": pa.array(
                [
                    [1.0, None, 3.0],
                    [4.0, 5.0, 6.0],
                    [0.0, 0.0, 0.0],
                    [-1.0, -2.0, -3.0],
                ],
                fixed3,
            ),
            "matrix2x2": pa.array(
                [
                    [[1, 2], [3, 4]],
                    [[5, 6], [7, 8]],
                    [[9, None], [11, 12]],
                    [[-1, -2], [-3, -4]],
                ],
                matrix2x2,
            ),
        }
    )
    pq.write_table(
        table,
        path,
        compression="zstd",
        row_group_size=2,
        data_page_size=128,
        store_schema=True,
    )

    return path


@pytest.fixture
def multi_file_dataset(tmp_path):
    """Copy immutable fixtures for multi-path and schema-mismatch coverage."""
    data_dir = tmp_path / "reader_parts"
    data_dir.mkdir()
    first = data_dir / "part-01.parquet"
    second = data_dir / "part-02.parquet"
    source = DATASET_DIR / "tinysnb" / "parquet" / "vPerson.parquet"
    shutil.copyfile(source, first)
    shutil.copyfile(source, second)
    mismatch = DATASET_DIR / "comprehensive_graph" / "parquet" / "node_a.parquet"
    return first, second, mismatch


def test_reader_preserves_types_nulls_and_nested_values(connection, type_file):
    path = type_file
    rows = list(
        connection.execute(
            f'LOAD FROM "{path.as_posix()}" '
            "RETURN id, enabled, signed_value, unsigned_value, score, label, "
            "event_date, timestamp_s, timestamp_ms, items, fixed3, matrix2x2 "
            "ORDER BY id"
        )
    )

    assert rows == [
        [
            1,
            True,
            -(2**31),
            0,
            1.25,
            "",
            date(1970, 1, 1),
            datetime(1970, 1, 1),
            datetime(1970, 1, 1, 0, 0, 0, 123000),
            [1, None, 3],
            [1.0, None, 3.0],
            [[1, 2], [3, 4]],
        ],
        [
            2,
            False,
            0,
            2**32 - 1,
            -2.5,
            "hello",
            date(1969, 12, 31),
            datetime(1969, 12, 31, 23, 59, 59),
            datetime(1969, 12, 31, 23, 59, 59),
            [],
            [4.0, 5.0, 6.0],
            [[5, 6], [7, 8]],
        ],
        [
            3,
            None,
            2**31 - 1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            [0.0, 0.0, 0.0],
            [[9, None], [11, 12]],
        ],
        [
            4,
            True,
            None,
            42,
            4.5,
            "中文",
            date(2024, 2, 29),
            datetime(2023, 6, 15, 12, 30, 45),
            datetime(2023, 6, 15, 12, 30, 45, 123000),
            [4, 5],
            [-1.0, -2.0, -3.0],
            [[-1, -2], [-3, -4]],
        ],
    ]

    filtered = list(
        connection.execute(
            f'LOAD FROM "{path.as_posix()}" WHERE score >= 0 '
            "RETURN id, label ORDER BY id"
        )
    )
    assert filtered == [[1, ""], [4, "中文"]]


@pytest.mark.parametrize("batch_read", [False, True])
def test_reader_preserves_remaining_physical_type_boundaries(connection, batch_read):
    """Read recorded Arrow values without loading Arrow in the test."""
    path = FIXTURE_DIR / "arrow_types.parquet"

    rows = list(
        connection.execute(
            f'LOAD FROM "{path}" '
            f"(batch_read={str(batch_read).lower()}, parquet_batch_rows=1) "
            "RETURN * ORDER BY row_id"
        )
    )

    assert [row[:10] for row in rows] == [
        [1, True, -128, -32768, -(2**31), -(2**63), 0, 0, 0, 0],
        [
            2,
            False,
            127,
            32767,
            2**31 - 1,
            2**63 - 1,
            255,
            65535,
            2**32 - 1,
            2**64 - 1,
        ],
        [3, None, None, None, None, None, None, None, None, None],
        [4, True, 0, 0, 0, 0, 1, 1, 1, 2**63],
    ]
    assert math.isnan(rows[0][10])
    assert rows[1][10] == float("inf")
    assert rows[2][10] == float("-inf")
    assert rows[3][10] is None
    assert math.copysign(1.0, rows[0][11]) == -1.0
    assert math.copysign(1.0, rows[1][11]) == 1.0
    assert rows[2][11] == 1.25
    assert rows[3][11] is None
    assert [row[12:] for row in rows] == [
        [
            "",
            "large",
            date(1969, 12, 31),
            datetime(1969, 12, 31, 23, 59, 59),
            datetime(1969, 12, 31, 23, 59, 59),
            datetime(1969, 12, 31, 23, 59, 59),
            datetime(1969, 12, 31, 23, 59, 59),
            datetime(1969, 12, 31, 23, 59, 59),
            [1, None, 3],
            [1, None],
            [1.0, None, 3.0],
        ],
        [
            "a\0b",
            "",
            date(1970, 1, 1),
            datetime(1970, 1, 1),
            datetime(1970, 1, 1),
            datetime(1970, 1, 1),
            datetime(1970, 1, 1),
            datetime(1970, 1, 1),
            [],
            [],
            [4.0, 5.0, 6.0],
        ],
        [
            "中文🙂",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            [0.0, 0.0, 0.0],
        ],
        [
            None,
            "终",
            date(2024, 2, 29),
            datetime(2023, 11, 14, 22, 13, 20),
            datetime(2023, 11, 14, 22, 13, 20, 123000),
            datetime(2023, 11, 14, 22, 13, 20, 123000),
            datetime(2023, 11, 14, 22, 13, 20, 123000),
            datetime(2023, 11, 14, 22, 13, 20, 123000),
            [4, 5],
            [3],
            [-1.0, -2.0, -3.0],
        ],
    ]


@pytest.mark.parametrize(
    "predicate, expected",
    [
        ("id + 1 > 3", [3, 4]),
        ("score * 2 > 3", [4]),
        ("CAST(id, 'DOUBLE') > 2", [3, 4]),
        ("CAST(id, 'STRING') = '3'", [3]),
        ("CAST(score, 'INT64') = 1", [1]),
        ("id >= 2 AND score * 2 > 3", [4]),
        ("score IS NULL", [3]),
        ("score IS NOT NULL", [1, 2, 4]),
        ("id > 999", []),
        ("NOT (score * 2 > 3)", [1, 2]),
        ("NOT (score * 2 > 3 AND id > 999)", [1, 2, 3, 4]),
        ("NOT (score * 2 > 3 OR id > 999)", [1, 2]),
        ("NOT (id > 999 AND score * 2 > 3)", [1, 2, 3, 4]),
        ("NOT (id > 999 OR score * 2 > 3)", [1, 2]),
        ("CASE WHEN score IS NULL THEN 10 ELSE score END > 3", [3, 4]),
        ("CAST(CAST(id, 'STRING'), 'INT64') + 1 > 3", [3, 4]),
        ("upper(CAST(id, 'STRING')) = '3' AND score IS NULL", [3]),
        (
            "CASE WHEN enabled THEN CAST(score, 'INT64') "
            "ELSE CAST(signed_value, 'INT64') END > 0",
            [1, 3, 4],
        ),
        (
            "id IN [CASE WHEN score IS NULL THEN 3 ELSE 0 END, "
            "CAST(signed_value, 'INT64')]",
            [3],
        ),
    ],
)
@pytest.mark.parametrize("batch_read", [False, True])
def test_reader_preserves_complete_predicates(
    connection, type_file, predicate, expected, batch_read
):
    rows = list(
        connection.execute(
            f'LOAD FROM "{type_file.as_posix()}" '
            f"(batch_read={str(batch_read).lower()}, parquet_batch_rows=1) "
            f"WHERE {predicate} RETURN id ORDER BY id"
        )
    )
    assert rows == [[value] for value in expected]


@pytest.mark.parametrize("batch_read", [False, True])
def test_reader_fallback_binds_current_parameters(connection, type_file, batch_read):
    query = (
        f'LOAD FROM "{type_file.as_posix()}" '
        f"(batch_read={str(batch_read).lower()}, parquet_batch_rows=1) "
        "WHERE CASE WHEN score IS NULL THEN $missing ELSE score END > $minimum "
        "RETURN id ORDER BY id"
    )
    for minimum, expected in [(3, [3, 4]), (10, []), (0, [1, 3, 4])]:
        assert list(
            connection.execute(query, parameters={"missing": 10.0, "minimum": minimum})
        ) == [[value] for value in expected]


def test_reader_fallback_propagates_conversion_errors(connection, type_file):
    with pytest.raises(RuntimeError):
        connection.execute(
            f'LOAD FROM "{type_file.as_posix()}" '
            "WHERE CAST(label, 'INT64') > 0 RETURN id"
        )


def test_reader_resolves_explicit_paths_and_globs(connection, multi_file_dataset):
    first, second, _ = multi_file_dataset
    expected = [
        [0, "Alice"],
        [0, "Alice"],
        [2, "Bob"],
        [2, "Bob"],
        [3, "Carol"],
        [3, "Carol"],
        [5, "Dan"],
        [5, "Dan"],
        [7, "Elizabeth"],
        [7, "Elizabeth"],
        [8, "Farooq"],
        [8, "Farooq"],
        [9, "Greg"],
        [9, "Greg"],
        [10, "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff"],
        [10, "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff"],
    ]
    explicit_paths = f'["{first.as_posix()}", "{second.as_posix()}"]'
    rows = list(
        connection.execute(
            f"LOAD FROM {explicit_paths} "
            "(parallel=false, batch_read=true, parquet_batch_rows=1) "
            "RETURN ID, fName ORDER BY ID"
        )
    )
    assert rows == expected

    pattern = first.parent / "part-*.parquet"
    rows = list(
        connection.execute(
            f'LOAD FROM GLOB("{pattern.as_posix()}") '
            "(parallel=true, batch_read=false, parquet_batch_rows=1) "
            "RETURN ID, fName ORDER BY ID"
        )
    )
    assert rows == expected


def test_reader_reports_path_and_cross_file_schema_errors(
    connection, multi_file_dataset, tmp_path
):
    first, _, mismatch = multi_file_dataset
    with pytest.raises(RuntimeError, match="schema"):
        list(
            connection.execute(
                f'LOAD FROM ["{first.as_posix()}", "{mismatch.as_posix()}"] '
                "RETURN ID"
            )
        )

    with pytest.raises(RuntimeError, match="No Parquet files match"):
        connection.execute(
            f'LOAD FROM GLOB("{(tmp_path / "missing-*.parquet").as_posix()}") '
            "RETURN *"
        )


@pytest.mark.parametrize(
    "options,error",
    [
        ("parquet_batch_rows=0", "PARQUET_BATCH_ROWS must be between"),
        ("batch_size=0", "BATCH_SIZE must be positive"),
    ],
)
def test_registered_reader_enforces_carquet_option_bounds(connection, options, error):
    path = DATASET_DIR / "tinysnb" / "parquet" / "vPerson.parquet"
    with pytest.raises(RuntimeError, match=error):
        connection.execute(f'LOAD FROM "{path}" ({options}) RETURN ID')


@pytest.mark.parametrize(
    "compression,page_version",
    [
        ("none", "1.0"),
        ("snappy", "1.0"),
        ("gzip", "2.0"),
        ("zstd", "2.0"),
    ],
)
def test_reader_preserves_encodings_pages_and_row_groups(
    connection, tmp_path, compression, page_version
):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "reader_encoding.parquet"
    table = pa.table(
        {
            "id": pa.array(range(1, 13), pa.int64()),
            "flag": pa.array(
                [
                    True,
                    False,
                    True,
                    False,
                    True,
                    True,
                    False,
                    True,
                    True,
                    True,
                    False,
                    False,
                ],
                pa.bool_(),
            ),
            "measurement": pa.array(
                [
                    1.25,
                    -2.5,
                    3.75,
                    100.5,
                    -200.25,
                    0.125,
                    42.0,
                    -7.5,
                    0.0,
                    1.0,
                    2.0,
                    3.0,
                ],
                pa.float32(),
            ),
            "category": pa.array(
                ["a", "b", "a", "b", "a", "c", "a", "b", "c", "a", "b", "c"],
                pa.string(),
            ),
        }
    )
    pq.write_table(
        table,
        path,
        compression=compression,
        row_group_size=5,
        data_page_size=64,
        data_page_version=page_version,
        use_dictionary=["category"],
        use_byte_stream_split=["measurement"],
        store_schema=True,
    )
    # Ensure producer upgrades do not silently remove the encoding coverage.
    metadata = pq.read_metadata(path)
    assert metadata.num_row_groups == 3
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        assert "BYTE_STREAM_SPLIT" in group.column(2).encodings
        assert "RLE_DICTIONARY" in group.column(3).encodings
    rows = list(
        connection.execute(
            f'LOAD FROM "{path.as_posix()}" '
            "RETURN id, flag, measurement, category ORDER BY id"
        )
    )

    assert rows == [
        [1, True, 1.25, "a"],
        [2, False, -2.5, "b"],
        [3, True, 3.75, "a"],
        [4, False, 100.5, "b"],
        [5, True, -200.25, "a"],
        [6, True, 0.125, "c"],
        [7, False, 42.0, "a"],
        [8, True, -7.5, "b"],
        [9, True, 0.0, "c"],
        [10, True, 1.0, "a"],
        [11, False, 2.0, "b"],
        [12, False, 3.0, "c"],
    ]


def test_reader_preserves_external_delta_encoding_fixture(connection):
    """Exercise mixed codecs and delta encodings without loading Arrow."""
    path = FIXTURE_DIR / "arrow_delta_encodings.parquet"
    rows = list(
        connection.execute(
            f'LOAD FROM "{path}" WHERE id >= 3 AND id <= 7 '
            "RETURN text_length, text_prefix, signed_value, id ORDER BY id"
        )
    )
    assert rows == [
        [
            f"length-{row + 17}-" + "x" * ((row + 17) % 7),
            f"shared-prefix-{(row + 17) // 4}-value-{row + 17}",
            (row + 17) * 37 - 911,
            row,
        ]
        for row in range(3, 8)
    ]


def test_reader_preserves_external_byte_stream_split_lz4_fixture(connection):
    path = FIXTURE_DIR / "arrow_byte_stream_split.parquet"
    rows = list(connection.execute(f'LOAD FROM "{path}" RETURN * ORDER BY id'))
    assert len(rows) == 12
    assert math.isnan(rows[0][1])
    assert rows[1][1] == float("inf")
    assert rows[2][1] == float("-inf")
    assert math.copysign(1.0, rows[3][1]) == -1.0
    assert rows[4][1:] == [0.0, 3.75]
    assert rows[-1] == [12, -200.25, float("-inf")]


def test_reader_handles_empty_files_and_all_null_row_groups(connection):
    empty_path = FIXTURE_DIR / "arrow_empty.parquet"
    assert list(connection.execute(f'LOAD FROM "{empty_path}" RETURN *')) == []

    null_path = FIXTURE_DIR / "arrow_all_null_groups.parquet"
    rows = list(
        connection.execute(
            f'LOAD FROM "{null_path}" WHERE text IS NULL '
            "RETURN id, text, numbers ORDER BY id"
        )
    )
    assert rows == [[row, None, None] for row in range(1, 7)]


@pytest.mark.parametrize(
    "dataset,query,expected",
    [
        (
            "tinysnb/parquet/vPerson.parquet",
            "WHERE age > 30 AND isStudent RETURN ID, fName ORDER BY ID",
            [[0, "Alice"]],
        ),
        (
            "comprehensive_graph/parquet/node_a.parquet",
            "WHERE i64_property < 0 AND u32_property = 0 "
            "RETURN id, i64_property, u64_property, date_property ORDER BY id",
            [[1, -(2**63), 0, date(2023, 6, 22)]],
        ),
    ],
)
def test_reader_preserves_projection_and_filter_semantics(
    connection, dataset, query, expected
):
    path = DATASET_DIR / dataset
    rows = list(connection.execute(f'LOAD FROM "{path.as_posix()}" {query}'))
    assert rows == expected


def test_writer_preserves_options_and_nested_values(connection, tmp_path):
    path = tmp_path / "writer_contract.parquet"
    connection.execute(
        "CREATE NODE TABLE ContractRow("
        "id INT64, label STRING, values INT64[], PRIMARY KEY(id));"
    )
    connection.execute(
        "CREATE (:ContractRow {"
        "id: 1, label: 'alpha', values: CAST([1, 2], 'INT64[]')});"
    )
    connection.execute(
        "CREATE (:ContractRow {" "id: 2, label: '中文', values: CAST([], 'INT64[]')});"
    )
    connection.execute(
        f"COPY (MATCH (r:ContractRow) "
        f"RETURN r.id, r.label, r.values ORDER BY r.id) TO '{path}' "
        "(COMPRESSION='zstd', ROW_GROUP_SIZE=1024, "
        "DICTIONARY_ENCODING=true)"
    )

    rows = list(connection.execute(f'LOAD FROM "{path}" RETURN * ORDER BY "r.id"'))
    assert rows == [[1, "alpha", [1, 2]], [2, "中文", []]]


def test_writer_applies_options_through_registered_function(connection, tmp_path):
    connection.execute(
        "CREATE NODE TABLE WriterOptionRow("
        "id INT64, category STRING, PRIMARY KEY(id));"
    )
    # Repeat each category inside its row group so Carquet keeps dictionary
    # encoding instead of intentionally falling back to PLAIN for unique data.
    for row, category in [(1, "a"), (2, "a"), (3, "b"), (4, "b")]:
        connection.execute(
            f"CREATE (:WriterOptionRow {{id: {row}, category: '{category}'}});"
        )

    for dictionary in (True, False):
        path = tmp_path / f"writer_options_{dictionary}.parquet"
        connection.execute(
            f"COPY (MATCH (r:WriterOptionRow) "
            f"RETURN r.id, r.category ORDER BY r.id) TO '{path}' "
            "(COMPRESSION='zstd', ROW_GROUP_SIZE=2, "
            f"DICTIONARY_ENCODING={str(dictionary).lower()})"
        )

        contents = path.read_bytes()
        assert contents[:4] == b"PAR1"
        assert contents[-4:] == b"PAR1"
        assert b"Carquet" in contents
        assert list(
            connection.execute(f'LOAD FROM "{path}" RETURN * ORDER BY "r.id"')
        ) == [[1, "a"], [2, "a"], [3, "b"], [4, "b"]]
