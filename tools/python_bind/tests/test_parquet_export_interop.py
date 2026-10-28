"""Read Carquet export fixtures with an independent Python implementation.

The C++ producer exercises QueryResponse inputs, including boundaries that SQL
cannot represent. It links Carquet only; pyarrow is a test-only Python dependency.
"""

import os
import subprocess
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

pytestmark = pytest.mark.skipif(
    os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower()
    not in ("1", "true", "yes", "on"),
    reason="Extension tests disabled by default; set NEUG_RUN_EXTENSION_TESTS=1.",
)


@pytest.fixture(scope="module")
def exports(tmp_path_factory):
    repo = Path(__file__).resolve().parents[3]
    build = Path(os.environ.get("NEUG_BUILD_DIR", repo / "build"))
    suffix = ".exe" if os.name == "nt" else ""
    binary = Path(
        os.environ.get(
            "NEUG_PARQUET_EXPORT_TEST_BINARY",
            build / "extension/parquet" / f"parquet_carquet_export_test{suffix}",
        )
    )
    assert binary.is_file(), f"Build parquet_carquet_export_test first: {binary}"
    directory = tmp_path_factory.mktemp("carquet_exports")
    env = os.environ.copy()
    # Always run the complete producer, even under a sharded/repeated test job.
    for key in ("GTEST_TOTAL_SHARDS", "GTEST_SHARD_INDEX", "GTEST_SHARD_STATUS_FILE"):
        env.pop(key, None)
    env["NEUG_PARQUET_EXPORT_TEST_DIR"] = str(directory)
    result = subprocess.run(
        [
            str(binary.resolve()),
            "--gtest_filter=CarquetExportWriterTest.*",
            "--gtest_repeat=1",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return directory


def files(exports, name, count=1):
    paths = sorted(
        exports.glob(f"{name}-*.parquet"),
        key=lambda path: int(path.stem.rsplit("-", 1)[1]),
    )
    assert len(paths) == count, (name, paths)
    return paths


def read(path):
    return pq.read_table(path).combine_chunks()


def values(table, column):
    array = table.column(column).combine_chunks()
    if pa.types.is_date32(array.type):
        array = array.cast(pa.int32())
    elif pa.types.is_timestamp(array.type):
        array = array.cast(pa.int64())
    return array.to_pylist()


def assert_flat(table, embedded_nul=False):
    expected = [
        [-2, -1, None, 1, 2],
        [0, 1, None, 3, 2**32 - 1],
        [-9, -8, None, 8, 9],
        [0, 1, None, 3, 2**64 - 1],
        [-1.5, 0.0, None, 2.5, 4.0],
        [-4.5, -0.0, None, 3.5, 9.0],
        [True, False, None, False, True],
        ["", "a\0b" if embedded_nul else "hello", None, "中文", "tail"],
        [-1, 0, None, 1, 2],
        [-1000001, -1, None, 1, 1000001],
    ]
    expected_types = [
        pa.int32(),
        pa.uint32(),
        pa.int64(),
        pa.uint64(),
        pa.float32(),
        pa.float64(),
        pa.bool_(),
        pa.string(),
        pa.date32(),
        pa.timestamp("ms", "UTC"),
    ]
    assert table.num_rows == 5
    for column, (expected_values, expected_type) in enumerate(
        zip(expected, expected_types)
    ):
        assert table.field(column).type == expected_type
        assert values(table, column) == expected_values


def test_flat_values(exports):
    (path,) = files(exports, "WritesFlatValues")
    table = read(path)
    assert table.num_columns == 10
    assert_flat(table)
    metadata = pq.read_metadata(path)
    assert metadata.num_rows == 5
    assert metadata.num_row_groups == 3
    for group in range(3):
        assert metadata.row_group(group).column(0).compression == "ZSTD"


def test_compression_dictionary_and_row_groups(exports):
    paths = files(exports, "HonorsCompressionDictionaryAndRowGroupOptions", 8)
    for index, path in enumerate(paths):
        codec = ("UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD")[index // 2]
        dictionary = bool(index % 2)
        metadata = pq.read_metadata(path)
        assert metadata.num_row_groups == 3
        chunk = metadata.row_group(0).column(0)
        assert chunk.compression == codec
        assert (
            bool({"RLE_DICTIONARY", "PLAIN_DICTIONARY"} & set(chunk.encodings))
            == dictionary
        )
        assert values(read(path), 0) == [
            f"repeated-value-{row % 3}" for row in range(96)
        ]


def test_temporal_boundaries(exports):
    (path,) = files(exports, "PreservesTimestampMillisecondsAndNormalizesDatePayloads")
    table = read(path)
    assert table.field(0).type == pa.timestamp("ms", "UTC")
    assert table.field(1).type == pa.date32()
    assert values(table, 0) == [-1001, -1, 0, 946684800000, 946684800123]
    assert values(table, 1) == [-(2**31), -1, 0, 10957, 2**31 - 1]


def test_all_null_row_groups(exports):
    for path in files(exports, "WritesAllNullScalarRowGroups", 2):
        table = read(path)
        assert (table.num_rows, table.num_columns) == (5, 10)
        for column in table.columns:
            assert column.null_count == 5


def test_column_name_precedence(exports):
    (path,) = files(exports, "ResolvesResponseEntryAndFallbackColumnNames")
    assert read(path).column_names[:3] == ["response_name", "entry_name", "col_2"]


def test_validity_across_bytes_and_row_groups(exports):
    for path in files(exports, "PreservesValidityAcrossByteAndRowGroupBoundaries", 2):
        table = read(path)
        assert table.num_rows == 19
        assert values(table, 0) == [row * 17 if row % 3 else None for row in range(19)]
        assert values(table, 1) == [
            row % 2 == 0 if row % 3 else None for row in range(19)
        ]
        assert values(table, 2) == [
            f"value-{row}" if row % 3 else None for row in range(19)
        ]


def test_multiple_pages(exports):
    (path,) = files(exports, "WritesMultiplePagesAcrossExplicitRowGroups")
    metadata = pq.read_metadata(path)
    assert metadata.num_row_groups == 3
    assert [metadata.row_group(i).num_rows for i in range(3)] == [300000, 300000, 1]
    assert path.stat().st_size > 4 * 1024 * 1024
    assert values(read(path), 0) == [row * 17 - 5 for row in range(600001)]


def test_nested_and_special_values(exports):
    (path,) = files(exports, "WritesNestedAndSpecialValues")
    metadata = pq.read_metadata(path)
    assert (metadata.num_row_groups, metadata.num_columns) == (2, 8)
    table = read(path)
    assert (table.num_rows, table.num_columns) == (4, 7)
    assert values(table, 0) == [[1, None, 3], [], None, [4, 5]]
    assert pa.types.is_list(table.field(1).type)
    assert values(table, 1) == [
        [[100, 101], [None, 103]],
        None,
        [[108, 109], None],
        [[112, 113], [114, 115]],
    ]
    record = table.column(2).combine_chunks()
    assert record.is_null().to_pylist() == [False, True, False, False]
    assert record.field(0).to_pylist()[0] == 7
    assert record.field(0).is_null()[2].as_py()
    assert record.field(1)[2].as_py() == "c"
    assert record.field(1).is_null()[3].as_py()
    for column in range(3, 7):
        assert table.column(column).null_count == 1
        assert table.column(column)[2].as_py() is None
    assert values(table, 3) == ["P0D", "P1D", None, "P3D"]
    assert values(table, 6) == ['{"path":0}', '{"path":1}', None, '{"path":3}']


def test_named_nested_fields(exports):
    (path,) = files(exports, "PreservesNamedStructFieldsInNestedValues")
    table = read(path)
    assert table.to_pylist() == [
        {
            "profile": {"address": {"city": "hz"}, "age": 30},
            "history": [{"city": "sh", "year": 2025}],
        }
    ]


def test_mixed_nested_scalar_types(exports):
    paths = files(exports, "PreservesAllScalarTypesInMixedNestedBatches", 9)
    expected = read(paths[0])
    assert_flat(expected, embedded_nul=True)
    for path in paths[1:]:
        table = read(path)
        assert (table.num_rows, table.num_columns) == (5, 13)
        record = table.column(10).combine_chunks()
        records = table.column(11).combine_chunks()
        outer = table.column(12).combine_chunks()
        inner = outer.field(0)
        assert record.is_null()[1].as_py()
        assert outer.is_null()[1].as_py()
        for array in (records, inner):
            assert array.is_null()[2].as_py()
            assert len(array[0].as_py()) == 0
            assert len(array[4].as_py()) == 2
        assert records.values.is_null()[1].as_py()
        for column in range(10):
            assert table.column(column).equals(expected.column(column))
            assert record.field(column).type == expected.field(column).type
            assert records.values.field(column).type == expected.field(column).type
            for row in (0, 2, 3, 4):
                scalar = expected.column(column)[row]
                assert record.field(column)[row].equals(scalar)
                assert records.values.field(column)[row].equals(scalar)


def test_nested_validity_boundaries(exports):
    (path,) = files(exports, "PreservesNestedValidityAcrossBytesAndRowGroups")
    assert pq.read_metadata(path).num_row_groups == 7
    expected = []
    for row in range(19):
        if row % 4 == 0 or 3 <= row < 6:
            expected.append(None)
        else:
            expected.append(
                [
                    index % 2 == 0 if index % 5 else None
                    for index in range(row * 3, row * 3 + 3)
                ]
            )
    assert values(read(path), 0) == expected


def test_special_strings(exports):
    (path,) = files(exports, "WritesSpecialStringsWithoutNestedColumns")
    assert read(path).to_pydict() == {
        "duration": ["PT0S", None, "PT2S"],
        "vertex": ["v0", None, "v2"],
        "edge": ["e0", None, "e2"],
        "path": ["p0", None, "p2"],
    }
