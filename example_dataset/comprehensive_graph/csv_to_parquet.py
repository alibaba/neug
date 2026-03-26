#!/usr/bin/env python3
# Copyright 2020 Alibaba Group Holding Limited.
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

"""
Preprocess comprehensive_graph CSV files into Parquet format.

Outputs are written to example_dataset/comprehensive_graph/parquet/.

Notes:
  - interval_property is kept as a Parquet STRING column (pyarrow CSV inference).
    NeuG's INTERVAL text format ("1year2months3days...") has no writable Parquet
    native interval equivalent via pyarrow, so string is the simplest lossless form.
  - All other columns (INT32/INT64/UINT32/UINT64/FLOAT/DOUBLE/STRING/DATE/DATETIME)
    are preserved with explicitly typed Arrow columns.
"""

import os
import sys

try:
    import pyarrow as pa
    import pyarrow.csv as pa_csv
    import pyarrow.parquet as pq
except ImportError:
    print("Error: pyarrow is required. Install with: pip install pyarrow")
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "parquet")

# Edge files: all have duplicate src/dst column both named 'node_a.id'.
# Rename the first two columns to src_id / dst_id for uniqueness in Parquet.
EDGE_FILES = {
    "rel_a.csv",
}

# All CSV files to convert
CSV_FILES = [
    "node_a.csv",
    "rel_a.csv",
]


def convert(csv_filename: str) -> None:
    csv_path = os.path.join(SCRIPT_DIR, csv_filename)
    parquet_filename = os.path.splitext(csv_filename)[0] + ".parquet"
    parquet_path = os.path.join(OUTPUT_DIR, parquet_filename)

    read_opts = pa_csv.ReadOptions()
    parse_opts = pa_csv.ParseOptions(delimiter="|")
    # timestamp_parsers: let Arrow auto-detect date/datetime columns.
    # Explicitly type columns where CSV auto-inference picks wrong types:
    #   - i32_property: inferred as int64 (pyarrow defaults all integers to int64)
    #   - u32/u64_property: inferred as double (values overflow int64)
    #   - f32_property: inferred as double (pyarrow has no float32 inference)
    node_column_types = {
        "i32_property": pa.int32(),
        "u32_property": pa.uint32(),
        "u64_property": pa.uint64(),
        "f32_property": pa.float32(),
    } if csv_filename in ("node_a.csv", "node_b.csv") else {}
    edge_column_types = {
        "i32_weight": pa.int32(),
        # datetime_weight values are date-only strings ("2023-05-17"); pyarrow infers
        # date32[day] by default, but DT_DATETIME requires timestamp[ms].
        "datetime_weight": pa.timestamp("ms"),
    } if csv_filename in EDGE_FILES else {}
    column_types = {**node_column_types, **edge_column_types}
    convert_opts = pa_csv.ConvertOptions(
        timestamp_parsers=["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"],
        column_types=column_types,
    )

    table = pa_csv.read_csv(
        csv_path,
        read_options=read_opts,
        parse_options=parse_opts,
        convert_options=convert_opts,
    )

    # Rename duplicate src/dst columns in edge files
    if csv_filename in EDGE_FILES:
        old_names = table.schema.names
        new_names = ["src_id" if i == 0 else "dst_id" if i == 1 else n
                     for i, n in enumerate(old_names)]
        table = table.rename_columns(new_names)
        print(f"  renamed src/dst columns: {old_names[:2]} -> ['src_id', 'dst_id']")

    # interval_property is kept as-is: pyarrow CSV infers it as string
    pq.write_table(table, parquet_path)
    print(f"  {csv_filename} -> parquet/{parquet_filename}  ({table.num_rows} rows, {len(table.schema)} cols)")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}\n")

    for csv_file in CSV_FILES:
        csv_path = os.path.join(SCRIPT_DIR, csv_file)
        if not os.path.exists(csv_path):
            print(f"[SKIP] {csv_file} not found")
            continue
        print(f"Converting {csv_file} ...")
        convert(csv_file)

    print("\nDone.")


if __name__ == "__main__":
    main()
