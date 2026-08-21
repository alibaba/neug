# FTS Extension

The FTS extension provides SQLite FTS5-backed full-text indexes and BM25
Top-K search, including scalar and graph filtering, online index maintenance,
and checkpoint recovery.

For installation, DDL, query syntax, filtering, and operational behavior, see
the [Full-Text Search Extension](../../doc/source/extensions/fts_search.md)
user documentation. This README covers the source-tree prerequisites, build,
local loading, and tests for the extension.

## Prerequisites

Initialize the bundled SQLite source before configuring the extension:

```bash
git submodule update --init third_party/sqlite
```

## Build

From the NeuG repository root, build NeuG, the Python binding, and the FTS
extension with:

```bash
cd tools/python_bind
BUILD_EXTENSIONS=fts make build
```

The extension library is written to:

```text
build/extension/fts/libfts.neug_extension
```

When using the in-repository Python binding, the loader discovers the root
build directory automatically, so the locally built extension can be loaded
with:

```cypher
LOAD fts;
```

## Test

Build with tests enabled, then run the C++ extension test:

```bash
cd tools/python_bind
BUILD_TEST=ON BUILD_EXTENSIONS=fts make build
../../build/extension/fts/fts_extension_test
```

Run the Python integration tests with extension tests enabled:

```bash
cd tools/python_bind
NEUG_RUN_EXTENSION_TESTS=1 python3 -m pytest -sv tests/test_fts_index.py
```
