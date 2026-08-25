# FTS Extension

The FTS extension provides SQLite FTS5-backed full-text indexes and BM25
Top-K search, including scalar and graph filtering, online index maintenance,
and checkpoint recovery.

For installation, DDL, query syntax, filtering, and operational behavior, see
the [Full-Text Search Extension](../../doc/source/extensions/fts_search.md)
user documentation. This README covers the source-tree prerequisites, build,
local loading, and tests for the extension.

## Prerequisites

Initialize the bundled SQLite and cppjieba sources before configuring the
extension:

```bash
git submodule update --init third_party/sqlite
git submodule update --init --recursive third_party/cppjieba
```

## Build

From the NeuG repository root, build NeuG, the Python binding, and the FTS
extension with:

```bash
cd tools/python_bind
BUILD_EXTENSIONS=fts make build
```

The extension library embeds cppjieba's small default dictionary and HMM
model as compressed dictionaries. A fresh build therefore produces a single
extension file:

```text
build/extension/fts/
└── libfts.neug_extension
```

On the first use of the Jieba tokenizer, the dictionaries are extracted beside
the extension library and reused by later processes:

```text
build/extension/fts/
├── libfts.neug_extension
├── jieba.dict.utf8
└── hmm_model.utf8
```

If the extension directory is not writable, extraction falls back to
`NEUG_DB_TMP_DIR`, or to the system temporary directory when the environment
variable is not set. Fallback files include a hash of the embedded compressed
data so matching dictionaries can be reused safely by later processes:

```text
<temporary-directory>/neug/fts/
├── jieba.dict.utf8.<hash>
└── hmm_model.utf8.<hash>
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
