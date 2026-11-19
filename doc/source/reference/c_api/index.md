# NeuG C API

Run NeuG inside your application, without a database server:

```text
C / Rust / Dart / Swift
          |
       C API              include <neug/c/neug.h>
          |
    NeuG shared library   link against neug
```

- **API reference:** [neug.h](https://github.com/alibaba/neug/blob/main/include/neug/c/neug.h)
- **Complete example:** [basic.c](https://github.com/alibaba/neug/blob/main/examples/c/basic.c) (queries, transactions, and cleanup)

## Use a binary package

Download a matching `neug-c-api-<version>-lite-<platform>.tar.gz` and its `.sha256`
file from [GitHub Releases](https://github.com/alibaba/neug/releases). The first
supported platforms are Linux x86_64, macOS ARM64 and Windows x86_64. These are
desktop packages, not Android or iOS libraries.

The package includes a self-contained `include/neug/c/neug.h`, a shared library,
and C/C++ examples. Third-party code, including protobuf, is built into the
library; no separate protobuf installation is needed. Operating-system libraries
are still required. Windows also includes `neug.lib` for linking with MSVC.

Check the SHA256 as described in the package README, extract the archive, then
run from the extracted directory with CMake and a C compiler installed:

```sh
cmake -S examples -B example-build -DCMAKE_PREFIX_PATH="$PWD"

cmake --build example-build --config Release

./example-build/neug_c_example
```

On Windows, use a Visual Studio developer PowerShell and run
`.\example-build\Release\neug_c_example.exe`. CMake copies `neug.dll` beside the
executable. The example prints `1 | O'Connor`.

For your application, add `include` to the header search path and link against
`neug` in `lib`. Deploy the shared library with your app and keep the header and
library from the same release. See the package README for runtime search paths
and `manifest.json` for system dependencies and minimum glibc/macOS settings.
C++ applications can use the same C header and library.

## Build and try

After preparing the NeuG source build environment, run from the repository root:

```sh
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_COMPILER=ON \
  -DBUILD_PYTHON=OFF -DBUILD_NODEJS=OFF -DBUILD_HTTP_SERVER=OFF \
  -DBUILD_TEST=OFF -DBUILD_EXAMPLES=ON

cmake --build build --config Release --parallel 2

./build/examples/c/neug_c_example

cmake --install build --config Release --prefix "$PWD/build/c-api-install"
```

The example uses a temporary database and prints `1 | O'Connor`. With multi-configuration generators, the executable is under `Release/`. On Windows, skip the in-tree example command: the core build disables in-tree examples. After installation, build the example from `build/c-api-install/share/neug/examples/c` using an MSVC developer shell and `-DCMAKE_PREFIX_PATH` pointing to the installation.

- **Lite:** add `-DNEUG_LITE=ON`. It retains the embedded query engine, storage, transactions and C API, and excludes the HTTP server and core OpenSSL linkage. Python/Node bindings and optional extensions have separate build options; extensions may add dependencies.
- **C API:** included whenever `BUILD_COMPILER=ON`, in both standard and Lite builds. Lite does not select a target OS or perform cross-compilation by itself.
- **Source installations:** the standalone example is in `<prefix>/share/neug/examples/c`; binary archives place it in `examples`. Source builds may retain dependencies that the Lite packaging script removes.

### Build a Lite binary package

Use CMake, Python 3 and a native C/C++ toolchain: GCC and binutils on Linux, Xcode Command Line Tools on macOS, or an MSVC developer shell with Ninja on Windows:

```sh
python3 scripts/package_c_api.py build --jobs 2
```

This reconfigures the shared `build/` directory for Lite Release, disables language bindings and optional dependencies, builds the library, and verifies the extracted package before placing the archive and SHA256 file in `build/c-api-dist/`. The package includes one public header, one library, examples, a README, a license and a manifest. Only operating-system dynamic dependencies are allowed. Windows also includes an MSVC import library (`neug.lib`) for linking. Build and verification logs are retained under the same output directory.

To check an existing archive independently:

```sh
python3 scripts/package_c_api.py verify /path/to/package.tar.gz
```

Keep its adjacent `.tar.gz.sha256` file. Verification builds the packaged C example and runs the transaction/persistence example as both C and C++. This entry builds for the current machine; it does not cross-compile or publish a release. Linux CI uses Ubuntu 22.04 and records the binary's minimum glibc version. Windows packages statically link the C/C++ runtime.

For a CI trial, run the **package** GitHub Actions workflow on your branch with **c_api_only** enabled. Choose `linux-x86_64`, `osx-arm64`, `windows-x86_64`, or `all`. Each selected package is downloaded and verified on a separate runner; normal package runs also include these C API artifacts. Download `c-api-<platform>-<run_id>` from the run's **Artifacts** section; it contains the `.tar.gz` and `.sha256` files. Build and verification logs are separate artifacts, retained for seven days. These are CI artifacts, not published GitHub Release assets.

## Call sequence

| Step | API | Purpose |
| --- | --- | --- |
| 1. Open | `neug_db_options_init`, `neug_db_open` | Set a database directory, or an empty path for a temporary database. |
| 2. Connect | `neug_db_connect` | Create a connection for queries. |
| 3. Query | `neug_query` | Pass Cypher text and optional JSON parameters. |
| 4. Read | `neug_result_*`, `neug_value_*` | Read columns and cells; check for NULL before reading a value. |
| 5. Release | `neug_result_destroy` | Release each result, including write-query results. Repeat from step 3. |
| 6. Shut down | `neug_connection_destroy`, `neug_db_close`, `neug_db_destroy` | Release the connection, save and close the database, then destroy its handle. |

### Query inputs

The C API supports two ways to query:

1. **Query only:** pass a complete Cypher query with values written directly in the query text.
2. **Query with parameters:** pass a Cypher query with `$name` placeholders and a separate JSON object containing their values.

Both use the same `neug_query` function, as shown below:

| Form | Query | Parameters |
| --- | --- | --- |
| Query only | `MATCH (p:Person {id:1}) RETURN p.name;` | Empty view `{NULL, 0}`, or JSON `{}` |
| Query with parameters | `MATCH (p:Person {id:$id}) RETURN p.name;` | JSON `{"id":1}` |

Pass query text and parameters as `neug_string_view` values: a UTF-8 pointer plus a byte length. Keep inputs alive until the call returns.

For parameterized queries, supply every `$parameter` in the JSON object, without `$` in its key. Prefer parameters for application-provided values to avoid inserting and escaping them in query text.

The query determines each parameter's type. Use an explicit cast when needed:

| Query | JSON parameters |
| --- | --- |
| `RETURN CAST($id, 'UINT64');` | `{"id":18446744073709551615}` |
| `RETURN CAST($ts, 'TIMESTAMP');` | `{"ts":"2026-11-17 12:00:00"}` |
| `RETURN CAST($ids, 'INT64[]');` | `{"ids":[1,2,3]}` |

| Expected type | Accepted JSON value |
| --- | --- |
| `BOOL` | Boolean; also an INT32 integer (`0` is false, any other value is true) |
| `INT32`, `INT64` | Integer in `[-2^31, 2^31-1]` or `[-2^63, 2^63-1]` |
| `UINT32`, `UINT64` | Integer in `[0, 2^32-1]` or `[0, 2^64-1]` |
| `FLOAT`, `DOUBLE` | JSON number; conversion may lose precision |
| `STRING` | JSON string |
| `DATE`, `TIMESTAMP` | Signed epoch-millisecond integer, or a date/time string such as `"2026-11-17"` / `"2026-11-17 12:00:00"` |
| `INTERVAL` | Duration string, such as `"1 day"` |
| `LIST`, `ARRAY` | Array of values accepted by the element type; fixed-size `ARRAY` requires exactly its declared length |
| `STRUCT`, graph objects | Non-null parameter binding is unsupported |

JSON `null` becomes a typed NULL; omitting a required parameter is an error. Unused keys are ignored. Integer parameters do not accept fractions or numeric strings. Preserve large integers when constructing JSON: do not pass them through a floating-point value. Invalid JSON (including `NaN`/`Infinity`), non-object roots and nesting deeper than 64 are rejected before engine execution.

### Result handling

After a successful query, check each read's status as you follow these steps:

```text
Get row and column counts
    |
    v
For each cell you need:
    Get cell -> Check for NULL -> If non-NULL, read -> Use or copy
    |
    v
Finish processing all needed cells
    |
    v
Release the entire result (neug_result_destroy)
```

Releasing the result invalidates its views; application-owned copies remain usable. For write queries whose output you do not need, skip reading and release the result directly.

- **Strings:** read `size` bytes, not until a zero byte. Copy the bytes, not just the view.
- **Cleanup:** the caller releases every result, including write-query results. A wrapper can do this for the app. Never use `free` on library objects.
- **Outputs:** initialize result and error pointers to `NULL`; release old objects before reuse.

Choose the reader using `neug_value_type`; the [header's result reader guide](https://github.com/alibaba/neug/blob/main/include/neug/c/neug.h) lists each type. In particular, read `INTERVAL` as a string and 64-bit integers with integer getters. Converting a large integer to `double` can lose precision even when the getter succeeds.

### Transactions

By default, each successful query commits automatically. A later query failure does not undo earlier commits. To save or undo several queries together:

```text
begin -> query -> query -> ... -> commit
           |                       |
      query fails             commit fails
           |                       |
           +------> rollback <-----+
                        |
                  reuse connection
```

Check every status, including commit and rollback. If rollback fails, report it and destroy the connection instead of reusing it. The complete C example destroys the connection on failure, which also rolls back an unfinished transaction.

- **Engine query failure or write-commit failure:** the transaction becomes rollback-only. Queries, a new begin, and another commit are blocked until rollback. `neug_connection_has_transaction` remains true; it does not mean the transaction can still run queries.
- **Rejected before engine execution:** malformed JSON, a non-object parameter root or an invalid input view does not change transaction state. Missing parameters and parameter type errors are checked inside the engine and make an explicit transaction rollback-only. A wrapper can simplify recovery by rolling back on any query failure.

## Essential rules

- **Errors:** only `NEUG_OK` means success. Check the status even if no error object is returned. Read optional details with `neug_error_message`, then call `neug_error_destroy`.
- **Close failures:** check `neug_db_is_closed`. If still open, retain the handle and retry after fixing the cause; if closed, destroy it. Always report the close error, even if the database closed.
- **Threads:** calls are synchronous. Serialize operations and lifecycle changes on a database and its connections. Read-write databases allow one connection.

See the public header for individual function contracts, supported value types, and ABI compatibility rules.
