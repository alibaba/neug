# NeuG C API

Run NeuG inside your application, without a database server:

```text
C / Rust / Dart / Swift
          |
       C API              include <neug/c/neug.h>
          |
    NeuG shared library   link against neug
```

- **API reference:** `include/neug/c/neug.h`
- **Complete example:** `examples/c/basic.c` (queries, transactions, and cleanup)

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

The example uses a temporary database and prints `1 | O'Connor`. With multi-configuration generators, the executable is under `Release/`.

- **Lite:** add `-DNEUG_LITE=ON`. The C API is included whenever `BUILD_COMPILER=ON`.
- **Install:** headers, shared library, and a standalone example under `share/neug/examples/c/`. Keep the installed header directory intact.
- **Deploy:** use matching headers and libraries for your NeuG release, OS, and CPU. Make shared-library dependencies available at runtime; on Windows, add the installation's `bin` directory to `PATH`.

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

## Essential rules

- **Errors:** only `NEUG_OK` means success. Check the status even if no error object is returned. Read optional details with `neug_error_message`, then call `neug_error_destroy`.
- **Transactions:** by default, each successful query commits its changes automatically; a later query failure does not undo earlier commits. To commit or undo several queries together, call `neug_connection_begin` before the first query. The caller must then call `neug_connection_commit` if all succeed, or `neug_connection_rollback` if any fails. Check the status of both commit and rollback.
- **Close failures:** check `neug_db_is_closed`. If still open, retain the handle and retry after fixing the cause; if closed, destroy it. Always report the close error, even if the database closed.
- **Threads:** calls are synchronous. Serialize operations and lifecycle changes on a database and its connections. Read-write databases allow one connection.

See the public header for individual function contracts, supported value types, and ABI compatibility rules.
