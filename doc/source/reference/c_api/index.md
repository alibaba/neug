# NeuG C API

The C API runs NeuG queries inside your application without a database server. Use it directly from C or through another language's foreign function interface (FFI).

Include `<neug/c/neug.h>` and link against the NeuG shared library. The header contains the full API reference; `examples/c/basic.c` provides a complete example with queries, transactions, and error handling.

## Build and install

After preparing the NeuG source build environment, run from the repository root:

```sh
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_COMPILER=ON \
  -DBUILD_PYTHON=OFF -DBUILD_NODEJS=OFF -DBUILD_HTTP_SERVER=OFF \
  -DBUILD_TEST=OFF -DBUILD_EXAMPLES=ON
cmake --build build --config Release --parallel 2
cmake --install build --config Release --prefix "$PWD/build/c-api-install"
```

The C API is included whenever `BUILD_COMPILER=ON`, in both standard and Lite builds. Add `-DNEUG_LITE=ON` for Lite. Existing build directories retain other cached options.

The installation provides headers under `include/`, the NeuG library, and a standalone C example under `share/neug/examples/c/`. Keep the installed header directory intact: `neug/c/neug.h` includes `neug/utils/api.h`. Build and run the example using only a C compiler and CMake:

```sh
cmake -S build/c-api-install/share/neug/examples/c \
  -B build/c-api-example \
  -DCMAKE_PREFIX_PATH="$PWD/build/c-api-install"
cmake --build build/c-api-example --config Release --parallel 2
./build/c-api-example/neug_c_example
```

The example uses a temporary database and prints `1 | O'Connor`. On Windows, multi-configuration generators place the executable under `Release/`; add the installation's `bin` directory to `PATH`. When deploying, make NeuG and its shared-library dependencies available to the runtime loader.

## Open and query a database

Initialize `neug_db_options` with `neug_db_options_init`, set `path` and optionally `read_only`, then call `neug_db_open` and `neug_db_connect`. An empty path creates a temporary database; otherwise, use an application-writable directory.

`neug_query` accepts Cypher text and an optional JSON object of named parameters:

```c
neug_result* result = NULL;
neug_error* error = NULL;
const char query[] = "MATCH (p:Person {id:$id}) RETURN p.name;";
const char params[] = "{\"id\":1}";
neug_string_view query_view = {query, sizeof(query) - 1};
neug_string_view params_view = {params, sizeof(params) - 1};
neug_status status = neug_query(connection, query_view, params_view,
                                &result, &error);
/* Check status before reading result; release result and error afterward. */
```

Parameter names omit `$`; a zero-length view or `{}` means no parameters. Supply every query parameter. Values must match the types inferred by the engine; use `CAST` in the query when an explicit type is needed. Preserve integer precision when encoding 64-bit values as JSON.

Inputs are UTF-8 byte ranges with explicit lengths and must remain valid until the call returns. Query and JSON text cannot contain raw zero bytes; use `\u0000` within JSON strings.

## Read and release results

Use `neug_result_*` to read column metadata and cells by zero-based index. `neug_result_get_value` returns a borrowed `neug_value_view`. Check `neug_value_is_null`, then use the appropriate `neug_value_get_*` reader. DATE and TIMESTAMP use epoch milliseconds through `get_int64`. Use `neug_value_size`, `child`, and `field_name` for nested and graph values; see the header for type-specific details.

- Result values and strings remain valid until `neug_result_destroy`, even if the connection has closed. Copy anything needed afterward.
- `neug_string_view` is not necessarily zero-terminated. Read exactly `size` bytes; do not use `strlen` or `%s` directly.
- Destroy every query result, including write-query results. Results are held in memory; use `LIMIT` for large reads.
- Release library objects with their corresponding destroy functions, never `free`. Initialize new-object output pointers to `NULL` and release existing objects before reusing those outputs.

## Errors, transactions, and shutdown

Fallible calls return `neug_status`; only `NEUG_OK` means success. An optional `neug_error` provides a message and must be released with `neug_error_destroy`. Its message is valid only while the error exists. Check the status even when no error object is returned, and treat unknown nonzero status codes as failures.

Queries use automatic transactions by default. To group queries, call `neug_connection_begin`, followed by `commit` or `rollback` on the same connection. Nested transactions are unsupported. Roll back after a query failure and check the rollback status.

Release results and connections, then call `neug_db_close` before `neug_db_destroy`. Close saves persistent data and can fail. On failure, preserve the error and inspect `neug_db_is_closed`: if still open, retain the handle and retry after resolving the cause; if closed, destroy the handle while still reporting the failure. Existing connections may already be closed. Destroying a connection or closing its database rolls back unfinished transactions.

## Threading and compatibility

Calls are synchronous. Serialize operations and lifecycle changes on a database and its connections. Read-write databases allow one connection. Independent result reads may run concurrently while their results remain alive.

Use headers and libraries from the same NeuG release and for the target operating system and CPU architecture. `NEUG_C_API_VERSION` identifies the C ABI version; `neug_version()` returns the product version. Existing signatures and public struct layouts remain fixed within an ABI version. Keep default struct alignment, initialize options through the API, and do not access opaque handles or private view fields.
