# NeuG C API

The NeuG C API is the public C interface to the embedded database engine. C programs can call it directly. Rust, Dart, Swift, and other languages can use their foreign function interfaces to call it and provide language-specific wrappers. Queries run inside the application process; no database server is required.

The public header is `include/neug/c/neug.h`, and the implementation is compiled into the NeuG shared library. Callers do not need C++, protobuf, or Python headers. The library still uses C++ and protobuf internally. Whether an application must ship additional shared libraries depends on its NeuG build configuration.

## Build, install, and run the example

The C API is built when `BUILD_COMPILER=ON`; there is no separate `BUILD_C_API` switch. Both standard NeuG and `NEUG_LITE=ON` builds include it. It is unavailable with `BUILD_COMPILER=OFF`. Here, compiler means the NeuG query compiler, which converts Cypher queries into execution plans.

After preparing the NeuG source build environment, run these commands from the repository root:

```sh
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_COMPILER=ON \
  -DBUILD_PYTHON=OFF -DBUILD_NODEJS=OFF -DBUILD_HTTP_SERVER=OFF \
  -DBUILD_TEST=OFF -DBUILD_EXAMPLES=ON
cmake --build build --config Release --parallel 2
cmake --install build --config Release --prefix "$PWD/build/c-api-install"
```

These commands retain other options cached in an existing build directory. Add `-DNEUG_LITE=ON` to the configuration command for a Lite build. Building the engine requires a C++ toolchain, but building the example against an installed library requires only a C compiler and CMake.

The installation includes the following files relevant to C callers:

| Location | Purpose |
| --- | --- |
| `include/neug/c/neug.h` | C API declarations |
| `include/neug/utils/api.h` | Symbol export definitions included by the public header; keep both headers |
| `libneug.so` / `libneug.dylib` in `lib/` or `lib64/` | Linux / macOS shared library |
| `bin/neug.dll` and `lib/neug.lib` on Windows | Runtime DLL and link-time import library |
| `share/neug/examples/c/` | `basic.c` and a standalone `CMakeLists.txt` |
| `share/doc/NeuG/c_api.md` | This guide; the location can be changed with `CMAKE_INSTALL_DOCDIR` |

Build and run the installed example independently:

```sh
cmake -S build/c-api-install/share/neug/examples/c \
  -B build/c-api-example \
  -DCMAKE_PREFIX_PATH="$PWD/build/c-api-install"
cmake --build build/c-api-example --config Release --parallel 2
./build/c-api-example/neug_c_example
```

On macOS / Linux, the example should print `1 | O'Connor`, possibly alongside engine logs. Multi-configuration generators on Windows usually place the executable in `build/c-api-example/Release/`; add the installation's `bin` directory to `PATH` before running it. On other platforms, the runtime must also be able to find NeuG and its dynamic dependencies. The example's CMake build sets the required build-time runtime search path on supported platforms. Arrange shared-library locations appropriately when deploying the application.

The example finds only the C headers and NeuG library. It does not use `find_package(neug)`, which loads C++ dependency configurations. Applications can also use their C compiler's include, library search, and linker options directly. Do not define `NEUG_EXPORTS` in a caller; it is reserved for building the library.

The same example is available in the source tree at `examples/c/basic.c`. Build it with `cmake --build build --target neug_c_example`, then run `build/examples/c/neug_c_example`. It uses a temporary database by default. Passing a directory creates or updates a persistent database there:

```sh
./build/c-api-example/neug_c_example /tmp/neug-c-demo
```

The example creates a table, writes a name with JSON parameters inside a transaction, reads the result, and releases resources. It uses `MERGE`, so it can be run repeatedly against its own example database. Do not point it at a production database: it modifies the `Person` record with `id=1`.

## How the interfaces fit together

A typical call sequence is:

1. Initialize options with `neug_db_options_init`, then set the database path and `read_only`.
2. Open the database with `neug_db_open` and create a query connection with `neug_db_connect`.
3. Execute queries with `neug_query`. Use `neug_connection_begin`, `commit`, and `rollback` when several queries must be committed together.
4. Use `neug_result_*` to inspect columns, row counts, and cells; use `neug_value_*` to read values.
5. Release results and connections, close the database, and destroy the database handle once it is closed.

A handle is a pointer to an object created by the library and retained by the caller. Callers must not access its internal representation or destroy it with `free` or another language's allocator.

| Object | Creation and destruction |
| --- | --- |
| `neug_database*` | Create with `neug_db_open`; close with `neug_db_close`, then destroy with `neug_db_destroy` |
| `neug_connection*` | Create with `neug_db_connect`; close and destroy with `neug_connection_destroy` |
| `neug_result*` | Create with `neug_query`; destroy with `neug_result_destroy`, including results from write queries |
| `neug_error*` | Created as needed by a failed call; destroy with `neug_error_destroy` |

Pointers receiving new objects must initially be `NULL`. Destroy an old result before reusing its output variable; the library must not overwrite an unreleased object. Successful destruction sets the supplied pointer to `NULL`, but does not update other copies of that pointer.

An empty path, `:memory`, or `:memory:` creates a temporary database. Persistent databases use an application-writable directory and save a checkpoint on close. The C interface currently uses one execution slot and the engine's default WAL synchronization policy. It does not expose every C++ configuration option, such as thread counts or automatic extension loading.

## Queries and JSON parameters

`neug_query` accepts query text, JSON parameters, and two output pointers:

```c
neug_result* result = NULL;
neug_error* error = NULL;
const char query[] = "MATCH (p:Person {id:$id}) RETURN p.name;";
const char params[] = "{\"id\":1}";
neug_string_view query_view = {query, sizeof(query) - 1};
neug_string_view params_view = {params, sizeof(params) - 1};
neug_status status = neug_query(connection, query_view, params_view,
                                &result, &error);
/* Check status, read the result, and release result and error separately.
 * See basic.c for a complete example. */
```

Parameter names omit the `$` prefix. The parameter root must be a JSON object. A zero-length view or `{}` means no parameters. Queries without placeholders can be passed as complete query strings.

The engine infers parameter types from the query. JSON nulls, booleans, numbers, strings, and containers are supported, but each value must match the query's requirements. Use expressions such as `CAST($value, 'INT64')` when a type must be explicit. Being valid JSON does not make an arbitrary object acceptable to every query. Every declared parameter must be supplied, including parameters bound to null. Unused parameters are ignored.

Inputs use UTF-8 bytes and explicit lengths. They must remain valid during the call and may be released or modified after it returns. Query text and raw JSON text must not contain actual zero bytes; a JSON string can represent a zero character with `\u0000`. Invalid JSON, invalid encoding, a non-object root, and nesting deeper than 64 levels are rejected before execution. JSON cannot represent NaN or infinity. Use a JSON encoder that preserves integer precision when passing 64-bit integers.

## Results and borrowed data

`neug_result_row_count`, `column_count`, `column_name`, and `column_type` expose result metadata. Column names and types remain available for empty results. Row, column, and child indexes are zero-based.

`neug_result_get_value` returns a `neug_value_view`: a borrowed reference to data inside the result, not a copy. It does not extend the result's lifetime. Check for null with `neug_value_is_null`, then use the appropriate reader. Reading a NULL scalar returns a type error. `neug_value_type` describes the type; nullness must still be checked separately.

| Result type | Reader |
| --- | --- |
| BOOL | `neug_value_get_bool`, returning 0 or 1 |
| INT32, INT64, UINT32, UINT64 | `get_int64` / `get_uint64`; report overflow without converting through floating point |
| FLOAT, DOUBLE | `get_double`; also accepts integers, but large integers may lose precision |
| STRING, INTERVAL | `get_string`; INTERVAL returns an interval string |
| DATE, TIMESTAMP | `get_int64`, in milliseconds since the Unix epoch |
| LIST, TUPLE | Use `size` and `child`, then read each child recursively |
| STRUCT, OBJECT | Enumerate fields using `size`, `field_name`, and `child` |
| NODE, RELATIONSHIP, PATH | Read named fields in the same way, as described below |

Graph objects retain metadata and properties. Nodes include `_ID` and `_LABEL`; relationships also include `_SRC_ID` and `_DST_ID`; paths include `nodes`, `rels`, and `length`. Nodes and relationships inside path lists have OBJECT type. Internal IDs are not stable business keys. Graph properties follow the engine's JSON representation: for example, a date property in a graph object may be a string, unlike the millisecond representation of a directly queried date column.

`neug_string_view` is also a borrowed byte range and is not necessarily zero-terminated. Read exactly `size` bytes; do not pass it directly to `strlen` or `printf("%s", ...)`.

- Value views, strings, column names, and field names from a result become invalid when that result is destroyed.
- Closing a database or destroying a connection does not invalidate a retained result.
- The string returned by `neug_error_message` becomes invalid when its error object is destroyed.
- Do not free a view itself. Copy data into caller-owned memory before destroying its owner if the data must remain available.

Results are held in memory. There is currently no streaming cursor or Arrow export. Use LIMIT as appropriate to avoid unbounded reads.

## Errors, transactions, and close failures

Fallible functions return `neug_status`; `NEUG_OK` indicates success. The optional `out_error` supplies a detailed message and must point to a `NULL` pointer on entry. Release an existing error before passing that output variable to another call. An out-of-memory failure may return only a status code with no error object, so do not use `error != NULL` as the sole success check.

Status codes preserve engine error numbers and may include nonzero values not yet named in the header. Wrappers must retain a generic error branch rather than treating unknown codes as success. C++ exceptions do not cross the C boundary. The library checks null pointers and some invalid arguments, but cannot make arbitrary forged or freed pointers safe.

Without an explicit transaction, queries use the engine's automatic transaction handling. Explicit transactions belong to a connection. The `read_only` argument to `begin` must be 0 or 1; use `commit` to commit and `rollback` to discard changes. Nested transactions are unsupported. If a statement fails, roll back rather than continuing to commit, and check the rollback outcome separately. Destroying a connection or closing its database also rolls back an unfinished transaction.

Database close and destruction are separate because saving a checkpoint can fail:

1. Call `neug_db_close`. The caller retains the handle whether close succeeds or fails.
2. On failure, record the error and inspect the actual state with `neug_db_is_closed`.
3. If the database remains open, retain the handle, resolve disk or other failures, and retry close. Existing connections may already be closed; do not assume they remain usable.
4. If the database is closed, release the handle with `neug_db_destroy`. A previously reported checkpoint failure must still be propagated to the caller, not turned into success.

`neug_db_destroy` refuses to destroy an open database. Long-running wrappers should retain objects after a failed close so the application can retry, rather than silently discarding the handle.

## Threading and ABI rules

All calls are synchronous. Do not modify or release inputs before the call completes. Serialize operations, transactions, and lifecycle changes on a database and its connections. Read-write databases currently allow only one connection. Read-only databases can create multiple connections, but a single connection must not be used concurrently. Independent result reads may run concurrently as long as no thread destroys the corresponding result during those reads. Language wrappers can enforce these rules with a queue or lock.

The C ABI is the binary calling contract: function symbols, parameter types, constant values, and struct layouts. `NEUG_C_API_VERSION` is currently 1 and identifies the header's C ABI version. `neug_version()` returns the NeuG product version. The macro is not a runtime version query and does not automatically prevent loading an incompatible library.

Maintain the public interface according to these rules:

- Within one C ABI version, do not remove functions, change existing signatures, renumber status codes or types, or change public struct layouts and ownership rules.
- The ABI v1 layout of `neug_db_options` is fixed. `struct_size` validates initialization; it does not permit appending fields directly. An older caller may have allocated less memory than a newer initializer would write. Extend configuration through a new interface or a new struct version.
- Functions and constants may be added. Programs using new functionality require a library that provides it. Wrappers should report unsupported result types and preserve unknown status codes.
- A necessary breaking change requires an explicit new ABI version and migration plan. Changing the macro alone is not a safe way to replace an old library silently.

These rules apply to binary calls on the same operating system and CPU architecture with compatible toolchains. They do not make an Android library loadable on iOS, or promise database file-format compatibility. For initial integration, use headers and libraries from the same NeuG release. Keep the platform's default struct alignment; do not use `packed` or redefine layouts. Do not pass C++ objects across the boundary or release library-owned memory with a caller allocator.

## Verification and release scope

Maintainers can enable `BUILD_TEST=ON` in the root build, build `test_c_api` and `c_api_smoke`, and run:

```sh
cmake --build build --target test_c_api c_api_smoke --parallel 2
ctest --test-dir build -R '^(CApi|c_api_smoke)' --output-on-failure
```

With `BUILD_EXAMPLES=ON` and the example built, also run `ctest --test-dir build -R '^c_api_example$' --output-on-failure`. Release verification should include installing into a separate directory and independently building and running the installed example, as shown above, rather than checking only source-tree linkage.

The interface ships in NeuG libraries built with the query compiler. This guide describes source builds and CMake installation artifacts; it does not imply that a Python wheel or npm package is a complete C development package. Platform support depends on the native libraries actually provided and validated for a release. Android / iOS builds, language SDKs, and application integrations are separate deliverables.
