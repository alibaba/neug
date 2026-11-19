/* Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include <neug/c/neug.h>

static neug_string_view text(const char* value) {
  neug_string_view view = {value, strlen(value)};
  return view;
}

static int check(neug_status status, neug_error** error) {
  if (status == NEUG_OK)
    return 1;
  fprintf(stderr, "NeuG error %" PRId32 ": ", status);
  if (*error) {
    neug_string_view message = neug_error_message(*error);
    fwrite(message.data, 1, message.size, stderr);
  } else {
    fputs("no error message", stderr);
  }
  fputc('\n', stderr);
  neug_error_destroy(error);
  return 0;
}

int main(int argc, char** argv) {
  neug_database* db = NULL;
  neug_connection* connection = NULL;
  neug_result* result = NULL;
  neug_error* error = NULL;
  neug_db_options options;
  neug_string_view no_parameters = {NULL, 0};
  uint64_t rows = 0;
  int success = 0;

#define CHECK(call)             \
  do {                          \
    if (!check((call), &error)) \
      goto cleanup;             \
  } while (0)

  if (argc > 2) {
    fprintf(stderr, "Usage: %s [database-directory]\n", argv[0]);
    return 2;
  }
  /* Without a path, use a temporary database; otherwise create or update the
   * database at that path. */
  neug_db_options_init(&options);
  options.path = text(argc == 2 ? argv[1] : "");
  CHECK(neug_db_open(&options, &db, &error));
  CHECK(neug_db_connect(db, &connection, &error));
  CHECK(neug_query(connection,
                   text("CREATE NODE TABLE IF NOT EXISTS Person("
                        "id INT64, name STRING, PRIMARY KEY(id));"),
                   no_parameters, &result, &error));
  neug_result_destroy(&result);

  CHECK(neug_connection_begin(connection, 0, &error));
  /* Pass parameters separately so the apostrophe in the name is not
   * interpolated into the query. */
  CHECK(neug_query(connection,
                   text("MERGE (p:Person {id:$id}) SET p.name=$name;"),
                   text("{\"id\":1,\"name\":\"O'Connor\"}"), &result, &error));
  neug_result_destroy(&result);
  CHECK(neug_connection_commit(connection, &error));

  CHECK(neug_query(connection,
                   text("MATCH (p:Person {id:1}) RETURN p.id,p.name;"),
                   no_parameters, &result, &error));
  CHECK(neug_result_row_count(result, &rows, &error));
  for (uint64_t row = 0; row < rows; ++row) {
    neug_value_view value = {0};
    neug_string_view name = {NULL, 0};
    int64_t id = 0;
    uint8_t is_null = 0;
    CHECK(neug_result_get_value(result, row, 0, &value, &error));
    CHECK(neug_value_get_int64(&value, &id, &error));
    CHECK(neug_result_get_value(result, row, 1, &value, &error));
    CHECK(neug_value_is_null(&value, &is_null, &error));
    printf("%" PRId64 " | ", id);
    if (is_null) {
      fputs("NULL", stdout);
    } else {
      CHECK(neug_value_get_string(&value, &name, &error));
      /* name is not necessarily zero-terminated. Use its length and copy it if
       * needed beyond the result lifetime. */
      fwrite(name.data, 1, name.size, stdout);
    }
    fputc('\n', stdout);
  }
  success = 1;

cleanup:
  neug_result_destroy(&result);
  /* Use the same cleanup path on query failure; destroying the connection rolls
   * back an active transaction. */
  if (connection &&
      !check(neug_connection_destroy(&connection, &error), &error))
    success = 0;
  if (db) {
    uint8_t closed = 0;
    if (!check(neug_db_close(db, &error), &error))
      success = 0;
    if (!check(neug_db_is_closed(db, &closed, &error), &error)) {
      success = 0;
    } else if (closed) {
      if (!check(neug_db_destroy(&db, &error), &error))
        success = 0;
    } else {
      /* Long-running applications should retain the handle and retry close
       * after resolving disk or other failures. This CLI example reports
       * failure and exits without forcibly destroying an open database. */
      fputs("Database remains open; fix the close failure before retrying.\n",
            stderr);
      success = 0;
    }
  }
  neug_error_destroy(&error);
  return success ? 0 : 1;
}
