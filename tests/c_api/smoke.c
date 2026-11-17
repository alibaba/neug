/* Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include <stdio.h>
#include <string.h>

#include "neug/c/neug.h"

static neug_string_view text(const char* value) {
  neug_string_view out = {value, strlen(value)};
  return out;
}

int main(int argc, char** argv) {
  neug_database* db = NULL;
  neug_connection* connection = NULL;
  neug_result* result = NULL;
  neug_error* error = NULL;
  neug_db_options options;
  neug_value_view value = {0};
  int64_t count = 0;
  neug_status status = NEUG_OK;
  int success = 0;
#define CHECK(call)        \
  do {                     \
    status = (call);       \
    if (status != NEUG_OK) \
      goto cleanup;        \
  } while (0)
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <empty-database-directory>\n", argv[0]);
    return 2;
  }
  neug_db_options_init(&options);
  options.path = text(argv[1]);
  CHECK(neug_db_open(&options, &db, &error));
  CHECK(neug_db_connect(db, &connection, &error));
  /* An invalid query must return an error; after releasing it, the connection
   * must remain usable. */
  status = neug_query(connection, text("RETURN ("), text(""), &result, &error);
  if (status == NEUG_OK || error == NULL || result != NULL) {
    fprintf(stderr, "Invalid query did not return the expected error\n");
    goto cleanup;
  }
  neug_error_destroy(&error);
  CHECK(neug_query(connection, text("RETURN CAST($value, 'INT64');"),
                   text("{\"value\":42}"), &result, &error));
  CHECK(neug_result_get_value(result, 0, 0, &value, &error));
  CHECK(neug_value_get_int64(&value, &count, &error));
  success = count == 42;
  if (!success)
    fprintf(stderr, "Expected 42 from parameterized query\n");
cleanup:
  if (status != NEUG_OK) {
    neug_string_view message = neug_error_message(error);
    fprintf(stderr, "%d: %.*s\n", (int) status, (int) message.size,
            message.data ? message.data : "");
  }
  neug_error_destroy(&error);
  neug_result_destroy(&result);
  if (connection) {
    if (neug_connection_destroy(&connection, &error) != NEUG_OK)
      success = 0;
    neug_error_destroy(&error);
  }
  if (db) {
    if (neug_db_close(db, &error) != NEUG_OK)
      success = 0;
    neug_error_destroy(&error);
    if (neug_db_destroy(&db, &error) != NEUG_OK)
      success = 0;
    neug_error_destroy(&error);
  }
  return success ? 0 : 1;
}
