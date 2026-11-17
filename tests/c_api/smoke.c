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
  neug_string_view no_parameters = {0};
  int64_t count = 0;
  neug_status status = NEUG_OK;
  int success = 0;
#define CHECK(call)        \
  do {                     \
    status = (call);       \
    if (status != NEUG_OK) \
      goto cleanup;        \
  } while (0)
#define QUERY(query)                                                          \
  do {                                                                        \
    neug_result_destroy(&result);                                             \
    CHECK(                                                                    \
        neug_query(connection, text(query), no_parameters, &result, &error)); \
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
  status =
      neug_query(connection, text("RETURN ("), no_parameters, &result, &error);
  if (status == NEUG_OK || error == NULL || result != NULL) {
    fprintf(stderr, "Invalid query did not return the expected error\n");
    goto cleanup;
  }
  neug_error_destroy(&error);
  QUERY("CREATE NODE TABLE Person(id INT64, age INT64, PRIMARY KEY(id));");
  QUERY("CREATE REL TABLE KNOWS(FROM Person TO Person);");
  QUERY("CREATE (:Person {id:1, age:25});");
  QUERY("CREATE (:Person {id:2, age:30});");
  QUERY("MATCH (a:Person {id:1}), (b:Person {id:2}) CREATE (a)-[:KNOWS]->(b);");
  QUERY("MATCH (p:Person) WHERE p.age=25 RETURN count(p);");
  CHECK(neug_result_get_value(result, 0, 0, &value, &error));
  CHECK(neug_value_get_int64(&value, &count, &error));
  if (count != 1)
    goto cleanup;
  neug_result_destroy(&result);
  CHECK(neug_query(connection,
                   text("MATCH (p:Person {id:$id}) SET p.age=$age;"),
                   text("{\"id\":1,\"age\":26}"), &result, &error));
  CHECK(neug_connection_begin(connection, 0, &error));
  QUERY("MATCH (p:Person {id:1}) SET p.age=99;");
  CHECK(neug_connection_rollback(connection, &error));
  QUERY("MATCH (p:Person {id:1}) RETURN p.age;");
  CHECK(neug_result_get_value(result, 0, 0, &value, &error));
  CHECK(neug_value_get_int64(&value, &count, &error));
  if (count != 26)
    goto cleanup;
  CHECK(neug_connection_begin(connection, 0, &error));
  QUERY("MATCH (p:Person {id:1}) SET p.age=27;");
  CHECK(neug_connection_commit(connection, &error));
  CHECK(neug_connection_destroy(&connection, &error));
  CHECK(neug_db_close(db, &error));
  CHECK(neug_db_destroy(&db, &error));
  CHECK(neug_db_open(&options, &db, &error));
  CHECK(neug_db_connect(db, &connection, &error));
  QUERY("MATCH (p:Person {id:1}) RETURN p.age;");
  CHECK(neug_result_get_value(result, 0, 0, &value, &error));
  CHECK(neug_value_get_int64(&value, &count, &error));
  if (count != 27)
    goto cleanup;
  QUERY("MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r);");
  CHECK(neug_result_get_value(result, 0, 0, &value, &error));
  CHECK(neug_value_get_int64(&value, &count, &error));
  success = count == 1;
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
