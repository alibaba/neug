/* Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include <stdio.h>
#include <string.h>

#include <neug/c/neug.h>

typedef struct {
  neug_database* db;
  neug_connection* connection;
  neug_result* result;
  neug_error* error;
} context;

static neug_string_view text(const char* value) {
  neug_string_view view = {value, strlen(value)};
  return view;
}

static int check(context* ctx, neug_status status) {
  if (status == NEUG_OK)
    return 1;
  neug_string_view message = neug_error_message(ctx->error);
  fprintf(stderr, "NeuG status %d: ", (int) status);
  if (message.data)
    fwrite(message.data, 1, message.size, stderr);
  fputc('\n', stderr);
  neug_error_destroy(&ctx->error);
  return 0;
}

static int query(context* ctx, const char* statement, const char* params) {
  neug_result_destroy(&ctx->result);
  return check(ctx, neug_query(ctx->connection, text(statement), text(params),
                               &ctx->result, &ctx->error));
}

static int expect_value(context* ctx, int64_t expected) {
  neug_value_view value = {0};
  int64_t actual = 0;
  if (!query(ctx, "MATCH (n:PackageCheck {id:1}) RETURN n.value;", "") ||
      !check(ctx,
             neug_result_get_value(ctx->result, 0, 0, &value, &ctx->error)) ||
      !check(ctx, neug_value_get_int64(&value, &actual, &ctx->error)))
    return 0;
  if (actual != expected) {
    fprintf(stderr, "Stored value differs from expectation\n");
    return 0;
  }
  return 1;
}

int main(int argc, char** argv) {
  context ctx = {0};
  neug_db_options options;
  neug_status status = NEUG_OK;
  uint8_t active = 0;
  int success = 0;
  int writing = 0;

#define REQUIRE(condition)                         \
  do {                                             \
    if (!(condition)) {                            \
      fprintf(stderr, "Failed: %s\n", #condition); \
      goto cleanup;                                \
    }                                              \
  } while (0)
#define CALL(expression) REQUIRE(check(&ctx, (expression)))

  if (argc != 4 || (strcmp(argv[3], "write") && strcmp(argv[3], "read"))) {
    fprintf(stderr, "Usage: %s <database-path> <version> <write|read>\n",
            argv[0]);
    return 2;
  }
  REQUIRE(strcmp(neug_version(), argv[2]) == 0);
  writing = strcmp(argv[3], "write") == 0;
  neug_db_options_init(&options);
  options.path = text(argv[1]);
  options.read_only = writing ? 0 : 1;
  CALL(neug_db_open(&options, &ctx.db, &ctx.error));
  CALL(neug_db_connect(ctx.db, &ctx.connection, &ctx.error));

  if (writing) {
    REQUIRE(query(&ctx,
                  "CREATE NODE TABLE PackageCheck("
                  "id INT64, value INT64, PRIMARY KEY(id));",
                  ""));
    CALL(neug_connection_begin(ctx.connection, 0, &ctx.error));
    REQUIRE(query(&ctx, "CREATE (:PackageCheck {id:1,value:$value});",
                  "{\"value\":42}"));
    CALL(neug_connection_commit(ctx.connection, &ctx.error));
    REQUIRE(expect_value(&ctx, 42));

    CALL(neug_connection_begin(ctx.connection, 0, &ctx.error));
    REQUIRE(query(&ctx, "MATCH (n:PackageCheck {id:1}) SET n.value=99;", ""));
    REQUIRE(expect_value(&ctx, 99));
    CALL(neug_connection_rollback(ctx.connection, &ctx.error));
    REQUIRE(expect_value(&ctx, 42));

    /* Engine errors in an explicit transaction require rollback before reuse.
     */
    CALL(neug_connection_begin(ctx.connection, 0, &ctx.error));
    neug_result_destroy(&ctx.result);
    status = neug_query(ctx.connection, text("RETURN ("), text(""), &ctx.result,
                        &ctx.error);
    REQUIRE(status != NEUG_OK && ctx.result == NULL && ctx.error != NULL);
    neug_error_destroy(&ctx.error);
    CALL(neug_connection_has_transaction(ctx.connection, &active, &ctx.error));
    REQUIRE(active == 1);
    CALL(neug_connection_rollback(ctx.connection, &ctx.error));
  }
  /* The read phase runs in another process with a read-only database. */
  REQUIRE(expect_value(&ctx, 42));
  success = 1;

cleanup:
  neug_error_destroy(&ctx.error);
  neug_result_destroy(&ctx.result);
  if (ctx.connection &&
      !check(&ctx, neug_connection_destroy(&ctx.connection, &ctx.error)))
    success = 0;
  if (ctx.db) {
    uint8_t closed = 0;
    if (!check(&ctx, neug_db_close(ctx.db, &ctx.error)))
      success = 0;
    if (!check(&ctx, neug_db_is_closed(ctx.db, &closed, &ctx.error))) {
      success = 0;
    } else if (closed) {
      if (!check(&ctx, neug_db_destroy(&ctx.db, &ctx.error)))
        success = 0;
    } else {
      /* Report failure rather than forcibly destroying an open database. */
      success = 0;
    }
  }
  neug_error_destroy(&ctx.error);
  if (success)
    printf("PASS: C API package %s\n", writing ? "write" : "read");
  return success ? 0 : 1;
}
