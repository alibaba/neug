/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "c_api_internal.h"

#include "neug/version.h"

using namespace neug::capi;

const char* neug_version(void) { return NEUG_VERSION_STRING; }

void neug_db_options_init(neug_db_options* options) {
  if (options) {
    *options = {};
    options->struct_size = sizeof(*options);
  }
}

neug_status neug_db_open(const neug_db_options* options, neug_database** out_db,
                         neug_error** error) {
  return Guard(error, [&] {
    NewOutput(out_db);
    Require(options && options->struct_size >= sizeof(neug_db_options),
            "Initialize database options with neug_db_options_init");
    Require(options->read_only <= 1, "read_only must be 0 or 1");
    neug::NeugDBConfig config(Text(options->path, false), 1);
    config.mode =
        options->read_only ? neug::DBMode::READ_ONLY : neug::DBMode::READ_WRITE;
    config.checkpoint_on_close = true;
    auto db = std::make_unique<neug_database>();
    db->impl = std::make_shared<neug::NeugDB>();
    Require(db->impl->Open(config), "Database open failed",
            NEUG_ERROR_INTERNAL);
    *out_db = db.release();
  });
}

neug_status neug_db_is_closed(const neug_database* db, uint8_t* output,
                              neug_error** error) {
  return Guard(error, [&] {
    auto& out = Output(output);
    Require(db && db->impl, "Database is NULL");
    out = db->impl->IsClosed();
  });
}
neug_status neug_db_close(neug_database* db, neug_error** error) {
  return Guard(error, [&] {
    Require(db && db->impl, "Database is NULL");
    db->impl->Close();
  });
}
neug_status neug_db_destroy(neug_database** db, neug_error** error) {
  return Guard(error, [&] {
    Require(db != nullptr, "Database pointer is NULL");
    if (*db) {
      Require((*db)->impl->IsClosed(),
              "Close the database before destroying it",
              NEUG_ERROR_ILLEGAL_STATE);
      delete *db;
      *db = nullptr;
    }
  });
}
