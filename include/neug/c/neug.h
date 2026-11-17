/** Copyright 2026 Alibaba Group Holding Limited.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 */
#ifndef NEUG_C_NEUG_H
#define NEUG_C_NEUG_H

#include <stddef.h>
#include <stdint.h>

#include "neug/utils/api.h"

#ifdef __cplusplus
extern "C" {
#endif

/* C ABI version, not the NeuG product version or a runtime version query.
 * Existing function signatures, constant values, and public struct layouts
 * remain unchanged within an ABI version. New functions require a NeuG release
 * that provides them. Do not mix headers and libraries with different ABIs.
 */
#define NEUG_C_API_VERSION 1

typedef int32_t neug_status;
enum {
  NEUG_OK = 0,
  NEUG_ERROR_PERMISSION = 1001,
  NEUG_ERROR_DATABASE_LOCKED = 1004,
  NEUG_ERROR_INVALID_ARGUMENT = 1009,
  NEUG_ERROR_NOT_FOUND = 1010,
  NEUG_ERROR_NOT_SUPPORTED = 1011,
  NEUG_ERROR_INTERNAL = 1012,
  NEUG_ERROR_ILLEGAL_STATE = 1013,
  NEUG_ERROR_IO = 1014,
  NEUG_ERROR_BAD_ENCODING = 1015,
  NEUG_ERROR_NO_CHECKPOINT = 1018,
  NEUG_ERROR_CONNECTION_CLOSED = 2003,
  NEUG_ERROR_COMPILATION = 3000,
  NEUG_ERROR_QUERY = 3001,
  NEUG_ERROR_TRANSACTION = 4001,
  NEUG_ERROR_TYPE = 5003,
  NEUG_ERROR_OVERFLOW = 5004,
  NEUG_ERROR_INDEX = 5005,
  NEUG_ERROR_OUT_OF_MEMORY = 8001
};

typedef int32_t neug_type;
enum {
  NEUG_TYPE_NULL = 0,
  NEUG_TYPE_BOOL,
  NEUG_TYPE_INT32,
  NEUG_TYPE_UINT32,
  NEUG_TYPE_INT64,
  NEUG_TYPE_UINT64,
  NEUG_TYPE_FLOAT,
  NEUG_TYPE_DOUBLE,
  NEUG_TYPE_STRING,
  NEUG_TYPE_DATE,
  NEUG_TYPE_TIMESTAMP,
  NEUG_TYPE_INTERVAL,
  NEUG_TYPE_LIST,
  NEUG_TYPE_STRUCT,
  NEUG_TYPE_TUPLE,
  NEUG_TYPE_NODE,
  NEUG_TYPE_RELATIONSHIP,
  NEUG_TYPE_PATH,
  NEUG_TYPE_OBJECT,
  NEUG_TYPE_UNKNOWN
};

typedef struct neug_database neug_database;
typedef struct neug_connection neug_connection;
typedef struct neug_result neug_result;
typedef struct neug_error neug_error;

/* UTF-8 byte ranges need not be zero-terminated. Never free a borrowed view. */
typedef struct {
  const char* data;
  size_t size;
} neug_string_view;

/* Copyable, borrowed locator. Treat all fields as private. Valid until its
 * result is destroyed, including across further reads and connection closure.
 * It does NOT keep the result alive. Never use a locator after result destroy.
 */
typedef struct {
  const void* _owner;
  const void* _data;
  uint64_t _index;
  uint32_t _source;
} neug_value_view;

/* Initialize with neug_db_options_init. read_only is 0 (default) or 1.
 * path is copied by open. An empty path, ":memory" or ":memory:" opens a
 * temporary database; otherwise use a writable application data directory.
 * Uses one execution slot and saves persistent databases when closed.
 * This struct has a fixed layout in ABI v1. struct_size validates
 * initialization; it does not permit appending fields or increasing the size
 * written by init in later releases.
 */
typedef struct {
  size_t struct_size;
  neug_string_view path;
  uint8_t read_only;
} neug_db_options;

/* Calls are synchronous. Serialize operations on a database/connection with
 * lifecycle changes; read-write databases allow one connection. Independent
 * result reads may be concurrent while their result remains alive.
 *
 * Fallible functions return the status code. Optional out_error supplies a
 * detailed message and must point to NULL.
 * When output pointer preconditions hold (including *out_error being NULL),
 * failure leaves new object outputs NULL and zeroes value/view outputs. Misused
 * output pointers have no such guarantee. Release an existing output before
 * reuse. Input handles must be live: NULL is checked, but arbitrary/dangling
 * pointer copies cannot be validated. Core status numbers are preserved, even
 * if not named above. Exceptions never escape this boundary. OUT_OF_MEMORY can
 * be returned without an error object.
 */
NEUG_API const char* neug_version(void);
NEUG_API neug_string_view neug_error_message(const neug_error* error);
NEUG_API void neug_error_destroy(neug_error** error);
NEUG_API void neug_db_options_init(neug_db_options* options);
NEUG_API neug_status neug_db_open(const neug_db_options* options,
                                  neug_database** out_db,
                                  neug_error** out_error);
NEUG_API neug_status neug_db_is_closed(const neug_database* db,
                                       uint8_t* out_closed,
                                       neug_error** out_error);
/* Close also closes connections, rolls back unfinished transactions, and may
 * fail during checkpointing. The handle survives failure; inspect is_closed.
 * Closed connections do not become usable again if database close is retried.
 */
NEUG_API neug_status neug_db_close(neug_database* db, neug_error** out_error);
/* Destroy refuses a still-open database. On success *db becomes NULL. */
NEUG_API neug_status neug_db_destroy(neug_database** db,
                                     neug_error** out_error);
NEUG_API neug_status neug_db_connect(neug_database* db,
                                     neug_connection** out_connection,
                                     neug_error** out_error);
NEUG_API neug_status
neug_connection_is_closed(const neug_connection* connection,
                          uint8_t* out_closed, neug_error** out_error);
/* Closes the connection, rolls back any active transaction, and sets NULL. */
NEUG_API neug_status neug_connection_destroy(neug_connection** connection,
                                             neug_error** out_error);
NEUG_API neug_status neug_connection_begin(neug_connection* connection,
                                           uint8_t read_only,
                                           neug_error** out_error);
NEUG_API neug_status neug_connection_commit(neug_connection* connection,
                                            neug_error** out_error);
NEUG_API neug_status neug_connection_rollback(neug_connection* connection,
                                              neug_error** out_error);
NEUG_API neug_status
neug_connection_has_transaction(const neug_connection* connection,
                                uint8_t* out_active, neug_error** out_error);
/* query may contain $name placeholders. parameters_json is a UTF-8 JSON
 * object mapping names (without '$') to values. A zero-length view or "{}"
 * means no parameters. Both inputs are copied during this synchronous call.
 * Invalid JSON, a non-object root, or nesting deeper than 64 is rejected
 * before query execution. Values are converted using the engine's parameter
 * types; unused parameters are ignored.
 * The engine infers the access mode from the query.
 * Inspect schema with ordinary queries, e.g. CALL SHOW_NODE_TABLES() RETURN *.
 */
NEUG_API neug_status neug_query(neug_connection* connection,
                                neug_string_view query,
                                neug_string_view parameters_json,
                                neug_result** out_result,
                                neug_error** out_error);

NEUG_API void neug_result_destroy(neug_result** result);
NEUG_API neug_status neug_result_row_count(const neug_result* result,
                                           uint64_t* out_count,
                                           neug_error** out_error);
NEUG_API neug_status neug_result_column_count(const neug_result* result,
                                              uint64_t* out_count,
                                              neug_error** out_error);
NEUG_API neug_status neug_result_column_name(const neug_result* result,
                                             uint64_t column,
                                             neug_string_view* out_name,
                                             neug_error** out_error);
/* Column type is available even for an empty result. */
NEUG_API neug_status neug_result_column_type(const neug_result* result,
                                             uint64_t column,
                                             neug_type* out_type,
                                             neug_error** out_error);
/* Row and column indexes are zero-based. */
NEUG_API neug_status neug_result_get_value(const neug_result* result,
                                           uint64_t row, uint64_t column,
                                           neug_value_view* out_value,
                                           neug_error** out_error);
NEUG_API neug_status neug_value_type(const neug_value_view* value,
                                     neug_type* out_type,
                                     neug_error** out_error);
NEUG_API neug_status neug_value_is_null(const neug_value_view* value,
                                        uint8_t* out_null,
                                        neug_error** out_error);
NEUG_API neug_status neug_value_get_bool(const neug_value_view* value,
                                         uint8_t* out_value,
                                         neug_error** out_error);
/* Integer getters widen without loss and check range; never pass through a
 * double. DATE/TIMESTAMP get_int64 returns epoch milliseconds. */
NEUG_API neug_status neug_value_get_int64(const neug_value_view* value,
                                          int64_t* out_value,
                                          neug_error** out_error);
NEUG_API neug_status neug_value_get_uint64(const neug_value_view* value,
                                           uint64_t* out_value,
                                           neug_error** out_error);
NEUG_API neug_status neug_value_get_double(const neug_value_view* value,
                                           double* out_value,
                                           neug_error** out_error);
NEUG_API neug_status neug_value_get_string(const neug_value_view* value,
                                           neug_string_view* out_value,
                                           neug_error** out_error);
NEUG_API neug_status neug_value_size(const neug_value_view* value,
                                     uint64_t* out_size,
                                     neug_error** out_error);
/* size/child inspect lists, tuples, structs and objects; indexes are
 * zero-based. Use field_name with child to enumerate named struct and object
 * fields, including graph values.
 *
 * Graph fields include metadata and properties:
 * - NODE: _ID (integer), _LABEL (string), followed by properties.
 * - RELATIONSHIP: _ID, _LABEL, _SRC_ID, _DST_ID, followed by properties.
 * - PATH: nodes (list), rels (list), length (integer).
 *
 * Path list elements have OBJECT type and the same node/relationship fields.
 * Internal IDs are not durable business keys. Graph property values follow
 * the core's JSON representation (e.g. date/time properties are strings).
 */
NEUG_API neug_status neug_value_child(const neug_value_view* value,
                                      uint64_t index,
                                      neug_value_view* out_child,
                                      neug_error** out_error);
NEUG_API neug_status neug_value_field_name(const neug_value_view* value,
                                           uint64_t index,
                                           neug_string_view* out_name,
                                           neug_error** out_error);

#ifdef __cplusplus
}
#endif
#endif
