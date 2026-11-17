/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "result_reader.h"

using namespace neug::capi;

void neug_result_destroy(neug_result** result) {
  if (result) {
    delete *result;
    *result = nullptr;
  }
}
neug_status neug_result_row_count(const neug_result* result, uint64_t* output,
                                  neug_error** error) {
  return Guard(error, [&] {
    auto& out = Output(output);
    out = Response(result).row_count();
  });
}
neug_status neug_result_column_count(const neug_result* result,
                                     uint64_t* output, neug_error** error) {
  return Guard(error, [&] {
    auto& out = Output(output);
    out = Response(result).arrays_size();
  });
}
neug_status neug_result_column_name(const neug_result* result, uint64_t column,
                                    neug_string_view* output,
                                    neug_error** error) {
  return Guard(error, [&] {
    auto& out = Output(output);
    const auto& response = Response(result);
    Require(column < static_cast<uint64_t>(response.schema().name_size()),
            "Column index out of range", NEUG_ERROR_INDEX);
    out = View(response.schema().name(column));
  });
}
neug_status neug_result_column_type(const neug_result* result, uint64_t column,
                                    neug_type* output, neug_error** error) {
  return Guard(error, [&] {
    auto& out = Output(output);
    const auto& response = Response(result);
    Require(column < static_cast<uint64_t>(response.arrays_size()),
            "Column index out of range", NEUG_ERROR_INDEX);
    out = Type({result, &response.arrays(column), 0, 1});
  });
}
neug_status neug_result_get_value(const neug_result* result, uint64_t row,
                                  uint64_t column, neug_value_view* output,
                                  neug_error** error) {
  return Guard(error, [&] {
    auto& out = Output(output);
    out = Cell(result, row, column);
  });
}
