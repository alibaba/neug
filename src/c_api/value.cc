/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "result_reader.h"

using namespace neug::capi;

// No C++ exception can escape, including errors while lazily decoding graph
// JSON. Scalar readers initialize their outputs before inspecting the input.
#define NEUG_READ_FUNCTION(name, out_type, expression)             \
  neug_status name(const neug_value_view* value, out_type* output, \
                   neug_error** error) {                           \
    return Guard(error, [&] {                                      \
      auto& out = Output(output);                                  \
      Require(value != nullptr, "Value pointer is NULL");          \
      out = (expression);                                          \
    });                                                            \
  }
NEUG_READ_FUNCTION(neug_value_type, neug_type, Type(*value))
NEUG_READ_FUNCTION(neug_value_is_null, uint8_t, IsNull(*value))
NEUG_READ_FUNCTION(neug_value_get_bool, uint8_t, Boolean(*value))
NEUG_READ_FUNCTION(neug_value_get_int64, int64_t, Signed(*value))
NEUG_READ_FUNCTION(neug_value_get_uint64, uint64_t, Unsigned(*value))
NEUG_READ_FUNCTION(neug_value_get_double, double, Number(*value))
NEUG_READ_FUNCTION(neug_value_get_string, neug_string_view, String(*value))
NEUG_READ_FUNCTION(neug_value_size, uint64_t, Size(*value))
#undef NEUG_READ_FUNCTION

neug_status neug_value_child(const neug_value_view* value, uint64_t index,
                             neug_value_view* output, neug_error** error) {
  return Guard(error, [&] {
    const auto input = value ? *value : neug_value_view{};
    auto& out = Output(output);
    Require(value != nullptr, "Value is NULL");
    out = Child(input, index);
  });
}
neug_status neug_value_field_name(const neug_value_view* value, uint64_t index,
                                  neug_string_view* output,
                                  neug_error** error) {
  return Guard(error, [&] {
    auto& out = Output(output);
    Require(value != nullptr, "Value is NULL");
    out = FieldName(*value, index);
  });
}
