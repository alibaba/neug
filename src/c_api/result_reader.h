/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#pragma once

#include "c_api_internal.h"

namespace neug {
namespace capi {

neug_value_view Cell(const neug_result* result, uint64_t row, uint64_t column);
neug_type Type(const neug_value_view& value);
bool IsNull(const neug_value_view& value);
bool Boolean(const neug_value_view& value);
int64_t Signed(const neug_value_view& value);
uint64_t Unsigned(const neug_value_view& value);
double Number(const neug_value_view& value);
neug_string_view String(const neug_value_view& value);
uint64_t Size(const neug_value_view& value);
neug_value_view Child(const neug_value_view& value, uint64_t index);
neug_string_view FieldName(const neug_value_view& value, uint64_t index);

}  // namespace capi
}  // namespace neug
