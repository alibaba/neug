#include "neug/execution/execute/ops/retrieve/range_expression.h"

#include <cstdint>
#include <string>

#include <glog/logging.h>

#include "neug/compiler/common/types/types.h"
#include "neug/utils/exception/exception.h"

namespace neug::execution::ops {
namespace {

constexpr auto INVALID_RANGE_ERROR =
    "The number of rows to skip/limit must be a non-negative integer.";

uint64_t value_as_uint64(const Value& value) {
  if (value.IsNull()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(INVALID_RANGE_ERROR);
  }
  switch (value.type().id()) {
  case DataTypeId::kInt32: {
    auto number = value.GetValue<int32_t>();
    if (number < 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(INVALID_RANGE_ERROR);
    }
    return static_cast<uint64_t>(number);
  }
  case DataTypeId::kInt64: {
    auto number = value.GetValue<int64_t>();
    if (number < 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(INVALID_RANGE_ERROR);
    }
    return static_cast<uint64_t>(number);
  }
  case DataTypeId::kUInt32:
    return value.GetValue<uint32_t>();
  case DataTypeId::kUInt64:
    return value.GetValue<uint64_t>();
  default:
    THROW_INVALID_ARGUMENT_EXCEPTION(INVALID_RANGE_ERROR);
  }
}

uint64_t bind_value(const ExprBase& expression,
                    const IStorageInterface* storage, const ParamsMap& params,
                    const std::string& parameter_name) {
  auto bound = expression.bind(storage, params);
  if (!bound) {
    if (!parameter_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Missing query parameter: $" +
                                       parameter_name + ".");
    }
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "The skip/limit expression contains an unbound parameter.");
  }
  auto value = bound->Cast<RecordExprBase>().eval_record(DataChunk(), 0);
  return value_as_uint64(value);
}

}  // namespace

RangeExpression::RangeExpression(const algebra::Range& range,
                                 const ContextMeta& ctx_meta) {
  if (range.has_offset()) {
    offset_ = parse_expression(range.offset(), ctx_meta, VarType::kRecord);
    if (range.offset().operators_size() == 1 &&
        range.offset().operators(0).has_param()) {
      offset_parameter_name_ = range.offset().operators(0).param().name();
    }
  }
  if (range.has_limit()) {
    limit_ = parse_expression(range.limit(), ctx_meta, VarType::kRecord);
    if (range.limit().operators_size() == 1 &&
        range.limit().operators(0).has_param()) {
      limit_parameter_name_ = range.limit().operators(0).param().name();
    }
  }
}

ResolvedRange RangeExpression::bind(const IStorageInterface* storage,
                                    const ParamsMap& params) const {
  constexpr uint64_t max_bound = common::MAX_RANGE_BOUND;
  auto offset =
      offset_ ? bind_value(*offset_, storage, params, offset_parameter_name_)
              : 0;
  if (offset > max_bound) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "The number of rows to skip/limit exceeds maximum allowed value: " +
        std::to_string(max_bound) + ".");
  }
  if (!limit_) {
    return {static_cast<uint32_t>(offset), static_cast<uint32_t>(max_bound)};
  }
  auto limit = bind_value(*limit_, storage, params, limit_parameter_name_);
  if (limit > max_bound) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "The number of rows to skip/limit exceeds maximum allowed value: " +
        std::to_string(max_bound) + ".");
  }
  if (limit > max_bound - offset) {
    LOG(WARNING) << "The requested SKIP " << offset << " plus LIMIT " << limit
                 << " exceeds the maximum range bound " << max_bound
                 << "; truncating the upper bound to " << max_bound << ".";
    return {static_cast<uint32_t>(offset), static_cast<uint32_t>(max_bound)};
  }
  return {static_cast<uint32_t>(offset), static_cast<uint32_t>(offset + limit)};
}

}  // namespace neug::execution::ops
