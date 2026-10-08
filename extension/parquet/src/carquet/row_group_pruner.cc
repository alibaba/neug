/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "carquet/row_group_pruner.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stack>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "neug/utils/io/read/common/operator_precedence.h"

namespace neug::parquet {
namespace {

enum Truth : uint8_t { kFalse = 1, kTrue = 2, kUnknown = 4, kAny = 7 };

using Scalar = std::variant<bool, int32_t, uint32_t, int64_t, uint64_t, float,
                            double, std::string>;

struct ColumnRef {
  int32_t leaf = -1;
  ::common::DataType type;
};

uint8_t negate(uint8_t input) {
  uint8_t output = 0;
  if (input & kFalse) {
    output |= kTrue;
  }
  if (input & kTrue) {
    output |= kFalse;
  }
  if (input & kUnknown) {
    output |= kUnknown;
  }
  return output;
}

Truth andValue(Truth left, Truth right) {
  if (left == kFalse || right == kFalse) {
    return kFalse;
  }
  if (left == kTrue && right == kTrue) {
    return kTrue;
  }
  return kUnknown;
}

Truth orValue(Truth left, Truth right) {
  if (left == kTrue || right == kTrue) {
    return kTrue;
  }
  if (left == kFalse && right == kFalse) {
    return kFalse;
  }
  return kUnknown;
}

template <typename Operation>
uint8_t combine(uint8_t left, uint8_t right, Operation operation) {
  uint8_t output = 0;
  for (const auto leftValue : {kFalse, kTrue, kUnknown}) {
    if (!(left & leftValue)) {
      continue;
    }
    for (const auto rightValue : {kFalse, kTrue, kUnknown}) {
      if (right & rightValue) {
        output |= operation(leftValue, rightValue);
      }
    }
  }
  return output;
}

std::optional<Scalar> constantForType(const ::common::Value& value,
                                      const ::common::DataType& type) {
  if (type.item_case() == ::common::DataType::kString &&
      value.item_case() == ::common::Value::kStr) {
    return Scalar(value.str());
  }
  if (type.item_case() != ::common::DataType::kPrimitiveType) {
    return std::nullopt;
  }
  switch (type.primitive_type()) {
  case ::common::PrimitiveType::DT_BOOL:
    if (value.item_case() == ::common::Value::kBoolean) {
      return Scalar(value.boolean());
    }
    break;
  case ::common::PrimitiveType::DT_SIGNED_INT32:
    if (value.item_case() == ::common::Value::kI32) {
      return Scalar(value.i32());
    }
    break;
  case ::common::PrimitiveType::DT_UNSIGNED_INT32:
    if (value.item_case() == ::common::Value::kU32) {
      return Scalar(value.u32());
    }
    break;
  case ::common::PrimitiveType::DT_SIGNED_INT64:
    if (value.item_case() == ::common::Value::kI64) {
      return Scalar(value.i64());
    }
    break;
  case ::common::PrimitiveType::DT_UNSIGNED_INT64:
    if (value.item_case() == ::common::Value::kU64) {
      return Scalar(value.u64());
    }
    break;
  case ::common::PrimitiveType::DT_FLOAT:
    if (value.item_case() == ::common::Value::kF32 &&
        std::isfinite(value.f32())) {
      return Scalar(value.f32());
    }
    break;
  case ::common::PrimitiveType::DT_DOUBLE:
    if (value.item_case() == ::common::Value::kF64 &&
        std::isfinite(value.f64())) {
      return Scalar(value.f64());
    }
    break;
  default:
    break;
  }
  return std::nullopt;
}

template <typename T>
std::optional<Scalar> decodeFixed(const void* data, int32_t size) {
  if (!data || size != static_cast<int32_t>(sizeof(T))) {
    return std::nullopt;
  }
  if constexpr (std::is_same_v<T, bool>) {
    const auto byte = *static_cast<const uint8_t*>(data);
    if (byte > 1) {
      return std::nullopt;
    }
    return Scalar(byte != 0);
  }
  T value;
  std::memcpy(&value, data, sizeof(value));
  if constexpr (std::is_floating_point_v<T>) {
    if (!std::isfinite(value)) {
      return std::nullopt;
    }
  }
  return Scalar(value);
}

std::optional<Scalar> decodeStat(const void* data, int32_t size,
                                 const ::common::DataType& type) {
  if (type.item_case() == ::common::DataType::kString) {
    if (size < 0 || (size > 0 && !data)) {
      return std::nullopt;
    }
    if (size == 0) {
      return Scalar(std::string{});
    }
    return Scalar(
        std::string(static_cast<const char*>(data), static_cast<size_t>(size)));
  }
  if (type.item_case() != ::common::DataType::kPrimitiveType) {
    return std::nullopt;
  }
  switch (type.primitive_type()) {
  case ::common::PrimitiveType::DT_BOOL:
    return decodeFixed<bool>(data, size);
  case ::common::PrimitiveType::DT_SIGNED_INT32:
    return decodeFixed<int32_t>(data, size);
  case ::common::PrimitiveType::DT_UNSIGNED_INT32:
    return decodeFixed<uint32_t>(data, size);
  case ::common::PrimitiveType::DT_SIGNED_INT64:
    return decodeFixed<int64_t>(data, size);
  case ::common::PrimitiveType::DT_UNSIGNED_INT64:
    return decodeFixed<uint64_t>(data, size);
  case ::common::PrimitiveType::DT_FLOAT:
    return decodeFixed<float>(data, size);
  case ::common::PrimitiveType::DT_DOUBLE:
    return decodeFixed<double>(data, size);
  default:
    return std::nullopt;
  }
}

std::optional<int> compareScalar(const Scalar& left, const Scalar& right) {
  if (left.index() != right.index()) {
    return std::nullopt;
  }
  return std::visit(
      [](const auto& lhs, const auto& rhs) -> std::optional<int> {
        using Left = std::decay_t<decltype(lhs)>;
        using Right = std::decay_t<decltype(rhs)>;
        if constexpr (!std::is_same_v<Left, Right>) {
          return std::nullopt;
        } else {
          return (lhs > rhs) - (lhs < rhs);
        }
      },
      left, right);
}

std::optional<::common::Logical> reverseComparison(::common::Logical op) {
  switch (op) {
  case ::common::Logical::EQ:
  case ::common::Logical::NE:
    return op;
  case ::common::Logical::LT:
    return ::common::Logical::GT;
  case ::common::Logical::LE:
    return ::common::Logical::GE;
  case ::common::Logical::GT:
    return ::common::Logical::LT;
  case ::common::Logical::GE:
    return ::common::Logical::LE;
  default:
    return std::nullopt;
  }
}

bool comparisonMightMatch(::common::Logical op, int valueVsMin,
                          int valueVsMax) {
  switch (op) {
  case ::common::Logical::EQ:
    return valueVsMin >= 0 && valueVsMax <= 0;
  case ::common::Logical::NE:
    return valueVsMin != 0 || valueVsMax != 0;
  case ::common::Logical::LT:
    return valueVsMin > 0;
  case ::common::Logical::LE:
    return valueVsMin >= 0;
  case ::common::Logical::GT:
    return valueVsMax < 0;
  case ::common::Logical::GE:
    return valueVsMax <= 0;
  default:
    return true;
  }
}

std::optional<ColumnRef> findColumn(const carquet_reader_t* reader,
                                    const reader::EntrySchema& schema,
                                    const std::string& name) {
  const auto iter =
      std::find(schema.columnNames.begin(), schema.columnNames.end(), name);
  if (iter == schema.columnNames.end()) {
    return std::nullopt;
  }
  const size_t schemaIndex =
      static_cast<size_t>(std::distance(schema.columnNames.begin(), iter));
  if (schemaIndex >= schema.columnTypes.size() ||
      !schema.columnTypes[schemaIndex]) {
    return std::nullopt;
  }

  const carquet_schema_t* carquetSchema = carquet_reader_schema(reader);
  const int32_t leaves = carquet_schema_num_columns(carquetSchema);
  for (int32_t leaf = 0; leaf < leaves; ++leaf) {
    const char* path[2] = {nullptr, nullptr};
    const int32_t depth =
        carquet_schema_column_path(carquetSchema, leaf, path, 2);
    if (depth == 1 && path[0] && name == path[0]) {
      return ColumnRef{leaf, *schema.columnTypes[schemaIndex]};
    }
  }
  return std::nullopt;
}

}  // namespace

struct CarquetRowGroupPruner::Node {
  enum class Kind { kUnsupported, kColumn, kConstant, kOperation };

  Kind kind = Kind::kUnsupported;
  ColumnRef column;
  ::common::Value constant;
  ::common::ExprOpr operation;
  std::unique_ptr<Node> left;
  std::unique_ptr<Node> right;

  uint8_t possible(const carquet_reader_t* reader, int32_t rowGroup,
                   int64_t rowCount) const {
    if (kind != Kind::kOperation ||
        operation.item_case() != ::common::ExprOpr::kLogical) {
      return kAny;
    }

    const auto op = operation.logical();
    if (op == ::common::Logical::NOT) {
      return right ? negate(right->possible(reader, rowGroup, rowCount)) : kAny;
    }
    if (op == ::common::Logical::AND || op == ::common::Logical::OR) {
      if (!left || !right) {
        return kAny;
      }
      const auto leftValues = left->possible(reader, rowGroup, rowCount);
      const auto rightValues = right->possible(reader, rowGroup, rowCount);
      return op == ::common::Logical::AND
                 ? combine(leftValues, rightValues, andValue)
                 : combine(leftValues, rightValues, orValue);
    }
    if (op == ::common::Logical::ISNULL) {
      if (!right || right->kind != Kind::kColumn) {
        return kAny;
      }
      carquet_column_statistics_t stats;
      if (carquet_reader_column_statistics(reader, rowGroup, right->column.leaf,
                                           &stats) != CARQUET_OK ||
          !stats.has_null_count || stats.num_values != rowCount ||
          stats.null_count < 0 || stats.null_count > stats.num_values) {
        return kAny;
      }
      if (stats.null_count == 0) {
        return kFalse;
      }
      if (stats.null_count == stats.num_values) {
        return kTrue;
      }
      return kFalse | kTrue;
    }

    const Node* columnNode = nullptr;
    const Node* constantNode = nullptr;
    auto comparison = reverseComparison(op);
    if (!comparison || !left || !right) {
      return kAny;
    }
    if (left->kind == Kind::kColumn && right->kind == Kind::kConstant) {
      columnNode = left.get();
      constantNode = right.get();
      comparison = op;
    } else if (left->kind == Kind::kConstant && right->kind == Kind::kColumn) {
      columnNode = right.get();
      constantNode = left.get();
    } else {
      return kAny;
    }

    carquet_column_statistics_t stats;
    if (carquet_reader_column_statistics(
            reader, rowGroup, columnNode->column.leaf, &stats) != CARQUET_OK ||
        stats.num_values != rowCount ||
        (stats.has_null_count &&
         (stats.null_count < 0 || stats.null_count > stats.num_values))) {
      return kAny;
    }
    if (stats.has_null_count && stats.null_count == stats.num_values) {
      return kUnknown;
    }
    if (!stats.has_min_max) {
      return kAny;
    }

    // Finite min/max do not prove that a floating-point group has no NaNs.
    // NaN matches NE even when every finite value equals the constant.
    const auto primitive = columnNode->column.type.primitive_type();
    if ((primitive == ::common::PrimitiveType::DT_FLOAT ||
         primitive == ::common::PrimitiveType::DT_DOUBLE) &&
        *comparison == ::common::Logical::NE) {
      return kAny;
    }

    const auto value =
        constantForType(constantNode->constant, columnNode->column.type);
    const auto minimum = decodeStat(stats.min_value, stats.min_value_size,
                                    columnNode->column.type);
    const auto maximum = decodeStat(stats.max_value, stats.max_value_size,
                                    columnNode->column.type);
    if (!value || !minimum || !maximum) {
      return kAny;
    }
    const auto minVsMax = compareScalar(*minimum, *maximum);
    const auto valueVsMin = compareScalar(*value, *minimum);
    const auto valueVsMax = compareScalar(*value, *maximum);
    if (!minVsMax || *minVsMax > 0 || !valueVsMin || !valueVsMax) {
      return kAny;
    }
    if (comparisonMightMatch(*comparison, *valueVsMin, *valueVsMax)) {
      return kAny;
    }
    return stats.has_null_count && stats.null_count == 0 ? kFalse
                                                         : kFalse | kUnknown;
  }
};

namespace {

std::unique_ptr<CarquetRowGroupPruner::Node> makeUnsupported() {
  return std::make_unique<CarquetRowGroupPruner::Node>();
}

std::unique_ptr<CarquetRowGroupPruner::Node> compile(
    const carquet_reader_t* reader, const reader::EntrySchema& schema,
    const ::common::Expression* filter) {
  if (!filter || filter->operators().empty()) {
    return makeUnsupported();
  }
  using Node = CarquetRowGroupPruner::Node;
  std::stack<std::unique_ptr<Node>> values;
  std::stack<::common::ExprOpr> operators;

  auto apply = [&]() -> bool {
    if (operators.empty()) {
      return false;
    }
    auto operation = operators.top();
    operators.pop();
    const bool unary = operation.item_case() == ::common::ExprOpr::kLogical &&
                       (operation.logical() == ::common::Logical::NOT ||
                        operation.logical() == ::common::Logical::ISNULL);
    if (values.empty() || (!unary && values.size() < 2)) {
      return false;
    }
    auto node = std::make_unique<Node>();
    node->kind = Node::Kind::kOperation;
    node->operation = std::move(operation);
    node->right = std::move(values.top());
    values.pop();
    if (!unary) {
      node->left = std::move(values.top());
      values.pop();
    }
    values.push(std::move(node));
    return true;
  };

  for (const auto& token : filter->operators()) {
    if (token.item_case() == ::common::ExprOpr::kVar) {
      auto node = std::make_unique<Node>();
      auto column = token.var().tag().has_name() && !token.var().has_property()
                        ? findColumn(reader, schema, token.var().tag().name())
                        : std::nullopt;
      if (column) {
        node->kind = Node::Kind::kColumn;
        node->column = std::move(*column);
      }
      values.push(std::move(node));
    } else if (token.item_case() == ::common::ExprOpr::kConst) {
      auto node = std::make_unique<Node>();
      node->kind = Node::Kind::kConstant;
      node->constant = token.const_();
      values.push(std::move(node));
    } else if (token.item_case() == ::common::ExprOpr::kBrace) {
      if (token.brace() == ::common::ExprOpr::LEFT_BRACE) {
        operators.push(token);
      } else {
        while (!operators.empty() &&
               operators.top().item_case() != ::common::ExprOpr::kBrace) {
          if (!apply()) {
            return makeUnsupported();
          }
        }
        if (operators.empty()) {
          return makeUnsupported();
        }
        operators.pop();
      }
    } else if (token.item_case() == ::common::ExprOpr::kLogical ||
               token.item_case() == ::common::ExprOpr::kArith) {
      // Prefix unary operators bind to the following operand, including another
      // unary operator. Keep this private parser aligned with exact evaluation.
      const auto precedenceOf = [](const ::common::ExprOpr& op) {
        if (op.has_logical() && (op.logical() == ::common::Logical::ISNULL ||
                                 op.logical() == ::common::Logical::NOT)) {
          return 2;
        }
        return reader::OperatorPrecedence::getPrecedence(op);
      };
      if (token.has_logical() &&
          (token.logical() == ::common::Logical::NOT ||
           token.logical() == ::common::Logical::ISNULL)) {
        operators.push(token);
        continue;
      }
      const int precedence = precedenceOf(token);
      while (!operators.empty() &&
             operators.top().item_case() != ::common::ExprOpr::kBrace &&
             precedenceOf(operators.top()) <= precedence) {
        if (!apply()) {
          return makeUnsupported();
        }
      }
      operators.push(token);
    } else {
      values.push(makeUnsupported());
    }
  }
  while (!operators.empty()) {
    if (operators.top().item_case() == ::common::ExprOpr::kBrace || !apply()) {
      return makeUnsupported();
    }
  }
  if (values.size() != 1) {
    return makeUnsupported();
  }
  return std::move(values.top());
}

}  // namespace

CarquetRowGroupPruner::CarquetRowGroupPruner(const carquet_reader_t* reader,
                                             std::unique_ptr<Node> root)
    : reader_(reader), root_(std::move(root)) {}

CarquetRowGroupPruner::~CarquetRowGroupPruner() = default;

std::unique_ptr<CarquetRowGroupPruner> CarquetRowGroupPruner::create(
    const carquet_reader_t* reader, const reader::EntrySchema& schema,
    const ::common::Expression* filter) {
  return std::unique_ptr<CarquetRowGroupPruner>(
      new CarquetRowGroupPruner(reader, compile(reader, schema, filter)));
}

bool CarquetRowGroupPruner::mightMatch(int32_t rowGroup) const {
  if (!reader_ || !root_) {
    return true;
  }
  carquet_row_group_metadata_t metadata;
  if (carquet_reader_row_group_metadata(reader_, rowGroup, &metadata) !=
      CARQUET_OK) {
    return true;
  }
  if (metadata.num_rows == 0) {
    return false;
  }
  return (root_->possible(reader_, rowGroup, metadata.num_rows) & kTrue) != 0;
}

}  // namespace neug::parquet
