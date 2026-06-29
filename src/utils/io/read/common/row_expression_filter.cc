/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/utils/io/read/common/row_expression_filter.h"

#include <cstdlib>
#include <functional>
#include <stack>

#include "neug/execution/common/types/value.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/operator_precedence.h"

namespace neug {
namespace reader {
namespace {

execution::Value proto_value_to_execution(const ::common::Value& value) {
  switch (value.item_case()) {
  case ::common::Value::kBoolean:
    return execution::Value::BOOLEAN(value.boolean());
  case ::common::Value::kI32:
    return execution::Value::INT32(value.i32());
  case ::common::Value::kI64:
    return execution::Value::INT64(value.i64());
  case ::common::Value::kU32:
    return execution::Value::UINT32(value.u32());
  case ::common::Value::kU64:
    return execution::Value::UINT64(value.u64());
  case ::common::Value::kF32:
    return execution::Value::FLOAT(value.f32());
  case ::common::Value::kF64:
    return execution::Value::DOUBLE(value.f64());
  case ::common::Value::kStr:
    return execution::Value::STRING(value.str());
  default:
    THROW_CONVERSION_EXCEPTION("Unsupported constant type in row filter");
  }
}

bool compare_values(const ::common::Logical& logical,
                    const execution::Value& left,
                    const execution::Value& right) {
  switch (logical) {
  case ::common::Logical::GT:
    return right < left;
  case ::common::Logical::GE:
    return !(left < right);
  case ::common::Logical::LT:
    return left < right;
  case ::common::Logical::LE:
    return !(right < left);
  case ::common::Logical::EQ:
    return left == right;
  case ::common::Logical::NE:
    return !(left == right);
  case ::common::Logical::AND:
    return left.GetValue<bool>() && right.GetValue<bool>();
  case ::common::Logical::OR:
    return left.GetValue<bool>() || right.GetValue<bool>();
  case ::common::Logical::NOT:
    return !left.GetValue<bool>();
  default:
    THROW_NOT_IMPLEMENTED_EXCEPTION(
        "Unsupported logical operator in row filter");
  }
}

void build_name_to_index(const std::vector<std::string>& column_names,
                         std::unordered_map<std::string, int>* name_to_index) {
  for (size_t i = 0; i < column_names.size(); ++i) {
    (*name_to_index)[column_names[i]] = static_cast<int>(i);
  }
}

}  // namespace

RowExpressionFilter::RowExpressionFilter(
    const ::common::Expression& expr,
    const std::unordered_map<std::string, int>& column_index) {
  using ValueFn =
      std::function<execution::Value(const execution::DataChunk&, size_t)>;

  std::stack<ValueFn> value_stack;
  std::stack<::common::ExprOpr> op_stack;

  auto apply_operator = [&](const ::common::ExprOpr& opr) {
    if (opr.item_case() != ::common::ExprOpr::kLogical) {
      THROW_NOT_IMPLEMENTED_EXCEPTION("Unsupported operator in row filter");
    }
    if (opr.logical() == ::common::Logical::NOT) {
      if (value_stack.empty()) {
        THROW_INVALID_ARGUMENT_EXCEPTION("Not enough operands for NOT");
      }
      auto operand = value_stack.top();
      value_stack.pop();
      value_stack.push(
          [operand](const execution::DataChunk& chunk, size_t row) {
            return execution::Value::BOOLEAN(
                compare_values(::common::Logical::NOT, operand(chunk, row),
                               execution::Value::BOOLEAN(false)));
          });
      return;
    }
    if (value_stack.size() < 2) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Not enough operands for binary logical operation");
    }
    auto right_fn = value_stack.top();
    value_stack.pop();
    auto left_fn = value_stack.top();
    value_stack.pop();
    auto logical = opr.logical();
    value_stack.push([left_fn, right_fn, logical](
                         const execution::DataChunk& chunk, size_t row) {
      auto left_val = left_fn(chunk, row);
      auto right_val = right_fn(chunk, row);
      if (logical == ::common::Logical::AND ||
          logical == ::common::Logical::OR) {
        return execution::Value::BOOLEAN(compare_values(
            logical, execution::Value::BOOLEAN(left_val.GetValue<bool>()),
            execution::Value::BOOLEAN(right_val.GetValue<bool>())));
      }
      return execution::Value::BOOLEAN(
          compare_values(logical, left_val, right_val));
    });
  };

  for (int i = 0; i < expr.operators_size(); ++i) {
    const auto& opr = expr.operators(i);
    switch (opr.item_case()) {
    case ::common::ExprOpr::kConst: {
      auto value = proto_value_to_execution(opr.const_());
      value_stack.push(
          [value](const execution::DataChunk&, size_t) { return value; });
      break;
    }
    case ::common::ExprOpr::kVar: {
      const std::string& column_name = opr.var().tag().name();
      auto iter = column_index.find(column_name);
      if (iter == column_index.end()) {
        THROW_INVALID_ARGUMENT_EXCEPTION("Filter column not found: " +
                                         column_name);
      }
      int col_idx = iter->second;
      value_stack.push(
          [col_idx](const execution::DataChunk& chunk, size_t row) {
            auto col = chunk.get(col_idx);
            if (!col) {
              THROW_RUNTIME_ERROR("Missing filter column at index " +
                                  std::to_string(col_idx));
            }
            return col->get_elem(row);
          });
      break;
    }
    case ::common::ExprOpr::kBrace: {
      if (opr.brace() == ::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE) {
        op_stack.push(opr);
      } else {
        while (!op_stack.empty() &&
               op_stack.top().item_case() != ::common::ExprOpr::kBrace) {
          apply_operator(op_stack.top());
          op_stack.pop();
        }
        if (op_stack.empty()) {
          THROW_INVALID_ARGUMENT_EXCEPTION("Mismatched parentheses in filter");
        }
        op_stack.pop();
      }
      break;
    }
    case ::common::ExprOpr::kLogical: {
      int current_prec = OperatorPrecedence::getPrecedence(opr);
      while (!op_stack.empty() &&
             op_stack.top().item_case() != ::common::ExprOpr::kBrace &&
             OperatorPrecedence::getPrecedence(op_stack.top()) <=
                 current_prec) {
        apply_operator(op_stack.top());
        op_stack.pop();
      }
      op_stack.push(opr);
      break;
    }
    default:
      THROW_NOT_IMPLEMENTED_EXCEPTION("Unsupported token in row filter");
    }
  }

  while (!op_stack.empty()) {
    if (op_stack.top().item_case() == ::common::ExprOpr::kBrace) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Mismatched parentheses in filter");
    }
    apply_operator(op_stack.top());
    op_stack.pop();
  }

  if (value_stack.empty()) {
    evaluator_ = nullptr;
    return;
  }
  if (value_stack.size() != 1) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid filter expression");
  }
  auto value_fn = value_stack.top();
  evaluator_ = [value_fn](const execution::DataChunk& chunk, size_t row) {
    return value_fn(chunk, row).GetValue<bool>();
  };
}

bool RowExpressionFilter::eval(const execution::DataChunk& chunk,
                               size_t row) const {
  if (!evaluator_) {
    return true;
  }
  return evaluator_(chunk, row);
}

execution::DataChunk read_all_chunks(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers) {
  execution::DataChunk merged;
  for (const auto& supplier : suppliers) {
    while (true) {
      auto chunk = supplier->GetNextChunk();
      if (!chunk) {
        break;
      }
      if (merged.row_num() == 0) {
        merged = *chunk;
      } else {
        merged = merged.union_chunk(*chunk);
      }
    }
  }
  return merged;
}

execution::DataChunk filter_chunk(
    const execution::DataChunk& input,
    const std::shared_ptr<::common::Expression>& filter_expr,
    const std::vector<std::string>& column_names) {
  if (!filter_expr || input.row_num() == 0) {
    return input;
  }

  std::unordered_map<std::string, int> name_to_index;
  build_name_to_index(column_names, &name_to_index);
  RowExpressionFilter filter(*filter_expr, name_to_index);

  sel_vec_t keep_offsets;
  keep_offsets.reserve(input.row_num());
  for (size_t row = 0; row < input.row_num(); ++row) {
    if (filter.eval(input, row)) {
      keep_offsets.push_back(static_cast<sel_t>(row));
    }
  }

  execution::DataChunk filtered = input;
  filtered.reshuffle(keep_offsets);
  return filtered;
}

execution::DataChunk project_chunk(
    const execution::DataChunk& input,
    const std::vector<std::string>& column_names,
    const std::vector<std::string>& project_columns) {
  if (project_columns.empty()) {
    return input;
  }

  std::unordered_map<std::string, int> name_to_index;
  build_name_to_index(column_names, &name_to_index);

  execution::DataChunk projected;
  for (size_t i = 0; i < project_columns.size(); ++i) {
    auto iter = name_to_index.find(project_columns[i]);
    if (iter == name_to_index.end()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Project column not found: " +
                                       project_columns[i]);
    }
    projected.set(static_cast<int>(i), input.get(iter->second));
  }
  return projected;
}

}  // namespace reader
}  // namespace neug
