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

#include <functional>
#include <limits>
#include <utility>
#include <vector>

#include "neug/common/columns/columns_utils.h"
#include "neug/common/types/value.h"
#include "neug/execution/expression/expr.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/type_converter.h"

namespace neug {
namespace reader {
namespace {

// File predicates use column names; the execution evaluator uses chunk slots.
// Bind the caller's working copy in place, including nested expressions.
void prepare_expression(
    ::common::Expression& expr,
    const std::unordered_map<std::string, int>& column_index,
    const DataChunk& input, const execution::ParamsMap& parameters) {
  std::vector<::common::Expression*> pending{&expr};
  while (!pending.empty()) {
    auto* expression = pending.back();
    pending.pop_back();
    for (auto& opr : *expression->mutable_operators()) {
      if (opr.has_var()) {
        auto& variable = *opr.mutable_var();
        const auto& tag = variable.tag();
        if (!tag.has_name() || variable.has_property()) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "File filter requires a column name without a graph property");
        }
        auto iter = column_index.find(tag.name());
        if (iter == column_index.end()) {
          THROW_INVALID_ARGUMENT_EXCEPTION("Filter column not found: " +
                                           tag.name());
        }
        const int index = iter->second;
        if (index < 0 || static_cast<size_t>(index) >= input.col_num() ||
            !input.get(index)) {
          THROW_INVALID_ARGUMENT_EXCEPTION("Missing filter column: " +
                                           tag.name());
        }
        variable.mutable_tag()->set_id(index);
        if (!variable.has_node_type()) {
          *variable.mutable_node_type()->mutable_data_type() =
              *NeuGTypeConverter().convert(input.get(index)->elem_type());
        }
      }
      const ::common::DynamicParam* parameter = nullptr;
      if (opr.has_param()) {
        parameter = &opr.param();
      } else if (opr.has_time_interval() && opr.time_interval().has_param()) {
        parameter = &opr.time_interval().param();
      }
      if (parameter && parameters.find(parameter->name()) == parameters.end()) {
        THROW_INVALID_ARGUMENT_EXCEPTION("Missing file filter parameter: " +
                                         parameter->name());
      }
      if (opr.has_case_()) {
        auto& cases = *opr.mutable_case_();
        for (auto& when : *cases.mutable_when_then_expressions()) {
          if (when.has_when_expression()) {
            pending.push_back(when.mutable_when_expression());
          }
          if (when.has_then_result_expression()) {
            pending.push_back(when.mutable_then_result_expression());
          }
        }
        if (cases.has_else_result_expression()) {
          pending.push_back(cases.mutable_else_result_expression());
        }
      }
      if (opr.has_scalar_func()) {
        for (auto& child : *opr.mutable_scalar_func()->mutable_parameters()) {
          pending.push_back(&child);
        }
      }
      if (opr.has_udf_func()) {
        for (auto& child : *opr.mutable_udf_func()->mutable_parameters()) {
          pending.push_back(&child);
        }
      }
      if (opr.has_to_tuple()) {
        for (auto& child : *opr.mutable_to_tuple()->mutable_fields()) {
          pending.push_back(&child);
        }
      }
      if (opr.has_to_list()) {
        for (auto& child : *opr.mutable_to_list()->mutable_fields()) {
          pending.push_back(&child);
        }
      }
      if (opr.has_to_array()) {
        for (auto& child : *opr.mutable_to_array()->mutable_fields()) {
          pending.push_back(&child);
        }
      }
    }
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
    const std::unordered_map<std::string, int>& column_index,
    const DataChunk& input, const execution::ParamsMap& parameters) {
  if (expr.operators().empty()) {
    return;
  }
  auto bound_expression = expr;
  prepare_expression(bound_expression, column_index, input, parameters);
  auto expression = execution::parse_expression(
      bound_expression, execution::ContextMeta(), execution::VarType::kRecord);
  if (!expression || expression->type() != DataType::BOOLEAN) {
    THROW_INVALID_ARGUMENT_EXCEPTION("File filter must return a boolean");
  }
  std::shared_ptr<execution::BindedExprBase> bound =
      expression->bind(nullptr, parameters);
  evaluator_ = [bound = std::move(bound), input](size_t row) {
    return bound->Cast<execution::RecordExprBase>()
        .eval_record(input, row)
        .IsTrue();
  };
}

bool RowExpressionFilter::eval(size_t row) const {
  return !evaluator_ || evaluator_(row);
}

DataChunk read_all_chunks(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers) {
  std::vector<std::shared_ptr<DataChunk>> chunks;
  for (const auto& supplier : suppliers) {
    if (!supplier) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Data chunk supplier is null");
    }
    while (true) {
      auto chunk = supplier->GetNextChunk();
      if (!chunk) {
        break;
      }
      chunks.push_back(std::move(chunk));
    }
  }
  return merge_chunks(std::move(chunks));
}

DataChunk merge_chunks(std::vector<std::shared_ptr<DataChunk>> chunks) {
  size_t total_rows = 0;
  size_t column_count = 0;
  size_t chunk_count = 0;
  const DataChunk* first = nullptr;
  for (const auto& chunk : chunks) {
    if (!chunk || chunk->col_num() == 0) {
      continue;
    }
    ++chunk_count;
    if (!first) {
      first = chunk.get();
      column_count = chunk->col_num();
    } else if (chunk->col_num() != column_count) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Cannot merge data chunks with different column counts");
    }
    const auto row_count = chunk->row_num();
    if (row_count > std::numeric_limits<size_t>::max() - total_rows) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Merged data chunk is too large");
    }
    for (size_t column = 0; column < column_count; ++column) {
      auto value_column = chunk->get(static_cast<int>(column));
      if (!value_column || value_column->size() != row_count) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Cannot merge a data chunk with missing or uneven columns");
      }
      if (!(value_column->elem_type() ==
            first->get(static_cast<int>(column))->elem_type())) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Cannot merge data chunks with different column types");
      }
    }
    total_rows += row_count;
  }

  DataChunk merged;
  if (!first) {
    return merged;
  }
  if (chunk_count == 1) {
    return *first;
  }

  std::vector<std::shared_ptr<IContextColumnBuilder>> builders;
  builders.reserve(column_count);
  for (size_t column = 0; column < column_count; ++column) {
    const auto& first_column = first->get(static_cast<int>(column));
    auto builder = ColumnsUtils::create_builder(first_column->elem_type());
    builder->reserve(total_rows);
    builders.push_back(std::move(builder));
  }

  for (const auto& chunk : chunks) {
    if (!chunk || chunk->col_num() == 0) {
      continue;
    }
    const auto row_count = chunk->row_num();
    for (size_t column = 0; column < column_count; ++column) {
      const auto& value_column = chunk->get(static_cast<int>(column));
      for (size_t row = 0; row < row_count; ++row) {
        builders[column]->push_back_elem(value_column->get_elem(row));
      }
    }
  }
  for (size_t column = 0; column < column_count; ++column) {
    merged.set(static_cast<int>(column), builders[column]->finish());
  }
  return merged;
}

DataChunk filter_chunk(const DataChunk& input,
                       const std::shared_ptr<::common::Expression>& filter_expr,
                       const std::vector<std::string>& column_names,
                       const execution::ParamsMap& parameters) {
  if (!filter_expr || input.row_num() == 0) {
    return input;
  }

  std::unordered_map<std::string, int> name_to_index;
  build_name_to_index(column_names, &name_to_index);
  RowExpressionFilter filter(*filter_expr, name_to_index, input, parameters);

  sel_vec_t keep_offsets;
  keep_offsets.reserve(input.row_num());
  for (size_t row = 0; row < input.row_num(); ++row) {
    if (filter.eval(row)) {
      keep_offsets.push_back(static_cast<sel_t>(row));
    }
  }

  DataChunk filtered = input;
  filtered.reshuffle(keep_offsets);
  return filtered;
}

DataChunk project_chunk(const DataChunk& input,
                        const std::vector<std::string>& column_names,
                        const std::vector<std::string>& project_columns) {
  if (project_columns.empty()) {
    return input;
  }

  std::unordered_map<std::string, int> name_to_index;
  build_name_to_index(column_names, &name_to_index);

  DataChunk projected;
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
