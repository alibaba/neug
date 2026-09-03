

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

#include "neug/execution/execute/ops/retrieve/scan_utils.h"
#include "neug/common/types/value.h"
#include "neug/execution/expression/expr.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace execution {
namespace ops {

static Value evaluate_index_expression(
    const algebra::IndexPredicate_Triplet& triplet, const ParamsMap& params) {
  auto expr =
      parse_expression(triplet.expression(), ContextMeta{}, VarType::kRecord);
  if (!expr) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "failed to parse primary key index predicate expression");
  }
  auto bound_expr = expr->bind(nullptr, params);
  if (!bound_expr) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "failed to bind primary key index predicate expression");
  }

  DataChunk empty_chunk;
  return bound_expr->Cast<RecordExprBase>().eval_record(empty_chunk, 0);
}

std::vector<Value> ScanUtils::parse_ids(
    const algebra::IndexPredicate_Triplet& triplet, const ParamsMap& params) {
  auto value = evaluate_index_expression(triplet, params);
  if (value.IsNull()) {
    return {};
  }
  std::vector<Value> values;
  if (value.type().id() == DataTypeId::kList) {
    values = ListValue::GetChildren(value);
  } else if (value.type().id() == DataTypeId::kArray) {
    values = ArrayValue::GetChildren(value);
  } else {
    values.emplace_back(std::move(value));
  }

  std::vector<Value> ret;
  ret.reserve(values.size());
  for (auto& item : values) {
    if (!item.IsNull()) {
      ret.emplace_back(std::move(item));
    }
  }
  return ret;
}
bool ScanUtils::check_idx_predicate(const physical::Scan& scan_opr) {
  if (scan_opr.scan_opt() != physical::Scan::VERTEX) {
    return false;
  }

  if (!scan_opr.has_params()) {
    return false;
  }

  if (!scan_opr.has_idx_predicate()) {
    return false;
  }
  const algebra::IndexPredicate& predicate = scan_opr.idx_predicate();
  if (predicate.or_predicates_size() != 1) {
    return false;
  }
  if (predicate.or_predicates(0).predicates_size() != 1) {
    return false;
  }
  const algebra::IndexPredicate_Triplet& triplet =
      predicate.or_predicates(0).predicates(0);
  if (!triplet.has_key()) {
    return false;
  }

  if (triplet.cmp() != common::Logical::EQ &&
      triplet.cmp() != common::Logical::WITHIN) {
    return false;
  }

  if (!triplet.has_expression() || triplet.expression().operators_size() == 0) {
    return false;
  }

  return true;
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
