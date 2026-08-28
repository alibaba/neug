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

#pragma once
#include <memory>
#include <vector>

#include "neug/execution/expression/expr.h"
#include "neug/utils/property/column.h"

namespace neug {
namespace execution {

// Extracts field `field_idx` from a struct-typed child expression, e.g. the
// `city` in `n.address.city`. At bind time the access is pushed down to the
// field's own property column when the child reads a struct column directly
// (so only that column is scanned); otherwise the whole struct value is
// evaluated and the field picked from it.
class StructExtractExpr : public ExprBase {
 public:
  StructExtractExpr(std::unique_ptr<ExprBase> child, size_t field_idx,
                    DataType field_type);
  ~StructExtractExpr() override = default;

  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;
  std::string name() const override { return "StructExtractExpr"; }

 private:
  std::unique_ptr<ExprBase> child_;
  size_t field_idx_;
  DataType type_;
};

// Projection-pushdown factories used by property accessors. `parent_columns`
// are the per-label struct ref columns backing a struct property; each is
// narrowed to its `field_idx` child column. Returns nullptr when any present
// column is not a struct ref column, letting the caller fall back to
// whole-struct evaluation.
std::unique_ptr<BindedExprBase> bind_vertex_struct_field(
    const std::vector<std::shared_ptr<RefColumnBase>>& parent_columns,
    size_t field_idx, const DataType& field_type);
std::unique_ptr<BindedExprBase> bind_record_vertex_struct_field(
    int tag, const std::vector<std::shared_ptr<RefColumnBase>>& parent_columns,
    size_t field_idx, const DataType& field_type);

}  // namespace execution
}  // namespace neug
