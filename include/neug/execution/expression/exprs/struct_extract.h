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

#include "neug/execution/expression/expr.h"

namespace neug {
namespace execution {

// Extracts field `field_idx` from a struct-typed child expression, e.g. the
// `city` in `n.address.city`. The child is evaluated to a complete struct
// Value before selecting the field.
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

}  // namespace execution
}  // namespace neug
