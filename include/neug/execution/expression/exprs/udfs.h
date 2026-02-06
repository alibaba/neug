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
#include "neug/execution/expression/expr.h"
#include "neug/utils/function_type.h"

namespace neug {
namespace execution {
class ScalarFunctionExpr : public ExprBase {
 public:
  ScalarFunctionExpr(neug_func_exec_t fn, const DataType& ret_type,
                     std::vector<std::unique_ptr<ExprBase>>&& children)
      : func_(fn), ret_type_(ret_type), children_(std::move(children)) {}
  ~ScalarFunctionExpr() override = default;
  const DataType& type() const override { return ret_type_; }

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  neug_func_exec_t func_;
  DataType ret_type_;
  std::vector<std::unique_ptr<ExprBase>> children_;
};
}  // namespace execution
}  // namespace neug