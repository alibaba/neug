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

#include "neug/execution/expression/exprs/udfs.h"

namespace neug {
namespace execution {
class BindedScalarFunctionExpr : public VertexExprBase,
                                 public EdgeExprBase,
                                 public RecordExprBase {
 public:
  BindedScalarFunctionExpr(
      neug_func_exec_t fn, const DataType& ret_type,
      std::vector<std::unique_ptr<BindedExprBase>>&& children)
      : func_(fn), ret_type_(ret_type), children_(std::move(children)) {}
  const DataType& type() const override { return ret_type_; }

  Value eval_record(const Context& ctx, size_t idx) const override {
    std::vector<Value> params;
    params.reserve(children_.size());
    for (auto& ch : children_) {
      params.emplace_back(ch->Cast<RecordExprBase>().eval_record(ctx, idx));
    }
    return func_(params);
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    std::vector<Value> params;
    params.reserve(children_.size());
    for (auto& ch : children_) {
      params.emplace_back(ch->Cast<VertexExprBase>().eval_vertex(label, v));
    }
    return func_(params);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    std::vector<Value> params;
    params.reserve(children_.size());
    for (auto& ch : children_) {
      params.emplace_back(
          ch->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr));
    }
    return func_(params);
  }

 private:
  neug_func_exec_t func_;
  DataType ret_type_;
  std::vector<std::unique_ptr<BindedExprBase>> children_;
};

std::unique_ptr<BindedExprBase> ScalarFunctionExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  std::vector<std::unique_ptr<BindedExprBase>> bound_children;
  for (const auto& child : children_) {
    bound_children.push_back(child->bind(storage, params));
  }
  return std::make_unique<BindedScalarFunctionExpr>(func_, ret_type_,
                                                    std::move(bound_children));
}
}  // namespace execution
}  // namespace neug