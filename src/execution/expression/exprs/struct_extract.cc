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

#include "neug/execution/expression/exprs/struct_extract.h"

#include "neug/common/types/value.h"

namespace neug {
namespace execution {
namespace {

// Deliberate tradeoff: always evaluate the child to a full struct Value and
// then select one field. This keeps property and computed expressions on the
// same path without struct-specific hooks in the expression interface. A
// column-backed struct may read and assemble sibling fields unnecessarily;
// optimize that only if a measured workload justifies the extra binding logic.
class BindedStructExtractExpr : public VertexExprBase,
                                public EdgeExprBase,
                                public RecordExprBase {
 public:
  BindedStructExtractExpr(std::unique_ptr<BindedExprBase> child,
                          size_t field_idx, const DataType& type)
      : child_(std::move(child)), field_idx_(field_idx), type_(type) {}

  const DataType& type() const override { return type_; }
  Value eval_vertex(label_t v_label, vid_t v) const override {
    return extract(child_->Cast<VertexExprBase>().eval_vertex(v_label, v));
  }
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    return extract(
        child_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr));
  }
  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    return extract(child_->Cast<RecordExprBase>().eval_record(chunk, idx));
  }

 private:
  Value extract(const Value& struct_val) const {
    if (struct_val.IsNull()) {
      return Value(type_);
    }
    return StructValue::GetChildren(struct_val)[field_idx_];
  }

  std::unique_ptr<BindedExprBase> child_;
  size_t field_idx_;
  DataType type_;
};

}  // namespace

StructExtractExpr::StructExtractExpr(std::unique_ptr<ExprBase> child,
                                     size_t field_idx, DataType field_type)
    : child_(std::move(child)),
      field_idx_(field_idx),
      type_(std::move(field_type)) {}

std::unique_ptr<BindedExprBase> StructExtractExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedStructExtractExpr>(
      child_->bind(storage, params), field_idx_, type_);
}

}  // namespace execution
}  // namespace neug
