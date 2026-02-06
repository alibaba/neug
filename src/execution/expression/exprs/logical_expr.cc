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

#include "neug/execution/expression/exprs/logical_expr.h"

#include <regex>

namespace neug {
namespace execution {
class BindedUnaryLogicalExpr : public VertexExprBase,
                               public EdgeExprBase,
                               public RecordExprBase {
 public:
  BindedUnaryLogicalExpr(std::unique_ptr<BindedExprBase>&& operand,
                         const ::common::Logical& logical)
      : operand_(std::move(operand)),
        type_(DataType::BOOLEAN),
        logical_(logical) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& val = operand_->Cast<RecordExprBase>().eval_record(ctx, idx);
    return eval_impl(logical_, val);
  }

  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    const auto& val =
        operand_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
    return eval_impl(logical_, val);
  }
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    const auto& val =
        operand_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr);
    return eval_impl(logical_, val);
  }

 private:
  Value eval_impl(const ::common::Logical& logical, const Value& val) const {
    switch (logical) {
    case ::common::Logical::NOT: {
      if (val.IsNull()) {
        return Value(DataType::BOOLEAN);
      }
      return Value::BOOLEAN(!val.GetValue<bool>());
    }
    case ::common::Logical::ISNULL: {
      return Value::BOOLEAN(val.IsNull());
    }
    default:
      LOG(FATAL) << "Unsupported unary logical operation: "
                 << static_cast<int>(logical);
      return Value(type_);
    }
  }
  std::unique_ptr<BindedExprBase> operand_;
  DataType type_;
  ::common::Logical logical_;
};

std::unique_ptr<BindedExprBase> UnaryLogicalExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedUnaryLogicalExpr>(
      operand_->bind(storage, params), logical_);
}

class BindedBinaryLogicalExpr : public VertexExprBase,
                                public EdgeExprBase,
                                public RecordExprBase {
 public:
  BindedBinaryLogicalExpr(std::unique_ptr<BindedExprBase>&& lhs,
                          std::unique_ptr<BindedExprBase>&& rhs,
                          const ::common::Logical& logical)
      : lhs_(std::move(lhs)),
        rhs_(std::move(rhs)),
        type_(DataType::BOOLEAN),
        logical_(logical) {}
  const DataType& type() const override { return type_; }
  Value eval_impl(const Value& lhs_val, const Value& rhs_val) const {
    if (lhs_val.IsNull() || rhs_val.IsNull()) {
      return Value(DataType::BOOLEAN);
    }
    switch (logical_) {
    case ::common::Logical::LT:
      return Value::BOOLEAN(lhs_val < rhs_val);
    case ::common::Logical::LE:
      return Value::BOOLEAN(lhs_val < rhs_val || lhs_val == rhs_val);
    case ::common::Logical::GT:
      return Value::BOOLEAN(rhs_val < lhs_val);
    case ::common::Logical::GE:
      return Value::BOOLEAN(rhs_val < lhs_val || lhs_val == rhs_val);
    case ::common::Logical::EQ:
      return Value::BOOLEAN(lhs_val == rhs_val);
    case ::common::Logical::NE:
      return Value::BOOLEAN(!(lhs_val == rhs_val));
    case ::common::Logical::REGEX: {
      auto lhs_str = lhs_val.GetValue<std::string>();
      auto rhs_str = rhs_val.GetValue<std::string>();
      return Value::BOOLEAN(std::regex_match(lhs_str, std::regex(rhs_str)));
    }
    default:
      LOG(FATAL) << "Unsupported binary logical operation: "
                 << static_cast<int>(logical_);
      return Value(type_);
    }
  }
  Value eval_record(const Context& ctx, size_t idx) const override {
    return eval_impl(lhs_->Cast<RecordExprBase>().eval_record(ctx, idx),
                     rhs_->Cast<RecordExprBase>().eval_record(ctx, idx));
  }
  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    return eval_impl(lhs_->Cast<VertexExprBase>().eval_vertex(v_label, v_id),
                     rhs_->Cast<VertexExprBase>().eval_vertex(v_label, v_id));
  }
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    return eval_impl(
        lhs_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr),
        rhs_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr));
  }

 private:
  std::unique_ptr<BindedExprBase> lhs_;
  std::unique_ptr<BindedExprBase> rhs_;
  DataType type_;
  ::common::Logical logical_;
};

// for short-circuit evaluation
class BindedAndExpr : public VertexExprBase,
                      public EdgeExprBase,
                      public RecordExprBase {
 public:
  BindedAndExpr(std::unique_ptr<BindedExprBase>&& lhs,
                std::unique_ptr<BindedExprBase>&& rhs)
      : lhs_(std::move(lhs)), rhs_(std::move(rhs)), type_(DataType::BOOLEAN) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& lhs_val = lhs_->Cast<RecordExprBase>().eval_record(ctx, idx);
    if (!lhs_val.IsTrue()) {
      return lhs_val;
    }
    return rhs_->Cast<RecordExprBase>().eval_record(ctx, idx);
  }
  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    const auto& lhs_val =
        lhs_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
    if (!lhs_val.IsTrue()) {
      return Value::BOOLEAN(false);
    }
    return rhs_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    const auto& lhs_val =
        lhs_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr);
    if (!lhs_val.IsTrue()) {
      return Value::BOOLEAN(false);
    }
    return rhs_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr);
  }

 private:
  std::unique_ptr<BindedExprBase> lhs_;
  std::unique_ptr<BindedExprBase> rhs_;
  DataType type_;
};

// for short-circuit evaluation
class BindedOrExpr : public VertexExprBase,
                     public EdgeExprBase,
                     public RecordExprBase {
 public:
  BindedOrExpr(std::unique_ptr<BindedExprBase>&& lhs,
               std::unique_ptr<BindedExprBase>&& rhs)
      : lhs_(std::move(lhs)), rhs_(std::move(rhs)), type_(DataType::BOOLEAN) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& lhs_val = lhs_->Cast<RecordExprBase>().eval_record(ctx, idx);

    if (lhs_val.IsTrue()) {
      return Value::BOOLEAN(true);
    }
    return rhs_->Cast<RecordExprBase>().eval_record(ctx, idx);
  }
  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    const auto& lhs_val =
        lhs_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);

    if (lhs_val.IsTrue()) {
      return Value::BOOLEAN(true);
    }
    return rhs_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    const auto& lhs_val =
        lhs_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr);

    if (lhs_val.IsTrue()) {
      return Value::BOOLEAN(true);
    }
    return rhs_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr);
  }

 private:
  std::unique_ptr<BindedExprBase> lhs_;
  std::unique_ptr<BindedExprBase> rhs_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> BinaryLogicalExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  if (logical_ == ::common::Logical::AND) {
    return std::make_unique<BindedAndExpr>(lhs_->bind(storage, params),
                                           rhs_->bind(storage, params));
  } else if (logical_ == ::common::Logical::OR) {
    return std::make_unique<BindedOrExpr>(lhs_->bind(storage, params),
                                          rhs_->bind(storage, params));
  }

  return std::make_unique<BindedBinaryLogicalExpr>(
      lhs_->bind(storage, params), rhs_->bind(storage, params), logical_);
}

class BindedWithInExpr : public VertexExprBase,
                         public EdgeExprBase,
                         public RecordExprBase {
 public:
  BindedWithInExpr(std::unique_ptr<BindedExprBase>&& lhs,
                   std::unique_ptr<BindedExprBase>&& rhs)
      : lhs_(std::move(lhs)), rhs_(std::move(rhs)), type_(DataType::BOOLEAN) {}

  const DataType& type() const override { return type_; }

  static Value eval_impl(const Value& lhs_val, const Value& rhs_val) {
    if (lhs_val.IsNull() || rhs_val.IsNull()) {
      return Value(DataType::BOOLEAN);
    }
    // rhs is list
    const auto& list_values = ListValue::GetChildren(rhs_val);
    for (const auto& val : list_values) {
      if (lhs_val == val) {
        return Value::BOOLEAN(true);
      }
    }
    return Value::BOOLEAN(false);
  }

  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& lhs_val = lhs_->Cast<RecordExprBase>().eval_record(ctx, idx);
    const auto& rhs_val = rhs_->Cast<RecordExprBase>().eval_record(ctx, idx);
    return eval_impl(lhs_val, rhs_val);
  }

  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    const auto& lhs_val =
        lhs_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
    const auto& rhs_val =
        rhs_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
    return eval_impl(lhs_val, rhs_val);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    const auto& lhs_val =
        lhs_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr);
    const auto& rhs_val =
        rhs_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr);
    return eval_impl(lhs_val, rhs_val);
  }

 private:
  std::unique_ptr<BindedExprBase> lhs_;
  std::unique_ptr<BindedExprBase> rhs_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> WithInExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedWithInExpr>(expr_->bind(storage, params),
                                            list_expr_->bind(storage, params));
}

}  // namespace execution
}  // namespace neug