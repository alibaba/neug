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

#include <algorithm>
#include <cctype>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/common/types/value.h"
#include "neug/execution/utils/var.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/utils/function_type.h"
#include "neug/utils/property/types.h"

namespace gs {

namespace runtime {
class Context;
struct LabelTriplet;

class ExprBase {
 public:
  virtual Value eval_path(size_t idx) const = 0;
  virtual Value eval_vertex(label_t label, vid_t v) const = 0;
  virtual Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const void* data_ptr) const = 0;
  virtual const DataType& type() const = 0;

  virtual bool is_optional() const { return false; }

  virtual ~ExprBase() = default;
};

class WithInListExpr : public ExprBase {
 public:
  WithInListExpr(const Context& ctx, std::unique_ptr<ExprBase>&& key,
                 std::unique_ptr<ExprBase>&& val_list)
      : key_(std::move(key)),
        val_list_(std::move(val_list)),
        type_(DataType(DataTypeId::kBoolean)) {}

  Value eval_path(size_t idx) const override {
    Value key_val = key_->eval_path(idx);
    if (key_val.IsNull()) {
      return Value::BOOLEAN(false);
    }
    Value list_val = val_list_->eval_path(idx);
    const auto& children = ListValue::GetChildren(list_val);
    for (const auto& child : children) {
      if (child == key_val) {
        return Value::BOOLEAN(true);
      }
    }
    return Value::BOOLEAN(false);
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    Value key_val = key_->eval_vertex(label, v);
    if (key_val.IsNull()) {
      return Value::BOOLEAN(false);
    }
    Value list_val = val_list_->eval_vertex(label, v);
    const auto& children = ListValue::GetChildren(list_val);
    for (const auto& child : children) {
      if (child == key_val) {
        return Value::BOOLEAN(true);
      }
    }
    return Value::BOOLEAN(false);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    Value key_val = key_->eval_edge(label, src, dst, data_ptr);
    if (key_val.IsNull()) {
      return Value::BOOLEAN(false);
    }
    Value list_val = val_list_->eval_edge(label, src, dst, data_ptr);
    const auto& children = ListValue::GetChildren(list_val);
    for (const auto& child : children) {
      if (child == key_val) {
        return Value::BOOLEAN(true);
      }
    }
    return Value::BOOLEAN(false);
  }

  const DataType& type() const override { return type_; }

  bool is_optional() const override { return key_->is_optional(); }

  std::unique_ptr<ExprBase> key_;
  std::unique_ptr<ExprBase> val_list_;
  DataType type_;
};

#define PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(dst_vector_name, array_name) \
  size_t len = array_name.item_size();                                   \
  for (size_t idx = 0; idx < len; ++idx) {                               \
    dst_vector_name.push_back(array_name.item(idx));                     \
  }

template <typename T>
class WithInExpr : public ExprBase {
 public:
  WithInExpr(const Context& ctx, std::unique_ptr<ExprBase>&& key,
             const ::common::Value& array)
      : key_(std::move(key)), type_(DataType(DataTypeId::kBoolean)) {
    if constexpr (std::is_same_v<T, int64_t>) {
      if (array.item_case() == ::common::Value::kI64Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else if (array.item_case() == ::common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else {
        // TODO(zhanglei,lexiao): We should support more types here, and if type
        // conversion fails, we should return an error.
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int64_t array";
      }
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      if (array.item_case() == ::common::Value::kI64Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else if (array.item_case() == ::common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else {
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int64_t array";
      }
    } else if constexpr (std::is_same_v<T, int32_t>) {
      if (array.item_case() == ::common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else if constexpr (std::is_same_v<T, int64_t>) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else {
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int32_t array";
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      assert(array.item_case() == ::common::Value::kStrArray);
      PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.str_array());
    } else {
      LOG(FATAL) << "not implemented";
    }
  }

  Value eval_impl(const Value& val) const {
    if (val.IsNull()) {
      return Value::BOOLEAN(false);
    }
    if constexpr (std::is_same_v<T, std::string>) {
      auto str_val = val.GetValue<std::string>();
      return Value::BOOLEAN(std::find(container_.begin(), container_.end(),
                                      str_val) != container_.end());
    } else {
      auto typed_val = val.GetValue<T>();
      return Value::BOOLEAN(std::find(container_.begin(), container_.end(),
                                      typed_val) != container_.end());
    }
  }

  Value eval_path(size_t idx) const override {
    auto any_val = key_->eval_path(idx);
    return eval_impl(any_val);
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    auto any_val = key_->eval_vertex(label, v);
    return eval_impl(any_val);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    auto any_val = key_->eval_edge(label, src, dst, data_ptr);
    return eval_impl(any_val);
  }
  const DataType& type() const override { return type_; }
  bool is_optional() const override { return key_->is_optional(); }

  std::unique_ptr<ExprBase> key_;
  std::vector<T> container_;
  DataType type_;
};

class VariableExpr : public ExprBase {
 public:
  VariableExpr(const StorageReadInterface* graph, const Context& ctx,
               const ::common::Variable& pb, VarType var_type)
      : var_(graph, ctx, pb, var_type) {
    type_ = var_.type();
  }
  Value eval_path(size_t idx) const override;
  Value eval_vertex(label_t label, vid_t v) const override;
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override;
  const DataType& type() const override { return type_; }

  bool is_optional() const override { return var_.is_optional(); }

 private:
  DataType type_;
  Var var_;
};

class UnaryLogicalExpr : public ExprBase {
 public:
  UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr, ::common::Logical logic);

  Value eval_path(size_t idx) const override;
  Value eval_vertex(label_t label, vid_t v) const override;
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override;

  const DataType& type() const override { return type_; }

  bool is_optional() const override { return expr_->is_optional(); }

 private:
  std::unique_ptr<ExprBase> expr_;
  ::common::Logical logic_;
  DataType type_;
};

class LogicalExpr : public ExprBase {
 public:
  LogicalExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs,
              ::common::Logical logic);

  Value eval_path(size_t idx) const override;
  Value eval_vertex(label_t label, vid_t v) const override;
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override;

  Value eval_impl(const Value& lhs_val, const Value& rhs_val) const;

  const DataType& type() const override { return type_; }

  bool is_optional() const override {
    return lhs_->is_optional() || rhs_->is_optional();
  }

 protected:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  std::function<bool(Value, Value)> op_;
  ::common::Logical logic_;
  DataType type_;
};

class AndOpExpr : public LogicalExpr {
 public:
  AndOpExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs)
      : LogicalExpr(std::move(lhs), std::move(rhs), common::Logical::AND) {}

  bool is_optional() const override { return LogicalExpr::is_optional(); }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    const auto& lhs_val = lhs_->eval_edge(label, src, dst, data_ptr);
    if (!lhs_val.IsTrue()) {
      return lhs_val;
    }
    return rhs_->eval_edge(label, src, dst, data_ptr);
  }

  Value eval_path(size_t idx) const override {
    const auto& lhs_val = lhs_->eval_path(idx);
    if (!lhs_val.IsTrue()) {
      return lhs_val;
    }
    return rhs_->eval_path(idx);
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    const auto& lhs_val = lhs_->eval_vertex(label, v);
    if (!lhs_val.IsTrue()) {
      return lhs_val;
    }
    return rhs_->eval_vertex(label, v);
  }
};

class OrOpExpr : public LogicalExpr {
 public:
  OrOpExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs)
      : LogicalExpr(std::move(lhs), std::move(rhs), common::Logical::OR) {}
  bool is_optional() const override { return LogicalExpr::is_optional(); }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    const auto& lhs_val = lhs_->eval_edge(label, src, dst, data_ptr);
    if (lhs_val.IsTrue()) {
      return lhs_val;
    }
    return rhs_->eval_edge(label, src, dst, data_ptr);
  }

  Value eval_path(size_t idx) const override {
    const auto& lhs_val = lhs_->eval_path(idx);
    if (lhs_val.IsTrue()) {
      return lhs_val;
    }
    return rhs_->eval_path(idx);
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    const auto& lhs_val = lhs_->eval_vertex(label, v);
    if (lhs_val.IsTrue()) {
      return lhs_val;
    }
    return rhs_->eval_vertex(label, v);
  }
};

int32_t extract_time_from_milli_second(int64_t ms, ::common::Extract extract);

template <typename T>
class ExtractExpr : public ExprBase {
 public:
  ExtractExpr(std::unique_ptr<ExprBase>&& expr,
              const ::common::Extract& extract)
      : expr_(std::move(expr)),
        extract_(extract),
        type_(DataType(DataTypeId::kInt64)) {}
  int64_t eval_impl(const Value& val) const {
    if constexpr (std::is_same_v<T, int64_t>) {
      return extract_time_from_milli_second(val.GetValue<int64_t>(), extract_);
    } else if constexpr (std::is_same_v<T, DateTime>) {
      return extract_time_from_milli_second(
          val.GetValue<DateTime>().milli_second, extract_);

    } else if constexpr (std::is_same_v<T, Date>) {
      if (extract_.interval() == ::common::Extract::DAY) {
        return val.GetValue<Date>().day();
      } else if (extract_.interval() == ::common::Extract::MONTH) {
        return val.GetValue<Date>().month();
      } else if (extract_.interval() == ::common::Extract::YEAR) {
        return val.GetValue<Date>().year();
      } else if (extract_.interval() == ::common::Extract::HOUR) {
        return val.GetValue<Date>().hour();
      } else if (extract_.interval() == ::common::Extract::MINUTE) {
        THROW_RUNTIME_ERROR(
            "Unsupported extract interval for Date type: MINUTE");
      } else if (extract_.interval() == ::common::Extract::SECOND) {
        THROW_RUNTIME_ERROR(
            "Unsupported extract interval for Date type: SECOND");
      } else if (extract_.interval() == ::common::Extract::MILLISECOND) {
        THROW_RUNTIME_ERROR(
            "Unsupported extract interval for Date type: MILLISECOND");
      } else {
        THROW_RUNTIME_ERROR("Unsupported extract interval for Date type");
      }
    } else if constexpr (std::is_same_v<T, Interval>) {
      if (extract_.interval() == ::common::Extract::DAY) {
        return val.GetValue<Interval>().day();
      } else if (extract_.interval() == ::common::Extract::MONTH) {
        return val.GetValue<Interval>().month();
      } else if (extract_.interval() == ::common::Extract::YEAR) {
        return val.GetValue<Interval>().year();
      } else if (extract_.interval() == ::common::Extract::HOUR) {
        return val.GetValue<Interval>().hour();
      } else if (extract_.interval() == ::common::Extract::MINUTE) {
        return val.GetValue<Interval>().minute();
      } else if (extract_.interval() == ::common::Extract::SECOND) {
        return val.GetValue<Interval>().second();
      } else if (extract_.interval() == ::common::Extract::MILLISECOND) {
        return val.GetValue<Interval>().millisecond();
      } else {
        THROW_RUNTIME_ERROR("Unsupported extract interval for Interval type");
      }
    }
    LOG(FATAL) << "not support" << extract_.DebugString();
    return 0;
  }

  Value eval_path(size_t idx) const override {
    auto any_val = expr_->eval_path(idx);
    if (any_val.IsNull()) {
      return Value(type_);
    }
    return Value::INT64(eval_impl(any_val));
  }
  Value eval_vertex(label_t label, vid_t v) const override {
    auto any_val = expr_->eval_vertex(label, v);
    if (any_val.IsNull()) {
      return Value(type_);
    }
    return Value::INT64(eval_impl(any_val));
  }
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    auto any_val = expr_->eval_edge(label, src, dst, data_ptr);
    if (any_val.IsNull()) {
      return Value(type_);
    }
    return Value::INT64(eval_impl(any_val));
  }

  const DataType& type() const override { return type_; }

 private:
  std::unique_ptr<ExprBase> expr_;
  const ::common::Extract extract_;
  DataType type_;
};
class ArithExpr : public ExprBase {
 public:
  ArithExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs,
            ::common::Arithmetic arith);

  Value eval_path(size_t idx) const override;
  Value eval_vertex(label_t label, vid_t v) const override;
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override;

  const DataType& type() const override { return type_; }

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  std::function<Value(Value, Value)> op_;
  ::common::Arithmetic arith_;
  DataType type_;
};

class ConstExpr : public ExprBase {
 public:
  explicit ConstExpr(const Value& val);
  Value eval_path(size_t idx) const override;
  Value eval_vertex(label_t label, vid_t v) const override;
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override;

  const DataType& type() const override { return type_; }

  bool is_optional() const override { return val_.IsNull(); }

 private:
  DataType type_;
  Value val_;
};

class CaseWhenExpr : public ExprBase {
 public:
  CaseWhenExpr(
      std::vector<std::pair<std::unique_ptr<ExprBase>,
                            std::unique_ptr<ExprBase>>>&& when_then_exprs,
      std::unique_ptr<ExprBase>&& else_expr);

  Value eval_path(size_t idx) const override;
  Value eval_vertex(label_t label, vid_t v) const override;
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override;

  const DataType& type() const override { return type_; }

  bool is_optional() const override {
    for (auto& expr_pair : when_then_exprs_) {
      if (expr_pair.first->is_optional() || expr_pair.second->is_optional()) {
        return true;
      }
    }
    return else_expr_->is_optional();
  }

 private:
  DataType type_;
  std::vector<std::pair<std::unique_ptr<ExprBase>, std::unique_ptr<ExprBase>>>
      when_then_exprs_;
  std::unique_ptr<ExprBase> else_expr_;
};

class ScalarFunctionExpr : public ExprBase {
 public:
  ScalarFunctionExpr(neug_func_exec_t fn, const DataType& ret_type,
                     std::vector<std::unique_ptr<ExprBase>>&& children)
      : func_(fn), ret_type_(ret_type), children_(std::move(children)) {}

  Value eval_path(size_t idx) const override {
    std::vector<Value> params;
    params.reserve(children_.size());
    for (auto& ch : children_) {
      params.emplace_back(ch->eval_path(idx));
    }
    return func_(params);
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    std::vector<Value> params;
    params.reserve(children_.size());
    for (auto& ch : children_) {
      params.emplace_back(ch->eval_vertex(label, v));
    }
    return func_(params);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    std::vector<Value> params;
    params.reserve(children_.size());
    for (auto& ch : children_) {
      params.emplace_back(ch->eval_edge(label, src, dst, data_ptr));
    }
    return func_(params);
  }

  const DataType& type() const override { return ret_type_; }

  bool is_optional() const override {
    for (auto& ch : children_) {
      if (ch->is_optional())
        return true;
    }
    return false;
  }

 private:
  neug_func_exec_t func_;
  DataType ret_type_;
  std::vector<std::unique_ptr<ExprBase>> children_;
};

class TupleExpr : public ExprBase {
 public:
  explicit TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs);

  Value eval_path(size_t idx) const override;
  Value eval_vertex(label_t label, vid_t v) const override;
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override;

  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  std::vector<std::unique_ptr<ExprBase>> exprs_;
};

class PathVertexPropsExpr : public ExprBase {
 public:
  explicit PathVertexPropsExpr(const StorageReadInterface& graph,
                               const Context& ctx, int tag,
                               const std::string& prop,
                               const DataType& prop_type)
      : graph_(graph),
        col_(dynamic_cast<const PathColumn&>(*ctx.get(tag))),
        prop_(prop),
        prop_type_(prop_type) {
    auto chd_type = std::make_shared<ListTypeInfo>(prop_type_);
    type_ = DataType(DataTypeId::kList, std::move(chd_type));
  }

  Value eval_path(size_t idx) const override;

  Value eval_vertex(label_t label, vid_t v) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  const DataType& type() const override { return type_; }

  DataType type_;
  const StorageReadInterface& graph_;
  const PathColumn& col_;
  std::string prop_;
  DataType prop_type_;
};

class PathEdgePropsExpr : public ExprBase {
 public:
  explicit PathEdgePropsExpr(const StorageReadInterface& graph,
                             const Context& ctx, int tag,
                             const std::string& prop, const DataType& prop_type)
      : graph_(graph),
        col_(dynamic_cast<const PathColumn&>(*ctx.get(tag))),
        prop_(prop),
        prop_type_(prop_type) {
    auto chd_type = std::make_shared<ListTypeInfo>(prop_type_);
    type_ = DataType(DataTypeId::kList, std::move(chd_type));
  }
  Value eval_path(size_t idx) const override;

  Value eval_vertex(label_t label, vid_t v) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  const DataType& type() const override { return type_; }

  const StorageReadInterface& graph_;
  const PathColumn& col_;
  std::string prop_;
  DataType prop_type_;
  DataType type_;
};

class RelationshipsExpr : public ExprBase {
 public:
  explicit RelationshipsExpr(std::unique_ptr<ExprBase>&& args)
      : args(std::move(args)) {
    auto chd_type = std::make_shared<ListTypeInfo>(DataType(DataTypeId::kEdge));
    type_ = DataType(DataTypeId::kList, std::move(chd_type));
  }
  Value eval_path(size_t idx) const override {
    assert(args->type().id() == DataTypeId::kPath);
    auto path_any = args->eval_path(idx);
    if (path_any.IsNull()) {
      return Value(type_);
    }
    auto path = PathValue::Get(path_any);
    auto rels = path.relationships();
    std::vector<Value> rel_list;
    rel_list.reserve(rels.size());
    for (const auto& rel : rels) {
      rel_list.emplace_back(Value::EDGE(rel));
    }
    return Value::LIST(DataType::EDGE, std::move(rel_list));
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  bool is_optional() const override { return args->is_optional(); }

  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  std::unique_ptr<ExprBase> args;
};

class NodesExpr : public ExprBase {
 public:
  explicit NodesExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {
    auto chd_type =
        std::make_shared<ListTypeInfo>(DataType(DataTypeId::kVertex));
    type_ = DataType(DataTypeId::kList, std::move(chd_type));
  }

  Value eval_path(size_t idx) const override {
    assert(args->type().id() == DataTypeId::kPath);
    auto path_any = args->eval_path(idx);
    if (path_any.IsNull()) {
      return Value(type_);
    }
    auto path = PathValue::Get(path_any);
    auto nodes = path.nodes();
    std::vector<Value> node_list;
    node_list.reserve(nodes.size());
    for (const auto& node : nodes) {
      node_list.emplace_back(Value::VERTEX(node));
    }
    return Value::LIST(DataType::VERTEX, std::move(node_list));
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  bool is_optional() const override { return args->is_optional(); }

  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  std::unique_ptr<ExprBase> args;
};

class StartNodeExpr : public ExprBase {
 public:
  explicit StartNodeExpr(std::unique_ptr<ExprBase>&& args)
      : args(std::move(args)), type_(DataType(DataTypeId::kVertex)) {}
  Value eval_path(size_t idx) const override {
    assert(args->type().id() == DataTypeId::kEdge);
    auto path_any = args->eval_path(idx);
    if (path_any.IsNull()) {
      return Value(type_);
    }
    auto edge = path_any.GetValue<EdgeRecord>();
    auto node = edge.start_node();
    return Value::VERTEX(node);
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  const DataType& type() const override { return type_; }

  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
  DataType type_;
};

class EndNodeExpr : public ExprBase {
 public:
  explicit EndNodeExpr(std::unique_ptr<ExprBase>&& args)
      : args(std::move(args)), type_(DataType(DataTypeId::kVertex)) {}
  Value eval_path(size_t idx) const override {
    assert(args->type().id() == DataTypeId::kEdge);
    auto path_any = args->eval_path(idx);
    if (path_any.IsNull()) {
      return Value(DataType(type_));
    }
    auto path = path_any.GetValue<EdgeRecord>();
    auto node = path.end_node();
    return Value::VERTEX(node);
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    LOG(FATAL) << "should not be called";
    return Value(type_);
  }

  const DataType& type() const override { return type_; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
  DataType type_;
};

class AbsExpr : public ExprBase {
 public:
  AbsExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {
    type_ = args->type();
  }

  Value abs_impl(const Value& val) const {
    switch (val.type().id()) {
    case DataTypeId::kInt32:
      return Value::INT32(std::abs(val.GetValue<int32_t>()));
    case DataTypeId::kInt64:
      return Value::INT64(std::abs(val.GetValue<int64_t>()));
    case DataTypeId::kFloat:
      return Value::FLOAT(std::fabs(val.GetValue<float>()));
    case DataTypeId::kDouble:
      return Value::DOUBLE(std::fabs(val.GetValue<double>()));
    default:
      break;
    }
    THROW_RUNTIME_ERROR("Abs: input value is not numeric");
  }

  Value eval_path(size_t idx) const override {
    auto val = args->eval_path(idx);
    return abs_impl(val);
  }

  Value eval_vertex(label_t label, vid_t v) const override {
    auto val = args->eval_vertex(label, v);
    return abs_impl(val);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data) const override {
    auto val = args->eval_edge(label, src, dst, data);
    return abs_impl(val);
  }
  const DataType& type() const override { return type_; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  DataType type_;
  std::unique_ptr<ExprBase> args;
};

std::unique_ptr<ExprBase> parse_expression(const StorageReadInterface* graph,
                                           const Context& ctx,
                                           const ParamsMap& params,
                                           const ::common::Expression& expr,
                                           VarType var_type);

bool graph_related_expr(const ::common::Expression& expr);

}  // namespace runtime

}  // namespace gs
