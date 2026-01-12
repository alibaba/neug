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

#include <assert.h>
#include <glog/logging.h>
#include <stdint.h>
#include <array>
#include <compare>
#include <cstddef>

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

#include "neug/execution/utils/var.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/utils/function_type.h"
#include "neug/utils/property/types.h"
#include "neug/utils/runtime/rt_any.h"

namespace gs {

namespace runtime {
class Context;
struct LabelTriplet;

class ExprBase {
 public:
  virtual RTAny eval_path(size_t idx, Arena&) const = 0;
  virtual RTAny eval_vertex(label_t label, vid_t v, Arena&) const = 0;
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const void* data_ptr, Arena&) const = 0;
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
        type_(DataType(DataTypeId::BOOLEAN)) {}

  RTAny eval_path(size_t idx, Arena& arena) const override {
    RTAny key_val = key_->eval_path(idx, arena);
    if (key_val.is_null()) {
      return RTAny::from_bool(false);
    }
    RTAny list_val = val_list_->eval_path(idx, arena);
    return RTAny::from_bool(list_val.as_list().contains(key_val));
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    RTAny key_val = key_->eval_vertex(label, v, arena);
    if (key_val.is_null()) {
      return RTAny::from_bool(false);
    }
    RTAny list_val = val_list_->eval_vertex(label, v, arena);
    return RTAny::from_bool(list_val.as_list().contains(key_val));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena& arena) const override {
    RTAny key_val = key_->eval_edge(label, src, dst, data_ptr, arena);
    if (key_val.is_null()) {
      return RTAny::from_bool(false);
    }
    RTAny list_val = val_list_->eval_edge(label, src, dst, data_ptr, arena);
    return RTAny::from_bool(list_val.as_list().contains(key_val));
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
      : key_(std::move(key)), type_(DataType(DataTypeId::BOOLEAN)) {
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

  RTAny eval_impl(const RTAny& val) const {
    if (val.is_null()) {
      return RTAny::from_bool(false);
    }
    if constexpr (std::is_same_v<T, std::string>) {
      auto str_val = std::string(val.as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        str_val) != container_.end());
    } else {
      auto typed_val = TypedConverter<T>::to_typed(val);
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        typed_val) != container_.end());
    }
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto any_val = key_->eval_path(idx, arena);
    return eval_impl(any_val);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    auto any_val = key_->eval_vertex(label, v, arena);
    return eval_impl(any_val);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena& arena) const override {
    auto any_val = key_->eval_edge(label, src, dst, data_ptr, arena);
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
  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override;
  const DataType& type() const override { return type_; }

  bool is_optional() const override { return var_.is_optional(); }

 private:
  DataType type_;
  Var var_;
};

class UnaryLogicalExpr : public ExprBase {
 public:
  UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr, ::common::Logical logic);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override;

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

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override;

  RTAny eval_impl(const RTAny& lhs_val, const RTAny& rhs_val) const;

  const DataType& type() const override { return type_; }

  bool is_optional() const override {
    return lhs_->is_optional() || rhs_->is_optional();
  }

 protected:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  std::function<bool(RTAny, RTAny)> op_;
  ::common::Logical logic_;
  DataType type_;
};

class AndOpExpr : public LogicalExpr {
 public:
  AndOpExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs)
      : LogicalExpr(std::move(lhs), std::move(rhs), common::Logical::AND) {}

  bool is_optional() const override { return LogicalExpr::is_optional(); }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena& arena) const override {
    if (lhs_->eval_edge(label, src, dst, data_ptr, arena).as_bool() == false) {
      return RTAny::from_bool(false);
    }
    return RTAny::from_bool(
        rhs_->eval_edge(label, src, dst, data_ptr, arena).as_bool());
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    if (lhs_->eval_path(idx, arena).as_bool() == false) {
      return RTAny::from_bool(false);
    }
    return RTAny::from_bool(rhs_->eval_path(idx, arena).as_bool());
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    if (lhs_->eval_vertex(label, v, arena).as_bool() == false) {
      return RTAny::from_bool(false);
    }
    return RTAny::from_bool(rhs_->eval_vertex(label, v, arena).as_bool());
  }
};

class OrOpExpr : public LogicalExpr {
 public:
  OrOpExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs)
      : LogicalExpr(std::move(lhs), std::move(rhs), common::Logical::OR) {}
  bool is_optional() const override { return LogicalExpr::is_optional(); }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena& arena) const override {
    if (lhs_->eval_edge(label, src, dst, data_ptr, arena).as_bool() == true) {
      return RTAny::from_bool(true);
    }
    return RTAny::from_bool(
        rhs_->eval_edge(label, src, dst, data_ptr, arena).as_bool());
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    if (lhs_->eval_path(idx, arena).as_bool() == true) {
      return RTAny::from_bool(true);
    }
    return RTAny::from_bool(rhs_->eval_path(idx, arena).as_bool());
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    if (lhs_->eval_vertex(label, v, arena).as_bool() == true) {
      return RTAny::from_bool(true);
    }
    return RTAny::from_bool(rhs_->eval_vertex(label, v, arena).as_bool());
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
        type_(DataType(DataTypeId::INTEGER)) {}
  int64_t eval_impl(const RTAny& val) const {
    if constexpr (std::is_same_v<T, int64_t>) {
      return extract_time_from_milli_second(val.as_int64(), extract_);
    } else if constexpr (std::is_same_v<T, DateTime>) {
      return extract_time_from_milli_second(val.as_datetime().milli_second,
                                            extract_);

    } else if constexpr (std::is_same_v<T, Date>) {
      if (extract_.interval() == ::common::Extract::DAY) {
        return val.as_date().day();
      } else if (extract_.interval() == ::common::Extract::MONTH) {
        return val.as_date().month();
      } else if (extract_.interval() == ::common::Extract::YEAR) {
        return val.as_date().year();
      } else if (extract_.interval() == ::common::Extract::HOUR) {
        return val.as_date().hour();
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
        return val.as_interval().day();
      } else if (extract_.interval() == ::common::Extract::MONTH) {
        return val.as_interval().month();
      } else if (extract_.interval() == ::common::Extract::YEAR) {
        return val.as_interval().year();
      } else if (extract_.interval() == ::common::Extract::HOUR) {
        return val.as_interval().hour();
      } else if (extract_.interval() == ::common::Extract::MINUTE) {
        return val.as_interval().minute();
      } else if (extract_.interval() == ::common::Extract::SECOND) {
        return val.as_interval().second();
      } else if (extract_.interval() == ::common::Extract::MILLISECOND) {
        return val.as_interval().millisecond();
      } else {
        THROW_RUNTIME_ERROR("Unsupported extract interval for Interval type");
      }
    }
    LOG(FATAL) << "not support" << extract_.DebugString();
    return 0;
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto any_val = expr_->eval_path(idx, arena);
    if (any_val.is_null()) {
      return RTAny(DataType(DataTypeId::SQLNULL));
    }
    return RTAny::from_int64(eval_impl(any_val));
  }
  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    auto any_val = expr_->eval_vertex(label, v, arena);
    if (any_val.is_null()) {
      return RTAny(DataType(DataTypeId::SQLNULL));
    }
    return RTAny::from_int64(eval_impl(any_val));
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena& arena) const override {
    auto any_val = expr_->eval_edge(label, src, dst, data_ptr, arena);
    if (any_val.is_null()) {
      return RTAny(DataType(DataTypeId::SQLNULL));
    }
    return RTAny::from_int64(eval_impl(any_val));
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

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override;

  const DataType& type() const override { return type_; }

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  std::function<RTAny(RTAny, RTAny)> op_;
  ::common::Arithmetic arith_;
  DataType type_;
};

class ConstExpr : public ExprBase {
 public:
  explicit ConstExpr(const RTAny& val, std::shared_ptr<Arena> ptr = nullptr);
  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override;

  const DataType& type() const override { return type_; }

  bool is_optional() const override { return val_.is_null(); }

 private:
  DataType type_;
  RTAny val_;
  std::shared_ptr<Arena> arena_;
};

class CaseWhenExpr : public ExprBase {
 public:
  CaseWhenExpr(
      std::vector<std::pair<std::unique_ptr<ExprBase>,
                            std::unique_ptr<ExprBase>>>&& when_then_exprs,
      std::unique_ptr<ExprBase>&& else_expr);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override;

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

  RTAny eval_path(size_t idx, Arena& arena) const override {
    std::vector<RTAny> params;
    params.reserve(children_.size());
    for (auto& ch : children_) {
      params.emplace_back(ch->eval_path(idx, arena));
    }
    return func_(arena, params);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    std::vector<RTAny> params;
    params.reserve(children_.size());
    for (auto& ch : children_) {
      params.emplace_back(ch->eval_vertex(label, v, arena));
    }
    return func_(arena, params);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena& arena) const override {
    std::vector<RTAny> params;
    params.reserve(children_.size());
    for (auto& ch : children_) {
      params.emplace_back(ch->eval_edge(label, src, dst, data_ptr, arena));
    }
    return func_(arena, params);
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

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override;

  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  std::vector<std::unique_ptr<ExprBase>> exprs_;
};

template <typename... Args>
class TypedTupleExpr : public ExprBase {
 public:
  explicit TypedTupleExpr(
      std::array<std::unique_ptr<ExprBase>, sizeof...(Args)>&& exprs)
      : exprs_(std::move(exprs)) {
    assert(exprs.size() == sizeof...(Args));
    std::vector<DataType> sub_types;
    for (size_t i = 0; i < sizeof...(Args); i++) {
      sub_types.emplace_back(exprs_[i]->type());
    }
    auto extra_type_info =
        std::make_shared<StructTypeInfo>(std::move(sub_types));
    type_ = DataType(DataTypeId::STRUCT, std::move(extra_type_info));
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_path_impl(std::index_sequence<Is...>, size_t idx,
                                     Arena& arena) const {
    return std::make_tuple(
        TypedConverter<Args>::to_typed(exprs_[Is]->eval_path(idx, arena))...);
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto tup = eval_path_impl(std::index_sequence_for<Args...>(), idx, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_vertex_impl(std::index_sequence<Is...>,
                                       label_t label, vid_t v,
                                       Arena& arena) const {
    return std::make_tuple(TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_vertex(label, v, arena))...);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    auto tup =
        eval_vertex_impl(std::index_sequence_for<Args...>(), label, v, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_edge_impl(std::index_sequence<Is...>,
                                     const LabelTriplet& label, vid_t src,
                                     vid_t dst, const void* data_ptr,
                                     Arena& arena) const {
    return std::make_tuple(TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_edge(label, src, dst, data_ptr, arena))...);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena& arena) const override {
    auto tup = eval_edge_impl(std::index_sequence_for<Args...>(), label, src,
                              dst, data_ptr, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  std::array<std::unique_ptr<ExprBase>, sizeof...(Args)> exprs_;
};

class ListExprBase : public ExprBase {
 public:
  ListExprBase() = default;
};

class PathVertexPropsExpr : public ListExprBase {
 public:
  explicit PathVertexPropsExpr(const StorageReadInterface& graph,
                               const Context& ctx, int tag,
                               const std::string& prop,
                               const DataType& prop_type)
      : graph_(graph),
        col_(dynamic_cast<const GeneralPathColumn&>(*ctx.get(tag))),
        prop_(prop),
        prop_type_(prop_type) {
    auto chd_type = std::make_shared<ListTypeInfo>(prop_type_);
    type_ = DataType(DataTypeId::LIST, std::move(chd_type));
  }

  RTAny eval_path(size_t idx, Arena& arena) const override;

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena& arena) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  const DataType& type() const override { return type_; }

  DataType type_;
  const StorageReadInterface& graph_;
  const GeneralPathColumn& col_;
  std::string prop_;
  DataType prop_type_;
};

class PathEdgePropsExpr : public ListExprBase {
 public:
  explicit PathEdgePropsExpr(const StorageReadInterface& graph,
                             const Context& ctx, int tag,
                             const std::string& prop, const DataType& prop_type)
      : graph_(graph),
        col_(dynamic_cast<const GeneralPathColumn&>(*ctx.get(tag))),
        prop_(prop),
        prop_type_(prop_type) {
    auto chd_type = std::make_shared<ListTypeInfo>(prop_type_);
    type_ = DataType(DataTypeId::LIST, std::move(chd_type));
  }
  RTAny eval_path(size_t idx, Arena& arena) const override;

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena& arena) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  const DataType& type() const override { return type_; }

  const StorageReadInterface& graph_;
  const GeneralPathColumn& col_;
  std::string prop_;
  DataType prop_type_;
  DataType type_;
};

class RelationshipsExpr : public ListExprBase {
 public:
  explicit RelationshipsExpr(std::unique_ptr<ExprBase>&& args)
      : args(std::move(args)) {
    auto chd_type = std::make_shared<ListTypeInfo>(DataType(DataTypeId::EDGE));
    type_ = DataType(DataTypeId::LIST, std::move(chd_type));
  }
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type().id() == DataTypeId::PATH);
    auto path_any = args->eval_path(idx, arena);
    if (path_any.is_null()) {
      return RTAny(DataType(DataTypeId::SQLNULL));
    }
    auto path = path_any.as_path();
    auto rels = path.relationships();
    auto ptr = ListImpl<EdgeRecord>::make_list_impl(std::move(rels));
    List rel_list(ptr.get());
    arena.emplace_back(std::move(ptr));
    return RTAny::from_list(rel_list);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  bool is_optional() const override { return args->is_optional(); }

  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  std::unique_ptr<ExprBase> args;
};

class NodesExpr : public ListExprBase {
 public:
  explicit NodesExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {
    auto chd_type =
        std::make_shared<ListTypeInfo>(DataType(DataTypeId::VERTEX));
    type_ = DataType(DataTypeId::LIST, std::move(chd_type));
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type().id() == DataTypeId::PATH);
    auto path_any = args->eval_path(idx, arena);
    if (path_any.is_null()) {
      return RTAny(DataType(DataTypeId::SQLNULL));
    }
    auto path = path_any.as_path();
    auto nodes = path.nodes();
    auto ptr = ListImpl<VertexRecord>::make_list_impl(std::move(nodes));
    List node_list(ptr.get());
    arena.emplace_back(std::move(ptr));
    return RTAny::from_list(node_list);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
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
      : args(std::move(args)), type_(DataType(DataTypeId::VERTEX)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type().id() == DataTypeId::EDGE);
    auto path_any = args->eval_path(idx, arena);
    if (path_any.is_null()) {
      return RTAny(DataType(DataTypeId::SQLNULL));
    }
    auto edge = path_any.as_edge();
    auto node = edge.start_node();
    return RTAny::from_vertex(node);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
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
      : args(std::move(args)), type_(DataType(DataTypeId::VERTEX)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type().id() == DataTypeId::EDGE);
    auto path_any = args->eval_path(idx, arena);
    if (path_any.is_null()) {
      return RTAny(DataType(DataTypeId::SQLNULL));
    }
    auto path = path_any.as_edge();
    auto node = path.end_node();
    return RTAny::from_vertex(node);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  const DataType& type() const override { return type_; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
  DataType type_;
};

class ToDateTimeExpr : public ExprBase {
 public:
  ToDateTimeExpr(std::unique_ptr<ExprBase>&& args)
      : args(std::move(args)), type_(DataType(DataTypeId::TIMESTAMP_MS)) {}

  RTAny to_date_time(const RTAny& val) const {
    int64_t ts = 0;
    if (val.type().id() == DataTypeId::DATE) {
      ts = val.as_date().to_timestamp();
    } else if (val.type().id() == DataTypeId::TIMESTAMP_MS) {
      ts = val.as_datetime().milli_second;
    } else {
      THROW_RUNTIME_ERROR("ToDateTime: input value is not date/timestamp");
    }
    DateTime t(ts);
    return RTAny::from_datetime(t);
  }
  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto val = args->eval_path(idx, arena);
    return to_date_time(val);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    return to_date_time(args->eval_vertex(label, v, arena));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data, Arena& arena) const override {
    return to_date_time(args->eval_edge(label, src, dst, data, arena));
  }

  const DataType& type() const override { return type_; }

  bool is_optional() const override { return args->is_optional(); }

  std::unique_ptr<ExprBase> args;
  DataType type_;
};

class ToDate32Expr : public ExprBase {
 public:
  ToDate32Expr(std::unique_ptr<ExprBase>&& args)
      : args(std::move(args)), type_(DataType(DataTypeId::DATE)) {}

  RTAny to_date_32(const RTAny& val) const {
    constexpr int64_t kMilliSecondsOfDay = 24 * 60 * 60 * 1000;
    int64_t ts = 0;
    if (val.type().id() == DataTypeId::DATE) {
      ts = val.as_date().to_timestamp();
    } else if (val.type().id() == DataTypeId::TIMESTAMP_MS) {
      ts = (val.as_datetime().milli_second / kMilliSecondsOfDay) *
           kMilliSecondsOfDay;
    } else {
      THROW_RUNTIME_ERROR("ToDate32: input value is not date/timestamp");
    }
    Date t(ts);
    return RTAny::from_date(t);
  }
  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto val = args->eval_path(idx, arena);
    return to_date_32(val);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    return to_date_32(args->eval_vertex(label, v, arena));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data, Arena& arena) const override {
    return to_date_32(args->eval_edge(label, src, dst, data, arena));
  }

  const DataType& type() const override { return type_; }

  bool is_optional() const override { return args->is_optional(); }

  std::unique_ptr<ExprBase> args;
  DataType type_;
};

class AbsExpr : public ExprBase {
 public:
  AbsExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {
    type_ = args->type();
  }

  RTAny abs_impl(const RTAny& val) const {
    switch (val.type().id()) {
    case DataTypeId::INTEGER:
      return RTAny::from_int32(std::abs(val.as_int32()));
    case DataTypeId::BIGINT:
      return RTAny::from_int64(std::abs(val.as_int64()));
    case DataTypeId::FLOAT:
      return RTAny::from_float(std::fabs(val.as_float()));
    case DataTypeId::DOUBLE:
      return RTAny::from_double(std::fabs(val.as_double()));
    default:
      break;
    }
    THROW_RUNTIME_ERROR("Abs: input value is not numeric");
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto val = args->eval_path(idx, arena);
    return abs_impl(val);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    auto val = args->eval_vertex(label, v, arena);
    return abs_impl(val);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data, Arena& arena) const override {
    auto val = args->eval_edge(label, src, dst, data, arena);
    return abs_impl(val);
  }
  const DataType& type() const override { return type_; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  DataType type_;
  std::unique_ptr<ExprBase> args;
};

std::unique_ptr<ExprBase> parse_expression(
    const StorageReadInterface* graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const ::common::Expression& expr, VarType var_type);

bool graph_related_expr(const ::common::Expression& expr);

}  // namespace runtime

}  // namespace gs
