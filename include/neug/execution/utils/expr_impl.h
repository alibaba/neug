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

#ifndef RUNTIME_UTILS_RUNTIME_EXPR_IMPL_H_
#define RUNTIME_UTILS_RUNTIME_EXPR_IMPL_H_

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

#include "neug/utils/neug_function_type.h"
#include "neug/execution/common/rt_any.h"
#include "neug/execution/utils/var.h"
#include "neug/utils/property/types.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#else
#include "neug/utils/proto/plan/common.pb.h"
#include "neug/utils/proto/plan/expr.pb.h"
#endif

namespace gs {
namespace common {

// messages
using ::common::Value;
using ::common::Expression;
using ::common::ExprOpr;
using ::common::Variable;
using ::common::DynamicParam;
using ::common::Case;
using ::common::Case_WhenThen;
using ::common::Extract;
using ::common::TimeInterval;
using ::common::DateTimeMinus;
using ::common::PathConcat;
using ::common::PathConcat_ConcatPathInfo;
using ::common::PathFunction;
using ::common::UserDefinedFunction;
using ::common::ToDate;
using ::common::ToDatetime;
using ::common::ToInterval;
using ::common::ToTuple;
using ::common::VariableKeys;
using ::common::VariableKeyValue;
using ::common::VariableKeyValues;
using ::common::IrDataType;
using ::common::NameOrId;
using ::common::PrimitiveType;
using ::common::GraphDataType;
using ::common::GraphDataType_GraphElementOpt;

// enums (类型)
using ::common::Logical;
using ::common::Arithmetic;
using ::common::ExprOpr_Brace;
using ::common::Extract_Interval;
using ::common::PathFunction_FuncOpt;
using ::common::PathConcat_Endpoint;

// 枚举值（需要时引入，否则可以不加；当前代码里使用 common::EQ/NE/...）
using ::common::EQ;
using ::common::NE;
using ::common::LT;
using ::common::LE;
using ::common::GT;
using ::common::GE;
using ::common::AND;
using ::common::OR;
using ::common::NOT;
using ::common::REGEX;
using ::common::WITHIN;
using ::common::WITHOUT;
} // namespace common
} // namespace gs

namespace gs {

namespace runtime {
class Context;
struct LabelTriplet;

class ExprBase {
 public:
  virtual RTAny eval_path(size_t idx, Arena&) const = 0;
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                            Arena&) const = 0;
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx, Arena&) const = 0;
  virtual RTAnyType type() const = 0;
  virtual RTAny eval_path(size_t idx, Arena& arena, int) const {
    return eval_path(idx, arena);
  }
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena& arena,
                            int) const {
    return eval_vertex(label, v, idx, arena);
  }
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx, Arena& arena,
                          int) const {
    return eval_edge(label, src, dst, data, idx, arena);
  }

  virtual bool is_optional() const { return false; }

  virtual RTAnyType elem_type() const { return RTAnyType::kEmpty; }

  virtual ~ExprBase() = default;
};

class VertexWithInSetExpr : public ExprBase {
 public:
  VertexWithInSetExpr(const Context& ctx, std::unique_ptr<ExprBase>&& key,
                      std::unique_ptr<ExprBase>&& val_set);
  RTAny eval_path(size_t idx, Arena& arena) const override;

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override;

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override;

  RTAnyType type() const override { return RTAnyType::kBoolValue; }

  bool is_optional() const override { return key_->is_optional(); }

 private:
  std::unique_ptr<ExprBase> key_;
  std::unique_ptr<ExprBase> val_set_;
};
class VertexWithInListExpr : public ExprBase {
 public:
  VertexWithInListExpr(const Context& ctx, std::unique_ptr<ExprBase>&& key,
                       std::unique_ptr<ExprBase>&& val_list);

  RTAny eval_path(size_t idx, Arena& arena) const override;

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override;

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override;

  RTAnyType type() const override { return RTAnyType::kBoolValue; }

  bool is_optional() const override { return key_->is_optional(); }
  std::unique_ptr<ExprBase> key_;
  std::unique_ptr<ExprBase> val_list_;
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
             const common::Value& array)
      : key_(std::move(key)) {
    if constexpr (std::is_same_v<T, int64_t>) {
      if (array.item_case() == common::Value::kI64Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else if (array.item_case() == common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else {
        // TODO(zhanglei,lexiao): We should support more types here, and if type
        // conversion fails, we should return an error.
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int64_t array";
      }
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      if (array.item_case() == common::Value::kI64Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else if (array.item_case() == common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else {
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int64_t array";
      }
    } else if constexpr (std::is_same_v<T, int32_t>) {
      if (array.item_case() == common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else if constexpr (std::is_same_v<T, int64_t>) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else {
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int32_t array";
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      assert(array.item_case() == common::Value::kStrArray);
      PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.str_array());
    } else {
      LOG(FATAL) << "not implemented";
    }
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(key_->eval_path(idx, arena).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = TypedConverter<T>::to_typed(key_->eval_path(idx, arena));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto any_val = key_->eval_path(idx, arena, 0);
    if (any_val.is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx, arena);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val =
          std::string(key_->eval_vertex(label, v, idx, arena).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val =
          TypedConverter<T>::to_typed(key_->eval_vertex(label, v, idx, arena));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena& arena,
                    int) const override {
    auto any_val = key_->eval_vertex(label, v, idx, arena, 0);
    if (any_val.is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, idx, arena);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(
          key_->eval_edge(label, src, dst, data, idx, arena).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = TypedConverter<T>::to_typed(
          key_->eval_edge(label, src, dst, data, idx, arena));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
    return RTAny::from_bool(false);
  }
  RTAnyType type() const override { return RTAnyType::kBoolValue; }
  bool is_optional() const override { return key_->is_optional(); }

  std::unique_ptr<ExprBase> key_;

  std::vector<T> container_;
};

class VariableExpr : public ExprBase {
 public:
  template <typename GraphInterface>
  VariableExpr(const GraphInterface& graph, const Context& ctx,
               const common::Variable& pb, VarType var_type)
      : var_(graph, ctx, pb, var_type) {}

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;
  RTAnyType type() const override;

  RTAny eval_path(size_t idx, Arena&, int) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&,
                    int) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&, int) const override;

  bool is_optional() const override { return var_.is_optional(); }

 private:
  Var var_;
};

class UnaryLogicalExpr : public ExprBase {
 public:
  UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr, common::Logical logic);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAny eval_path(size_t idx, Arena&, int) const override;
  RTAnyType type() const override;

  bool is_optional() const override { return expr_->is_optional(); }

 private:
  std::unique_ptr<ExprBase> expr_;
  common::Logical logic_;
};
class LogicalExpr : public ExprBase {
 public:
  LogicalExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs,
              common::Logical logic);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    if (logic_ == common::Logical::OR) {
      bool flag = false;
      if (!lhs_->eval_path(idx, arena, 0).is_null()) {
        flag |= lhs_->eval_path(idx, arena, 0).as_bool();
      }
      if (!rhs_->eval_path(idx, arena, 0).is_null()) {
        flag |= rhs_->eval_path(idx, arena, 0).as_bool();
      }
      return RTAny::from_bool(flag);
    }

    if (lhs_->eval_path(idx, arena, 0).is_null() ||
        rhs_->eval_path(idx, arena, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx, arena);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena& arena,
                    int) const override {
    if (logic_ == common::Logical::OR) {
      bool flag = false;
      if (!lhs_->eval_vertex(label, v, idx, arena, 0).is_null()) {
        flag |= lhs_->eval_vertex(label, v, idx, arena, 0).as_bool();
      }
      if (!rhs_->eval_vertex(label, v, idx, arena, 0).is_null()) {
        flag |= rhs_->eval_vertex(label, v, idx, arena, 0).as_bool();
      }
      return RTAny::from_bool(flag);
    }
    if (lhs_->eval_vertex(label, v, idx, arena, 0).is_null() ||
        rhs_->eval_vertex(label, v, idx, arena, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, idx, arena);
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&, int) const override {
    LOG(FATAL) << "not implemented";
    return RTAny();
  }

  RTAnyType type() const override;

  bool is_optional() const override {
    return lhs_->is_optional() || rhs_->is_optional();
  }

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  std::function<bool(RTAny, RTAny)> op_;
  common::Logical logic_;
};

int32_t extract_time_from_milli_second(int64_t ms, common::Extract extract);

template <typename T>
class ExtractExpr : public ExprBase {
 public:
  ExtractExpr(std::unique_ptr<ExprBase>&& expr, const common::Extract& extract)
      : expr_(std::move(expr)), extract_(extract) {}
  int64_t eval_impl(const RTAny& val) const {
    if constexpr (std::is_same_v<T, int64_t>) {
      return extract_time_from_milli_second(val.as_int64(), extract_);
    } else if constexpr (std::is_same_v<T, DateTime>) {
      return extract_time_from_milli_second(val.as_datetime().milli_second,
                                            extract_);

    } else if constexpr (std::is_same_v<T, TimeStamp>) {
      return extract_time_from_milli_second(val.as_timestamp().milli_second,
                                            extract_);
    } else if constexpr (std::is_same_v<T, Date>) {
      if (extract_.interval() == common::Extract::DAY) {
        return val.as_date().day();
      } else if (extract_.interval() == common::Extract::MONTH) {
        return val.as_date().month();
      } else if (extract_.interval() == common::Extract::YEAR) {
        return val.as_date().year();
      } else if (extract_.interval() == common::Extract::HOUR) {
        return val.as_date().hour();
      } else if (extract_.interval() == common::Extract::MINUTE) {
        THROW_RUNTIME_ERROR(
            "Unsupported extract interval for Date type: MINUTE");
      } else if (extract_.interval() == common::Extract::SECOND) {
        THROW_RUNTIME_ERROR(
            "Unsupported extract interval for Date type: SECOND");
      } else if (extract_.interval() == common::Extract::MILLISECOND) {
        THROW_RUNTIME_ERROR(
            "Unsupported extract interval for Date type: MILLISECOND");
      } else {
        THROW_RUNTIME_ERROR("Unsupported extract interval for Date type");
      }
    } else if constexpr (std::is_same_v<T, Interval>) {
      if (extract_.interval() == common::Extract::DAY) {
        return val.as_interval().day();
      } else if (extract_.interval() == common::Extract::MONTH) {
        return val.as_interval().month();
      } else if (extract_.interval() == common::Extract::YEAR) {
        return val.as_interval().year();
      } else if (extract_.interval() == common::Extract::HOUR) {
        return val.as_interval().hour();
      } else if (extract_.interval() == common::Extract::MINUTE) {
        return val.as_interval().minute();
      } else if (extract_.interval() == common::Extract::SECOND) {
        return val.as_interval().second();
      } else if (extract_.interval() == common::Extract::MILLISECOND) {
        return val.as_interval().millisecond();
      } else {
        THROW_RUNTIME_ERROR("Unsupported extract interval for Interval type");
      }
    }
    LOG(FATAL) << "not support" << extract_.DebugString();
    return 0;
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    return RTAny::from_int64(eval_impl(expr_->eval_path(idx, arena)));
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    return RTAny::from_int64(
        eval_impl(expr_->eval_vertex(label, v, idx, arena)));
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    return RTAny::from_int64(
        eval_impl(expr_->eval_edge(label, src, dst, data, idx, arena)));
  }

  RTAnyType type() const override { return RTAnyType::kI32Value; }

 private:
  std::unique_ptr<ExprBase> expr_;
  const common::Extract extract_;
};
class ArithExpr : public ExprBase {
 public:
  ArithExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs,
            common::Arithmetic arith);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAnyType type() const override;

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  std::function<RTAny(RTAny, RTAny)> op_;
  common::Arithmetic arith_;
};

class DateMinusExpr : public ExprBase {
 public:
  DateMinusExpr(std::unique_ptr<ExprBase>&& lhs,
                std::unique_ptr<ExprBase>&& rhs);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAnyType type() const override;

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
};

class ConstExpr : public ExprBase {
 public:
  ConstExpr(const RTAny& val, bool take_ownership = false);
  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAnyType type() const override;

  bool is_optional() const override { return val_.is_null(); }

 private:
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
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAnyType type() const override;

  bool is_optional() const override {
    for (auto& expr_pair : when_then_exprs_) {
      if (expr_pair.first->is_optional() || expr_pair.second->is_optional()) {
        return true;
      }
    }
    return else_expr_->is_optional();
  }

 private:
  std::vector<std::pair<std::unique_ptr<ExprBase>, std::unique_ptr<ExprBase>>>
      when_then_exprs_;
  std::unique_ptr<ExprBase> else_expr_;
};

class ScalarFunctionExpr : public ExprBase {
  public:
    ScalarFunctionExpr(std::string signature, neug_func_exec_t fn,
                      RTAnyType ret_type,
                      std::vector<std::unique_ptr<ExprBase>>&& children)
        : signature_(std::move(signature)),
          func_(fn),
          ret_type_(ret_type),
          children_(std::move(children)) {}

    RTAny eval_path(size_t idx, Arena& arena) const override {
      std::vector<RTAny> params;
      params.reserve(children_.size());
      for (auto& ch : children_) {
        params.emplace_back(ch->eval_path(idx, arena));
      }
      return func_(idx, arena, params);
    }

    RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                      Arena& arena) const override {
      std::vector<RTAny> params;
      params.reserve(children_.size());
      for (auto& ch : children_) {
        params.emplace_back(ch->eval_vertex(label, v, idx, arena));
      }
      return func_(idx, arena, params);
    }

    RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                    const Any& data, size_t idx, Arena& arena) const override {
      std::vector<RTAny> params;
      params.reserve(children_.size());
      for (auto& ch : children_) {
        params.emplace_back(
            ch->eval_edge(label, src, dst, data, idx, arena));
      }
      return func_(idx, arena, params);
    }

    RTAnyType type() const override { return ret_type_; }

    bool is_optional() const override {
      for (auto& ch : children_) {
        if (ch->is_optional()) return true;
      }
      return false;
    }

  private:
    std::string signature_;
    neug_func_exec_t func_;
    RTAnyType ret_type_;
    std::vector<std::unique_ptr<ExprBase>> children_;
};

class TupleExpr : public ExprBase {
 public:
  TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAnyType type() const override;

 private:
  std::vector<std::unique_ptr<ExprBase>> exprs_;
};

template <typename... Args>
class TypedTupleExpr : public ExprBase {
 public:
  TypedTupleExpr(std::array<std::unique_ptr<ExprBase>, sizeof...(Args)>&& exprs)
      : exprs_(std::move(exprs)) {
    assert(exprs.size() == sizeof...(Args));
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_path_impl(std::index_sequence<Is...>, size_t idx,
                                     Arena& arena) const {
    // TODO: The `to_typed` function will fail if the evaluated value of any
    // input is null. For example, `TypedConverter<int32_t>::to_typed`
    // requires its input value to be non-null.
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
                                       label_t label, vid_t v, size_t idx,
                                       Arena& arena) const {
    return std::make_tuple(TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_vertex(label, v, idx, arena))...);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    auto tup = eval_vertex_impl(std::index_sequence_for<Args...>(), label, v,
                                idx, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_edge_impl(std::index_sequence<Is...>,
                                     const LabelTriplet& label, vid_t src,
                                     vid_t dst, const Any& data, size_t idx,
                                     Arena& arena) const {
    return std::make_tuple(TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_edge(label, src, dst, data, idx, arena))...);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    auto tup = eval_edge_impl(std::index_sequence_for<Args...>(), label, src,
                              dst, data, idx, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  RTAnyType type() const override { return RTAnyType::kTuple; }

 private:
  std::array<std::unique_ptr<ExprBase>, sizeof...(Args)> exprs_;
};

class MapExpr : public ExprBase {
 public:
  MapExpr(const std::vector<RTAny>& keys,
          std::vector<std::unique_ptr<ExprBase>>&& values)
      : value_exprs(std::move(values)) {
    // Maintain the keys here
    for (auto& key : keys) {
      if (key.type() != RTAnyType::kStringValue) {
        LOG(FATAL) << "key type should be string";
      }
      keys_str_.push_back(std::string(key.as_string()));
    }
    for (auto& key : keys_str_) {
      keys_.push_back(RTAny::from_string(key));
    }
    assert(keys_.size() == value_exprs.size());
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    std::vector<RTAny> ret;
    for (size_t i = 0; i < keys_.size(); i++) {
      ret.push_back(value_exprs[i]->eval_path(idx, arena));
    }
    auto map_impl = MapImpl::make_map_impl(keys_, ret);
    auto map = Map::make_map(map_impl.get());
    arena.emplace_back(std::move(map_impl));
    return RTAny::from_map(map);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    std::vector<RTAny> ret;
    for (size_t i = 0; i < keys_.size(); i++) {
      ret.push_back(value_exprs[i]->eval_path(idx, arena, 0));
    }
    auto map_impl = MapImpl::make_map_impl(keys_, ret);
    auto map = Map::make_map(map_impl.get());
    arena.emplace_back(std::move(map_impl));
    return RTAny::from_map(map);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override {
    LOG(FATAL) << "not implemented";
    return RTAny();
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override {
    LOG(FATAL) << "not implemented";
    return RTAny();
  }

  RTAnyType type() const override { return RTAnyType::kMap; }

  bool is_optional() const override {
    for (auto& expr : value_exprs) {
      if (expr->is_optional()) {
        return true;
      }
    }
    return false;
  }

 private:
  // Stores string keys here.
  std::vector<std::string> keys_str_;
  std::vector<RTAny> keys_;
  std::vector<std::unique_ptr<ExprBase>> value_exprs;
};

class ListExprBase : public ExprBase {
 public:
  ListExprBase() = default;
  RTAnyType type() const override { return RTAnyType::kList; }
};
class RelationshipsExpr : public ListExprBase {
 public:
  RelationshipsExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto path = args->eval_path(idx, arena).as_path();
    auto rels = path.relationships();
    auto ptr = ListImpl<Relation>::make_list_impl(std::move(rels));
    List rel_list(ptr.get());
    arena.emplace_back(std::move(ptr));
    return RTAny::from_list(rel_list);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  bool is_optional() const override { return args->is_optional(); }

  RTAnyType elem_type() const override { return RTAnyType::kRelation; }

 private:
  std::unique_ptr<ExprBase> args;
};

class NodesExpr : public ListExprBase {
 public:
  NodesExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto path = args->eval_path(idx, arena).as_path();
    auto nodes = path.nodes();
    auto ptr = ListImpl<VertexRecord>::make_list_impl(std::move(nodes));
    List node_list(ptr.get());
    arena.emplace_back(std::move(ptr));
    return RTAny::from_list(node_list);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  bool is_optional() const override { return args->is_optional(); }

  RTAnyType elem_type() const override { return RTAnyType::kVertex; }

 private:
  std::unique_ptr<ExprBase> args;
};

class StartNodeExpr : public ExprBase {
 public:
  StartNodeExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type() == RTAnyType::kRelation);
    auto path = args->eval_path(idx, arena).as_relation();
    auto node = path.start_node();
    return RTAny::from_vertex(node);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAnyType type() const override { return RTAnyType::kVertex; }

  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
};

class EndNodeExpr : public ExprBase {
 public:
  EndNodeExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type() == RTAnyType::kRelation);
    auto path = args->eval_path(idx, arena).as_relation();
    auto node = path.end_node();
    return RTAny::from_vertex(node);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAnyType type() const override { return RTAnyType::kVertex; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
};

class ToFloatExpr : public ExprBase {
 public:
  static double to_double(const RTAny& val) {
    if (val.type() == RTAnyType::kI64Value) {
      return static_cast<double>(val.as_int64());
    } else if (val.type() == RTAnyType::kI32Value) {
      return static_cast<double>(val.as_int32());
    } else if (val.type() == RTAnyType::kF32Value) {
      return static_cast<double>(val.as_float());
    } else if (val.type() == RTAnyType::kF64Value) {
      return val.as_double();
    } else {
      LOG(FATAL) << "invalid type";
    }
  }

  ToFloatExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto val = args->eval_path(idx, arena);
    return RTAny::from_double(to_double(val));
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto val = args->eval_path(idx, arena, 0);
    if (val.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    auto val = args->eval_vertex(label, v, idx, arena);
    return RTAny::from_double(to_double(val));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    auto val = args->eval_edge(label, src, dst, data, idx, arena);
    return RTAny::from_double(to_double(val));
  }

  RTAnyType type() const override { return RTAnyType::kF64Value; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
};

class StrConcatExpr : public ExprBase {
 public:
  StrConcatExpr(std::unique_ptr<ExprBase>&& lhs,
                std::unique_ptr<ExprBase>&& rhs)
      : lhs(std::move(lhs)), rhs(std::move(rhs)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    std::string ret = std::string(lhs->eval_path(idx, arena).as_string()) +
                      ";" + std::string(rhs->eval_path(idx, arena).as_string());
    auto ptr = StringImpl::make_string_impl(ret);
    auto sv = ptr->str_view();
    arena.emplace_back(std::move(ptr));

    return RTAny::from_string(sv);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    if (lhs->eval_path(idx, arena, 0).is_null() ||
        rhs->eval_path(idx, arena, 0).is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    std::string ret =
        std::string(lhs->eval_vertex(label, v, idx, arena).as_string()) + ";" +
        std::string(rhs->eval_vertex(label, v, idx, arena).as_string());
    auto ptr = StringImpl::make_string_impl(ret);
    auto sv = ptr->str_view();
    arena.emplace_back(std::move(ptr));

    return RTAny::from_string(sv);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    std::string ret =
        std::string(
            lhs->eval_edge(label, src, dst, data, idx, arena).as_string()) +
        ";" +
        std::string(
            rhs->eval_edge(label, src, dst, data, idx, arena).as_string());
    auto ptr = StringImpl::make_string_impl(ret);
    auto sv = ptr->str_view();
    arena.emplace_back(std::move(ptr));

    return RTAny::from_string(sv);
  }

  RTAnyType type() const override { return RTAnyType::kStringValue; }
  bool is_optional() const override {
    return lhs->is_optional() || rhs->is_optional();
  }

 private:
  std::unique_ptr<ExprBase> lhs;
  std::unique_ptr<ExprBase> rhs;
};

class StrListSizeExpr : public ExprBase {
 public:
  StrListSizeExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}

  RTAny eval_path(size_t idx, Arena& arena) const override {
    CHECK(args->type() == RTAnyType::kStringValue);
    auto str_list = args->eval_path(idx, arena).as_string();
    return RTAny::from_int32(_size(str_list));
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto list = args->eval_path(idx, arena, 0);
    if (list.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    auto str_list = args->eval_vertex(label, v, idx, arena).as_string();
    return RTAny::from_int32(_size(str_list));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    auto str_list =
        args->eval_edge(label, src, dst, data, idx, arena).as_string();
    return RTAny::from_int32(_size(str_list));
  }

  RTAnyType type() const override { return RTAnyType::kI32Value; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  int32_t _size(const std::string_view& sv) const {
    if (sv.empty()) {
      return 0;
    }
    int64_t ret = 1;
    for (auto c : sv) {
      if (c == ';') {
        ++ret;
      }
    }
    return ret;
  }
  std::unique_ptr<ExprBase> args;
};

template <typename GraphInterface>
std::unique_ptr<ExprBase> parse_expression(
    const GraphInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const common::Expression& expr, VarType var_type);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_UTILS_RUNTIME_EXPR_IMPL_H_