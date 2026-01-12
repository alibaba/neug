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

#include "neug/execution/utils/expr_impl.h"
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/gopt/g_catalog_holder.h"

#include <time.h>

#include <iterator>
#include <regex>
#include <sstream>
#include <stack>

namespace gs {

namespace runtime {
class Context;
struct LabelTriplet;

VertexWithInSetExpr::VertexWithInSetExpr(const Context& ctx,
                                         std::unique_ptr<ExprBase>&& key,
                                         std::unique_ptr<ExprBase>&& val_set)
    : key_(std::move(key)),
      val_set_(std::move(val_set)),
      type_(DataType(DataTypeId::BOOLEAN)) {
  assert(key_->type().id() == DataTypeId::VERTEX);
  assert(val_set_->type().id() == DataTypeId::SET);
}

RTAny VertexWithInSetExpr::eval_path(size_t idx, Arena& arena) const {
  auto key = key_->eval_path(idx, arena).as_vertex();
  auto set = val_set_->eval_path(idx, arena).as_set();
  assert(set.impl_ != nullptr);
  auto ptr = dynamic_cast<SetImpl<VertexRecord>*>(set.impl_);
  assert(ptr != nullptr);
  return RTAny::from_bool(ptr->exists(key));
}

RTAny VertexWithInSetExpr::eval_vertex(label_t label, vid_t v,
                                       Arena& arena) const {
  auto key = key_->eval_vertex(label, v, arena).as_vertex();
  auto set = val_set_->eval_vertex(label, v, arena).as_set();
  return RTAny::from_bool(
      dynamic_cast<SetImpl<VertexRecord>*>(set.impl_)->exists(key));
}

RTAny VertexWithInSetExpr::eval_edge(const LabelTriplet& label, vid_t src,
                                     vid_t dst, const void* data_ptr,
                                     Arena& arena) const {
  auto key = key_->eval_edge(label, src, dst, data_ptr, arena).as_vertex();
  auto set = val_set_->eval_edge(label, src, dst, data_ptr, arena).as_set();
  return RTAny::from_bool(
      dynamic_cast<SetImpl<VertexRecord>*>(set.impl_)->exists(key));
}

VertexWithInListExpr::VertexWithInListExpr(const Context& ctx,
                                           std::unique_ptr<ExprBase>&& key,
                                           std::unique_ptr<ExprBase>&& val_list)
    : key_(std::move(key)),
      val_list_(std::move(val_list)),
      type_(DataType(DataTypeId::BOOLEAN)) {
  assert(key_->type().id() == DataTypeId::VERTEX);
  assert(val_list_->type().id() == DataTypeId::LIST);
}

RTAny VertexWithInListExpr::eval_path(size_t idx, Arena& arena) const {
  auto key = key_->eval_path(idx, arena).as_vertex();
  auto list = val_list_->eval_path(idx, arena).as_list();
  for (size_t i = 0; i < list.size(); i++) {
    if (list.get(i).as_vertex() == key) {
      return RTAny::from_bool(true);
    }
  }
  return RTAny::from_bool(false);
}

RTAny VertexWithInListExpr::eval_vertex(label_t label, vid_t v,
                                        Arena& arena) const {
  auto key = key_->eval_vertex(label, v, arena).as_vertex();
  auto list = val_list_->eval_vertex(label, v, arena).as_list();
  for (size_t i = 0; i < list.size(); i++) {
    if (list.get(i).as_vertex() == key) {
      return RTAny::from_bool(true);
    }
  }
  return RTAny::from_bool(false);
}

RTAny VertexWithInListExpr::eval_edge(const LabelTriplet& label, vid_t src,
                                      vid_t dst, const void* data_ptr,
                                      Arena& arena) const {
  auto key = key_->eval_edge(label, src, dst, data_ptr, arena).as_vertex();
  auto list = val_list_->eval_edge(label, src, dst, data_ptr, arena).as_list();
  for (size_t i = 0; i < list.size(); i++) {
    if (list.get(i).as_vertex() == key) {
      return RTAny::from_bool(true);
    }
  }
  return RTAny::from_bool(false);
}

RTAny VariableExpr::eval_path(size_t idx, Arena&) const {
  return var_.get(idx);
}
RTAny VariableExpr::eval_vertex(label_t label, vid_t v, Arena&) const {
  return var_.get_vertex(label, v);
}
RTAny VariableExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const void* data_ptr, Arena&) const {
  return var_.get_edge(label, src, dst, data_ptr);
}

LogicalExpr::LogicalExpr(std::unique_ptr<ExprBase>&& lhs,
                         std::unique_ptr<ExprBase>&& rhs,
                         ::common::Logical logic)
    : lhs_(std::move(lhs)),
      rhs_(std::move(rhs)),
      logic_(logic),
      type_(DataType(DataTypeId::BOOLEAN)) {
  switch (logic) {
  case ::common::Logical::LT: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs < rhs; };
    break;
  }
  case ::common::Logical::GT: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return rhs < lhs; };
    break;
  }
  case ::common::Logical::GE: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return !(lhs < rhs); };
    break;
  }
  case ::common::Logical::LE: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return !(rhs < lhs); };
    break;
  }
  case ::common::Logical::EQ: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs == rhs; };
    break;
  }
  case ::common::Logical::NE: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return !(lhs == rhs); };
    break;
  }
  case ::common::Logical::AND: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) {
      return lhs.as_bool() && rhs.as_bool();
    };
    break;
  }
  case ::common::Logical::OR: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) {
      return lhs.as_bool() || rhs.as_bool();
    };
    break;
  }
  case ::common::Logical::REGEX: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) {
      auto lhs_str = std::string(lhs.as_string());
      auto rhs_str = std::string(rhs.as_string());
      return std::regex_match(lhs_str, std::regex(rhs_str));
    };
    break;
  }
  default: {
    LOG(FATAL) << "not support..." << static_cast<int>(logic);
    break;
  }
  }
}

RTAny LogicalExpr::eval_impl(const RTAny& lhs_val, const RTAny& rhs_val) const {
  if (lhs_val.is_null() || rhs_val.is_null()) {
    return RTAny(DataType(DataTypeId::SQLNULL));
  }
  return RTAny::from_bool(op_(lhs_val, rhs_val));
}

RTAny LogicalExpr::eval_path(size_t idx, Arena& arena) const {
  return eval_impl(lhs_->eval_path(idx, arena), rhs_->eval_path(idx, arena));
}

RTAny LogicalExpr::eval_vertex(label_t label, vid_t v, Arena& arena) const {
  return eval_impl(lhs_->eval_vertex(label, v, arena),
                   rhs_->eval_vertex(label, v, arena));
}

RTAny LogicalExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                             const void* data_ptr, Arena& arena) const {
  return eval_impl(lhs_->eval_edge(label, src, dst, data_ptr, arena),
                   rhs_->eval_edge(label, src, dst, data_ptr, arena));
}

UnaryLogicalExpr::UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr,
                                   ::common::Logical logic)
    : expr_(std::move(expr)),
      logic_(logic),
      type_(DataType(DataTypeId::BOOLEAN)) {}

RTAny UnaryLogicalExpr::eval_path(size_t idx, Arena& arena) const {
  auto any_val = expr_->eval_path(idx, arena);
  if (logic_ == ::common::Logical::NOT) {
    if (any_val.is_null()) {
      return RTAny(DataType(DataTypeId::SQLNULL));
    }
    return RTAny::from_bool(!any_val.as_bool());
  } else if (logic_ == ::common::Logical::ISNULL) {
    return RTAny::from_bool(any_val.type() == DataType::SQLNULL);
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAny UnaryLogicalExpr::eval_vertex(label_t label, vid_t v,
                                    Arena& arena) const {
  auto any_val = expr_->eval_vertex(label, v, arena);
  if (logic_ == ::common::Logical::NOT) {
    return RTAny::from_bool(!any_val.as_bool());
  } else if (logic_ == ::common::Logical::ISNULL) {
    return RTAny::from_bool(any_val.is_null());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAny UnaryLogicalExpr::eval_edge(const LabelTriplet& label, vid_t src,
                                  vid_t dst, const void* data_ptr,
                                  Arena& arena) const {
  auto any_val = expr_->eval_edge(label, src, dst, data_ptr, arena);
  if (logic_ == ::common::Logical::NOT) {
    return RTAny::from_bool(!any_val.as_bool());
  } else if (logic_ == ::common::Logical::ISNULL) {
    return RTAny::from_bool(any_val.is_null());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

ArithExpr::ArithExpr(std::unique_ptr<ExprBase>&& lhs,
                     std::unique_ptr<ExprBase>&& rhs,
                     ::common::Arithmetic arith)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)), arith_(arith) {
  if (lhs_->type().id() == DataTypeId::DOUBLE ||
      rhs_->type().id() == DataTypeId::DOUBLE) {
    type_ = DataType(DataTypeId::DOUBLE);
  } else if (lhs_->type().id() == DataTypeId::FLOAT ||
             rhs_->type().id() == DataTypeId::FLOAT) {
    type_ = DataType(DataTypeId::FLOAT);
  } else if (lhs_->type().id() == DataTypeId::BIGINT ||
             rhs_->type().id() == DataTypeId::BIGINT) {
    type_ = DataType(DataTypeId::BIGINT);
  } else {
    type_ = lhs_->type();
  }
  switch (arith_) {
  case ::common::Arithmetic::ADD: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs + rhs; };
    break;
  }
  case ::common::Arithmetic::SUB: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs - rhs; };
    break;
  }
  case ::common::Arithmetic::DIV: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs / rhs; };
    break;
  }
  case ::common::Arithmetic::MOD: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs % rhs; };
    break;
  }

  case ::common::Arithmetic::MUL: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs * rhs; };
    break;
  }

  default: {
    LOG(FATAL) << "not support..." << static_cast<int>(arith);
    break;
  }
  }
}

RTAny ArithExpr::eval_path(size_t idx, Arena& arena) const {
  return op_(lhs_->eval_path(idx, arena), rhs_->eval_path(idx, arena));
}

RTAny ArithExpr::eval_vertex(label_t label, vid_t v, Arena& arena) const {
  return op_(lhs_->eval_vertex(label, v, arena),
             rhs_->eval_vertex(label, v, arena));
}

RTAny ArithExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const void* data_ptr, Arena& arena) const {
  return op_(lhs_->eval_edge(label, src, dst, data_ptr, arena),
             rhs_->eval_edge(label, src, dst, data_ptr, arena));
}

DateMinusExpr::DateMinusExpr(std::unique_ptr<ExprBase>&& lhs,
                             std::unique_ptr<ExprBase>&& rhs)
    : lhs_(std::move(lhs)),
      rhs_(std::move(rhs)),
      type_(DataType(DataTypeId::BIGINT)) {}

RTAny DateMinusExpr::eval_path(size_t idx, Arena& arena) const {
  auto lhs = lhs_->eval_path(idx, arena).as_datetime();
  auto rhs = rhs_->eval_path(idx, arena).as_datetime();
  return RTAny::from_int64(lhs.milli_second - rhs.milli_second);
}

RTAny DateMinusExpr::eval_vertex(label_t label, vid_t v, Arena& arena) const {
  auto lhs = lhs_->eval_vertex(label, v, arena).as_datetime();
  auto rhs = rhs_->eval_vertex(label, v, arena).as_datetime();
  return RTAny::from_int64(lhs.milli_second - rhs.milli_second);
}

RTAny DateMinusExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                               const void* data_ptr, Arena& arena) const {
  auto lhs = lhs_->eval_edge(label, src, dst, data_ptr, arena).as_datetime();
  auto rhs = rhs_->eval_edge(label, src, dst, data_ptr, arena).as_datetime();
  return RTAny::from_int64(lhs.milli_second - rhs.milli_second);
}

ConstExpr::ConstExpr(const RTAny& val, std::shared_ptr<Arena> ptr)
    : val_(val), arena_(ptr) {
  type_ = val_.type();
}
RTAny ConstExpr::eval_path(size_t idx, Arena& arena) const {
  if (arena_) {
    arena.emplace_back(std::make_unique<ArenaRef>(arena_));
  }
  return val_;
}
RTAny ConstExpr::eval_vertex(label_t label, vid_t v, Arena& arena) const {
  if (arena_) {
    arena.emplace_back(std::make_unique<ArenaRef>(arena_));
  }
  return val_;
}
RTAny ConstExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const void* data_ptr, Arena& arena) const {
  if (arena_) {
    arena.emplace_back(std::make_unique<ArenaRef>(arena_));
  }
  return val_;
}

static int32_t extract_year(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
  return tm.tm_year + 1900;
}

static int32_t extract_month(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
  return tm.tm_mon + 1;
}

static int32_t extract_day(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
  return tm.tm_mday;
}

static int32_t extract_second(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
  return tm.tm_sec;
}

static int32_t extract_hour(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
  return tm.tm_hour;
}

static int32_t extract_minute(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
  return tm.tm_min;
}

int32_t extract_time_from_milli_second(int64_t ms, ::common::Extract extract) {
  if (extract.interval() == ::common::Extract::YEAR) {
    return extract_year(ms);
  } else if (extract.interval() == ::common::Extract::MONTH) {
    return extract_month(ms);
  } else if (extract.interval() == ::common::Extract::DAY) {
    return extract_day(ms);
  } else if (extract.interval() == ::common::Extract::SECOND) {
    return extract_second(ms);
  } else if (extract.interval() == ::common::Extract::HOUR) {
    return extract_hour(ms);
  } else if (extract.interval() == ::common::Extract::MINUTE) {
    return extract_minute(ms);
  } else if (extract.interval() == ::common::Extract::MILLISECOND) {
    return ms % 1000;
  } else {
    LOG(FATAL) << "not support: " << extract.DebugString();
  }
  return 0;
}

CaseWhenExpr::CaseWhenExpr(
    std::vector<std::pair<std::unique_ptr<ExprBase>,
                          std::unique_ptr<ExprBase>>>&& when_then_exprs,
    std::unique_ptr<ExprBase>&& else_expr)
    : when_then_exprs_(std::move(when_then_exprs)),
      else_expr_(std::move(else_expr)) {
  type_ = (DataTypeId::SQLNULL);
  if (when_then_exprs_.size() > 0) {
    if (when_then_exprs_[0].second->type().id() != DataTypeId::SQLNULL) {
      type_ = when_then_exprs_[0].second->type();
    }
  }
  if (else_expr_->type().id() != DataTypeId::SQLNULL) {
    type_ = else_expr_->type();
  }
}

RTAny CaseWhenExpr::eval_path(size_t idx, Arena& arena) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_path(idx, arena).as_bool()) {
      return pair.second->eval_path(idx, arena);
    }
  }
  return else_expr_->eval_path(idx, arena);
}

RTAny CaseWhenExpr::eval_vertex(label_t label, vid_t v, Arena& arena) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_vertex(label, v, arena).as_bool()) {
      return pair.second->eval_vertex(label, v, arena);
    }
  }
  return else_expr_->eval_vertex(label, v, arena);
}

RTAny CaseWhenExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const void* data_ptr, Arena& arena) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_edge(label, src, dst, data_ptr, arena).as_bool()) {
      return pair.second->eval_edge(label, src, dst, data_ptr, arena);
    }
  }
  return else_expr_->eval_edge(label, src, dst, data_ptr, arena);
}

TupleExpr::TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs)
    : exprs_(std::move(exprs)) {
  std::vector<DataType> components;
  for (const auto& expr : exprs_) {
    components.push_back(expr->type());
  }
  type_ = DataType(DataTypeId::STRUCT,
                   std::make_shared<StructTypeInfo>(components));
}

RTAny TupleExpr::eval_path(size_t idx, Arena& arena) const {
  std::vector<RTAny> ret;
  for (auto& expr : exprs_) {
    ret.push_back(expr->eval_path(idx, arena));
  }
  auto tup = Tuple::make_generic_tuple_impl(std::move(ret));
  Tuple t(tup.get());
  arena.emplace_back(std::move(tup));
  return RTAny::from_tuple(t);
}

RTAny TupleExpr::eval_vertex(label_t label, vid_t v, Arena& arena) const {
  std::vector<RTAny> ret;
  for (auto& expr : exprs_) {
    ret.push_back(expr->eval_vertex(label, v, arena));
  }
  auto tup = Tuple::make_generic_tuple_impl(std::move(ret));
  Tuple t(tup.get());
  arena.emplace_back(std::move(tup));
  return RTAny::from_tuple(t);
}

RTAny TupleExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const void* data_ptr, Arena& arena) const {
  std::vector<RTAny> ret;
  for (auto& expr : exprs_) {
    ret.push_back(expr->eval_edge(label, src, dst, data_ptr, arena));
  }
  auto tup = Tuple::make_generic_tuple_impl(std::move(ret));
  Tuple t(tup.get());
  arena.emplace_back(std::move(tup));
  return RTAny::from_tuple(t);
}

template <typename T>
RTAny create_list_impl(std::vector<RTAny>&& elements, Arena& arena) {
  auto list_impl = ListImpl<T>::make_list_impl(std::move(elements));
  List list(list_impl.get());
  arena.emplace_back(std::move(list_impl));
  return RTAny::from_list(list);
}
RTAny create_list(std::vector<RTAny>&& elements, DataType elem_type,
                  Arena& arena) {
  switch (elem_type.id()) {
#define TYPE_DISPATCHER(enum_val, type)                        \
  case DataTypeId::enum_val: {                                 \
    return create_list_impl<type>(std::move(elements), arena); \
  }
    FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER

  default:
    LOG(FATAL) << "not support list of " << static_cast<int>(elem_type.id());
    return RTAny();
  }
}

RTAny PathVertexPropsExpr::eval_path(size_t idx, Arena& arena) const {
  auto path = col_.get_elem(idx);
  if (path.is_null()) {
    return RTAny(DataType(DataTypeId::SQLNULL));
  }
  const auto& nodes = path.as_path().nodes();
  std::vector<RTAny> props;
  for (const auto& node : nodes) {
    const auto& name = graph_.schema().get_vertex_property_names(node.label_);
    auto it = std::find(name.begin(), name.end(), prop_);
    if (it == name.end()) {
      props.push_back(RTAny(DataType(DataTypeId::SQLNULL)));
    } else {
      size_t index = std::distance(name.begin(), it);
      auto prop = graph_.GetVertexProperty(node.label_, node.vid_, index);
      props.push_back(RTAny(prop));
    }
  }
  return create_list(std::move(props), prop_type_, arena);
}

RTAny PathEdgePropsExpr::eval_path(size_t idx, Arena& arena) const {
  auto path = col_.get_elem(idx);
  if (path.is_null()) {
    return RTAny(DataType(DataTypeId::SQLNULL));
  }
  const auto& edges = path.as_path().relationships();
  std::vector<RTAny> props;
  for (const auto& edge : edges) {
    const auto& name = graph_.schema().get_edge_property_names(
        edge.label.src_label, edge.label.dst_label, edge.label.edge_label);
    auto it = std::find(name.begin(), name.end(), prop_);
    if (it == name.end()) {
      props.push_back(RTAny(DataType(DataTypeId::SQLNULL)));
    } else {
      size_t index = std::distance(name.begin(), it);
      auto accessor =
          graph_.GetEdgeDataAccessor(edge.label.src_label, edge.label.dst_label,
                                     edge.label.edge_label, index);

      if (prop_type_.id() == DataTypeId::VARCHAR) {
        auto ret = accessor.template get_typed_data_from_ptr<std::string_view>(
            edge.prop);
        props.push_back(RTAny::from_string(ret));
      } else if (prop_type_.id() == DataTypeId::INTEGER) {
        auto ret =
            accessor.template get_typed_data_from_ptr<int32_t>(edge.prop);
        props.push_back(RTAny::from_int32(ret));
      } else if (prop_type_.id() == DataTypeId::BIGINT) {
        auto ret =
            accessor.template get_typed_data_from_ptr<int64_t>(edge.prop);
        props.push_back(RTAny::from_int64(ret));
      } else if (prop_type_.id() == DataTypeId::DOUBLE) {
        auto ret = accessor.template get_typed_data_from_ptr<double>(edge.prop);
        props.push_back(RTAny::from_double(ret));
      } else if (prop_type_.id() == DataTypeId::BOOLEAN) {
        auto ret = accessor.template get_typed_data_from_ptr<bool>(edge.prop);
        props.push_back(RTAny::from_bool(ret));
      } else if (prop_type_.id() == DataTypeId::TIMESTAMP_MS) {
        auto ret =
            accessor.template get_typed_data_from_ptr<DateTime>(edge.prop);
        props.push_back(RTAny::from_datetime(ret));
      } else {
        LOG(FATAL) << "not support edge prop type "
                   << static_cast<int>(prop_type_.id());
      }
    }
  }
  return create_list(std::move(props), prop_type_, arena);
}

static RTAny parse_const_value(const ::common::Value& val,
                               std::shared_ptr<Arena>& arena) {
  switch (val.item_case()) {
  case ::common::Value::kI32:
    return RTAny::from_int32(val.i32());
  case ::common::Value::kU32:
    return RTAny::from_uint32(val.u32());
  case ::common::Value::kStr: {
    arena = std::make_shared<Arena>();
    auto ptr = StringImpl::make_string_impl(val.str());
    RTAny val = RTAny::from_string(ptr->str_view());
    arena->emplace_back(std::move(ptr));
    return val;
  }
  case ::common::Value::kI64:
    return RTAny::from_int64(val.i64());
  case ::common::Value::kU64:
    return RTAny::from_uint64(val.u64());
  case ::common::Value::kBoolean:
    return RTAny::from_bool(val.boolean());
  case ::common::Value::kNone:
    return RTAny(DataType(DataTypeId::SQLNULL));
  case ::common::Value::kF64:
    return RTAny::from_double(val.f64());
  case ::common::Value::kF32:
    return RTAny::from_float(val.f32());
  default:
    LOG(FATAL) << "not support for " << val.item_case();
  }
  return RTAny();
}

template <size_t N, size_t I, typename... Args>
struct TypedTupleBuilder {
  std::unique_ptr<ExprBase> build_typed_tuple(
      std::array<std::unique_ptr<ExprBase>, N>&& exprs) {
    switch (exprs[I - 1]->type().id()) {
    case DataTypeId::INTEGER:
      return TypedTupleBuilder<N, I - 1, int, Args...>().build_typed_tuple(
          std::move(exprs));
    case DataTypeId::BIGINT:
      return TypedTupleBuilder<N, I - 1, int64_t, Args...>().build_typed_tuple(
          std::move(exprs));
    case DataTypeId::DOUBLE:
      return TypedTupleBuilder<N, I - 1, double, Args...>().build_typed_tuple(
          std::move(exprs));
    case DataTypeId::VARCHAR:
      return TypedTupleBuilder<N, I - 1, std::string_view, Args...>()
          .build_typed_tuple(std::move(exprs));
    default: {
      std::vector<std::unique_ptr<ExprBase>> exprs_vec;
      for (auto& expr : exprs) {
        exprs_vec.emplace_back(std::move(expr));
      }
      return std::make_unique<TupleExpr>(std::move(exprs_vec));
    }
    }
  }
};

template <size_t N, typename... Args>
struct TypedTupleBuilder<N, 0, Args...> {
  std::unique_ptr<ExprBase> build_typed_tuple(
      std::array<std::unique_ptr<ExprBase>, N>&& exprs) {
    return std::make_unique<TypedTupleExpr<Args...>>(std::move(exprs));
  }
};

static RTAny parse_param(const ::common::DynamicParam& param,
                         const std::map<std::string, std::string>& input,
                         std::shared_ptr<Arena>& arena) {
  if (param.data_type().type_case() ==
      ::common::IrDataType::TypeCase::kDataType) {
    auto type = parse_from_ir_data_type(param.data_type());

    const std::string& name = param.name();
    if (type.id() == DataTypeId::DATE) {
      Date val = Date(input.at(name));
      return RTAny::from_date(val);
    } else if (type.id() == DataTypeId::VARCHAR) {
      const std::string& val = input.at(name);
      return RTAny::from_string(val);
    } else if (type.id() == DataTypeId::INTEGER) {
      int val = std::stoi(input.at(name));
      return RTAny::from_int32(val);
    } else if (type.id() == DataTypeId::BIGINT) {
      int64_t val = std::stoll(input.at(name));
      return RTAny::from_int64(val);
    } else if (type.id() == DataTypeId::TIMESTAMP_MS) {
      DateTime val = DateTime(input.at(name));
      return RTAny::from_datetime(val);
    } else if (type.id() == DataTypeId::INTERVAL) {
      Interval val = Interval(input.at(name));
      return RTAny::from_interval(val);
    } else if (type.id() == DataTypeId::UINTEGER) {
      uint32_t val = std::stoul(input.at(name));
      return RTAny::from_uint32(val);
    } else if (type.id() == DataTypeId::UBIGINT) {
      uint64_t val = std::stoull(input.at(name));
      return RTAny::from_uint64(val);
    } else if (type.id() == DataTypeId::BOOLEAN) {
      bool val = input.at(name) == "true" || input.at(name) == "1";
      return RTAny::from_bool(val);
    } else if (type.id() == DataTypeId::FLOAT) {
      float val = std::stof(input.at(name));
      return RTAny::from_float(val);
    } else if (type.id() == DataTypeId::DOUBLE) {
      double val = std::stod(input.at(name));
      return RTAny::from_double(val);
    } else if (type.id() == DataTypeId::LIST) {
      auto type = param.data_type().data_type().array().component_type();
      if (type.has_string()) {
        std::vector<std::string_view> vec;
        const auto& val = input.at(name);
        size_t start = 0;
        for (size_t i = 0; i <= val.size(); ++i) {
          if (i == val.size() || val[i] == ';') {
            vec.emplace_back(val.data() + start, i - start);
            start = i + 1;
          }
        }
        arena = std::make_shared<Arena>();
        auto ptr = ListImpl<std::string_view>::make_list_impl(std::move(vec));
        List list(ptr.get());
        arena->emplace_back(std::move(ptr));
        return RTAny::from_list(list);
      } else if (type.item_case() ==
                     ::common::DataType::ItemCase::kPrimitiveType &&
                 type.primitive_type() ==
                     ::common::PrimitiveType::DT_SIGNED_INT64) {
        std::vector<int64_t> vec;
        const auto& val = input.at(name);
        size_t start = 0;
        for (size_t i = 0; i <= val.size(); ++i) {
          if (i == val.size() || val[i] == ';') {
            vec.emplace_back(
                std::stoll(std::string(val.data() + start, i - start)));
            start = i + 1;
          }
        }
        arena = std::make_shared<Arena>();
        auto ptr = ListImpl<int64_t>::make_list_impl(std::move(vec));
        List list(ptr.get());
        arena->emplace_back(std::move(ptr));
        return RTAny::from_list(list);
      } else if (type.has_array() &&
                 type.array().component_type().has_primitive_type() &&
                 type.array().component_type().primitive_type() ==
                     ::common::PrimitiveType::DT_SIGNED_INT64) {
        std::vector<List> vec;
        arena = std::make_shared<Arena>();
        const auto& val = input.at(name);
        size_t start = 0;
        for (size_t i = 0; i <= val.size(); ++i) {
          if (i == val.size() || val[i] == ';') {
            std::vector<int64_t> inner_vec;
            size_t inner_start = start;
            for (size_t j = start; j <= i; ++j) {
              if (j == i || val[j] == ',') {
                inner_vec.emplace_back(std::stoll(
                    std::string(val.data() + inner_start, j - inner_start)));
                inner_start = j + 1;
              }
            }
            auto inner_ptr =
                ListImpl<int64_t>::make_list_impl(std::move(inner_vec));
            List inner_list(inner_ptr.get());
            arena->emplace_back(std::move(inner_ptr));
            vec.emplace_back(inner_list);
            start = i + 1;
          }
        }
        auto ptr = ListImpl<List>::make_list_impl(std::move(vec));
        List list(ptr.get());
        arena->emplace_back(std::move(ptr));
        return RTAny::from_list(list);
      }
    }

    LOG(FATAL) << "not support type: " << param.DebugString();
  }
  LOG(FATAL) << "graph data type not expected....";
  return RTAny();
}

static inline int get_proiority(const ::common::ExprOpr& opr) {
  switch (opr.item_case()) {
  case ::common::ExprOpr::kBrace: {
    return 17;
  }
  case ::common::ExprOpr::kExtract: {
    return 2;
  }
  case ::common::ExprOpr::kToDate: {
    return 2;
  }
  case ::common::ExprOpr::kLogical: {
    switch (opr.logical()) {
    case ::common::Logical::AND:
      return 11;
    case ::common::Logical::OR:
      return 12;
    case ::common::Logical::NOT:
    case ::common::Logical::WITHIN:
    case ::common::Logical::WITHOUT:
    case ::common::Logical::REGEX:
      return 2;
    case ::common::Logical::EQ:
    case ::common::Logical::NE:
      return 7;
    case ::common::Logical::GE:
    case ::common::Logical::GT:
    case ::common::Logical::LT:
    case ::common::Logical::LE:
      return 6;
    default:
      return 16;
    }
  }
  case ::common::ExprOpr::kArith: {
    switch (opr.arith()) {
    case ::common::Arithmetic::ADD:
    case ::common::Arithmetic::SUB:
      return 4;
    case ::common::Arithmetic::MUL:
    case ::common::Arithmetic::DIV:
    case ::common::Arithmetic::MOD:
      return 3;
    default:
      return 16;
    }
  }
  case ::common::ExprOpr::kDateTimeMinus:
    return 4;
  default:
    return 16;
  }
  return 16;
}

static std::unique_ptr<ExprBase> parse_expression_impl(
    const StorageReadInterface* graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const ::common::Expression& expr, VarType var_type);

static std::unique_ptr<ExprBase> build_expr(
    const StorageReadInterface* graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    std::stack<::common::ExprOpr>& opr_stack, VarType var_type) {
  while (!opr_stack.empty()) {
    auto opr = opr_stack.top();
    opr_stack.pop();
    switch (opr.item_case()) {
    case ::common::ExprOpr::kConst: {
      std::shared_ptr<Arena> arena = nullptr;
      return std::make_unique<ConstExpr>(parse_const_value(opr.const_(), arena),
                                         arena);
    }
    case ::common::ExprOpr::kParam: {
      std::shared_ptr<Arena> arena = nullptr;
      return std::make_unique<ConstExpr>(
          parse_param(opr.param(), params, arena), arena);
    }
    case ::common::ExprOpr::kVar: {
      return std::make_unique<VariableExpr>(graph, ctx, opr.var(), var_type);
    }
    case ::common::ExprOpr::kLogical: {
      if (opr.logical() == ::common::Logical::WITHIN) {
        auto lhs = opr_stack.top();
        opr_stack.pop();
        auto rhs = opr_stack.top();
        opr_stack.pop();
        assert(lhs.has_var());
        if (rhs.has_const_()) {
          auto key =
              std::make_unique<VariableExpr>(graph, ctx, lhs.var(), var_type);
          if (key->type().id() == DataTypeId::BIGINT) {
            return std::make_unique<WithInExpr<int64_t>>(ctx, std::move(key),
                                                         rhs.const_());
          } else if (key->type().id() == DataTypeId::UBIGINT) {
            return std::make_unique<WithInExpr<uint64_t>>(ctx, std::move(key),
                                                          rhs.const_());
          } else if (key->type().id() == DataTypeId::INTEGER) {
            return std::make_unique<WithInExpr<int32_t>>(ctx, std::move(key),
                                                         rhs.const_());
          } else if (key->type().id() == DataTypeId::VARCHAR) {
            return std::make_unique<WithInExpr<std::string>>(
                ctx, std::move(key), rhs.const_());
          } else {
            LOG(FATAL) << "not support";
          }
        } else if (rhs.has_var()) {
          auto key =
              std::make_unique<VariableExpr>(graph, ctx, lhs.var(), var_type);
          if (key->type().id() == DataTypeId::VERTEX) {
            auto val =
                std::make_unique<VariableExpr>(graph, ctx, rhs.var(), var_type);
            if (val->type().id() == DataTypeId::LIST) {
              return std::make_unique<VertexWithInListExpr>(ctx, std::move(key),
                                                            std::move(val));
            } else if (val->type().id() == DataTypeId::SET) {
              return std::make_unique<VertexWithInSetExpr>(ctx, std::move(key),
                                                           std::move(val));
            } else {
              LOG(FATAL) << "not support" << static_cast<int>(val->type().id());
            }
          }

        } else if (rhs.has_param()) {
          auto key =
              std::make_unique<VariableExpr>(graph, ctx, lhs.var(), var_type);
          std::shared_ptr<Arena> arena = nullptr;
          auto val = std::make_unique<ConstExpr>(
              parse_param(rhs.param(), params, arena), arena);
          return std::make_unique<WithInListExpr>(ctx, std::move(key),
                                                  std::move(val));
        } else {
          LOG(FATAL) << "not support" << rhs.DebugString();
        }
      } else if (opr.logical() == ::common::Logical::NOT ||
                 opr.logical() == ::common::Logical::ISNULL) {
        auto lhs = build_expr(graph, ctx, params, opr_stack, var_type);
        return std::make_unique<UnaryLogicalExpr>(std::move(lhs),
                                                  opr.logical());
      } else {
        auto lhs = build_expr(graph, ctx, params, opr_stack, var_type);
        auto rhs = build_expr(graph, ctx, params, opr_stack, var_type);
        if (opr.logical() == ::common::Logical::AND) {
          return std::make_unique<AndOpExpr>(std::move(lhs), std::move(rhs));
        } else if (opr.logical() == ::common::Logical::OR) {
          return std::make_unique<OrOpExpr>(std::move(lhs), std::move(rhs));
        }
        return std::make_unique<LogicalExpr>(std::move(lhs), std::move(rhs),
                                             opr.logical());
      }
      break;
    }
    case ::common::ExprOpr::kArith: {
      auto lhs = build_expr(graph, ctx, params, opr_stack, var_type);
      auto rhs = build_expr(graph, ctx, params, opr_stack, var_type);
      return std::make_unique<ArithExpr>(std::move(lhs), std::move(rhs),
                                         opr.arith());
    }
    case ::common::ExprOpr::kCase: {
      auto op = opr.case_();
      size_t len = op.when_then_expressions_size();
      std::vector<
          std::pair<std::unique_ptr<ExprBase>, std::unique_ptr<ExprBase>>>
          when_then_exprs;
      for (size_t i = 0; i < len; ++i) {
        auto when_expr = op.when_then_expressions(i).when_expression();
        auto then_expr = op.when_then_expressions(i).then_result_expression();
        when_then_exprs.emplace_back(
            parse_expression_impl(graph, ctx, params, when_expr, var_type),
            parse_expression_impl(graph, ctx, params, then_expr, var_type));
      }
      auto else_expr = parse_expression_impl(
          graph, ctx, params, op.else_result_expression(), var_type);
      return std::make_unique<CaseWhenExpr>(std::move(when_then_exprs),
                                            std::move(else_expr));
    }
    case ::common::ExprOpr::kExtract: {
      auto hs = build_expr(graph, ctx, params, opr_stack, var_type);
      if (hs->type().id() == DataTypeId::BIGINT) {
        return std::make_unique<ExtractExpr<int64_t>>(std::move(hs),
                                                      opr.extract());
      } else if (hs->type().id() == DataTypeId::DATE) {
        return std::make_unique<ExtractExpr<Date>>(std::move(hs),
                                                   opr.extract());
      } else if (hs->type().id() == DataTypeId::TIMESTAMP_MS) {
        return std::make_unique<ExtractExpr<DateTime>>(std::move(hs),
                                                       opr.extract());
      } else if (hs->type().id() == DataTypeId::INTERVAL) {
        return std::make_unique<ExtractExpr<Interval>>(std::move(hs),
                                                       opr.extract());
      } else {
        LOG(FATAL) << "not support" << static_cast<int>(hs->type().id());
      }
    }

    case ::common::ExprOpr::kToDate: {
      auto date_str = opr.to_date().date_str();
      // Parse the date string into Date
      auto date = Date(date_str);
      // Create a ConstExpr with the parsed Date
      return std::make_unique<ConstExpr>(RTAny::from_date(date));
    }

    case ::common::ExprOpr::kToDatetime: {
      auto date_time_str = opr.to_datetime().datetime_str();
      // Parse the date time string into DateTime
      auto date_time = DateTime(date_time_str);
      // Create a ConstExpr with the parsed DateTime
      return std::make_unique<ConstExpr>(RTAny::from_datetime(date_time));
    }

    case ::common::ExprOpr::kToInterval: {
      auto interval_str = opr.to_interval().interval_str();
      // Parse the interval string into Interval
      Interval interval(interval_str);
      // Create a ConstExpr with the parsed Interval
      return std::make_unique<ConstExpr>(RTAny::from_interval(interval));
    }

    case ::common::ExprOpr::kToTuple: {
      const auto& compisite_fields = opr.to_tuple().fields();

      std::vector<std::unique_ptr<ExprBase>> exprs_vec;
      bool has_optional = false;
      for (int i = 0; i < compisite_fields.size(); ++i) {
        exprs_vec.push_back(std::move(parse_expression_impl(
            graph, ctx, params, compisite_fields[i], var_type)));
        if (exprs_vec.back()->is_optional()) {
          has_optional = true;
        }
      }

      if (compisite_fields.size() == 3 && !has_optional) {
        std::array<std::unique_ptr<ExprBase>, 3> exprs;
        for (int i = 0; i < compisite_fields.size(); ++i) {
          exprs[i] = std::move(exprs_vec[i]);
        }
        return TypedTupleBuilder<3, 3>().build_typed_tuple(std::move(exprs));
      } else if (compisite_fields.size() == 2 && !has_optional) {
        std::array<std::unique_ptr<ExprBase>, 2> exprs;
        for (int i = 0; i < compisite_fields.size(); ++i) {
          exprs[i] = std::move(exprs_vec[i]);
        }
        return TypedTupleBuilder<2, 2>().build_typed_tuple(std::move(exprs));
      }

      return std::make_unique<TupleExpr>(std::move(exprs_vec));
    }

    case ::common::ExprOpr::kScalarFunc: {
      auto op = opr.scalar_func();
      const std::string& signature = op.unique_name();
      gs::runtime::neug_func_exec_t fn = nullptr;

      auto gCatalog = catalog::GCatalogHolder::getGCatalog();
      auto func = gCatalog->getFunctionWithSignature(
          &gs::transaction::DUMMY_TRANSACTION, signature);
      if (!func) {
        THROW_RUNTIME_ERROR("Function not found in catalog for signature: " +
                            signature);
      }

      auto* scalarFunc = dynamic_cast<function::NeugScalarFunction*>(func);
      fn = scalarFunc->neugExecFunc;
      if (!fn) {
        THROW_RUNTIME_ERROR(
            "ScalarFunction neugExecFunc is null for signature: " + signature);
      }

      DataType ret_type = DataType(DataTypeId::UNKNOWN);
      if (opr.has_node_type()) {
        ret_type = parse_from_ir_data_type(opr.node_type());
      }
      std::vector<std::unique_ptr<ExprBase>> children;
      children.reserve(op.parameters_size());
      for (int i = 0; i < op.parameters_size(); ++i) {
        children.emplace_back(parse_expression_impl(
            graph, ctx, params, op.parameters(i), var_type));
      }
      return std::make_unique<ScalarFunctionExpr>(fn, ret_type,
                                                  std::move(children));
    }

    case ::common::ExprOpr::kUdfFunc: {
      auto op = opr.udf_func();
      std::string name = op.name();
      auto expr =
          parse_expression_impl(graph, ctx, params, op.parameters(0), var_type);
      if (name == "gs.function.relationships") {
        return std::make_unique<RelationshipsExpr>(std::move(expr));
      } else if (name == "gs.function.nodes") {
        return std::make_unique<NodesExpr>(std::move(expr));
      } else if (name == "gs.function.startNode") {
        return std::make_unique<StartNodeExpr>(std::move(expr));
      } else if (name == "gs.function.endNode") {
        return std::make_unique<EndNodeExpr>(std::move(expr));
      } else if (name == "gs.function.datetime") {
        return std::make_unique<ToDateTimeExpr>(std::move(expr));
      } else if (name == "gs.function.date32") {
        return std::make_unique<ToDate32Expr>(std::move(expr));
      } else if (name == "gs.function.abs") {
        return std::make_unique<AbsExpr>(std::move(expr));
      } else {
        LOG(FATAL) << "not support udf" << opr.DebugString();
      }
    }
    case ::common::ExprOpr::kDateTimeMinus: {
      auto lhs = build_expr(graph, ctx, params, opr_stack, var_type);
      auto rhs = build_expr(graph, ctx, params, opr_stack, var_type);
      return std::make_unique<DateMinusExpr>(std::move(lhs), std::move(rhs));
    }
    case ::common::ExprOpr::kPathFunc: {
      auto opt = opr.path_func().opt();
      const auto& name = opr.path_func().property().key().name();
      int tag = opr.path_func().has_tag() ? opr.path_func().tag().id() : -1;
      if (opr.node_type().data_type().item_case() !=
          ::common::DataType::kArray) {
        LOG(FATAL) << "path function node_type is not array type";
        return nullptr;
      }
      auto type = parse_from_data_type(
          opr.node_type().data_type().array().component_type());

      if (opt == ::common::PathFunction_FuncOpt::PathFunction_FuncOpt_VERTEX) {
        return std::make_unique<PathVertexPropsExpr>(*graph, ctx, tag, name,
                                                     type);
      } else if (opt ==
                 ::common::PathFunction_FuncOpt::PathFunction_FuncOpt_EDGE) {
        return std::make_unique<PathEdgePropsExpr>(*graph, ctx, tag, name,
                                                   type);
      } else {
        LOG(FATAL) << "unsupport path function opt" << opr.DebugString();
      }

      break;
    }
    default:
      LOG(FATAL) << "not support" << opr.DebugString();
      break;
    }
  }
  return nullptr;
}

static std::unique_ptr<ExprBase> parse_expression_impl(
    const StorageReadInterface* graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const ::common::Expression& expr, VarType var_type) {
  std::stack<::common::ExprOpr> opr_stack;
  std::stack<::common::ExprOpr> opr_stack2;
  const auto& oprs = expr.operators();
  for (auto it = oprs.rbegin(); it != oprs.rend(); ++it) {
    switch ((*it).item_case()) {
    case ::common::ExprOpr::kBrace: {
      auto brace = (*it).brace();
      if (brace == ::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE) {
        while (!opr_stack.empty() &&
               opr_stack.top().item_case() != ::common::ExprOpr::kBrace) {
          opr_stack2.push(opr_stack.top());
          opr_stack.pop();
        }
        assert(!opr_stack.empty());
        opr_stack.pop();
      } else if (brace == ::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
        opr_stack.emplace(*it);
      }
      break;
    }
    case ::common::ExprOpr::kConst:
    case ::common::ExprOpr::kVar:
    case ::common::ExprOpr::kParam: {
      opr_stack2.push(*it);
      break;
    }
    case ::common::ExprOpr::kArith:
    case ::common::ExprOpr::kLogical:
    case ::common::ExprOpr::kDateTimeMinus: {
      // unary operator
      if ((*it).logical() == ::common::Logical::NOT ||
          (*it).logical() == ::common::Logical::ISNULL) {
        opr_stack2.push(*it);
        break;
      }

      while (!opr_stack.empty() &&
             get_proiority(opr_stack.top()) <= get_proiority(*it)) {
        opr_stack2.push(opr_stack.top());
        opr_stack.pop();
      }
      opr_stack.push(*it);
      break;
    }
    case ::common::ExprOpr::kExtract:
    case ::common::ExprOpr::kCase:
    case ::common::ExprOpr::kUdfFunc:
    case ::common::ExprOpr::kToInterval:
    case ::common::ExprOpr::kToDate:
    case ::common::ExprOpr::kToDatetime:
    case ::common::ExprOpr::kToTuple:
    case ::common::ExprOpr::kScalarFunc:
    case ::common::ExprOpr::kPathFunc: {
      opr_stack2.push(*it);
      break;
    }
    default: {
      LOG(FATAL) << "not support" << (*it).DebugString();
      break;
    }
    }
  }
  while (!opr_stack.empty()) {
    opr_stack2.push(opr_stack.top());
    opr_stack.pop();
  }
  return build_expr(graph, ctx, params, opr_stack2, var_type);
}

std::unique_ptr<ExprBase> parse_expression(
    const StorageReadInterface* graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const ::common::Expression& expr, VarType var_type) {
  return parse_expression_impl(graph, ctx, params, expr, var_type);
}

bool graph_related_expr(const ::common::Expression& expr) {
  const auto& oprs = expr.operators();
  for (size_t i = 0; i < oprs.size(); ++i) {
    if (oprs[i].item_case() == ::common::ExprOpr::kVar) {
      if (Var::graph_related_var(oprs[i].var())) {
        return true;
      }
    }
    if (oprs[i].item_case() == ::common::ExprOpr::kPathFunc) {
      return true;
    }
  }
  return false;
}

}  // namespace runtime

}  // namespace gs
