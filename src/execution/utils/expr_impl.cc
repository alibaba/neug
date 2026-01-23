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
#include "neug/execution/utils/pb_parse_utils.h"

#include <time.h>

#include <iterator>
#include <regex>
#include <sstream>
#include <stack>

namespace gs {

namespace runtime {

struct LabelTriplet;

Value VariableExpr::eval_path(size_t idx) const { return var_.get(idx); }

Value VariableExpr::eval_vertex(label_t label, vid_t v) const {
  return var_.get_vertex(label, v);
}
Value VariableExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const void* data_ptr) const {
  return var_.get_edge(label, src, dst, data_ptr);
}

LogicalExpr::LogicalExpr(std::unique_ptr<ExprBase>&& lhs,
                         std::unique_ptr<ExprBase>&& rhs,
                         ::common::Logical logic)
    : lhs_(std::move(lhs)),
      rhs_(std::move(rhs)),
      logic_(logic),
      type_(DataType(DataTypeId::kBoolean)) {
  switch (logic) {
  case ::common::Logical::LT: {
    op_ = [](const Value& lhs, const Value& rhs) { return lhs < rhs; };
    break;
  }
  case ::common::Logical::GT: {
    op_ = [](const Value& lhs, const Value& rhs) { return rhs < lhs; };
    break;
  }
  case ::common::Logical::GE: {
    op_ = [](const Value& lhs, const Value& rhs) { return !(lhs < rhs); };
    break;
  }
  case ::common::Logical::LE: {
    op_ = [](const Value& lhs, const Value& rhs) { return !(rhs < lhs); };
    break;
  }
  case ::common::Logical::EQ: {
    op_ = [](const Value& lhs, const Value& rhs) { return lhs == rhs; };
    break;
  }
  case ::common::Logical::NE: {
    op_ = [](const Value& lhs, const Value& rhs) { return !(lhs == rhs); };
    break;
  }
  case ::common::Logical::REGEX: {
    op_ = [](const Value& lhs, const Value& rhs) {
      auto lhs_str = std::string(lhs.GetValue<std::string>());
      auto rhs_str = std::string(rhs.GetValue<std::string>());
      return std::regex_match(lhs_str, std::regex(rhs_str));
    };
    break;
  }
  case ::common::Logical::AND:
  case ::common::Logical::OR: {
    break;
  }
  default: {
    LOG(FATAL) << "not support..." << static_cast<int>(logic);
    break;
  }
  }
}

Value LogicalExpr::eval_impl(const Value& lhs_val, const Value& rhs_val) const {
  if (lhs_val.IsNull() || rhs_val.IsNull()) {
    return Value(DataType::BOOLEAN);
  }
  return Value::BOOLEAN(op_(lhs_val, rhs_val));
}

Value LogicalExpr::eval_path(size_t idx) const {
  return eval_impl(lhs_->eval_path(idx), rhs_->eval_path(idx));
}

Value LogicalExpr::eval_vertex(label_t label, vid_t v) const {
  return eval_impl(lhs_->eval_vertex(label, v), rhs_->eval_vertex(label, v));
}

Value LogicalExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                             const void* data_ptr) const {
  return eval_impl(lhs_->eval_edge(label, src, dst, data_ptr),
                   rhs_->eval_edge(label, src, dst, data_ptr));
}

UnaryLogicalExpr::UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr,
                                   ::common::Logical logic)
    : expr_(std::move(expr)),
      logic_(logic),
      type_(DataType(DataTypeId::kBoolean)) {}

Value UnaryLogicalExpr::eval_path(size_t idx) const {
  auto any_val = expr_->eval_path(idx);
  if (logic_ == ::common::Logical::NOT) {
    if (any_val.IsNull()) {
      return Value(DataType::BOOLEAN);
    }
    return Value::BOOLEAN(!any_val.GetValue<bool>());
  } else if (logic_ == ::common::Logical::ISNULL) {
    return Value::BOOLEAN(any_val.IsNull());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return Value::BOOLEAN(false);
}

Value UnaryLogicalExpr::eval_vertex(label_t label, vid_t v) const {
  auto any_val = expr_->eval_vertex(label, v);
  if (logic_ == ::common::Logical::NOT) {
    if (any_val.IsNull()) {
      return Value(DataType::BOOLEAN);
    }
    return Value::BOOLEAN(!any_val.GetValue<bool>());
  } else if (logic_ == ::common::Logical::ISNULL) {
    return Value::BOOLEAN(any_val.IsNull());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return Value::BOOLEAN(false);
}

Value UnaryLogicalExpr::eval_edge(const LabelTriplet& label, vid_t src,
                                  vid_t dst, const void* data_ptr) const {
  auto any_val = expr_->eval_edge(label, src, dst, data_ptr);
  if (logic_ == ::common::Logical::NOT) {
    if (any_val.IsNull()) {
      return Value(DataType::BOOLEAN);
    }
    return Value::BOOLEAN(!any_val.GetValue<bool>());
  } else if (logic_ == ::common::Logical::ISNULL) {
    return Value::BOOLEAN(any_val.IsNull());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return Value::BOOLEAN(false);
}

ArithExpr::ArithExpr(std::unique_ptr<ExprBase>&& lhs,
                     std::unique_ptr<ExprBase>&& rhs,
                     ::common::Arithmetic arith)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)), arith_(arith) {
  if (lhs_->type().id() == DataTypeId::kDouble ||
      rhs_->type().id() == DataTypeId::kDouble) {
    type_ = DataType(DataTypeId::kDouble);
  } else if (lhs_->type().id() == DataTypeId::kFloat ||
             rhs_->type().id() == DataTypeId::kFloat) {
    type_ = DataType(DataTypeId::kFloat);
  } else if (lhs_->type().id() == DataTypeId::kInt64 ||
             rhs_->type().id() == DataTypeId::kInt64) {
    type_ = DataType(DataTypeId::kInt64);
  } else {
    type_ = lhs_->type();
  }
  switch (arith_) {
  case ::common::Arithmetic::ADD: {
    op_ = [](const Value& lhs, const Value& rhs) { return lhs + rhs; };
    break;
  }
  case ::common::Arithmetic::SUB: {
    op_ = [](const Value& lhs, const Value& rhs) { return lhs - rhs; };
    break;
  }
  case ::common::Arithmetic::DIV: {
    op_ = [](const Value& lhs, const Value& rhs) { return lhs / rhs; };
    break;
  }
  case ::common::Arithmetic::MOD: {
    op_ = [](const Value& lhs, const Value& rhs) { return lhs % rhs; };
    break;
  }

  case ::common::Arithmetic::MUL: {
    op_ = [](const Value& lhs, const Value& rhs) { return lhs * rhs; };
    break;
  }

  default: {
    LOG(FATAL) << "not support..." << static_cast<int>(arith);
    break;
  }
  }
}

Value ArithExpr::eval_path(size_t idx) const {
  return op_(lhs_->eval_path(idx), rhs_->eval_path(idx));
}

Value ArithExpr::eval_vertex(label_t label, vid_t v) const {
  return op_(lhs_->eval_vertex(label, v), rhs_->eval_vertex(label, v));
}

Value ArithExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const void* data_ptr) const {
  return op_(lhs_->eval_edge(label, src, dst, data_ptr),
             rhs_->eval_edge(label, src, dst, data_ptr));
}

ConstExpr::ConstExpr(const Value& val) : val_(val) { type_ = val_.type(); }
Value ConstExpr::eval_path(size_t idx) const { return val_; }
Value ConstExpr::eval_vertex(label_t label, vid_t v) const { return val_; }
Value ConstExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const void* data_ptr) const {
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
  type_ = (DataTypeId::kNull);
  if (when_then_exprs_.size() > 0) {
    if (when_then_exprs_[0].second->type().id() != DataTypeId::kNull) {
      type_ = when_then_exprs_[0].second->type();
    }
  }
  if (else_expr_->type().id() != DataTypeId::kNull) {
    type_ = else_expr_->type();
  }
}

Value CaseWhenExpr::eval_path(size_t idx) const {
  for (auto& pair : when_then_exprs_) {
    const auto& cond_val = pair.first->eval_path(idx);
    if (cond_val.IsTrue()) {
      return pair.second->eval_path(idx);
    }
  }
  return else_expr_->eval_path(idx);
}

Value CaseWhenExpr::eval_vertex(label_t label, vid_t v) const {
  for (auto& pair : when_then_exprs_) {
    const auto& cond_val = pair.first->eval_vertex(label, v);
    if (cond_val.IsTrue()) {
      return pair.second->eval_vertex(label, v);
    }
  }
  return else_expr_->eval_vertex(label, v);
}

Value CaseWhenExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const void* data_ptr) const {
  for (auto& pair : when_then_exprs_) {
    const auto& cond_val = pair.first->eval_edge(label, src, dst, data_ptr);
    if (cond_val.IsTrue()) {
      return pair.second->eval_edge(label, src, dst, data_ptr);
    }
  }
  return else_expr_->eval_edge(label, src, dst, data_ptr);
}

TupleExpr::TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs)
    : exprs_(std::move(exprs)) {
  std::vector<DataType> components;
  for (const auto& expr : exprs_) {
    components.push_back(expr->type());
  }
  type_ = DataType(DataTypeId::kStruct,
                   std::make_shared<StructTypeInfo>(components));
}

Value TupleExpr::eval_path(size_t idx) const {
  std::vector<Value> ret;
  for (auto& expr : exprs_) {
    auto val = expr->eval_path(idx);
    ret.push_back(val);
  }
  return Value::STRUCT(std::move(ret));
}

Value TupleExpr::eval_vertex(label_t label, vid_t v) const {
  std::vector<Value> ret;
  for (auto& expr : exprs_) {
    auto val = expr->eval_vertex(label, v);
    ret.push_back(val);
  }
  return Value::STRUCT(std::move(ret));
}

Value TupleExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const void* data_ptr) const {
  std::vector<Value> ret;
  for (auto& expr : exprs_) {
    auto val = expr->eval_edge(label, src, dst, data_ptr);
    ret.push_back(val);
  }
  return Value::STRUCT(std::move(ret));
}

Value PathVertexPropsExpr::eval_path(size_t idx) const {
  auto path = col_.get_elem(idx);
  if (path.IsNull()) {
    return Value(type());
  }
  const auto& nodes = PathValue::Get(path).nodes();
  std::vector<Value> props;
  for (const auto& node : nodes) {
    const auto& name = graph_.schema().get_vertex_property_names(node.label_);
    auto it = std::find(name.begin(), name.end(), prop_);
    if (it == name.end()) {
      props.push_back(Value(ListType::GetChildType(type_)));
    } else {
      size_t index = std::distance(name.begin(), it);
      auto prop = graph_.GetVertexProperty(node.label_, node.vid_, index);
      props.push_back(property_to_value(prop));
    }
  }
  return Value::LIST(std::move(props));
}

Value PathEdgePropsExpr::eval_path(size_t idx) const {
  auto path = col_.get_elem(idx);
  if (path.IsNull()) {
    return Value(type());
  }
  const auto& edges = PathValue::Get(path).relationships();
  std::vector<Value> props;
  for (const auto& edge : edges) {
    const auto& name = graph_.schema().get_edge_property_names(
        edge.label.src_label, edge.label.dst_label, edge.label.edge_label);
    auto it = std::find(name.begin(), name.end(), prop_);
    if (it == name.end()) {
      props.push_back(Value(ListType::GetChildType(type_)));
    } else {
      size_t index = std::distance(name.begin(), it);
      auto accessor =
          graph_.GetEdgeDataAccessor(edge.label.src_label, edge.label.dst_label,
                                     edge.label.edge_label, index);

      if (prop_type_.id() == DataTypeId::kVarchar) {
        auto ret = accessor.template get_typed_data_from_ptr<std::string_view>(
            edge.prop);
        props.push_back(Value::STRING(std::string(ret)));
      } else if (prop_type_.id() == DataTypeId::kInt32) {
        auto ret =
            accessor.template get_typed_data_from_ptr<int32_t>(edge.prop);
        props.push_back(Value::INT32(ret));
      } else if (prop_type_.id() == DataTypeId::kInt64) {
        auto ret =
            accessor.template get_typed_data_from_ptr<int64_t>(edge.prop);
        props.push_back(Value::INT64(ret));
      } else if (prop_type_.id() == DataTypeId::kDouble) {
        auto ret = accessor.template get_typed_data_from_ptr<double>(edge.prop);
        props.push_back(Value::DOUBLE(ret));
      } else if (prop_type_.id() == DataTypeId::kBoolean) {
        auto ret = accessor.template get_typed_data_from_ptr<bool>(edge.prop);
        props.push_back(Value::BOOLEAN(ret));
      } else if (prop_type_.id() == DataTypeId::kTimestampMs) {
        auto ret =
            accessor.template get_typed_data_from_ptr<DateTime>(edge.prop);
        props.push_back(Value::TIMESTAMPMS(ret));
      } else {
        LOG(FATAL) << "not support edge prop type "
                   << static_cast<int>(prop_type_.id());
      }
    }
  }
  return Value::LIST(std::move(props));
}

static Value parse_const_value(const ::common::Value& val) {
  switch (val.item_case()) {
  case ::common::Value::kI32:
    return Value::INT32(val.i32());
  case ::common::Value::kU32:
    return Value::UINT32(val.u32());
  case ::common::Value::kStr: {
    return Value::STRING(std::string(val.str()));
  }
  case ::common::Value::kI64:
    return Value::INT64(val.i64());
  case ::common::Value::kU64:
    return Value::UINT64(val.u64());
  case ::common::Value::kBoolean:
    return Value::BOOLEAN(val.boolean());
  case ::common::Value::kNone:
    return Value(DataType::SQLNULL);
  case ::common::Value::kF64:
    return Value::DOUBLE(val.f64());
  case ::common::Value::kF32:
    return Value::FLOAT(val.f32());
  default:
    LOG(FATAL) << "not support for " << val.item_case();
  }
  return Value(DataType::UNKNOWN);
}

static Value parse_param(const ::common::DynamicParam& param,
                         const std::map<std::string, std::string>& input) {
  if (param.data_type().type_case() ==
      ::common::IrDataType::TypeCase::kDataType) {
    auto type = parse_from_ir_data_type(param.data_type());

    const std::string& name = param.name();
    if (type.id() == DataTypeId::kDate) {
      Date val = Date(input.at(name));
      return Value::DATE(val);
    } else if (type.id() == DataTypeId::kVarchar) {
      const std::string& val = input.at(name);
      return Value::STRING(val);
    } else if (type.id() == DataTypeId::kInt32) {
      int val = std::stoi(input.at(name));
      return Value::INT32(val);
    } else if (type.id() == DataTypeId::kInt64) {
      int64_t val = std::stoll(input.at(name));
      return Value::INT64(val);
    } else if (type.id() == DataTypeId::kTimestampMs) {
      DateTime val = DateTime(input.at(name));
      return Value::TIMESTAMPMS(val);
    } else if (type.id() == DataTypeId::kInterval) {
      Interval val = Interval(input.at(name));
      return Value::INTERVAL(val);
    } else if (type.id() == DataTypeId::kUInt32) {
      uint32_t val = std::stoul(input.at(name));
      return Value::UINT32(val);
    } else if (type.id() == DataTypeId::kUInt64) {
      uint64_t val = std::stoull(input.at(name));
      return Value::UINT64(val);
    } else if (type.id() == DataTypeId::kBoolean) {
      bool val = input.at(name) == "true" || input.at(name) == "1";
      return Value::BOOLEAN(val);
    } else if (type.id() == DataTypeId::kFloat) {
      float val = std::stof(input.at(name));
      return Value::FLOAT(val);
    } else if (type.id() == DataTypeId::kDouble) {
      double val = std::stod(input.at(name));
      return Value::DOUBLE(val);
    } else if (type.id() == DataTypeId::kList) {
      auto type = param.data_type().data_type().array().component_type();
      if (type.has_string()) {
        std::vector<Value> vec;
        const auto& val = input.at(name);
        size_t start = 0;
        for (size_t i = 0; i <= val.size(); ++i) {
          if ((i == val.size() || val[i] == ';') && (i - start > 0)) {
            vec.emplace_back(
                Value::STRING(std::string(val.data() + start, i - start)));
            start = i + 1;
          }
        }
        return Value::LIST(DataType::VARCHAR, std::move(vec));
      } else if (type.item_case() ==
                     ::common::DataType::ItemCase::kPrimitiveType &&
                 type.primitive_type() ==
                     ::common::PrimitiveType::DT_SIGNED_INT64) {
        std::vector<Value> vec;
        const auto& val = input.at(name);
        size_t start = 0;
        for (size_t i = 0; i <= val.size(); ++i) {
          if ((i == val.size() || val[i] == ';') && (i - start > 0)) {
            vec.emplace_back(Value::INT64(
                std::stoll(std::string(val.data() + start, i - start))));
            start = i + 1;
          }
        }
        return Value::LIST(DataType::INT64, std::move(vec));
      } else if (type.has_array() &&
                 type.array().component_type().has_primitive_type() &&
                 type.array().component_type().primitive_type() ==
                     ::common::PrimitiveType::DT_SIGNED_INT64) {
        std::vector<Value> vec;
        const auto& val = input.at(name);
        size_t start = 0;
        for (size_t i = 0; i <= val.size(); ++i) {
          if ((i == val.size() || val[i] == ';') && (i - start > 0)) {
            std::vector<Value> inner_vec;
            size_t inner_start = start;
            for (size_t j = start; j <= i; ++j) {
              if ((j == i || val[j] == ',') && (j - inner_start > 0)) {
                inner_vec.emplace_back(Value::INT64(std::stoll(
                    std::string(val.data() + inner_start, j - inner_start))));
                inner_start = j + 1;
              }
            }
            vec.emplace_back(
                Value::LIST(DataType::INT64, std::move(inner_vec)));
            start = i + 1;
          }
        }
        auto type = DataType::List(DataType::INT64);
        return Value::LIST(type, std::move(vec));
      }
    }

    LOG(FATAL) << "not support type: " << param.DebugString();
  }
  LOG(FATAL) << "graph data type not expected....";
  return Value(DataType::SQLNULL);
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
      return std::make_unique<ConstExpr>(parse_const_value(opr.const_()));
    }
    case ::common::ExprOpr::kParam: {
      return std::make_unique<ConstExpr>(parse_param(opr.param(), params));
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
          if (key->type().id() == DataTypeId::kInt64) {
            return std::make_unique<WithInExpr<int64_t>>(ctx, std::move(key),
                                                         rhs.const_());
          } else if (key->type().id() == DataTypeId::kUInt64) {
            return std::make_unique<WithInExpr<uint64_t>>(ctx, std::move(key),
                                                          rhs.const_());
          } else if (key->type().id() == DataTypeId::kInt32) {
            return std::make_unique<WithInExpr<int32_t>>(ctx, std::move(key),
                                                         rhs.const_());
          } else if (key->type().id() == DataTypeId::kVarchar) {
            return std::make_unique<WithInExpr<std::string>>(
                ctx, std::move(key), rhs.const_());
          } else {
            LOG(FATAL) << "not support";
          }
        } else if (rhs.has_var()) {
          auto key =
              std::make_unique<VariableExpr>(graph, ctx, lhs.var(), var_type);
          auto val =
              std::make_unique<VariableExpr>(graph, ctx, rhs.var(), var_type);
          return std::make_unique<WithInListExpr>(ctx, std::move(key),
                                                  std::move(val));

        } else if (rhs.has_param()) {
          auto key =
              std::make_unique<VariableExpr>(graph, ctx, lhs.var(), var_type);
          auto val =
              std::make_unique<ConstExpr>(parse_param(rhs.param(), params));
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
      if (hs->type().id() == DataTypeId::kInt64) {
        return std::make_unique<ExtractExpr<int64_t>>(std::move(hs),
                                                      opr.extract());
      } else if (hs->type().id() == DataTypeId::kDate) {
        return std::make_unique<ExtractExpr<Date>>(std::move(hs),
                                                   opr.extract());
      } else if (hs->type().id() == DataTypeId::kTimestampMs) {
        return std::make_unique<ExtractExpr<DateTime>>(std::move(hs),
                                                       opr.extract());
      } else if (hs->type().id() == DataTypeId::kInterval) {
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
      return std::make_unique<ConstExpr>(Value::DATE(date));
    }

    case ::common::ExprOpr::kToDatetime: {
      auto date_time_str = opr.to_datetime().datetime_str();
      // Parse the date time string into DateTime
      auto date_time = DateTime(date_time_str);
      // Create a ConstExpr with the parsed DateTime
      return std::make_unique<ConstExpr>(Value::TIMESTAMPMS(date_time));
    }

    case ::common::ExprOpr::kToInterval: {
      auto interval_str = opr.to_interval().interval_str();
      // Parse the interval string into Interval
      Interval interval(interval_str);
      // Create a ConstExpr with the parsed Interval
      return std::make_unique<ConstExpr>(Value::INTERVAL(interval));
    }

    case ::common::ExprOpr::kToTuple: {
      const auto& compisite_fields = opr.to_tuple().fields();

      std::vector<std::unique_ptr<ExprBase>> exprs_vec;
      for (int i = 0; i < compisite_fields.size(); ++i) {
        exprs_vec.emplace_back(parse_expression_impl(
            graph, ctx, params, compisite_fields[i], var_type));
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

      DataType ret_type = DataType(DataTypeId::kUnknown);
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
      } else if (name == "gs.function.abs") {
        return std::make_unique<AbsExpr>(std::move(expr));
      } else {
        LOG(FATAL) << "not support udf" << opr.DebugString();
      }
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
