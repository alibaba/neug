
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
#include "neug/execution/expression/special_predicates.h"
#include "neug/generated/proto/plan/expr.pb.h"

namespace neug {
namespace execution {
bool is_pk_oid_exact_check(const neug::Schema& schema, label_t label,
                           const common::Expression& expr) {
  if (expr.operators_size() != 3) {
    return false;
  }
  if (!(expr.operators(0).has_var() && expr.operators(0).var().has_property() &&
        expr.operators(0).var().property().has_key())) {
    auto& key = expr.operators(7).var().property().key();
    if (!(key.item_case() == common::NameOrId::ItemCase::kName &&
          key.name() == schema.get_vertex_primary_key_name(label))) {
      return false;
    }
    return false;
  }
  if (!(expr.operators(1).item_case() == common::ExprOpr::kLogical &&
        expr.operators(1).logical() == common::Logical::EQ)) {
    return false;
  }

  if (expr.operators(2).has_param()) {
    auto& p = expr.operators(2).param();
    auto name = p.name();
    // todo: check data type
    auto type_ = parse_from_ir_data_type(p.data_type());
    if (type_.id() != DataTypeId::kInt64) {
      return false;
    }
    return true;
  } else if (expr.operators(2).has_const_()) {
    auto& c = expr.operators(2).const_();
    if (c.item_case() == common::Value::kI64) {
      return true;
    } else {
      return false;
    }

  } else {
    return false;
  }
  return false;
}

SPPredicateType parse_sp_pred(const common::Expression& expr) {
  if (expr.operators_size() != 3) {
    return SPPredicateType::kUnknown;
  }

  if (!(expr.operators(0).has_var() &&
        expr.operators(0).var().has_property())) {
    return SPPredicateType::kUnknown;
  }
  if (!(expr.operators(1).item_case() == common::ExprOpr::ItemCase::kLogical)) {
    return SPPredicateType::kUnknown;
  }
  if (!expr.operators(2).has_param() && !expr.operators(2).has_const_()) {
    return SPPredicateType::kUnknown;
  }
  switch (expr.operators(1).logical()) {
  case common::Logical::GT:
    return SPPredicateType::kPropertyGT;
  case common::Logical::LT:
    return SPPredicateType::kPropertyLT;
  case common::Logical::LE:
    return SPPredicateType::kPropertyLE;
  case common::Logical::GE:
    return SPPredicateType::kPropertyGE;
  case common::Logical::EQ:
    return SPPredicateType::kPropertyEQ;
  case common::Logical::NE:
    return SPPredicateType::kPropertyNE;
  case common::Logical::WITHIN:
    return SPPredicateType::kWithIn;
  default:
    return SPPredicateType::kUnknown;
  }
}

bool is_special_edge_predicate(const Schema& schema,
                               const std::vector<LabelTriplet>& labels,
                               const common::Expression& expr,
                               SpecialPredicateConfig& config) {
  if (expr.operators_size() == 3) {
    const common::ExprOpr& op0 = expr.operators(0);
    if (!op0.has_var()) {
      return false;
    }
    if (!op0.var().has_property()) {
      return false;
    }
    if (!op0.var().property().has_key()) {
      return false;
    }
    if (!(op0.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      return false;
    }

    config.property_name = op0.var().property().key().name();
    for (const auto& label : labels) {
      const auto& prop_names = schema.get_edge_property_names(
          label.src_label, label.dst_label, label.edge_label);
      if (std::find(prop_names.begin(), prop_names.end(),
                    config.property_name) == prop_names.end()) {
        return false;
      }
    }

    const common::ExprOpr& op1 = expr.operators(1);
    if (!(op1.item_case() == common::ExprOpr::kLogical)) {
      return false;
    }
    SPPredicateType ptype;
    if (op1.logical() == common::Logical::LT) {
      ptype = SPPredicateType::kPropertyLT;
    } else if (op1.logical() == common::Logical::GT) {
      ptype = SPPredicateType::kPropertyGT;
    } else if (op1.logical() == common::Logical::EQ) {
      ptype = SPPredicateType::kPropertyEQ;
    } else if (op1.logical() == common::Logical::LE) {
      ptype = SPPredicateType::kPropertyLE;
    } else if (op1.logical() == common::Logical::GE) {
      ptype = SPPredicateType::kPropertyGE;
    } else if (op1.logical() == common::Logical::NE) {
      ptype = SPPredicateType::kPropertyNE;
    } else {
      return false;
    }
    config.ptype = ptype;

    const common::ExprOpr& op2 = expr.operators(2);
    if (!op2.has_param()) {
      return false;
    }
    if (!op2.param().has_data_type()) {
      return false;
    }
    if (!(op2.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      return false;
    }
    config.param_names.emplace_back(op2.param().name());
    config.param_type = parse_from_ir_data_type(op2.param().data_type()).id();
    return config.param_type == DataTypeId::kInt64 ||
           config.param_type == DataTypeId::kInt32 ||
           config.param_type == DataTypeId::kTimestampMs ||
           config.param_type == DataTypeId::kVarchar;
  }
  return false;
}

bool is_special_vertex_predicate(const Schema& schema,
                                 const std::vector<label_t>& labels,
                                 const common::Expression& expr,
                                 SpecialPredicateConfig& config) {
  if (expr.operators_size() == 3) {
    const common::ExprOpr& op0 = expr.operators(0);
    if (!op0.has_var()) {
      return false;
    }
    if (!op0.var().has_property()) {
      return false;
    }
    if (!op0.var().property().has_key()) {
      return false;
    }
    if (!(op0.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      return false;
    }

    config.property_name = op0.var().property().key().name();
    for (const auto& label : labels) {
      const auto& pk = schema.get_vertex_primary_key_name(label);
      if (config.property_name == pk) {
        continue;
      }
      const auto& prop_names = schema.get_vertex_property_names(label);
      if (std::find(prop_names.begin(), prop_names.end(),
                    config.property_name) == prop_names.end()) {
        return false;
      }
    }

    const common::ExprOpr& op1 = expr.operators(1);
    if (!(op1.item_case() == common::ExprOpr::kLogical)) {
      return false;
    }
    SPPredicateType ptype;
    if (op1.logical() == common::Logical::LT) {
      ptype = SPPredicateType::kPropertyLT;
    } else if (op1.logical() == common::Logical::GT) {
      ptype = SPPredicateType::kPropertyGT;
    } else if (op1.logical() == common::Logical::EQ) {
      ptype = SPPredicateType::kPropertyEQ;
    } else if (op1.logical() == common::Logical::LE) {
      ptype = SPPredicateType::kPropertyLE;
    } else if (op1.logical() == common::Logical::GE) {
      ptype = SPPredicateType::kPropertyGE;
    } else if (op1.logical() == common::Logical::NE) {
      ptype = SPPredicateType::kPropertyNE;
    } else {
      return false;
    }
    config.ptype = ptype;

    const common::ExprOpr& op2 = expr.operators(2);
    if (!op2.has_param()) {
      return false;
    }
    if (!op2.param().has_data_type()) {
      return false;
    }
    if (!(op2.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      return false;
    }
    config.param_names.push_back(op2.param().name());
    config.param_type = parse_from_ir_data_type(op2.param().data_type()).id();
    return config.param_type == DataTypeId::kInt64 ||
           config.param_type == DataTypeId::kInt32 ||
           config.param_type == DataTypeId::kTimestampMs ||
           config.param_type == DataTypeId::kVarchar;
  } else if (expr.operators_size() == 7) {
    // between
    const common::ExprOpr& op0 = expr.operators(0);
    if (!op0.has_var()) {
      return false;
    }
    if (!op0.var().has_property()) {
      return false;
    }
    if (!op0.var().property().has_key()) {
      return false;
    }
    if (!(op0.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      return false;
    }
    config.property_name = op0.var().property().key().name();

    const common::ExprOpr& op1 = expr.operators(1);
    if (!(op1.item_case() == common::ExprOpr::kLogical)) {
      return false;
    }

    const common::ExprOpr& op2 = expr.operators(2);
    if (!op2.has_param()) {
      return false;
    }
    if (!op2.param().has_data_type()) {
      return false;
    }
    if (!(op2.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      return false;
    }
    config.param_names.push_back(op2.param().name());

    const common::ExprOpr& op3 = expr.operators(3);
    if (!(op3.item_case() == common::ExprOpr::kLogical)) {
      return false;
    }
    if (op3.logical() != common::Logical::AND) {
      return false;
    }

    const common::ExprOpr& op4 = expr.operators(4);
    if (!op4.has_var()) {
      return false;
    }
    if (!op4.var().has_property()) {
      return false;
    }
    if (!op4.var().property().has_key()) {
      return false;
    }
    if (!(op4.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      return false;
    }
    if (config.property_name != op4.var().property().key().name()) {
      return false;
    }

    const common::ExprOpr& op5 = expr.operators(5);
    if (!(op5.item_case() == common::ExprOpr::kLogical)) {
      return false;
    }
    const common::ExprOpr& op6 = expr.operators(6);
    if (!op6.has_param()) {
      return false;
    }
    if (!op6.param().has_data_type()) {
      return false;
    }
    if (!(op6.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      return false;
    }
    config.param_names.push_back(op6.param().name());
    if (op1.logical() == common::Logical::LT &&
        op5.logical() == common::Logical::GE) {
      std::swap(config.param_names[0], config.param_names[1]);
    } else if (op1.logical() == common::Logical::GE &&
               op5.logical() == common::Logical::LT) {
    } else {
      return false;
    }
    auto type = parse_from_ir_data_type(op2.param().data_type());
    auto type1 = parse_from_ir_data_type(op6.param().data_type());
    if (type != type1) {
      return false;
    }
    config.ptype = SPPredicateType::kPropertyBetween;
    config.param_type = type.id();
    return config.param_type == DataTypeId::kInt64 ||
           config.param_type == DataTypeId::kInt32 ||
           config.param_type == DataTypeId::kTimestampMs ||
           config.param_type == DataTypeId::kVarchar;
  }
  return false;
}

}  // namespace execution
}  // namespace neug