
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

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/utils/pb_parse_utils.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

namespace neug {

namespace runtime {

inline bool is_pk_oid_exact_check(
    const neug::Schema& schema, label_t label, const common::Expression& expr,
    std::function<Property(const std::map<std::string, std::string>&)>& value) {
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
    if (type_.id() != DataTypeId::kInt64 && type_.id() != DataTypeId::kInt32) {
      return false;
    }
    value = [name](const ParamsMap& params) {
      return Property::from_int64(
          static_cast<int64_t>(std::stoll(params.at(name))));
    };
    return true;
  } else if (expr.operators(2).has_const_()) {
    auto& c = expr.operators(2).const_();
    if (c.item_case() == common::Value::kI64) {
      value = [c](const ParamsMap&) { return Property::from_int64(c.i64()); };

    } else if (c.item_case() == common::Value::kI32) {
      value = [c](const ParamsMap&) {
        return Property::from_int64(static_cast<int64_t>(c.i32()));
      };
    } else {
      return false;
    }
    return true;
  } else {
    return false;
  }
  return false;
}

enum class SPPredicateType {
  kPropertyGT,
  kPropertyLT,
  kPropertyLE,
  kPropertyGE,
  kPropertyEQ,
  kPropertyNE,
  kPropertyBetween,
  kWithIn,
  kUnknown
};

inline SPPredicateType parse_sp_pred(const common::Expression& expr) {
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

template <typename T>
class SLEdgePropertyGetter {
 public:
  SLEdgePropertyGetter(const StorageReadInterface& graph,
                       const std::vector<LabelTriplet>& labels,
                       const std::string& property_name) {
    CHECK_EQ(labels.size(), 1);
    int prop_id = 0;
    for (auto& name : graph.schema().get_edge_property_names(
             labels[0].src_label, labels[0].dst_label, labels[0].edge_label)) {
      if (name == property_name) {
        break;
      }
      ++prop_id;
    }
    ed_accessor_ =
        graph.GetEdgeDataAccessor(labels[0].src_label, labels[0].dst_label,
                                  labels[0].edge_label, prop_id);
  }
  ~SLEdgePropertyGetter() = default;

  inline T get(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
               label_t edge_label, Direction dir, const void* data_ptr) const {
    return ed_accessor_.get_typed_data_from_ptr<T>(data_ptr);
  }

 private:
  EdgeDataAccessor ed_accessor_;
};

template <typename T>
class MLEdgePropertyGetter {
 public:
  MLEdgePropertyGetter(const IStorageInterface& gi,
                       const std::vector<LabelTriplet>& labels,
                       const std::string& property_name) {
    const auto& graph = dynamic_cast<const StorageReadInterface&>(gi);
    // property_name -> prop_id
    for (const auto& lt : labels) {
      int prop_id = 0;
      for (auto& name : graph.schema().get_edge_property_names(
               lt.src_label, lt.dst_label, lt.edge_label)) {
        if (name == property_name) {
          break;
        }
        ++prop_id;
      }
      ed_accessors_.emplace(
          lt, graph.GetEdgeDataAccessor(lt.src_label, lt.dst_label,
                                        lt.edge_label, prop_id));
    }
  }
  ~MLEdgePropertyGetter() = default;

  inline T get(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
               label_t edge_label, Direction dir, const void* data_ptr) const {
    auto label_triplet = (dir == Direction::kOut)
                             ? LabelTriplet{v_label, nbr_label, edge_label}
                             : LabelTriplet{nbr_label, v_label, edge_label};
    return ed_accessors_.at(label_triplet)
        .template get_typed_data_from_ptr<T>(data_ptr);
  }

 private:
  std::map<LabelTriplet, EdgeDataAccessor> ed_accessors_;
};

template <typename T>
class SLVertexPropertyGetter {
 public:
  SLVertexPropertyGetter(const IStorageInterface& graph, label_t label,
                         const std::string& property_name) {
    column_ =
        dynamic_cast<const StorageReadInterface&>(graph).GetVertexPropColumn<T>(
            label, property_name);
  }
  ~SLVertexPropertyGetter() = default;

  inline T get(label_t label, vid_t v) const { return column_->get_view(v); }

 private:
  std::shared_ptr<StorageReadInterface::vertex_column_t<T>> column_;
};

template <typename T>
class MLVertexPropertyGetter {
 public:
  MLVertexPropertyGetter(const IStorageInterface& gi,
                         const std::string& property_name) {
    const auto& graph = dynamic_cast<const StorageReadInterface&>(gi);
    for (label_t i = 0; i < graph.schema().vertex_label_num(); ++i) {
      columns_.emplace_back(graph.GetVertexPropColumn<T>(i, property_name));
    }
  }
  ~MLVertexPropertyGetter() = default;

  inline T get(label_t label, vid_t v) const {
    return columns_[label]->get_view(v);
  }

 private:
  std::vector<std::shared_ptr<StorageReadInterface::vertex_column_t<T>>>
      columns_;
};

template <typename T>
class GTCmp {
 public:
  using data_t = T;

  GTCmp() = default;
  explicit GTCmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return target_ < v; }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class LTCmp {
 public:
  using data_t = T;

  LTCmp() = default;
  explicit LTCmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return v < target_; }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class EQCmp {
 public:
  using data_t = T;

  EQCmp() = default;
  explicit EQCmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return target_ == v; }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class GECmp {
 public:
  using data_t = T;

  GECmp() = default;
  explicit GECmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return !(v < target_); }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class LECmp {
 public:
  using data_t = T;

  LECmp() = default;
  explicit LECmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return !(target_ < v); }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class NECmp {
 public:
  using data_t = T;

  NECmp() = default;
  explicit NECmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return !(target_ == v); }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class BetweenCmp {
 public:
  using data_t = T;

  BetweenCmp() = default;
  BetweenCmp(const T& from, const T& to) : from_(from), to_(to) {}

  bool operator()(const T& v) const { return (v < to_) && !(v < from_); }

  void reset(const std::vector<T>& targets) {
    from_ = targets[0];
    to_ = targets[1];
  }

 private:
  T from_;
  T to_;
};

template <typename T, typename GETTER_T, typename CMP_T>
class EdgePropertyCmpPredicate {
 public:
  using data_t = T;
  static constexpr bool is_dummy = false;

  EdgePropertyCmpPredicate(const GETTER_T& getter, const CMP_T& cmp)
      : getter_(getter), cmp_(cmp) {}
  ~EdgePropertyCmpPredicate() = default;

  bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
                  label_t edge_label, Direction dir,
                  const void* data_ptr) const {
    T val = getter_.get(v_label, v, nbr_label, nbr, edge_label, dir, data_ptr);
    return cmp_(val);
  }

 private:
  GETTER_T getter_;
  CMP_T cmp_;
};

template <typename T, typename GETTER_T, typename CMP_T>
class VertexPropertyCmpPredicate {
 public:
  using data_t = T;
  static constexpr bool is_dummy = false;

  VertexPropertyCmpPredicate(const GETTER_T& getter, const CMP_T& cmp)
      : getter_(getter), cmp_(cmp) {}

  bool operator()(label_t label, vid_t v) const {
    T val = getter_.get(label, v);
    return cmp_(val);
  }

 private:
  GETTER_T getter_;
  CMP_T cmp_;
};

struct SpecialEdgePredicateConfig {
  std::string property_name;
  SPPredicateType ptype;
  std::string param_name;
  DataTypeId param_type;
};

inline bool is_special_edge_predicate(const common::Expression& expr,
                                      SpecialEdgePredicateConfig& config) {
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
    config.param_name = op2.param().name();
    config.param_type = parse_from_ir_data_type(op2.param().data_type()).id();
    return true;
  }
  return false;
}

struct SpecialVertexPredicateConfig {
  std::string property_name;
  SPPredicateType ptype;
  std::vector<std::string> param_names;
  DataTypeId param_type;
};

inline bool is_special_vertex_predicate(const common::Expression& expr,
                                        SpecialVertexPredicateConfig& config) {
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
    return true;
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
    return true;
  }
  return false;
}

template <typename OP_T, typename CMP_T, typename... Args>
static neug::result<Context> dispatch_vertex_predicate_impl_cmp_type(
    const IStorageInterface& graph, const std::set<label_t>& expected_labels,
    const SpecialVertexPredicateConfig& config, const ParamsMap& params,
    const CMP_T& cmp_val, Args&&... args) {
  if (expected_labels.size() == 1) {
    // single label
    label_t label = *expected_labels.begin();
    using GETTER_T = SLVertexPropertyGetter<typename CMP_T::data_t>;
    GETTER_T getter(graph, label, config.property_name);
    using PRED_T =
        VertexPropertyCmpPredicate<typename CMP_T::data_t, GETTER_T, CMP_T>;
    auto pred = PRED_T(getter, cmp_val);
    return OP_T::template eval_with_predicate<PRED_T>(
        pred, std::forward<Args>(args)...);
  } else {
    // multi labels
    using GETTER_T = MLVertexPropertyGetter<typename CMP_T::data_t>;
    GETTER_T getter(graph, config.property_name);
    using PRED_T =
        VertexPropertyCmpPredicate<typename CMP_T::data_t, GETTER_T, CMP_T>;
    auto pred = PRED_T(getter, cmp_val);
    return OP_T::template eval_with_predicate<PRED_T>(
        pred, std::forward<Args>(args)...);
  }
}

template <typename OP_T, typename T, typename... Args>
static neug::result<Context> dispatch_vertex_predicate_impl_typed(
    const IStorageInterface& graph, const std::set<label_t>& expected_labels,
    const SpecialVertexPredicateConfig& config, const ParamsMap& params,
    Args&&... args) {
  auto get_value = [&](const std::string& param_name) -> T {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return std::string_view(params.at(param_name));
    } else {
      return ValueConverter<T>::typed_from_string(params.at(param_name));
    }
  };
  if (config.ptype == SPPredicateType::kPropertyLT) {
    using CMP_T = LTCmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyGT) {
    using CMP_T = GTCmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyEQ) {
    using CMP_T = EQCmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyLE) {
    using CMP_T = LECmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyGE) {
    using CMP_T = GECmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyNE) {
    using CMP_T = NECmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyBetween) {
    using CMP_T = BetweenCmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]),
                         get_value(config.param_names[1]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  }
  LOG(ERROR) << "Unsupported predicate type for special vertex predicate: "
             << static_cast<int>(config.ptype);
  RETURN_UNSUPPORTED_ERROR(
      "Unsupported predicate type for special vertex predicate");
}

template <typename OP_T, typename... Args>
neug::result<Context> dispatch_vertex_predicate(
    const IStorageInterface& graph, const std::set<label_t>& expected_labels,
    const SpecialVertexPredicateConfig& config, const ParamsMap& params,
    Args&&... args) {
  switch (config.param_type) {
#define TYPE_DISPATCHER(enum_val, type)                      \
  case DataTypeId::enum_val:                                 \
    return dispatch_vertex_predicate_impl_typed<OP_T, type>( \
        graph, expected_labels, config, params, std::forward<Args>(args)...);
    TYPE_DISPATCHER(kInt32, int32_t)
    TYPE_DISPATCHER(kInt64, int64_t)
    TYPE_DISPATCHER(kTimestampMs, DateTime)
    TYPE_DISPATCHER(kVarchar, std::string_view)
#undef TYPE_DISPATCHER
  default:
    break;
  }
  LOG(ERROR) << "Unsupported param type for special vertex predicate: "
             << static_cast<int>(config.param_type);
  RETURN_UNSUPPORTED_ERROR(
      "Unsupported param type for special vertex predicate");
}

}  // namespace runtime

}  // namespace neug
