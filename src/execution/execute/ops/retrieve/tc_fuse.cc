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

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/edge_expand.h"
#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/execute/operator.h"
#include "neug/execution/execute/ops/retrieve/edge.h"
#include "neug/execution/expression/special_predicates.h"
#include "neug/execution/utils/pb_parse_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace execution {
class OprTimer;

namespace ops {

template <typename T1>
class TCOpr : public IOperator {
 public:
  TCOpr(const physical::EdgeExpand& ee_opr0,
        const physical::EdgeExpand& ee_opr1,
        const physical::EdgeExpand& ee_opr2, const LabelTriplet& label0,
        const LabelTriplet& label1, const LabelTriplet& label2, int alias1,
        int alias2)
      : label0_(label0), label1_(label1), label2_(label2) {
    input_tag_ = ee_opr0.has_v_tag() ? ee_opr0.v_tag().value() : -1;
    dir0_ = parse_direction(ee_opr0.direction());
    dir1_ = parse_direction(ee_opr1.direction());
    dir2_ = parse_direction(ee_opr2.direction());
    alias1_ = alias1;
    alias2_ = alias2;
    is_lt_ = ee_opr0.params().predicate().operators(1).logical() ==
             common::Logical::LT;
    auto val = ee_opr0.params().predicate().operators(2);
    param_name_ = val.param().name();
    if (dir0_ == Direction::kOut) {
      labels_[0] = std::make_tuple(label0_.src_label, label0_.dst_label,
                                   label0_.edge_label, dir0_);
    } else {
      labels_[0] = std::make_tuple(label0_.dst_label, label0_.src_label,
                                   label0_.edge_label, dir0_);
    }
    if (dir1_ == Direction::kOut) {
      labels_[1] = std::make_tuple(label1_.src_label, label1_.dst_label,
                                   label1_.edge_label, dir1_);
    } else {
      labels_[1] = std::make_tuple(label1_.dst_label, label1_.src_label,
                                   label1_.edge_label, dir1_);
    }
    if (dir2_ == Direction::kOut) {
      labels_[2] = std::make_tuple(label2_.src_label, label2_.dst_label,
                                   label2_.edge_label, dir2_);
    } else {
      labels_[2] = std::make_tuple(label2_.dst_label, label2_.src_label,
                                   label2_.edge_label, dir2_);
    }
  }

  std::string get_operator_name() const override { return "TCOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    auto& graph = dynamic_cast<const StorageReadInterface&>(graph_interface);
    return EdgeExpand::tc<T1>(graph, std::move(ctx), labels_, input_tag_,
                              alias1_, alias2_, is_lt_, params.at(param_name_));
  }

 private:
  LabelTriplet label0_, label1_, label2_;
  Direction dir0_, dir1_, dir2_;
  int input_tag_;
  int alias1_;
  int alias2_;
  bool is_lt_;
  std::array<std::tuple<label_t, label_t, label_t, Direction>, 3> labels_;
  std::string param_name_;
};

bool tc_fusable(const physical::PhysicalPlan& plan, int op_idx) {
  // ee_opr0
  const auto& ee_opr0 = plan.plan(op_idx).opr().edge();
  if (ee_opr0.is_optional() || (!ee_opr0.has_v_tag()) ||
      (!ee_opr0.has_alias())) {
    return false;
  }
  // predicate
  if (!ee_opr0.params().has_predicate()) {
    return false;
  }

  auto sp_pred = parse_sp_pred(ee_opr0.params().predicate());
  if (sp_pred != SPPredicateType::kPropertyGT &&
      sp_pred != SPPredicateType::kPropertyLT) {
    return false;
  }
  const auto& op2 = ee_opr0.params().predicate().operators(2);
  if (op2.item_case() != common::ExprOpr::ItemCase::kParam) {
    return false;
  }
  int start_tag = ee_opr0.v_tag().value();
  auto dir0 = ee_opr0.direction();
  if (dir0 == physical::EdgeExpand_Direction::EdgeExpand_Direction_BOTH) {
    return false;
  }

  // vertex_opr
  const auto& vertex = plan.plan(op_idx + 1).opr().vertex();
  int alias1 = vertex.has_alias() ? vertex.alias().value() : -1;
  // group_by_opr
  const auto& group_by_opr = plan.plan(op_idx + 3).opr().group_by();
  if (group_by_opr.mappings_size() != 1 || group_by_opr.functions_size() != 1) {
    return false;
  }
  auto mapping = group_by_opr.mappings(0);
  if ((!mapping.has_key()) || mapping.key().tag().id() != start_tag) {
    return false;
  }
  int alias3 = mapping.alias().value();
  const auto& func = group_by_opr.functions(0);
  if (func.aggregate() != physical::GroupBy_AggFunc::TO_SET) {
    return false;
  }
  if (func.vars_size() != 1 || (!func.vars(0).has_tag()) ||
      func.vars(0).tag().id() != alias1 || func.vars(0).has_property()) {
    return false;
  }
  int alias4 = func.alias().value();

  const auto& ee_opr1 = plan.plan(op_idx + 5).opr().edge();

  // ee_opr1 and v_opr1
  if (ee_opr1.is_optional() || (!ee_opr1.has_v_tag()) ||
      ee_opr1.v_tag().value() != alias3 || ee_opr1.has_alias()) {
    return false;
  }
  if (ee_opr1.direction() ==
      physical::EdgeExpand_Direction::EdgeExpand_Direction_BOTH) {
    return false;
  }
  if (ee_opr1.params().has_predicate()) {
    return false;
  }
  // tag -1
  auto v_opr1 = plan.plan(op_idx + 6).opr().vertex();
  if (v_opr1.has_tag() || (!v_opr1.has_alias())) {
    return false;
  }

  const auto& ee_opr2 = plan.plan(op_idx + 7).opr().edge();
  // ee_opr2, tag -1
  if (ee_opr2.is_optional() || (!ee_opr2.has_v_tag()) ||
      (!ee_opr2.has_alias())) {
    return false;
  }
  if (ee_opr2.direction() ==
      physical::EdgeExpand_Direction::EdgeExpand_Direction_BOTH) {
    return false;
  }
  if (ee_opr2.params().has_predicate()) {
    return false;
  }

  int alias7 = ee_opr2.alias().value();
  // select_opr
  const auto& select_opr = plan.plan(op_idx + 8).opr().select();
  if (select_opr.predicate().operators_size() != 7) {
    return false;
  }
  auto& var = select_opr.predicate().operators(1);
  auto& within = select_opr.predicate().operators(3);
  auto& v_set = select_opr.predicate().operators(5);
  if ((!var.has_var()) || (!var.var().has_tag()) || var.var().has_property()) {
    return false;
  }
  if (var.var().tag().id() != alias7) {
    return false;
  }
  if (within.item_case() != common::ExprOpr::ItemCase::kLogical ||
      within.logical() != common::Logical::WITHIN) {
    return false;
  }
  if ((!v_set.has_var()) || (!v_set.var().has_tag()) ||
      v_set.var().has_property()) {
    return false;
  }

  int v_set_tag = v_set.var().tag().id();
  if (v_set_tag != alias4) {
    return false;
  }
  return true;
}

inline bool parse_edge_type(const Schema& schema, const LabelTriplet& label,
                            DataTypeId& ep) {
  auto properties0 = schema.get_edge_properties(
      label.src_label, label.dst_label, label.edge_label);
  if (properties0.empty()) {
    ep = DataTypeId::kEmpty;
    return true;
  } else {
    if (1 == properties0.size()) {
      ep = properties0[0].id();
      return true;
    }
  }
  return false;
}

std::unique_ptr<IOperator> make_tc_opr(
    const physical::EdgeExpand& ee_opr0, const physical::EdgeExpand& ee_opr1,
    const physical::EdgeExpand& ee_opr2, const LabelTriplet& label0,
    const LabelTriplet& label1, const LabelTriplet& label2,
    const std::array<DataTypeId, 3>& eps, int alias1, int alias2) {
  if (eps[0] == DataTypeId::kTimestampMs) {
    return std::make_unique<TCOpr<DateTime>>(ee_opr0, ee_opr1, ee_opr2, label0,
                                             label1, label2, alias1, alias2);
  } else if (eps[0] == DataTypeId::kInt64) {
    return std::make_unique<TCOpr<int64_t>>(ee_opr0, ee_opr1, ee_opr2, label0,
                                            label1, label2, alias1, alias2);
  } else if (eps[0] == DataTypeId::kInt32) {
    return std::make_unique<TCOpr<int32_t>>(ee_opr0, ee_opr1, ee_opr2, label0,
                                            label1, label2, alias1, alias2);
  }
  return nullptr;
}

neug::result<OpBuildResultT> TCOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  if (tc_fusable(plan, op_idx)) {
    int alias1 = plan.plan(op_idx + 6).opr().vertex().alias().value();

    int alias2 = plan.plan(op_idx + 7).opr().edge().alias().value();

    auto labels0 = parse_label_triplets(plan.plan(op_idx).meta_data(0));
    auto labels1 = parse_label_triplets(plan.plan(op_idx + 5).meta_data(0));
    auto labels2 = parse_label_triplets(plan.plan(op_idx + 7).meta_data(0));

    if (labels0.size() != 1 || labels1.size() != 1 || labels2.size() != 1) {
      return std::make_pair(nullptr, ContextMeta());
    }
    std::array<DataTypeId, 3> eps;
    if (!parse_edge_type(schema, labels0[0], eps[0]) ||
        !parse_edge_type(schema, labels1[0], eps[1]) ||
        !parse_edge_type(schema, labels2[0], eps[2])) {
      return std::make_pair(nullptr, ContextMeta());
    }
    auto opr = make_tc_opr(plan.plan(op_idx).opr().edge(),
                           plan.plan(op_idx + 5).opr().edge(),
                           plan.plan(op_idx + 7).opr().edge(), labels0[0],
                           labels1[0], labels2[0], eps, alias1, alias2);
    if (opr == nullptr) {
      return std::make_pair(nullptr, ContextMeta());
    }
    ContextMeta meta = ctx_meta;
    meta.set(alias1, DataType::VERTEX);
    meta.set(alias2, DataType::VERTEX);

    return std::make_pair(std::move(opr), meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

}  // namespace ops
}  // namespace execution
}  // namespace neug