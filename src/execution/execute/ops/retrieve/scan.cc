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

#include "neug/execution/execute/ops/retrieve/scan.h"

#include <unordered_set>

#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "neug/execution/common/operators/retrieve/scan.h"
#include "neug/execution/execute/ops/retrieve/scan_utils.h"
#include "neug/execution/expression/predicates.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace execution {
class OprTimer;

namespace ops {

static std::vector<Value> deduplicate_ids(std::vector<Value> values) {
  auto hash = [](const Value& value) {
    switch (value.type().id()) {
    case DataTypeId::kInt32:
      return std::hash<int32_t>{}(value.GetValue<int32_t>());
    case DataTypeId::kInt64:
      return std::hash<int64_t>{}(value.GetValue<int64_t>());
    case DataTypeId::kUInt32:
      return std::hash<uint32_t>{}(value.GetValue<uint32_t>());
    case DataTypeId::kUInt64:
      return std::hash<uint64_t>{}(value.GetValue<uint64_t>());
    case DataTypeId::kVarchar:
      return std::hash<std::string>{}(StringValue::Get(value));
    default:
      return size_t{0};
    }
  };
  std::unordered_set<Value, decltype(hash)> seen(0, hash);
  seen.reserve(values.size());
  std::vector<Value> result;
  result.reserve(values.size());
  for (auto& value : values) {
    if (!value.IsNull() && seen.insert(value).second) {
      result.emplace_back(std::move(value));
    }
  }
  return result;
}

class FilterOidsGPredOpr : public IOperator {
 public:
  FilterOidsGPredOpr(ScanParams params,
                     const algebra::IndexPredicate_Triplet& oids,
                     std::unique_ptr<neug::execution::ExprBase>&& pred)
      : params_(params), oids_(oids), pred_(std::move(pred)) {}

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    ctx = Context();
    ctx.append_chunk(DataChunk());
    const auto& rhs_op = oids_.expression().operators(0);
    if ((rhs_op.has_const_() && rhs_op.const_().has_none()) ||
        (rhs_op.has_param() && params.at(rhs_op.param().name()).IsNull())) {
      static const std::vector<Value> no_oids;
      auto empty_chunk = Scan::filter_oids(std::move(ctx.chunk(0)), graph,
                                           params_, DummyPred(), no_oids);
      if (!empty_chunk) {
        return tl::make_unexpected(empty_chunk.error());
      }
      ctx.chunk(0) = std::move(*empty_chunk);
      return ctx;
    }
    std::vector<Value> oid_values = ScanUtils::parse_ids(oids_, params);
    if (oids_.cmp() == common::Logical::WITHIN) {
      oid_values = deduplicate_ids(std::move(oid_values));
    }

    if (pred_ == nullptr) {
      if (params_.tables.size() == 1 && oid_values.size() == 1) {
        return ctx.apply_chunks(
            [&](ContextChunk&& chunk) -> neug::result<ContextChunk> {
              return Scan::find_vertex_with_oid(std::move(chunk), graph,
                                                params_.tables[0],
                                                oid_values[0], params_.alias);
            });
      }
      return ctx.apply_chunks(
          [&](ContextChunk&& chunk) -> neug::result<ContextChunk> {
            return Scan::filter_oids(std::move(chunk), graph, params_,
                                     DummyPred(), oid_values);
          });
    } else {
      auto pred = pred_->bind(&graph, params);
      GeneralPred predicate_wrapper(std::move(pred));
      return ctx.apply_chunks(
          [&](ContextChunk&& chunk) -> neug::result<ContextChunk> {
            return Scan::filter_oids(std::move(chunk), graph, params_,
                                     predicate_wrapper, oid_values);
          });
    }
  }

  std::string get_operator_name() const override {
    return "FilterOidsGPredOpr";
  }

 private:
  ScanParams params_;
  algebra::IndexPredicate_Triplet oids_;
  std::unique_ptr<neug::execution::ExprBase> pred_;
};

class ScanWithSPredOpr : public IOperator {
 public:
  ScanWithSPredOpr(const ScanParams& scan_params,
                   const SpecialPredicateConfig& config)
      : scan_params_(scan_params), config_(config) {}

  std::string get_operator_name() const override { return "ScanWithSPredOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    ctx = Context();
    ctx.append_chunk(DataChunk());

    return ctx.apply_chunks(
        [&](ContextChunk&& chunk) -> neug::result<ContextChunk> {
          return Scan::scan_vertex_with_special_vertex_predicate(
              std::move(chunk), graph, scan_params_, config_, params);
        });
  }

 private:
  ScanParams scan_params_;
  SpecialPredicateConfig config_;
};

class ScanWithGPredOpr : public IOperator {
 public:
  ScanWithGPredOpr(const ScanParams& scan_params,
                   std::unique_ptr<neug::execution::ExprBase> pred)
      : scan_params_(scan_params), pred_(std::move(pred)) {}
  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    ctx = Context();
    ctx.append_chunk(DataChunk());
    if (pred_ == nullptr) {
      return ctx.apply_chunks(
          [&](ContextChunk&& chunk) -> neug::result<ContextChunk> {
            return Scan::scan_vertex(std::move(chunk), graph, scan_params_,
                                     DummyPred());
          });
    } else {
      auto pred = pred_->bind(&graph, params);
      GeneralPred pred_wrapper(std::move(pred));
      return ctx.apply_chunks(
          [&](ContextChunk&& chunk) -> neug::result<ContextChunk> {
            return Scan::scan_vertex(std::move(chunk), graph, scan_params_,
                                     pred_wrapper);
          });
    }
  }
  std::string get_operator_name() const override { return "ScanWithGPredOpr"; }

 private:
  ScanParams scan_params_;
  std::unique_ptr<neug::execution::ExprBase> pred_;
};

neug::result<OpBuildResultT> ScanOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta;
  int alias = -1;
  if (plan.plan(op_idx).opr().scan().has_alias()) {
    alias = plan.plan(op_idx).opr().scan().alias().value();
  }
  ret_meta.set(alias, DataType::VERTEX);
  const auto& scan_opr = plan.plan(op_idx).opr().scan();
  if (scan_opr.scan_opt() != physical::Scan::VERTEX) {
    LOG(ERROR) << "Currently only support scan vertex";
    return std::make_pair(nullptr, ret_meta);
  }
  if (!scan_opr.has_params()) {
    LOG(ERROR) << "Scan operator should have params";
    return std::make_pair(nullptr, ret_meta);
  }

  ScanParams scan_params;
  scan_params.alias = scan_opr.has_alias() ? scan_opr.alias().value() : -1;

  for (const auto& table : scan_opr.params().tables()) {
    scan_params.tables.emplace_back(table.id());
  }

  if (scan_opr.has_idx_predicate()) {
    if (!ScanUtils::check_idx_predicate(scan_opr)) {
      LOG(ERROR) << "Index predicate is not supported"
                 << scan_opr.DebugString();
      return std::make_pair(nullptr, ret_meta);
    }

    // without predicate
    std::unique_ptr<ExprBase> pred = nullptr;
    if (scan_opr.params().has_predicate()) {
      pred = parse_expression(scan_opr.params().predicate(), ctx_meta,
                              VarType::kVertex);
    }

    const algebra::IndexPredicate_Triplet& idxs =
        scan_opr.idx_predicate().or_predicates(0).predicates(0);
    return std::make_pair(std::make_unique<FilterOidsGPredOpr>(
                              scan_params, idxs, std::move(pred)),
                          ret_meta);

  } else {
    if (scan_opr.params().has_predicate()) {
      SpecialPredicateConfig config;
      if (is_special_vertex_predicate(schema, scan_params.tables,
                                      scan_opr.params().predicate(), config)) {
        return std::make_pair(
            std::make_unique<ScanWithSPredOpr>(scan_params, config), ret_meta);
      }
    }
    std::unique_ptr<ExprBase> pred = nullptr;
    if (scan_opr.params().has_predicate()) {
      pred = parse_expression(scan_opr.params().predicate(), ctx_meta,
                              VarType::kVertex);
    }
    return std::make_pair(
        std::make_unique<ScanWithGPredOpr>(scan_params, std::move(pred)),
        ret_meta);
  }
}

class DummySourceOpr : public IOperator {
 public:
  DummySourceOpr() {}

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    Context out;
    ContextChunk chunk;
    ValueColumnBuilder<int32_t> builder;
    builder.push_back_opt(0);
    chunk.set(-1, builder.finish());
    out.append_chunk(std::move(chunk));
    return out;
  }

  std::string get_operator_name() const override { return "DummySourceOpr"; }
};  // namespace ops

neug::result<OpBuildResultT> DummySourceOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_pair(std::make_unique<DummySourceOpr>(), ctx_meta);
}
}  // namespace ops
}  // namespace execution
}  // namespace neug
