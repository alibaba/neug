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

#include <glog/logging.h>
#include <google/protobuf/wrappers.pb.h>
#include <stddef.h>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <utility>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/scan.h"
#include "neug/execution/execute/ops/retrieve/scan_utils.h"
#include "neug/execution/utils/expr_impl.h"
#include "neug/execution/utils/params.h"
#include "neug/execution/utils/special_predicates.h"
#include "neug/execution/utils/var.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"

namespace gs {
namespace runtime {
class OprTimer;

namespace ops {

typedef const std::map<std::string, std::string>& ParamsType;

class FilterOidsGPredOpr : public IOperator {
 public:
  FilterOidsGPredOpr(
      ScanParams params,
      const std::function<std::vector<Property>(ParamsType)>& oids,
      const std::optional<common::Expression>& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  gs::result<gs::runtime::Context> Eval(gs::runtime::IStorageInterface& graph,
                                        ParamsType params,
                                        gs::runtime::Context&& ctx,
                                        gs::runtime::OprTimer* timer) override {
    ctx = Context();
    std::vector<Property> oids = oids_(params);

    if (!pred_.has_value()) {
      if (params_.tables.size() == 1 && oids.size() == 1) {
        return Scan::find_vertex_with_oid(
            std::move(ctx), graph, params_.tables[0], oids[0], params_.alias);
      }
      return Scan::filter_oids(
          std::move(ctx), graph, params_,
          [](label_t, vid_t, size_t) { return true; }, oids);
    } else {
      Arena arena;
      const StorageReadInterface* graph_ptr = nullptr;
      if (graph.readable()) {
        graph_ptr = dynamic_cast<const StorageReadInterface*>(&graph);
      }
      auto expr = parse_expression(graph_ptr, ctx, params, pred_.value(),
                                   VarType::kVertexVar);
      return Scan::filter_oids(
          std::move(ctx), graph, params_,
          [&expr, &arena](label_t label, vid_t vid, size_t idx) {
            return expr->eval_vertex(label, vid, idx, arena).as_bool();
          },
          oids);
    }
  }

  std::string get_operator_name() const override {
    return "FilterOidsGPredOpr";
  }

 private:
  ScanParams params_;
  std::function<std::vector<Property>(ParamsType)> oids_;
  std::optional<common::Expression> pred_;
};

class ScanWithSPredOpr : public IOperator {
 public:
  ScanWithSPredOpr(const ScanParams& scan_params,
                   const SpecialVertexPredicateConfig& config)
      : scan_params_(scan_params), config_(config) {}

  std::string get_operator_name() const override { return "ScanWithSPredOpr"; }

  gs::result<gs::runtime::Context> Eval(
      gs::runtime::IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    return Scan::scan_vertex_with_special_vertex_predicate(
        std::move(ctx), graph, scan_params_, config_, params);
  }

 private:
  ScanParams scan_params_;
  SpecialVertexPredicateConfig config_;
};

class ScanWithGPredOpr : public IOperator {
 public:
  ScanWithGPredOpr(const ScanParams& scan_params,
                   const std::optional<common::Expression>& pred)
      : scan_params_(scan_params), pred_(pred) {}

  gs::result<gs::runtime::Context> Eval(
      gs::runtime::IStorageInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    Arena arena;
    if (!pred_.has_value()) {
      if (scan_params_.limit == std::numeric_limits<int32_t>::max()) {
        return Scan::scan_vertex(std::move(ctx), graph, scan_params_,
                                 [](label_t, vid_t, size_t) { return true; });
      } else {
        return Scan::scan_vertex_with_limit(
            std::move(ctx), graph, scan_params_,
            [](label_t, vid_t, size_t) { return true; });
      }
    } else {
      StorageReadInterface* graph_ptr = nullptr;
      if (graph.readable()) {
        graph_ptr = dynamic_cast<StorageReadInterface*>(&graph);
      }
      auto expr = parse_expression(graph_ptr, ctx, params, pred_.value(),
                                   VarType::kVertexVar);
      if (scan_params_.limit == std::numeric_limits<int32_t>::max()) {
        auto ret = Scan::scan_vertex(
            std::move(ctx), graph, scan_params_,
            [&expr, &arena](label_t label, vid_t vid, size_t idx) {
              return expr->eval_vertex(label, vid, idx, arena).as_bool();
            });
        return ret;
      } else {
        auto ret = Scan::scan_vertex_with_limit(
            std::move(ctx), graph, scan_params_,
            [&expr, &arena](label_t label, vid_t vid, size_t idx) {
              return expr->eval_vertex(label, vid, idx, arena).as_bool();
            });
        return ret;
      }
    }
  }
  std::string get_operator_name() const override { return "ScanWithGPredOpr"; }

 private:
  ScanParams scan_params_;
  std::optional<common::Expression> pred_;
};

gs::result<OpBuildResultT> ScanOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta;
  int alias = -1;
  if (plan.query_plan().plan(op_idx).opr().scan().has_alias()) {
    alias = plan.query_plan().plan(op_idx).opr().scan().alias().value();
  }
  ret_meta.set(alias);
  auto scan_opr = plan.query_plan().plan(op_idx).opr().scan();
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
  scan_params.limit = std::numeric_limits<int32_t>::max();
  if (scan_opr.params().has_limit()) {
    auto& limit_range = scan_opr.params().limit();
    if (limit_range.lower() != 0) {
      LOG(FATAL) << "Scan with lower limit expect 0, but got "
                 << limit_range.lower();
    }
    if (limit_range.upper() > 0) {
      scan_params.limit = limit_range.upper();
    }
  }
  for (auto& table : scan_opr.params().tables()) {
    // bug here, exclude invalid vertex label id
    if (schema.vertex_label_num() <= table.id()) {
      continue;
    }
    scan_params.tables.emplace_back(table.id());
  }
  if (scan_opr.has_idx_predicate()) {
    if (!ScanUtils::check_idx_predicate(scan_opr)) {
      LOG(ERROR) << "Index predicate is not supported"
                 << scan_opr.DebugString();
      return std::make_pair(nullptr, ret_meta);
    }

    // without predicate
    std::optional<::common::Expression> pred = std::nullopt;
    if (scan_opr.params().has_predicate()) {
      pred = scan_opr.params().predicate();
    }

    std::function<std::vector<Property>(ParamsType)> oids;
    for (auto& table : scan_params.tables) {
      const auto& pks = schema.get_vertex_primary_key(table);
      const auto type = std::get<0>(pks[0]);
      oids = ScanUtils::parse_ids_with_type(type, scan_opr.idx_predicate());
      break;
    }
    return std::make_pair(
        std::make_unique<FilterOidsGPredOpr>(scan_params, oids, pred),
        ret_meta);

  } else {
    if (scan_opr.params().has_predicate()) {
      SpecialVertexPredicateConfig config;
      if (is_special_vertex_predicate(scan_opr.params().predicate(), config)) {
        return std::make_pair(
            std::make_unique<ScanWithSPredOpr>(scan_params, config), ret_meta);
      }
    }
    std::optional<::common::Expression> pred = std::nullopt;
    if (scan_opr.params().has_predicate()) {
      pred = scan_opr.params().predicate();
    }
    return std::make_pair(std::make_unique<ScanWithGPredOpr>(scan_params, pred),
                          ret_meta);
  }
}

class DummySourceOpr : public IOperator {
 public:
  DummySourceOpr() {}

  gs::result<gs::runtime::Context> Eval(
      gs::runtime::IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    ValueColumnBuilder<int32_t> builder;
    builder.push_back_opt(0);
    ctx.set(-1, builder.finish());
    return ctx;
  }

  std::string get_operator_name() const override { return "DummySourceOpr"; }
};

gs::result<OpBuildResultT> DummySourceOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_pair(std::make_unique<DummySourceOpr>(), ctx_meta);
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs