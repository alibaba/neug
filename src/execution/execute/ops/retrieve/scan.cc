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
class FilterOidsWithoutPredOpr : public IReadOperator {
 public:
  FilterOidsWithoutPredOpr(
      ScanParams params,
      const std::function<std::vector<Prop>(ParamsType)>& oids)
      : params_(params), oids_(std::move(oids)) {}

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph, ParamsType params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    std::vector<Prop> oids = oids_(params);
    if (params_.tables.size() == 1 && oids.size() == 1) {
      return Scan::find_vertex_with_oid(
          std::move(ctx), graph, params_.tables[0], oids[0], params_.alias);
    }
    return Scan::filter_oids(
        std::move(ctx), graph, params_,
        [](label_t, vid_t, size_t) { return true; }, oids);
  }

  std::string get_operator_name() const override { return "FilterOidsOpr"; }

 private:
  ScanParams params_;
  std::function<std::vector<Prop>(ParamsType)> oids_;
};

class FilterMultiTypeOidsWithoutPredOpr : public IReadOperator {
 public:
  FilterMultiTypeOidsWithoutPredOpr(
      ScanParams params,
      const std::vector<std::function<std::vector<Prop>(ParamsType)>>& oids)
      : params_(params), oids_(oids) {}

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph, ParamsType params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    std::vector<Prop> oids;
    for (auto& _oid : oids_) {
      auto oid = _oid(params);
      for (auto& o : oid) {
        oids.push_back(o);
      }
    }
    return Scan::filter_oids(
        std::move(ctx), graph, params_,
        [](label_t, vid_t, size_t) { return true; }, oids);
  }

  std::string get_operator_name() const override { return "FilterOidsOpr"; }

 private:
  ScanParams params_;
  std::vector<std::function<std::vector<Prop>(ParamsType)>> oids_;
};

class FilterGidsWithoutPredOpr : public IReadOperator {
 public:
  FilterGidsWithoutPredOpr(
      ScanParams params,
      const std::function<std::vector<Prop>(ParamsType)>& oids)
      : params_(params), oids_(std::move(oids)) {}

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph, ParamsType params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    auto ids = oids_(params);
    std::vector<int64_t> gids;
    for (size_t i = 0; i < ids.size(); i++) {
      gids.push_back(ids[i].as_int64());
    }
    if (params_.tables.size() == 1 && gids.size() == 1) {
      return Scan::find_vertex_with_gid(
          std::move(ctx), graph, params_.tables[0], gids[0], params_.alias);
    }
    return Scan::filter_gids(
        std::move(ctx), graph, params_,
        [](label_t, vid_t, size_t) { return true; }, gids);
  }

  std::string get_operator_name() const override { return "FilterGidsOpr"; }

 private:
  ScanParams params_;
  std::function<std::vector<Prop>(ParamsType)> oids_;
};

class FilterOidsSPredOpr : public IReadOperator {
 public:
  FilterOidsSPredOpr(ScanParams params,
                     const std::function<std::vector<Prop>(ParamsType)>& oids,
                     const SpecialVertexPredicateConfig& config)
      : params_(params), oids_(std::move(oids)), config_(config) {}

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph, ParamsType params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    auto ids = oids_(params);
    return Scan::filter_oids_with_special_vertex_predicate(
        std::move(ctx), graph, params_, config_, ids);
  }

  std::string get_operator_name() const override {
    return "FilterOidsSPredOpr";
  }

 private:
  ScanParams params_;
  std::function<std::vector<Prop>(ParamsType)> oids_;
  SpecialVertexPredicateConfig config_;
};

class FilterOidsGPredOpr : public IReadOperator {
 public:
  FilterOidsGPredOpr(ScanParams params,
                     const std::function<std::vector<Prop>(ParamsType)>& oids,
                     const common::Expression& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph, ParamsType params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    auto ids = oids_(params);
    Arena arena;
    auto expr =
        parse_expression(graph, ctx, params, pred_, VarType::kVertexVar);
    return Scan::filter_oids(
        std::move(ctx), graph, params_,
        [&expr, &arena](label_t label, vid_t vid, size_t idx) {
          return expr->eval_vertex(label, vid, idx, arena).as_bool();
        },
        ids);
  }

  std::string get_operator_name() const override {
    return "FilterOidsGPredOpr";
  }

 private:
  ScanParams params_;
  std::function<std::vector<Prop>(ParamsType)> oids_;
  common::Expression pred_;
};

class FilterOidsMultiTypeSPredOpr : public IReadOperator {
 public:
  FilterOidsMultiTypeSPredOpr(
      ScanParams params,
      const std::vector<std::function<std::vector<Prop>(ParamsType)>>& oids,
      const SpecialVertexPredicateConfig& config)
      : params_(params), oids_(oids), config_(config) {}

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph, ParamsType params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    std::vector<Prop> all_ids;
    for (auto& _oid : oids_) {
      auto oid = _oid(params);
      for (auto& o : oid) {
        all_ids.push_back(o);
      }
    }
    return Scan::filter_oids_with_special_vertex_predicate(
        std::move(ctx), graph, params_, config_, all_ids);
  }

  std::string get_operator_name() const override {
    return "FilterOidsMultiTypeSPredOpr";
  }

 private:
  ScanParams params_;
  std::vector<std::function<std::vector<Prop>(ParamsType)>> oids_;
  SpecialVertexPredicateConfig config_;
};

class FilterOidsMultiTypeGPredOpr : public IReadOperator {
 public:
  FilterOidsMultiTypeGPredOpr(
      ScanParams params,
      const std::vector<std::function<std::vector<Prop>(ParamsType)>>& oids,
      const common::Expression& pred)
      : params_(params), oids_(oids), pred_(pred) {}

  std::string get_operator_name() const override {
    return "FilterOidsMultiTypeGPredOpr";
  }

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph, ParamsType params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    std::vector<Prop> all_ids;
    for (auto& _oid : oids_) {
      auto oid = _oid(params);
      for (auto& o : oid) {
        all_ids.push_back(o);
      }
    }
    auto expr =
        parse_expression(graph, ctx, params, pred_, VarType::kVertexVar);
    Arena arena;

    return Scan::filter_oids(
        std::move(ctx), graph, params_,
        [&expr, &arena](label_t label, vid_t vid, size_t idx) {
          return expr->eval_vertex(label, vid, idx, arena).as_bool();
        },
        all_ids);
  }

 private:
  ScanParams params_;
  std::vector<std::function<std::vector<Prop>(ParamsType)>> oids_;
  common::Expression pred_;
};

class FilterGidsSPredOpr : public IReadOperator {
 public:
  FilterGidsSPredOpr(ScanParams params,
                     const std::function<std::vector<Prop>(ParamsType)>& oids,
                     const SpecialVertexPredicateConfig& config)
      : params_(params), oids_(std::move(oids)), config_(config) {}

  std::string get_operator_name() const override {
    return "FilterGidsSPredOpr";
  }

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph, ParamsType params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    auto ids = oids_(params);
    std::vector<int64_t> gids;
    for (size_t i = 0; i < ids.size(); i++) {
      gids.push_back(ids[i].as_int64());
    }
    return Scan::filter_gids_with_special_vertex_predicate(
        std::move(ctx), graph, params_, config_, gids);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Prop>(ParamsType)> oids_;
  SpecialVertexPredicateConfig config_;
};

class FilterGidsGPredOpr : public IReadOperator {
 public:
  FilterGidsGPredOpr(ScanParams params,
                     const std::function<std::vector<Prop>(ParamsType)>& oids,
                     const common::Expression& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  std::string get_operator_name() const override {
    return "FilterGidsGPredOpr";
  }

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph, ParamsType params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    auto ids = oids_(params);
    std::vector<int64_t> gids;
    for (size_t i = 0; i < ids.size(); i++) {
      gids.push_back(ids[i].as_int64());
    }

    Arena arena;

    auto expr =
        parse_expression(graph, ctx, params, pred_, VarType::kVertexVar);
    return Scan::filter_gids(
        std::move(ctx), graph, params_,
        [&expr, &arena](label_t label, vid_t vid, size_t idx) {
          return expr->eval_vertex(label, vid, idx, arena).as_bool();
        },
        gids);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Prop>(ParamsType)> oids_;
  common::Expression pred_;
};

class ScanWithSPredOpr : public IReadOperator {
 public:
  ScanWithSPredOpr(const ScanParams& scan_params,
                   const SpecialVertexPredicateConfig& config)
      : scan_params_(scan_params), config_(config) {}

  std::string get_operator_name() const override { return "ScanWithSPredOpr"; }

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    return Scan::scan_vertex_with_special_vertex_predicate(
        std::move(ctx), graph, scan_params_, config_, params);
  }

 private:
  ScanParams scan_params_;
  SpecialVertexPredicateConfig config_;
};

class ScanWithGPredOpr : public IReadOperator {
 public:
  ScanWithGPredOpr(const ScanParams& scan_params,
                   const common::Expression& pred)
      : scan_params_(scan_params), pred_(pred) {}

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    Arena arena;
    auto expr =
        parse_expression(graph, ctx, params, pred_, VarType::kVertexVar);
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
  std::string get_operator_name() const override { return "ScanWithGPredOpr"; }

 private:
  ScanParams scan_params_;
  common::Expression pred_;
};

class ScanWithoutPredOpr : public IReadOperator {
 public:
  explicit ScanWithoutPredOpr(const ScanParams& scan_params)
      : scan_params_(scan_params) {}

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    if (scan_params_.limit == std::numeric_limits<int32_t>::max()) {
      return Scan::scan_vertex(std::move(ctx), graph, scan_params_,
                               [](label_t, vid_t, size_t) { return true; });
    } else {
      return Scan::scan_vertex_with_limit(
          std::move(ctx), graph, scan_params_,
          [](label_t, vid_t, size_t) { return true; });
    }
  }

  std::string get_operator_name() const override {
    return "ScanWithoutPredOpr";
  }

 private:
  ScanParams scan_params_;
};

gs::result<ReadOpBuildResultT> ScanOprBuilder::Build(
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
    bool scan_oid = false;
    if (!ScanUtils::check_idx_predicate(scan_opr, scan_oid)) {
      LOG(ERROR) << "Index predicate is not supported"
                 << scan_opr.DebugString();
      return std::make_pair(nullptr, ret_meta);
    }
    // only one label and without predicate
    if (scan_params.tables.size() == 1 && scan_oid &&
        (!scan_opr.params().has_predicate())) {
      const auto& pks = schema.get_vertex_primary_key(scan_params.tables[0]);
      const auto& [type, _, __] = pks[0];
      auto oids =
          ScanUtils::parse_ids_with_type(type, scan_opr.idx_predicate());
      return std::make_pair(
          std::make_unique<FilterOidsWithoutPredOpr>(scan_params, oids),
          ret_meta);
    }

    // without predicate
    if (!scan_opr.params().has_predicate()) {
      if (!scan_oid) {
        auto gids = ScanUtils::parse_ids_with_type(PropertyType::kInt64,
                                                   scan_opr.idx_predicate());
        return std::make_pair(
            std::make_unique<FilterGidsWithoutPredOpr>(scan_params, gids),
            ret_meta);
      } else {
        std::vector<std::function<std::vector<Prop>(ParamsType)>> oids;
        std::set<int> types;
        for (auto& table : scan_params.tables) {
          const auto& pks = schema.get_vertex_primary_key(table);
          const auto& [type, _, __] = pks[0];
          int type_impl = static_cast<int>(type.type_enum);
          if (types.find(type_impl) == types.end()) {
            types.insert(type_impl);
            const auto& oid =
                ScanUtils::parse_ids_with_type(type, scan_opr.idx_predicate());
            oids.emplace_back(oid);
          }
        }
        if (types.size() == 1) {
          return std::make_pair(
              std::make_unique<FilterOidsWithoutPredOpr>(scan_params, oids[0]),
              ret_meta);
        } else {
          return std::make_pair(
              std::make_unique<FilterMultiTypeOidsWithoutPredOpr>(scan_params,
                                                                  oids),
              ret_meta);
        }
      }
    } else {
      SpecialVertexPredicateConfig sp_config;
      bool is_sp_pred =
          is_special_vertex_predicate(scan_opr.params().predicate(), sp_config);
      if (scan_oid) {
        std::set<int> types;
        std::vector<std::function<std::vector<Prop>(ParamsType)>> oids;
        for (auto& table : scan_params.tables) {
          const auto& pks = schema.get_vertex_primary_key(table);
          const auto& [type, _, __] = pks[0];
          auto type_impl = static_cast<int>(type.type_enum);
          if (types.find(type_impl) == types.end()) {
            auto oid =
                ScanUtils::parse_ids_with_type(type, scan_opr.idx_predicate());
            types.insert(type_impl);
            oids.emplace_back(oid);
          }
        }
        if (types.size() == 1) {
          if (is_sp_pred) {
            return std::make_pair(std::make_unique<FilterOidsSPredOpr>(
                                      scan_params, oids[0], sp_config),
                                  ret_meta);
          } else {
            return std::make_pair(
                std::make_unique<FilterOidsGPredOpr>(
                    scan_params, oids[0], scan_opr.params().predicate()),
                ret_meta);
          }
        } else {
          if (is_sp_pred) {
            return std::make_pair(std::make_unique<FilterOidsMultiTypeSPredOpr>(
                                      scan_params, oids, sp_config),
                                  ret_meta);
          } else {
            return std::make_pair(
                std::make_unique<FilterOidsMultiTypeGPredOpr>(
                    scan_params, oids, scan_opr.params().predicate()),
                ret_meta);
          }
        }

      } else {
        auto gids = ScanUtils::parse_ids_with_type(PropertyType::kInt64,
                                                   scan_opr.idx_predicate());
        if (is_sp_pred) {
          return std::make_pair(std::make_unique<FilterGidsSPredOpr>(
                                    scan_params, gids, sp_config),
                                ret_meta);
        } else {
          return std::make_pair(
              std::make_unique<FilterGidsGPredOpr>(
                  scan_params, gids, scan_opr.params().predicate()),
              ret_meta);
        }
      }
    }

  } else {
    if (scan_opr.params().has_predicate()) {
      SpecialVertexPredicateConfig config;
      if (is_special_vertex_predicate(scan_opr.params().predicate(), config)) {
        return std::make_pair(
            std::make_unique<ScanWithSPredOpr>(scan_params, config), ret_meta);
      } else {
        return std::make_pair(std::make_unique<ScanWithGPredOpr>(
                                  scan_params, scan_opr.params().predicate()),
                              ret_meta);
      }
    } else {
      return std::make_pair(std::make_unique<ScanWithoutPredOpr>(scan_params),
                            ret_meta);
    }
  }
}

class DummySourceOpr : public IReadOperator {
 public:
  DummySourceOpr() {}

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    ctx = Context();
    ValueColumnBuilder<int32_t> builder;
    builder.push_back_opt(0);
    ctx.set(0, builder.finish());
    return ctx;
  }

  std::string get_operator_name() const override { return "DummySourceOpr"; }
};

gs::result<ReadOpBuildResultT> DummySourceOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_pair(std::make_unique<DummySourceOpr>(), ctx_meta);
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
