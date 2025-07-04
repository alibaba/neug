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

#include <google/protobuf/wrappers.pb.h>
#include <memory>
#include <vector>
#include "src/include/binder/expression/expression.h"
#include "src/include/catalog/catalog.h"
#include "src/include/catalog/catalog_entry/node_table_catalog_entry.h"
#include "src/include/common/copier_config/file_scan_info.h"
#include "src/include/common/enums/table_type.h"
#include "src/include/common/types/types.h"
#include "src/include/gopt/g_alias_manager.h"
#include "src/include/gopt/g_catalog.h"
#include "src/include/gopt/g_expr_converter.h"
#include "src/include/gopt/g_type_converter.h"
#include "src/include/planner/operator/extend/logical_extend.h"
#include "src/include/planner/operator/extend/logical_recursive_extend.h"
#include "src/include/planner/operator/logical_aggregate.h"
#include "src/include/planner/operator/logical_cross_product.h"
#include "src/include/planner/operator/logical_filter.h"
#include "src/include/planner/operator/logical_get_v.h"
#include "src/include/planner/operator/logical_intersect.h"
#include "src/include/planner/operator/logical_limit.h"
#include "src/include/planner/operator/logical_operator.h"
#include "src/include/planner/operator/logical_order_by.h"
#include "src/include/planner/operator/logical_plan.h"
#include "src/include/planner/operator/logical_projection.h"
#include "src/include/planner/operator/logical_table_function_call.h"
#include "src/include/planner/operator/persistent/logical_copy_from.h"
#include "src/include/planner/operator/persistent/logical_delete.h"
#include "src/include/planner/operator/persistent/logical_insert.h"
#include "src/include/planner/operator/persistent/logical_set.h"
#include "src/include/planner/operator/scan/logical_scan_node_table.h"
#include "src/proto_generated_gie/algebra.pb.h"
#include "src/proto_generated_gie/cypher_dml.pb.h"
#include "src/proto_generated_gie/physical.pb.h"

namespace gs {
namespace gopt {
const static common::alias_id_t INVALID_ALIAS_ID = -1;

struct EdgeLabelId {
  common::table_id_t edgeId;
  common::table_id_t srcId;
  common::table_id_t dstId;
};
class GQueryConvertor {
 public:
  GQueryConvertor(std::shared_ptr<GAliasManager> aliasManager,
                  gs::catalog::Catalog* catalog);

  std::unique_ptr<::physical::QueryPlan> convert(
      const planner::LogicalPlan& plan);
  static bool skipColumn(const std::string& columnName);

 private:
  void convertOperator(const planner::LogicalOperator& op,
                       ::physical::QueryPlan* plan, bool skipScan = false);
  void convertScan(const planner::LogicalScanNodeTable& scan,
                   ::physical::QueryPlan* plan);
  void convertExtend(const planner::LogicalExtend& extend,
                     ::physical::QueryPlan* plan);
  void convertRecursiveExtend(const planner::LogicalRecursiveExtend& extend,
                              ::physical::QueryPlan* plan);
  void convertGetV(const planner::LogicalGetV& getV,
                   ::physical::QueryPlan* plan);
  void convertFilter(const planner::LogicalFilter& filter,
                     ::physical::QueryPlan* plan);
  void convertProject(const planner::LogicalProjection& project,
                      ::physical::QueryPlan* plan);
  void convertAggregate(const planner::LogicalAggregate& project,
                        ::physical::QueryPlan* plan);
  void convertOrder(const planner::LogicalOrderBy& order,
                    ::physical::QueryPlan* plan);
  void convertLimit(const planner::LogicalLimit& limit,
                    ::physical::QueryPlan* plan);
  void convertIntersect(const planner::LogicalIntersect& intersect,
                        ::physical::QueryPlan* plan);
  void convertTableFunc(const planner::LogicalTableFunctionCall& tableFunc,
                        ::physical::QueryPlan* plan);
  void convertCopyFrom(const planner::LogicalCopyFrom& copyFrom,
                       ::physical::QueryPlan* plan);
  void convertBatchInsertVertex(catalog::NodeTableCatalogEntry* nodeEntry,
                                const binder::expression_vector& columnExprs,
                                ::physical::QueryPlan* plan);
  void convertBatchInsertEdge(catalog::GRelTableCatalogEntry* relEntry,
                              const binder::expression_vector& columnExprs,
                              ::physical::QueryPlan* plan);
  void convertInsert(const planner::LogicalInsert& insert,
                     ::physical::QueryPlan* plan);
  void convertInsertVertex(const planner::LogicalInsert& insert,
                           ::physical::QueryPlan* plan);
  void convertInsertEdge(const planner::LogicalInsert& insert,
                         ::physical::QueryPlan* plan);
  void convertSetProperty(const planner::LogicalSetProperty& set,
                          ::physical::QueryPlan* plan);
  void convertSetVertexProperty(const planner::LogicalSetProperty& set,
                                ::physical::QueryPlan* plan);
  void convertSetEdgeProperty(const planner::LogicalSetProperty& set,
                              ::physical::QueryPlan* plan);
  void convertDelete(const planner::LogicalDelete& deleteOp,
                     ::physical::QueryPlan* plan);
  void convertDeleteVertex(const planner::LogicalDelete& deleteOp,
                           ::physical::QueryPlan* plan);
  void convertDeleteEdge(const planner::LogicalDelete& deleteOp,
                         ::physical::QueryPlan* plan);
  void convertCrossProduct(const planner::LogicalCrossProduct& cross,
                           ::physical::QueryPlan* plan);

  // help functions
  common::TableType getTableType(const planner::LogicalInsert& insert);

  void setMetaData(::physical::PhysicalOpr* physicalOpr,
                   const planner::LogicalOperator& op,
                   std::vector<std::shared_ptr<binder::Expression>> exprs,
                   std::vector<common::alias_id_t>& aliasIds);
  std::unique_ptr<::algebra::QueryParams> convertParams(
      const std::vector<common::table_id_t>& labelIds,
      std::shared_ptr<binder::Expression> predicates);
  std::string getAliasName(const planner::LogicalOperator& op);
  std::unique_ptr<::physical::PropertyMapping> convertPropMapping(
      const binder::Expression& expr, common::alias_id_t columnId);
  std::unique_ptr<::physical::PropertyMapping> convertPropMapping(
      const std::string& propertyName, const binder::Expression& data);
  std::unique_ptr<::physical::DataSource> convertDataSource(
      const common::FileScanInfo& fileInfo);
  std::unique_ptr<::physical::ReadCSV::options> convertCSVOptions(
      const common::FileScanInfo& fileInfo);
  std::unique_ptr<::physical::EdgeType> convertToEdgeType(
      const EdgeLabelId& label);
  std::shared_ptr<binder::Expression> bindPKExpr(common::table_id_t labelId);
  std::unique_ptr<::physical::EdgeExpand> convertExtendBase(
      const planner::LogicalRecursiveExtend& extend, planner::ExtendOpt opt);
  std::unique_ptr<::physical::GetV> convertGetVBase(
      const planner::LogicalRecursiveExtend& extend, planner::GetVOpt getVOpt);
  std::unique_ptr<::physical::PathExpand_ExpandBase> convertPathBase(
      const planner::LogicalRecursiveExtend& extend);
  ::physical::PathExpand_PathOpt convertPathOpt(common::PathSemantic semantic);
  ::physical::PathExpand_ResultOpt convertResultOpt(
      gs::planner::ResultOpt resultOpt);
  std::unique_ptr<::algebra::Range> convertRange(uint64_t skip, uint64_t limit);
  std::unique_ptr<::algebra::Range> convertRange(
      std::shared_ptr<binder::Expression> skip,
      std::shared_ptr<binder::Expression> limit);
  uint64_t convertValueAsUint64(common::Value value);

 private:
  std::shared_ptr<GAliasManager> aliasManager;
  std::unique_ptr<GExprConverter> exprConvertor;
  std::unique_ptr<GTypeConverter> typeConverter;
  gs::catalog::Catalog* catalog;
};

}  // namespace gopt
}  // namespace gs
