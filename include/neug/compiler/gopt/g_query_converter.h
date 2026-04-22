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

#include <google/protobuf/map.h>
#include <google/protobuf/wrappers.pb.h>
#include <memory>
#include <string>
#include <vector>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/common/copier_config/file_scan_info.h"
#include "neug/compiler/common/enums/table_type.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/gopt/g_ddl_converter.h"
#include "neug/compiler/gopt/g_expr_converter.h"
#include "neug/compiler/gopt/g_type_converter.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/extend/logical_recursive_extend.h"
#include "neug/compiler/planner/operator/logical_aggregate.h"
#include "neug/compiler/planner/operator/logical_alias_map.h"
#include "neug/compiler/planner/operator/logical_cross_product.h"
#include "neug/compiler/planner/operator/logical_distinct.h"
#include "neug/compiler/planner/operator/logical_filter.h"
#include "neug/compiler/planner/operator/logical_get_v.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_intersect.h"
#include "neug/compiler/planner/operator/logical_limit.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_order_by.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/logical_projection.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/operator/logical_transaction.h"
#include "neug/compiler/planner/operator/logical_union.h"
#include "neug/compiler/planner/operator/logical_unwind.h"
#include "neug/compiler/planner/operator/persistent/logical_copy_from.h"
#include "neug/compiler/planner/operator/persistent/logical_copy_to.h"
#include "neug/compiler/planner/operator/persistent/logical_delete.h"
#include "neug/compiler/planner/operator/persistent/logical_insert.h"
#include "neug/compiler/planner/operator/persistent/logical_set.h"
#include "neug/compiler/planner/operator/scan/logical_dummy_scan.h"
#include "neug/compiler/planner/operator/scan/logical_expressions_scan.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include "neug/compiler/planner/operator/simple/logical_extension.h"
#include "neug/generated/proto/plan/algebra.pb.h"
#include "neug/generated/proto/plan/cypher_dml.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"

namespace neug {
namespace gopt {
const static common::alias_id_t INVALID_ALIAS_ID = -1;
typedef ::google::protobuf::Map<std::string, std::string> Options;
struct EdgeLabelId {
  EdgeLabelId(common::table_id_t eid, common::table_id_t sid,
              common::table_id_t did)
      : edgeId(eid), srcId(sid), dstId(did) {}
  common::table_id_t edgeId;
  common::table_id_t srcId;
  common::table_id_t dstId;
};
class GQueryConvertor {
 public:
  GQueryConvertor(std::shared_ptr<GAliasManager> aliasManager,
                  neug::catalog::Catalog* catalog);

  std::unique_ptr<::physical::PhysicalPlan> convert(
      const planner::LogicalPlan& plan, bool skipSink);
  static bool skipColumn(const std::string& columnName);

 private:
  void convertOperator(const planner::LogicalOperator& op,
                       ::physical::PhysicalPlan* plan, bool skipScan = false);
  void convertScan(const planner::LogicalScanNodeTable& scan,
                   ::physical::PhysicalPlan* plan);
  void convertExtend(const planner::LogicalExtend& extend,
                     ::physical::PhysicalPlan* plan);
  void convertRecursiveExtend(const planner::LogicalRecursiveExtend& extend,
                              ::physical::PhysicalPlan* plan);
  void convertGetV(const planner::LogicalGetV& getV,
                   ::physical::PhysicalPlan* plan);
  void convertFilter(const planner::LogicalFilter& filter,
                     ::physical::PhysicalPlan* plan);
  void convertProject(const planner::LogicalProjection& project,
                      ::physical::PhysicalPlan* plan);
  void convertAggregate(const planner::LogicalAggregate& project,
                        ::physical::PhysicalPlan* plan);
  void convertOrder(const planner::LogicalOrderBy& order,
                    ::physical::PhysicalPlan* plan);
  void convertLimit(const planner::LogicalLimit& limit,
                    ::physical::PhysicalPlan* plan);
  void convertIntersect(const planner::LogicalIntersect& intersect,
                        ::physical::PhysicalPlan* plan);
  void convertTableFunc(const planner::LogicalTableFunctionCall& tableFunc,
                        ::physical::PhysicalPlan* plan);
  void convertCopyFrom(const planner::LogicalCopyFrom& copyFrom,
                       ::physical::PhysicalPlan* plan);
  void convertBatchInsertVertex(
      const binder::BoundCopyFromInfo* info,
      const binder::expression_vector& columnExprs,
      const std::vector<common::alias_id_t>& columnIdMap,
      ::physical::PhysicalPlan* plan);
  void convertBatchInsertEdge(
      const binder::BoundCopyFromInfo* info,
      const binder::expression_vector& columnExprs,
      const std::vector<common::alias_id_t>& columnIdMap,
      ::physical::PhysicalPlan* plan);
  void convertInsert(const planner::LogicalInsert& insert,
                     ::physical::PhysicalPlan* plan);
  void convertInsertVertex(const planner::LogicalInsert& insert,
                           ::physical::PhysicalPlan* plan);
  void convertInsertEdge(const planner::LogicalInsert& insert,
                         ::physical::PhysicalPlan* plan);
  void convertSetProperty(const planner::LogicalSetProperty& set,
                          ::physical::PhysicalPlan* plan);
  void convertSetVertexProperty(const planner::LogicalSetProperty& set,
                                ::physical::PhysicalPlan* plan);
  void convertSetEdgeProperty(const planner::LogicalSetProperty& set,
                              ::physical::PhysicalPlan* plan);
  void convertDelete(const planner::LogicalDelete& deleteOp,
                     ::physical::PhysicalPlan* plan);
  void convertDeleteVertex(const planner::LogicalDelete& deleteOp,
                           ::physical::PhysicalPlan* plan);
  void convertDeleteEdge(const planner::LogicalDelete& deleteOp,
                         ::physical::PhysicalPlan* plan);
  void convertCrossProduct(const planner::LogicalCrossProduct& cross,
                           ::physical::PhysicalPlan* plan);
  void convertHashJoin(const planner::LogicalHashJoin& join,
                       ::physical::PhysicalPlan* plan);
  void convertCopyTo(const planner::LogicalCopyTo& copyTo,
                     ::physical::PhysicalPlan* plan);
  void convertDummyScan(const planner::LogicalDummyScan& dummyScan,
                        ::physical::PhysicalPlan* plan);

  void convertUnion(const planner::LogicalUnion& unionOp,
                    ::physical::PhysicalPlan* plan);

  void convertAliasMap(const planner::LogicalAliasMap& aliasMap,
                       ::physical::PhysicalPlan* plan);

  void convertExpressionScan(
      const planner::LogicalExpressionsScan& expressionScan,
      ::physical::PhysicalPlan* plan);

  void convertDistinct(const planner::LogicalDistinct& distinct,
                       ::physical::PhysicalPlan* plan);
  void convertProcedureCall(const planner::LogicalTableFunctionCall& funcCall,
                            ::physical::PhysicalPlan* plan);

  void convertUnwind(const planner::LogicalUnwind& unwind,
                     ::physical::PhysicalPlan* plan);

  // help functions
  ::physical::Join::JoinKind convertJoinKind(common::JoinType joinType);
  void extractJoinKeys(
      const std::vector<planner::join_condition_t>& joinConditions,
      std::vector<std::shared_ptr<binder::Expression>>& leftKeys,
      std::vector<std::shared_ptr<binder::Expression>>& rightKeys);
  common::TableType getTableType(const planner::LogicalInsert& insert);

  void setMetaData(::physical::PhysicalOpr* physicalOpr,
                   const planner::LogicalOperator& op,
                   const binder::expression_vector& exprs);
  std::unique_ptr<::algebra::QueryParams> convertParams(
      const std::vector<common::table_id_t>& labelIds,
      std::shared_ptr<binder::Expression> predicates);
  std::string getAliasName(const planner::LogicalOperator& op);
  std::unique_ptr<::physical::PropertyMapping> convertPropMapping(
      const binder::Expression& expr, common::alias_id_t columnId);
  std::unique_ptr<::physical::PropertyMapping> convertPropMapping(
      const std::string& columnName, common::alias_id_t columnId);
  std::unique_ptr<::physical::PropertyMapping> convertPropMapping(
      const std::string& propertyName, const binder::Expression& data,
      const planner::LogicalOperator& op);
  void convertDataSource(const planner::LogicalTableFunctionCall& fileInfo,
                         ::physical::PhysicalPlan* plan);
  std::unique_ptr<Options> convertDataSourceOptions(
      const common::FileScanInfo& fileInfo);
  std::unique_ptr<Options> convertExportOptions(
      const function::ExportFuncBindData* bindData);
  std::unique_ptr<::physical::EdgeType> convertToEdgeType(
      const EdgeLabelId& label);
  std::shared_ptr<binder::Expression> bindPKExpr(common::table_id_t labelId);
  std::unique_ptr<::physical::EdgeExpand> convertExtendBase(
      const planner::LogicalRecursiveExtend& extend, planner::ExtendOpt opt);
  std::unique_ptr<::physical::GetV> convertGetVBase(
      const planner::LogicalRecursiveExtend& extend, planner::GetVOpt getVOpt);
  std::unique_ptr<::physical::PathExpand_ExpandBase> convertPathBase(
      const planner::LogicalRecursiveExtend& extend);
  ::physical::PathExpand_PathOpt convertPathOpt(
      const planner::LogicalRecursiveExtend& extend);
  ::physical::PathExpand_ResultOpt convertResultOpt(
      neug::planner::ResultOpt resultOpt);
  std::unique_ptr<::algebra::Range> convertRange(uint64_t skip, uint64_t limit);
  std::unique_ptr<::algebra::Range> convertRange(
      std::shared_ptr<binder::Expression> skip,
      std::shared_ptr<binder::Expression> limit);
  uint64_t convertValueAsUint64(common::Value value);
  std::string getExtensionName(const planner::LogicalCopyTo& copyTo);
  void convertDataExport(const planner::LogicalCopyTo& copyTo,
                         ::physical::PhysicalPlan* plan);
  void convertExtraInfo(const planner::LogicalRecursiveExtend& extend,
                        ::physical::PathExpand* pathPB);
  void convertCheckpoint(const planner::LogicalTransaction& op,
                         ::physical::PhysicalPlan* plan);
  void convertExtension(const planner::LogicalExtension& op,
                        ::physical::PhysicalPlan* plan);

  std::unique_ptr<::physical::FileSchema> convertFileSchema(
      const function::ScanFileBindData* scanBindData);

  std::unique_ptr<::physical::FileSchema> convertFileSchema(
      const function::ExportFuncBindData* exportBindData);

  std::unique_ptr<::physical::EntrySchema> convertEntrySchema(
      const function::ScanFileBindData* scanBindData);

  std::unique_ptr<::physical::EntrySchema> convertEntrySchema(
      const function::ExportFuncBindData* exportBindData);

 private:
  std::shared_ptr<GAliasManager> aliasManager;
  std::unique_ptr<GExprConverter> exprConvertor;
  std::unique_ptr<GPhysicalTypeConverter> typeConverter;
  neug::catalog::Catalog* catalog;
  neug::gopt::GDDLConverter ddlConverter;
};

}  // namespace gopt
}  // namespace neug
