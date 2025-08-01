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

#include "neug/compiler/gopt/g_query_converter.h"

#include <google/protobuf/map.h>
#include <google/protobuf/wrappers.pb.h>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>
#include "common/enums/accumulate_type.h"
#include "common/enums/join_type.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/common/enums/table_type.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/export/export_function.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_ddl_converter.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/gopt/g_physical_convertor.h"
#include "neug/compiler/planner/operator/logical_filter.h"
#include "neug/compiler/planner/operator/logical_intersect.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/logical_projection.h"
#include "neug/compiler/planner/operator/persistent/logical_copy_to.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include "neug/proto_generated_gie/algebra.pb.h"
#include "neug/proto_generated_gie/common.pb.h"
#include "neug/proto_generated_gie/cypher_ddl.pb.h"
#include "neug/proto_generated_gie/cypher_dml.pb.h"
#include "neug/proto_generated_gie/expr.pb.h"
#include "neug/proto_generated_gie/physical.pb.h"
#include "planner/operator/logical_hash_join.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace gopt {

GQueryConvertor::GQueryConvertor(std::shared_ptr<GAliasManager> aliasManager,
                                 gs::catalog::Catalog* catalog)
    : aliasManager(aliasManager),
      catalog(catalog),
      exprConvertor(std::make_unique<GExprConverter>(aliasManager)),
      typeConverter(std::make_unique<GTypeConverter>()) {}

std::unique_ptr<::physical::QueryPlan> GQueryConvertor::convert(
    const planner::LogicalPlan& plan, bool skipSink) {
  auto planPB = std::make_unique<::physical::QueryPlan>();
  convertOperator(*plan.getLastOperator(), planPB.get());
  if (!skipSink) {
    auto sink = std::make_unique<::physical::Sink>();
    auto physicalOpr = std::make_unique<::physical::PhysicalOpr>();
    auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
    oprPB->set_allocated_sink(sink.release());
    physicalOpr->set_allocated_opr(oprPB.release());
    planPB->mutable_plan()->AddAllocated(physicalOpr.release());
  }
  return planPB;
}

void GQueryConvertor::convertOperator(const planner::LogicalOperator& op,
                                      ::physical::QueryPlan* plan,
                                      bool skipScan) {
  if (op.getOperatorType() == planner::LogicalOperatorType::INTERSECT) {
    auto intersect = op.constPtrCast<planner::LogicalIntersect>();
    // convert children in the nested function
    convertIntersect(*intersect, plan);
    return;
  } else if (op.getOperatorType() ==
             planner::LogicalOperatorType::CROSS_PRODUCT) {
    auto cross = op.constPtrCast<planner::LogicalCrossProduct>();
    convertCrossProduct(*cross, plan);
    return;
  } else if (op.getOperatorType() == planner::LogicalOperatorType::HASH_JOIN) {
    auto hashJoin = op.constPtrCast<planner::LogicalHashJoin>();
    convertHashJoin(*hashJoin, plan);
    return;
  }
  for (auto child : op.getChildren()) {
    convertOperator(*child, plan, skipScan);
  }
  switch (op.getOperatorType()) {
  case planner::LogicalOperatorType::SCAN_NODE_TABLE: {
    if (!skipScan) {
      auto scanNodeTable = op.constPtrCast<planner::LogicalScanNodeTable>();
      convertScan(*scanNodeTable, plan);
    }
    break;
  }
  case planner::LogicalOperatorType::EXTEND: {
    auto extend = op.constPtrCast<planner::LogicalExtend>();
    convertExtend(*extend, plan);
    break;
  }
  case planner::LogicalOperatorType::RECURSIVE_EXTEND: {
    auto recursiveExtend = op.constPtrCast<planner::LogicalRecursiveExtend>();
    convertRecursiveExtend(*recursiveExtend, plan);
    break;
  }
  case planner::LogicalOperatorType::GET_V: {
    auto getV = op.constPtrCast<planner::LogicalGetV>();
    convertGetV(*getV, plan);
    break;
  }
  case planner::LogicalOperatorType::PROJECTION: {
    auto project = op.constPtrCast<planner::LogicalProjection>();
    convertProject(*project, plan);
    break;
  }
  case planner::LogicalOperatorType::AGGREGATE: {
    auto aggregate = op.constPtrCast<planner::LogicalAggregate>();
    convertAggregate(*aggregate, plan);
    break;
  }
  case planner::LogicalOperatorType::FILTER: {
    auto filter = op.constPtrCast<planner::LogicalFilter>();
    convertFilter(*filter, plan);
    break;
  }
  case planner::LogicalOperatorType::COPY_FROM: {
    auto copyFrom = op.constPtrCast<planner::LogicalCopyFrom>();
    convertCopyFrom(*copyFrom, plan);
    break;
  }
  case planner::LogicalOperatorType::TABLE_FUNCTION_CALL: {
    auto tableFunc = op.constPtrCast<planner::LogicalTableFunctionCall>();
    convertTableFunc(*tableFunc, plan);
    break;
  }
  case planner::LogicalOperatorType::COPY_TO: {
    auto copyTo = op.constPtrCast<planner::LogicalCopyTo>();
    convertCopyTo(*copyTo, plan);
    break;
  }
  case planner::LogicalOperatorType::ORDER_BY: {
    auto order = op.constPtrCast<planner::LogicalOrderBy>();
    convertOrder(*order, plan);
    break;
  }
  case planner::LogicalOperatorType::LIMIT: {
    auto limit = op.constPtrCast<planner::LogicalLimit>();
    convertLimit(*limit, plan);
    break;
  }
  case planner::LogicalOperatorType::INSERT: {
    auto insert = op.constPtrCast<planner::LogicalInsert>();
    convertInsert(*insert, plan);
    break;
  }
  case planner::LogicalOperatorType::SET_PROPERTY: {
    auto set = op.constPtrCast<planner::LogicalSetProperty>();
    convertSetProperty(*set, plan);
    break;
  }
  case planner::LogicalOperatorType::DELETE: {
    auto deleteOp = op.constPtrCast<planner::LogicalDelete>();
    convertDelete(*deleteOp, plan);
    break;
  }
  case planner::LogicalOperatorType::PARTITIONER:
  case planner::LogicalOperatorType::INDEX_LOOK_UP:
  case planner::LogicalOperatorType::MULTIPLICITY_REDUCER:
  case planner::LogicalOperatorType::DUMMY_SCAN:
  case planner::LogicalOperatorType::FLATTEN:
  case planner::LogicalOperatorType::ACCUMULATE:
    break;
  default:
    throw common::Exception(
        "Unsupported operator type in logical plan: " +
        std::to_string(static_cast<int>(op.getOperatorType())));
  }
}

std::unique_ptr<::physical::EdgeType> GQueryConvertor::convertToEdgeType(
    const EdgeLabelId& label) {
  auto edgeTypePB = std::make_unique<::physical::EdgeType>();
  auto typeName = std::make_unique<::common::NameOrId>();
  typeName->set_id(label.edgeId);
  edgeTypePB->set_allocated_type_name(typeName.release());
  auto srcTypeName = std::make_unique<::common::NameOrId>();
  srcTypeName->set_id(label.srcId);
  edgeTypePB->set_allocated_src_type_name(srcTypeName.release());
  auto dstTypeName = std::make_unique<::common::NameOrId>();
  dstTypeName->set_id(label.dstId);
  edgeTypePB->set_allocated_dst_type_name(dstTypeName.release());
  return edgeTypePB;
}

void GQueryConvertor::convertScan(const planner::LogicalScanNodeTable& scan,
                                  ::physical::QueryPlan* plan) {
  auto scanPB = std::make_unique<::physical::Scan>();
  scanPB->set_scan_opt(::physical::Scan_ScanOpt::Scan_ScanOpt_VERTEX);
  scanPB->set_allocated_params(
      convertParams(scan.getTableIDs(), scan.getPredicates()).release());

  auto aliasId = aliasManager->getAliasId(scan.getAliasName());

  // set alias Id if valid
  if (aliasId != DEFAULT_ALIAS_ID) {
    auto aliasValue = std::make_unique<::google::protobuf::Int32Value>();
    aliasValue->set_value(aliasId);
    scanPB->set_allocated_alias(aliasValue.release());
  }

  // set primary key if exist
  auto pkOpt = scan.getPrimaryKey(catalog);
  if (pkOpt.has_value()) {
    scanPB->set_allocated_idx_predicate(
        exprConvertor->convertPrimaryKey(pkOpt->key, *pkOpt->value->key)
            .release());
  }

  auto metaData = std::make_unique<::physical::PhysicalOpr_MetaData>();
  auto nodeType = scan.getNodeType(catalog);
  // set meta data type
  metaData->set_allocated_type(
      typeConverter->convertNodeType(*nodeType).release());
  // set meta data alias, can be set a DEFAULT_ALIAS_ID if no alias
  metaData->set_alias(aliasId);

  // construct physical operator with scan
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_scan(scanPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  physicalPB->mutable_meta_data()->AddAllocated(metaData.release());

  // append scan in query plan
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

::physical::EdgeExpand::Direction convertDirection(
    common::ExtendDirection direction) {
  switch (direction) {
  case common::ExtendDirection::FWD:
    return ::physical::EdgeExpand::OUT;
  case common::ExtendDirection::BWD:
    return ::physical::EdgeExpand::IN;
  case common::ExtendDirection::BOTH:
    return ::physical::EdgeExpand::BOTH;
  default:
    throw common::Exception("Unsupported extend direction: " +
                            std::to_string(static_cast<int>(direction)));
  }
}

::physical::GetV::VOpt convertGetVOpt(planner::GetVOpt direction) {
  switch (direction) {
  case planner::GetVOpt::END:
    return ::physical::GetV::END;
  case planner::GetVOpt::START:
    return ::physical::GetV::START;
  case planner::GetVOpt::BOTH:
    return ::physical::GetV::BOTH;
  case planner::GetVOpt::OTHER:
    return ::physical::GetV::OTHER;
  case planner::GetVOpt::ITSELF:
    return ::physical::GetV::ITSELF;
  default:
    throw common::Exception("Unsupported getV direction: " +
                            std::to_string(static_cast<int>(direction)));
  }
}

::physical::EdgeExpand_ExpandOpt convertExpandOpt(planner::ExtendOpt opt) {
  switch (opt) {
  case planner::ExtendOpt::VERTEX:
    return ::physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX;
  case planner::ExtendOpt::EDGE:
    return ::physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE;
  case planner::ExtendOpt::DEGREE:
    return ::physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_DEGREE;
  default:
    throw common::Exception("Unsupported extend option: " +
                            std::to_string(static_cast<int>(opt)));
  }
}

std::unique_ptr<::physical::EdgeExpand> GQueryConvertor::convertExtendBase(
    const planner::LogicalRecursiveExtend& extend,
    planner::ExtendOpt extendOpt) {
  auto extendPB = std::make_unique<::physical::EdgeExpand>();
  // set expand options
  extendPB->set_expand_opt(convertExpandOpt(extendOpt));
  // set direction
  extendPB->set_direction(convertDirection(extend.getDirection()));

  gopt::GRelType extendType(*extend.getRel());

  auto recursiveInfo = extend.getRel()->getRecursiveInfo();
  auto relPred = recursiveInfo ? recursiveInfo->relPredicate : nullptr;
  // set params
  extendPB->set_allocated_params(
      convertParams(extendType.getLabelIds(), relPred).release());
  return extendPB;
}

planner::GetVOpt getBaseGetVOpt(common::ExtendDirection direction) {
  switch (direction) {
  case common::ExtendDirection::FWD:
    return planner::GetVOpt::END;
  case common::ExtendDirection::BWD:
    return planner::GetVOpt::START;
  case common::ExtendDirection::BOTH:
  default:
    return planner::GetVOpt::OTHER;
  }
}

std::unique_ptr<::physical::GetV> GQueryConvertor::convertGetVBase(
    const planner::LogicalRecursiveExtend& extend, planner::GetVOpt getVOpt) {
  auto getVPB = std::make_unique<::physical::GetV>();

  getVPB->set_opt(convertGetVOpt(getVOpt));

  gopt::GNodeType nbrType(*extend.getNbrNode());
  auto recursiveInfo = extend.getRel()->getRecursiveInfo();
  auto nodePred = recursiveInfo ? recursiveInfo->nodePredicate : nullptr;
  // set params
  getVPB->set_allocated_params(
      convertParams(nbrType.getLabelIds(), nodePred).release());

  return getVPB;
}

std::unique_ptr<::physical::PathExpand_ExpandBase>
GQueryConvertor::convertPathBase(
    const planner::LogicalRecursiveExtend& extend) {
  switch (extend.getFusionType()) {
  case gs::optimizer::FusionType::EXPANDE_GETV: {
    auto pathBasePB = std::make_unique<::physical::PathExpand_ExpandBase>();
    pathBasePB->set_allocated_edge_expand(
        convertExtendBase(extend, planner::EDGE).release());
    pathBasePB->set_allocated_get_v(
        convertGetVBase(extend, getBaseGetVOpt(extend.getDirection()))
            .release());
    return pathBasePB;
  }
  case gs::optimizer::FusionType::EXPANDV: {
    auto pathBasePB = std::make_unique<::physical::PathExpand_ExpandBase>();
    pathBasePB->set_allocated_edge_expand(
        convertExtendBase(extend, planner::VERTEX).release());
    // no getV base for this fusion type
    return pathBasePB;
  }
  default:
    throw common::Exception(
        "Unsupported fusion type for recursive extend: " +
        std::to_string(static_cast<int>(extend.getFusionType())));
  }
}

::physical::PathExpand_PathOpt GQueryConvertor::convertPathOpt(
    common::PathSemantic semantic) {
  switch (semantic) {
  case common::PathSemantic::WALK:
    return ::physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY;
  case common::PathSemantic::TRAIL:
    return ::physical::PathExpand_PathOpt::PathExpand_PathOpt_TRAIL;
  case common::PathSemantic::ACYCLIC:
  default:
    return ::physical::PathExpand_PathOpt::PathExpand_PathOpt_SIMPLE;
  }
}

::physical::PathExpand_ResultOpt GQueryConvertor::convertResultOpt(
    gs::planner::ResultOpt resultOpt) {
  switch (resultOpt) {
  case gs::planner::ResultOpt::ALL_V:
    return ::physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V;
  case gs::planner::ResultOpt::END_V:
    return ::physical::PathExpand_ResultOpt::PathExpand_ResultOpt_END_V;
  case gs::planner::ResultOpt::ALL_V_E:
  default:
    return ::physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V_E;
  }
}

uint64_t GQueryConvertor::convertValueAsUint64(common::Value value) {
  std::string valueStr = value.toString();
  return std::stoull(valueStr);
}

std::unique_ptr<::algebra::Range> GQueryConvertor::convertRange(
    std::shared_ptr<binder::Expression> skip,
    std::shared_ptr<binder::Expression> limit) {
  if (skip && skip->expressionType != common::ExpressionType::LITERAL ||
      limit && limit->expressionType != common::ExpressionType::LITERAL) {
    throw common::Exception("Skip and limit must be literal expressions.");
  }
  uint64_t skipValue = 0;
  uint64_t limitValue = gs::Constants::MAX_UPPER_BOUND;
  if (skip) {
    auto valueExpr = skip->ptrCast<binder::LiteralExpression>()->getValue();
    skipValue = convertValueAsUint64(valueExpr);
  }
  if (limit) {
    auto valueExpr = limit->ptrCast<binder::LiteralExpression>()->getValue();
    limitValue = convertValueAsUint64(valueExpr);
  }
  return convertRange(skipValue, limitValue);
}

std::unique_ptr<::algebra::Range> GQueryConvertor::convertRange(
    uint64_t skip, uint64_t limit) {
  auto rangePB = std::make_unique<::algebra::Range>();
  if (skip > gs::Constants::MAX_UPPER_BOUND) {
    throw common::Exception("Skip value exceeds maximum allowed value: " +
                            std::to_string(gs::Constants::MAX_UPPER_BOUND));
  }
  int32_t upper = 0;
  if (limit > gs::Constants::MAX_UPPER_BOUND - skip) {
    upper = gs::Constants::MAX_UPPER_BOUND;
  } else {
    upper = static_cast<int32_t>(skip + limit);
  }
  rangePB->set_lower(skip);
  rangePB->set_upper(upper);
  return rangePB;
}

void GQueryConvertor::convertRecursiveExtend(
    const planner::LogicalRecursiveExtend& extend,
    ::physical::QueryPlan* plan) {
  auto pathPB = std::make_unique<::physical::PathExpand>();
  // set expand base
  pathPB->set_allocated_base(convertPathBase(extend).release());

  // set start tag
  auto startAlias = aliasManager->getAliasId(extend.getStartAliasName());
  if (startAlias != DEFAULT_ALIAS_ID) {
    auto aliasPB = std::make_unique<::google::protobuf::Int32Value>();
    aliasPB->set_value(startAlias);
    pathPB->set_allocated_start_tag(aliasPB.release());
  }

  // set alias
  auto aliasId = aliasManager->getAliasId(extend.getAliasName());
  if (aliasId != DEFAULT_ALIAS_ID) {
    auto aliasValue = std::make_unique<::google::protobuf::Int32Value>();
    aliasValue->set_value(aliasId);
    pathPB->set_allocated_alias(aliasValue.release());
  }

  // set hop ranges
  auto rangePB = std::make_unique<::algebra::Range>();
  auto bindData = extend.getBindData();
  // the range in physical pb is [lower, upper), so we need to add 1 to upper
  rangePB->set_lower(bindData.lowerBound);
  rangePB->set_upper(bindData.upperBound + 1);
  pathPB->set_allocated_hop_range(rangePB.release());

  // set path opt
  pathPB->set_path_opt(convertPathOpt(bindData.semantic));
  // set result opt
  pathPB->set_result_opt(convertResultOpt(extend.getResultOpt()));

  // set edge type as the metadata
  auto metaData = std::make_unique<::physical::PhysicalOpr_MetaData>();
  gopt::GRelType relType(*extend.getRel());
  // Set meta data type
  metaData->set_allocated_type(
      typeConverter->convertRelType(relType).release());
  // Set meta data alias
  metaData->set_alias(aliasId);

  // Construct physical operator with path expand
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_path(pathPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  physicalPB->mutable_meta_data()->AddAllocated(metaData.release());
  // Append path expand in query plan
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertExtend(const planner::LogicalExtend& extend,
                                    ::physical::QueryPlan* plan) {
  auto extendPB = std::make_unique<::physical::EdgeExpand>();
  // set expand options
  extendPB->set_expand_opt(convertExpandOpt(extend.getExtendOpt()));
  // set direction
  extendPB->set_direction(convertDirection(extend.getDirection()));

  // set v tag
  auto startAlias = aliasManager->getAliasId(extend.getStartAliasName());
  if (startAlias != DEFAULT_ALIAS_ID) {
    auto aliasPB = std::make_unique<::google::protobuf::Int32Value>();
    aliasPB->set_value(startAlias);
    extendPB->set_allocated_v_tag(aliasPB.release());
  }

  // set params
  extendPB->set_allocated_params(
      convertParams(extend.getLabelIds(), extend.getPredicates()).release());

  auto aliasId = aliasManager->getAliasId(extend.getAliasName());

  // Set alias ID if valid
  if (aliasId != DEFAULT_ALIAS_ID) {
    auto aliasValue = std::make_unique<::google::protobuf::Int32Value>();
    aliasValue->set_value(aliasId);
    extendPB->set_allocated_alias(aliasValue.release());
  }

  auto metaData = std::make_unique<::physical::PhysicalOpr_MetaData>();
  auto relType = extend.getRelType();
  // Set meta data type
  metaData->set_allocated_type(
      typeConverter->convertRelType(*relType).release());
  // Set meta data alias
  metaData->set_alias(aliasId);

  // Construct physical operator with extend
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_edge(extendPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  physicalPB->mutable_meta_data()->AddAllocated(metaData.release());

  // Append extend in query plan
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertGetV(const planner::LogicalGetV& getV,
                                  ::physical::QueryPlan* plan) {
  auto getVPB = std::make_unique<::physical::GetV>();
  // set opt
  getVPB->set_opt(convertGetVOpt(getV.getGetVOpt()));

  // todo: set start v tag

  // Set params
  getVPB->set_allocated_params(
      convertParams(getV.getTableIDs(), getV.getPredicates()).release());

  auto aliasId = aliasManager->getAliasId(getV.getAliasName());

  // Set alias ID if valid
  if (aliasId != DEFAULT_ALIAS_ID) {
    auto aliasValue = std::make_unique<::google::protobuf::Int32Value>();
    aliasValue->set_value(aliasId);
    getVPB->set_allocated_alias(aliasValue.release());
  }

  // Set metadata
  auto metaData = std::make_unique<::physical::PhysicalOpr_MetaData>();
  auto nodeType = getV.getNodeType(catalog);
  metaData->set_allocated_type(
      typeConverter->convertNodeType(*nodeType).release());
  metaData->set_alias(aliasId);

  // Construct physical operator with getV
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_vertex(getVPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  physicalPB->mutable_meta_data()->AddAllocated(metaData.release());

  // Append getV in query plan
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertFilter(const planner::LogicalFilter& filter,
                                    ::physical::QueryPlan* plan) {
  // check the queryPlan is empty, if empty, throw exception
  if (plan->plan_size() == 0) {
    throw common::Exception("Query plan is empty, cannot convert filter.");
  }

  if (filter.getChildren().empty()) {
    throw common::Exception("filter operation should have at least one child");
  }

  auto child = filter.getChild(0);

  auto predicate = filter.getPredicate();
  auto predicatePB = exprConvertor->convert(*predicate, *child);
  auto filterPB = std::make_unique<::algebra::Select>();
  filterPB->set_allocated_predicate(predicatePB.release());

  // Construct physical operator with filter
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_select(filterPB.release());
  physicalPB->set_allocated_opr(oprPB.release());

  // Append filter in query plan
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::setMetaData(::physical::PhysicalOpr* physicalOpr,
                                  const planner::LogicalOperator& op,
                                  binder::expression_vector exprs,
                                  std::vector<common::alias_id_t>& aliasIds) {
  auto schema = op.getSchema()->getExpressionsInScope();
  if (schema.size() != aliasIds.size()) {
    throw common::Exception(
        "Number of schema expressions does not match the number "
        "of alias names.");
  }
  if (aliasIds.size() != exprs.size()) {
    throw common::Exception(
        "Number of expressions does not match the number of alias "
        "names.");
  }
  // Set metadata by enumerating schema and aliasNames
  for (size_t i = 0; i < schema.size(); i++) {
    auto metaPB = std::make_unique<::physical::PhysicalOpr_MetaData>();
    // Convert alias name to ID
    auto& aliasId = aliasIds[i];
    metaPB->set_alias(aliasId);
    auto& type = schema[i]->dataType;
    // Get type from schema expression
    auto typePB =
        typeConverter->convertLogicalType(schema[i]->dataType, *exprs[i]);
    metaPB->set_allocated_type(typePB.release());
    physicalOpr->mutable_meta_data()->AddAllocated(metaPB.release());
  }
}

::algebra::OrderBy_OrderingPair_Order convertOrderOpt(bool isAsc) {
  return isAsc ? ::algebra::OrderBy_OrderingPair_Order::
                     OrderBy_OrderingPair_Order_ASC
               : ::algebra::OrderBy_OrderingPair_Order::
                     OrderBy_OrderingPair_Order_DESC;
}

void GQueryConvertor::convertIntersect(
    const planner::LogicalIntersect& intersect, ::physical::QueryPlan* plan) {
  auto children = intersect.getChildren();
  if (children.empty()) {
    throw common::Exception("intersect should have at least one child");
  }
  convertOperator(*children[0], plan);
  if (children.size() < 2)
    return;
  // buid intersect opr
  auto intersectPB = std::make_unique<::physical::Intersect>();
  // set intersect key
  auto keyNodeID = intersect.getIntersectNodeID();
  if (keyNodeID->expressionType != common::ExpressionType::PROPERTY) {
    throw common::Exception("Node ID expression is not a property expression.");
  }
  auto propertyExpr = keyNodeID->ptrCast<binder::PropertyExpression>();
  auto aliasID = aliasManager->getAliasId(propertyExpr->getVariableName());
  if (aliasID == DEFAULT_ALIAS_ID) {
    throw common::Exception("invalid intersect key: " + aliasID);
  }
  intersectPB->set_key(aliasID);
  // set intersect sub plans
  for (size_t childIdx = 1; childIdx < children.size(); ++childIdx) {
    auto childPlan = std::make_unique<::physical::QueryPlan>();
    convertOperator(*children[childIdx], childPlan.get(), true);
    intersectPB->add_sub_plans()->set_allocated_query_plan(childPlan.release());
  }
  // set intersect opr as physical opr
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_intersect(intersectPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  // add physical opr to query plan
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertLimit(const planner::LogicalLimit& limit,
                                   ::physical::QueryPlan* plan) {
  auto limitPB = std::make_unique<::algebra::Limit>();
  auto rangePB = convertRange(limit.getSkipNum(), limit.getLimitNum());
  limitPB->set_allocated_range(rangePB.release());
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_limit(limitPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  // add physical opr to query plan
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertOrder(const planner::LogicalOrderBy& order,
                                   ::physical::QueryPlan* plan) {
  auto exprVec = order.getExpressionsToOrderBy();
  auto orderVec = order.getIsAscOrders();
  if (exprVec.empty()) {
    throw common::Exception("No expressions to order by in order by operator.");
  }
  if (orderVec.size() != exprVec.size()) {
    throw common::Exception(
        "Number of expressions to order by does not match "
        "the number of sort orders.");
  }
  auto orderPB = std::make_unique<::algebra::OrderBy>();
  if (order.getChildren().empty()) {
    throw common::Exception("Order by operator must have at least one child.");
  }
  auto child = order.getChild(0);
  for (size_t i = 0; i < exprVec.size(); i++) {
    auto& expr = exprVec[i];
    auto exprPB = exprConvertor->convert(*expr, *child);
    auto pairPB = std::make_unique<::algebra::OrderBy::OrderingPair>();
    *pairPB->mutable_key() =
        std::move(*exprPB->mutable_operators(0)->mutable_var());
    pairPB->set_order(convertOrderOpt(orderVec[i]));
    orderPB->mutable_pairs()->AddAllocated(pairPB.release());
  }
  if (order.isTopK()) {
    auto rangePB = convertRange(order.getSkipNum(), order.getLimitNum());
    orderPB->set_allocated_limit(rangePB.release());
  }
  // todo: set metadata
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_order_by(orderPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  // add physical opr to query plan
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertAggregate(
    const planner::LogicalAggregate& aggregate, ::physical::QueryPlan* plan) {
  std::vector<common::alias_id_t> aliasIds;
  aliasManager->extractAliasIds(aggregate, aliasIds);
  size_t exprSize =
      aggregate.getKeys().size() + aggregate.getAggregates().size();
  if (exprSize != aliasIds.size()) {
    throw common::Exception(
        "Number of expressions in aggregate does not match "
        "the number of alias names.");
  }
  auto groupPB = std::make_unique<::physical::GroupBy>();
  size_t aliasPos = 0;
  auto child = aggregate.getChild(0);
  for (auto& key : aggregate.getKeys()) {
    auto keyPB = exprConvertor->convert(*key, *child);
    if (!keyPB) {
      throw common::Exception("Failed to convert key expression: " +
                              key->toString());
    }
    auto& aliasId = aliasIds[aliasPos++];
    auto aliasPB = std::make_unique<::google::protobuf::Int32Value>();
    aliasPB->set_value(aliasId);
    auto keyAliasPB = std::make_unique<::physical::GroupBy::KeyAlias>();
    auto keyAlias = keyAliasPB->mutable_key();
    *keyAlias = std::move(*keyPB->mutable_operators(0)->mutable_var());
    keyAliasPB->set_allocated_alias(aliasPB.release());
    groupPB->mutable_mappings()->AddAllocated(keyAliasPB.release());
  }
  for (auto& value : aggregate.getAggregates()) {
    auto aggFunc = value->ptrCast<binder::AggregateFunctionExpression>();
    auto aggFuncPB = exprConvertor->convertAggFunc(*aggFunc, *child);
    if (!aggFuncPB) {
      throw common::Exception("Failed to convert aggregate function: " +
                              value->toString());
    }
    auto& aliasId = aliasIds[aliasPos++];
    auto aliasPB = std::make_unique<::google::protobuf::Int32Value>();
    aliasPB->set_value(aliasId);
    aggFuncPB->set_allocated_alias(aliasPB.release());
    groupPB->mutable_functions()->AddAllocated(aggFuncPB.release());
  }

  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_group_by(groupPB.release());
  physicalPB->set_allocated_opr(oprPB.release());

  auto schema = aggregate.getSchema()->getExpressionsInScope();
  auto aggregateExprs = binder::expression_vector();
  aggregateExprs.reserve(exprSize);
  for (auto& expr : aggregate.getKeys()) {
    aggregateExprs.emplace_back(expr);
  }
  for (auto& expr : aggregate.getAggregates()) {
    aggregateExprs.emplace_back(expr);
  }
  setMetaData(physicalPB.get(), aggregate, aggregateExprs, aliasIds);

  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertProject(const planner::LogicalProjection& project,
                                     ::physical::QueryPlan* plan) {
  auto exprs = project.getExpressionsToProject();
  // todo: hack ways to handle the case, Match (n) Return count(*) will add a
  // empty projection before the aggregate, it's better to implement a
  // ProjectRemoveRule.
  if (exprs.empty()) {
    return;
  }
  auto projectPB = std::make_unique<::physical::Project>();
  std::vector<common::alias_id_t> aliasIds;
  aliasManager->extractAliasIds(project, aliasIds);
  if (exprs.size() != aliasIds.size()) {
    throw common::Exception(
        "Number of expressions to project does not match "
        "the number of schema expressions.");
  }
  auto child = project.getChild(0);
  // set project mappings
  for (size_t i = 0; i < exprs.size(); i++) {
    auto& expr = exprs[i];
    auto exprPB = exprConvertor->convert(*expr, *child);
    if (!exprPB) {
      throw common::Exception("Failed to convert expression: " +
                              expr->toString());
    }
    auto exprAliasPB = std::make_unique<::physical::Project::ExprAlias>();
    exprAliasPB->set_allocated_expr(exprPB.release());
    // the aliasId should have existed in aliasManager
    auto& aliasId = aliasIds[i];
    auto aliasPB = std::make_unique<::google::protobuf::Int32Value>();
    aliasPB->set_value(aliasId);
    exprAliasPB->set_allocated_alias(aliasPB.release());
    projectPB->mutable_mappings()->AddAllocated(exprAliasPB.release());
  }

  // construct physical opr with project
  auto physicalOpr = std::make_unique<::physical::PhysicalOpr>();
  auto opKind = std::make_unique<::physical::PhysicalOpr_Operator>();
  opKind->set_allocated_project(projectPB.release());
  physicalOpr->set_allocated_opr(opKind.release());

  // set meta data
  auto schema = project.getSchema()->getExpressionsInScope();
  if (schema.size() != aliasIds.size()) {
    throw common::Exception(
        "Number of schema expressions does not match the number of alias "
        "names.");
  }
  setMetaData(physicalOpr.get(), project, exprs, aliasIds);

  plan->mutable_plan()->AddAllocated(physicalOpr.release());
}

std::unique_ptr<algebra::QueryParams> GQueryConvertor::convertParams(
    const std::vector<common::table_id_t>& labelIds,
    std::shared_ptr<binder::Expression> predicates) {
  auto queryParams = std::make_unique<algebra::QueryParams>();
  for (auto& label : labelIds) {
    auto tableId = std::make_unique<::common::NameOrId>();
    tableId->set_id(label);
    queryParams->mutable_tables()->AddAllocated(tableId.release());
  }
  if (predicates != nullptr) {
    auto predicatePB = exprConvertor->convert(*predicates, {});
    if (!predicatePB) {
      throw common::Exception("Failed to convert predicate: " +
                              predicates->toString());
    }
    queryParams->set_allocated_predicate(predicatePB.release());
  }
  return queryParams;
}

std::unique_ptr<::physical::DataSource> GQueryConvertor::convertDataSource(
    const common::FileScanInfo& fileInfo) {
  // set extension_name from file type info
  auto extensionName = fileInfo.fileTypeInfo.fileTypeStr;
  if (extensionName.empty()) {
    throw common::Exception("File type info is not set");
  }
  auto sourcePB = std::make_unique<::physical::DataSource>();
  sourcePB->set_extension_name(extensionName);
  sourcePB->set_file_path(fileInfo.filePaths[0]);
  *sourcePB->mutable_options() = std::move(*convertDataSourceOptions(fileInfo));
  return sourcePB;
}

std::unique_ptr<Options> GQueryConvertor::convertDataSourceOptions(
    const common::FileScanInfo& fileInfo) {
  auto options = std::make_unique<Options>();
  for (auto& option : fileInfo.options) {
    options->insert({option.first, option.second.toString()});
  }
  return options;
}

std::unique_ptr<Options> GQueryConvertor::convertExportOptions(
    const planner::LogicalCopyTo& copyTo) {
  auto bindData = copyTo.getBindData();
  auto csvBindData = bindData->ptrCast<function::ExportCSVBindData>();
  auto options = std::make_unique<Options>();
  if (csvBindData) {
    auto csvOptions = csvBindData->exportOption;
    // csvOptions is not a map, but has defined fields; insert each as a
    // key-value pair.
    options->insert({"ESCAPE", std::string(1, csvOptions.escapeChar)});
    options->insert({"DELIM", std::string(1, csvOptions.delimiter)});
    options->insert({"QUOTE", std::string(1, csvOptions.quoteChar)});
    options->insert({"HEADER", csvOptions.hasHeader ? "True" : "False"});
  }
  return options;
}

void GQueryConvertor::convertTableFunc(
    const planner::LogicalTableFunctionCall& tableFunc,
    ::physical::QueryPlan* plan) {
  auto bindData = tableFunc.getBindData();
  auto scanBindData = bindData->constPtrCast<function::ScanFileBindData>();
  if (!scanBindData) {
    throw common::Exception(
        "Table function bind data is not of type ScanFileBindData.");
  }
  auto dataSource = convertDataSource(scanBindData->fileScanInfo);
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_source(dataSource.release());
  physicalPB->set_allocated_opr(oprPB.release());
  common::alias_id_t columnId = 0;
  for (auto& column : scanBindData->columns) {
    if (skipColumn(column->toString())) {
      continue;  // skip internal columns
    }
    auto dataPB = std::make_unique<::physical::PhysicalOpr_MetaData>();
    auto typePB =
        typeConverter->convertSimpleLogicalType(column->getDataType());
    dataPB->set_allocated_type(typePB.release());
    dataPB->set_alias(columnId++);
    physicalPB->mutable_meta_data()->AddAllocated(dataPB.release());
  }
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

bool GQueryConvertor::skipColumn(const std::string& columnName) {
  return columnName.empty() || columnName == common::InternalKeyword::ID ||
         columnName == common::InternalKeyword::LABEL ||
         columnName == common::InternalKeyword::ROW_OFFSET ||
         columnName == common::InternalKeyword::SRC_OFFSET ||
         columnName == common::InternalKeyword::DST_OFFSET ||
         columnName == common::InternalKeyword::SRC ||
         columnName == common::InternalKeyword::DST;
}

void GQueryConvertor::convertCopyFrom(const planner::LogicalCopyFrom& copyFrom,
                                      ::physical::QueryPlan* plan) {
  auto info = copyFrom.getInfo();
  auto tableEntry = info->tableEntry;
  switch (tableEntry->getTableType()) {
  case common::TableType::NODE: {
    auto nodeEntry = tableEntry->ptrCast<catalog::NodeTableCatalogEntry>();
    convertBatchInsertVertex(nodeEntry, info->columnExprs, plan);
    break;
  }
  case common::TableType::REL: {
    auto relEntry = tableEntry->ptrCast<catalog::GRelTableCatalogEntry>();
    convertBatchInsertEdge(relEntry, info->columnExprs, plan);
    break;
  }
  default: {
    throw common::Exception(
        "Unsupported table type for COPY FROM: " +
        std::to_string(static_cast<int>(tableEntry->getTableType())));
  }
  }
}

void GQueryConvertor::convertBatchInsertEdge(
    catalog::GRelTableCatalogEntry* relEntry,
    const binder::expression_vector& columnExprs, ::physical::QueryPlan* plan) {
  gs::gopt::EdgeLabelId edgeLabelId(relEntry->getLabelId(),
                                    relEntry->getSrcTableID(),
                                    relEntry->getDstTableID());
  auto batchEdge = std::make_unique<::physical::BatchInsertEdge>();
  batchEdge->set_allocated_edge_type(convertToEdgeType(edgeLabelId).release());
  common::alias_id_t columnId = 0;
  auto srcColumn = bindPKExpr(relEntry->getSrcTableID());
  auto srcPropMap = convertPropMapping(*srcColumn, columnId++);
  batchEdge->mutable_property_mappings()->AddAllocated(srcPropMap.release());
  auto dstColumn = bindPKExpr(relEntry->getDstTableID());
  auto dstPropMap = convertPropMapping(*dstColumn, columnId++);
  batchEdge->mutable_property_mappings()->AddAllocated(dstPropMap.release());
  for (auto& column : columnExprs) {
    if (skipColumn(column->toString())) {
      continue;  // skip internal columns
    }
    auto propMap = convertPropMapping(*column, columnId++);
    batchEdge->mutable_property_mappings()->AddAllocated(propMap.release());
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_load_edge(batchEdge.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertInsert(const planner::LogicalInsert& insert,
                                    ::physical::QueryPlan* plan) {
  auto& infos = insert.getInfos();
  if (infos.empty()) {
    throw common::Exception("Insert info should not be empty");
  }
  common::TableType tableType = infos[0].tableType;
  for (auto& info : infos) {
    if (info.tableType != common::TableType::NODE &&
        info.tableType != common::TableType::REL) {
      throw common::Exception("Invalid tableType for Insert: " +
                              static_cast<uint8_t>(info.tableType));
    }
    if (info.tableType != tableType) {
      throw common::Exception("tableType of Insert is not consistent");
    }
  }
  if (tableType == common::TableType::NODE) {
    convertInsertVertex(insert, plan);
  } else {  // REL
    convertInsertEdge(insert, plan);
  }
}

void GQueryConvertor::convertInsertVertex(const planner::LogicalInsert& insert,
                                          ::physical::QueryPlan* plan) {
  auto& infos = insert.getInfos();
  auto insertPB = std::make_unique<::physical::InsertVertex>();
  for (auto& info : infos) {
    auto nodeExpr = info.pattern->ptrCast<binder::NodeExpression>();
    // set vertex type by NodeExpression
    GNodeType nodeType(*nodeExpr);
    auto typeIds = nodeType.getLabelIds();
    if (typeIds.size() != 1) {
      throw common::Exception(
          "insert vertex with multiple labels is not supported");
    }
    auto labelId = typeIds[0];
    auto labelPB = std::make_unique<::common::NameOrId>();
    labelPB->set_id(labelId);
    auto entryPB = std::make_unique<::physical::InsertVertex::Entry>();
    entryPB->set_allocated_vertex_type(labelPB.release());
    // set property mappings
    if (info.columnExprs.size() != info.columnDataExprs.size()) {
      throw common::Exception(
          "Number of column expressions does not match the number of column "
          "data expressions");
    }
    for (size_t i = 0; i < info.columnExprs.size(); i++) {
      auto column = info.columnExprs[i];
      std::string columnName;
      if (column->expressionType == common::ExpressionType::PROPERTY) {
        auto property = column->ptrCast<binder::PropertyExpression>();
        columnName = property->getPropertyName();
      } else {
        columnName = column->toString();
      }
      if (skipColumn(columnName)) {
        continue;
      }
      auto data = info.columnDataExprs[i];
      entryPB->mutable_property_mappings()->AddAllocated(
          convertPropMapping(columnName, *data).release());
    }
    // set alias id by NodeExpression
    auto aliasId = aliasManager->getAliasId(nodeExpr->getUniqueName());
    if (aliasId != DEFAULT_ALIAS_ID) {
      auto aliasPB = std::make_unique<::common::NameOrId>();
      aliasPB->set_id(aliasId);
      entryPB->set_allocated_alias(aliasPB.release());
    }
    // set conflict action
    entryPB->set_conflict_action(
        static_cast<::physical::ConflictAction>(info.conflictAction));
    insertPB->mutable_entries()->AddAllocated(entryPB.release());
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_create_vertex(insertPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertInsertEdge(const planner::LogicalInsert& insert,
                                        ::physical::QueryPlan* plan) {
  auto& infos = insert.getInfos();
  auto insertPB = std::make_unique<::physical::InsertEdge>();
  for (auto& info : infos) {
    auto relExpr = info.pattern->ptrCast<binder::RelExpression>();
    // // set edge type by RelExpression
    GRelType relType(*relExpr);
    auto& rels = relType.relTables;
    if (rels.size() != 1) {
      throw common::Exception(
          "insert edge bound by multiple node labels is not supported");
    }
    EdgeLabelId edgeLabel(rels[0]->getLabelId(), rels[0]->getSrcTableID(),
                          rels[0]->getDstTableID());
    auto entryPB = std::make_unique<::physical::InsertEdge::Entry>();
    entryPB->set_allocated_edge_type(convertToEdgeType(edgeLabel).release());
    // set property mappings
    if (info.columnExprs.size() != info.columnDataExprs.size()) {
      throw common::Exception(
          "Number of column expressions does not match the number of column "
          "data expressions");
    }
    for (size_t i = 0; i < info.columnExprs.size(); i++) {
      auto column = info.columnExprs[i];
      std::string columnName;
      if (column->expressionType == common::ExpressionType::PROPERTY) {
        auto property = column->ptrCast<binder::PropertyExpression>();
        columnName = property->getPropertyName();
      } else {
        columnName = column->toString();
      }
      if (skipColumn(columnName)) {
        continue;
      }
      auto data = info.columnDataExprs[i];
      entryPB->mutable_property_mappings()->AddAllocated(
          convertPropMapping(columnName, *data).release());
    }
    // set source binding
    auto srcAliasId = aliasManager->getAliasId(relExpr->getSrcNodeName());
    if (srcAliasId != DEFAULT_ALIAS_ID) {
      auto srcAliasPB = std::make_unique<::common::NameOrId>();
      srcAliasPB->set_id(srcAliasId);
      entryPB->set_allocated_source_vertex_binding(srcAliasPB.release());
    } else {
      throw common::Exception("Source vertex binding not found: " +
                              relExpr->getSrcNodeName());
    }
    // set destination binding
    auto dstAliasId = aliasManager->getAliasId(relExpr->getDstNodeName());
    if (dstAliasId != DEFAULT_ALIAS_ID) {
      auto dstAliasPB = std::make_unique<::common::NameOrId>();
      dstAliasPB->set_id(dstAliasId);
      entryPB->set_allocated_destination_vertex_binding(dstAliasPB.release());
    } else {
      throw common::Exception("Destination vertex binding not found: " +
                              relExpr->getDstNodeName());
    }
    // set alias id by RelExpression
    auto aliasId = aliasManager->getAliasId(relExpr->getUniqueName());
    if (aliasId != DEFAULT_ALIAS_ID) {
      auto aliasPB = std::make_unique<::common::NameOrId>();
      aliasPB->set_id(aliasId);
      entryPB->set_allocated_alias(aliasPB.release());
    }
    // set conflict action
    entryPB->set_conflict_action(
        static_cast<::physical::ConflictAction>(info.conflictAction));
    insertPB->mutable_entries()->AddAllocated(entryPB.release());
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_create_edge(insertPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertSetVertexProperty(
    const planner::LogicalSetProperty& set, ::physical::QueryPlan* plan) {
  auto& infos = set.getInfos();
  auto setPB = std::make_unique<::physical::SetVertexProperty>();
  for (const auto& info : infos) {
    // set vertex binding
    auto vertexExpr = exprConvertor->convert(*info.pattern, {});
    auto entryPB = std::make_unique<::physical::SetVertexProperty::Entry>();
    *entryPB->mutable_vertex_binding() =
        std::move(*vertexExpr->mutable_operators(0)->mutable_var());
    // set property mappings
    auto& column = info.column;
    std::string columnName;
    if (column->expressionType == common::ExpressionType::PROPERTY) {
      auto property = column->ptrCast<binder::PropertyExpression>();
      columnName = property->getPropertyName();
    } else {
      columnName = column->toString();
    }
    if (skipColumn(columnName)) {
      continue;
    }
    auto data = info.columnData;
    entryPB->set_allocated_property_mapping(
        convertPropMapping(columnName, *data).release());
    setPB->mutable_entries()->AddAllocated(entryPB.release());
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_set_vertex(setPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertSetEdgeProperty(
    const planner::LogicalSetProperty& set, ::physical::QueryPlan* plan) {
  auto& infos = set.getInfos();
  auto setPB = std::make_unique<::physical::SetEdgeProperty>();
  for (const auto& info : infos) {
    // set edge binding
    auto edgeExpr = exprConvertor->convert(*info.pattern, {});
    auto entryPB = std::make_unique<::physical::SetEdgeProperty::Entry>();
    *entryPB->mutable_edge_binding() =
        std::move(*edgeExpr->mutable_operators(0)->mutable_var());
    // set property mappings
    auto& column = info.column;
    std::string columnName;
    if (column->expressionType == common::ExpressionType::PROPERTY) {
      auto property = column->ptrCast<binder::PropertyExpression>();
      columnName = property->getPropertyName();
    } else {
      columnName = column->toString();
    }
    if (skipColumn(columnName)) {
      continue;
    }
    auto data = info.columnData;
    entryPB->set_allocated_property_mapping(
        convertPropMapping(columnName, *data).release());
    setPB->mutable_entries()->AddAllocated(entryPB.release());
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_set_edge(setPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertSetProperty(const planner::LogicalSetProperty& set,
                                         ::physical::QueryPlan* plan) {
  auto& infos = set.getInfos();
  if (infos.empty()) {
    throw common::Exception("SetProperty info should not be empty");
  }
  common::TableType tableType = infos[0].tableType;
  for (auto& info : infos) {
    if (info.tableType != common::TableType::NODE &&
        info.tableType != common::TableType::REL) {
      throw common::Exception("Invalid tableType for SetProperty: " +
                              static_cast<uint8_t>(info.tableType));
    }
    if (info.tableType != tableType) {
      throw common::Exception("tableType of SetProperty is not consistent");
    }
  }
  if (tableType == common::TableType::NODE) {
    convertSetVertexProperty(set, plan);
  } else {  // REL
    convertSetEdgeProperty(set, plan);
  }
}

void GQueryConvertor::convertDelete(const planner::LogicalDelete& deleteOp,
                                    ::physical::QueryPlan* plan) {
  auto& infos = deleteOp.getInfos();
  if (infos.empty()) {
    throw common::Exception("Delete info should not be empty");
  }
  common::TableType tableType = infos[0].tableType;
  for (auto& info : infos) {
    if (info.tableType != common::TableType::NODE &&
        info.tableType != common::TableType::REL) {
      throw common::Exception("Invalid tableType for Delete: " +
                              static_cast<uint8_t>(info.tableType));
    }
    if (info.tableType != tableType) {
      throw common::Exception("tableType of Delete is not consistent");
    }
  }
  if (tableType == common::TableType::NODE) {
    convertDeleteVertex(deleteOp, plan);
  } else {  // REL
    convertDeleteEdge(deleteOp, plan);
  }
}
void GQueryConvertor::convertDeleteVertex(
    const planner::LogicalDelete& deleteOp, ::physical::QueryPlan* plan) {
  auto& infos = deleteOp.getInfos();
  auto deletePB = std::make_unique<::physical::DeleteVertex>();
  for (const auto& info : infos) {
    auto vertexExpr = exprConvertor->convert(*info.pattern, {});
    auto entryPB = std::make_unique<::physical::DeleteVertex::Entry>();
    *entryPB->mutable_vertex_binding() =
        std::move(*vertexExpr->mutable_operators(0)->mutable_var());
    entryPB->set_delete_type(
        static_cast<::physical::DeleteVertex::type>(info.deleteType));
    deletePB->mutable_entries()->AddAllocated(entryPB.release());
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_delete_vertex(deletePB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

void GQueryConvertor::convertDeleteEdge(const planner::LogicalDelete& deleteOp,
                                        ::physical::QueryPlan* plan) {
  auto& infos = deleteOp.getInfos();
  auto deletePB = std::make_unique<::physical::DeleteEdge>();
  for (const auto& info : infos) {
    auto edgeExpr = exprConvertor->convert(*info.pattern, {});
    *deletePB->add_edge_binding() =
        std::move(*edgeExpr->mutable_operators(0)->mutable_var());
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_delete_edge(deletePB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

common::TableType GQueryConvertor::getTableType(
    const planner::LogicalInsert& insert) {
  auto& infos = insert.getInfos();
  if (infos.empty()) {
    return common::TableType::UNKNOWN;
  }
  auto firstType = infos[0].tableType;
  for (auto& info : infos) {
    if (info.tableType != firstType) {
      throw common::Exception("Insert table type is not consistent");
    }
  }
  return firstType;
}

void GQueryConvertor::convertCrossProduct(
    const planner::LogicalCrossProduct& cross, ::physical::QueryPlan* plan) {
  auto joinPB = std::make_unique<::physical::Join>();
  GPhysicalConvertor convertor(aliasManager, catalog);
  // convert left plan
  planner::LogicalPlan leftPlan;
  leftPlan.setLastOperator(cross.getChild(0));
  auto leftPB = convertor.convert(leftPlan, true);
  // convert right plan
  planner::LogicalPlan rightPlan;
  rightPlan.setLastOperator(cross.getChild(1));
  auto rightPB = convertor.convert(rightPlan, true);
  // set join PB
  joinPB->set_allocated_left_plan(leftPB.release());
  joinPB->set_allocated_right_plan(rightPB.release());
  if (cross.getAccumulateType() == common::AccumulateType::OPTIONAL_) {
    joinPB->set_join_kind(::physical::Join::JoinKind::Join_JoinKind_LEFT_OUTER);
  } else {
    joinPB->set_join_kind(::physical::Join::JoinKind::Join_JoinKind_TIMES);
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_join(joinPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

::physical::Join::JoinKind GQueryConvertor::convertJoinKind(
    common::JoinType joinType) {
  switch (joinType) {
  case common::JoinType::INNER:
    return ::physical::Join::JoinKind::Join_JoinKind_INNER;
  case common::JoinType::LEFT:
    return ::physical::Join::JoinKind::Join_JoinKind_LEFT_OUTER;
  default:
    throw common::Exception("Unsupported join type: " +
                            static_cast<uint8_t>(joinType));
  }
}

void GQueryConvertor::extractJoinKeys(
    const std::vector<planner::join_condition_t>& joinConditions,
    std::vector<std::shared_ptr<binder::Expression>>& leftKeys,
    std::vector<std::shared_ptr<binder::Expression>>& rightKeys) {
  for (auto& condition : joinConditions) {
    leftKeys.emplace_back(condition.first);
    rightKeys.emplace_back(condition.second);
  }
}

void GQueryConvertor::convertHashJoin(const planner::LogicalHashJoin& join,
                                      ::physical::QueryPlan* plan) {
  auto joinPB = std::make_unique<::physical::Join>();
  GPhysicalConvertor convertor(aliasManager, catalog);
  // convert left plan
  planner::LogicalPlan leftPlan;
  auto leftOp = join.getChild(0);
  leftPlan.setLastOperator(leftOp);
  auto leftPB = convertor.convert(leftPlan, true);
  // convert right plan
  planner::LogicalPlan rightPlan;
  auto rightOp = join.getChild(1);
  rightPlan.setLastOperator(rightOp);
  auto rightPB = convertor.convert(rightPlan, true);
  // set join PB
  joinPB->set_allocated_left_plan(leftPB.release());
  joinPB->set_allocated_right_plan(rightPB.release());
  // set join kind
  joinPB->set_join_kind(convertJoinKind(join.getJoinType()));
  // set join conditions
  auto conditions = join.getJoinConditions();
  if (conditions.empty()) {
    throw common::Exception(
        "Hash join should have at least one join condition");
  }
  std::vector<std::shared_ptr<binder::Expression>> leftKeys, rightKeys;
  extractJoinKeys(conditions, leftKeys, rightKeys);
  if (leftKeys.size() != rightKeys.size()) {
    throw common::Exception(
        "Number of left keys does not match the number of right keys");
  }
  for (auto leftKey : leftKeys) {
    auto leftKeyPB = exprConvertor->convert(*leftKey, *leftOp);
    joinPB->mutable_left_keys()->AddAllocated(
        leftKeyPB->mutable_operators(0)->release_var());
  }
  for (auto rightKey : rightKeys) {
    auto rightKeyPB = exprConvertor->convert(*rightKey, *rightOp);
    joinPB->mutable_right_keys()->AddAllocated(
        rightKeyPB->mutable_operators(0)->release_var());
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_join(joinPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

std::shared_ptr<binder::Expression> GQueryConvertor::bindPKExpr(
    common::table_id_t labelId) {
  auto& transaction = gs::Constants::DEFAULT_TRANSACTION;
  auto table = catalog->getTableCatalogEntry(&transaction, labelId);
  if (!table) {
    throw common::Exception("Source vertex table not found: " +
                            std::to_string(labelId));
  }
  auto nodeTable = table->constPtrCast<gs::catalog::NodeTableCatalogEntry>();
  if (!nodeTable) {
    throw common::Exception("Source vertex table is not a node table: " +
                            table->getName());
  }
  std::string pk = nodeTable->getPrimaryKeyName();
  if (pk.empty()) {
    throw common::Exception(
        "Source vertex table does not have a primary key: " +
        nodeTable->getName());
  }
  // todo: set actual type of primary key
  return std::make_shared<binder::VariableExpression>(
      std::move(gs::common::LogicalType(gs::common::LogicalTypeID::ANY)), pk,
      pk);
}

void GQueryConvertor::convertBatchInsertVertex(
    catalog::NodeTableCatalogEntry* nodeEntry,
    const binder::expression_vector& columnExprs, ::physical::QueryPlan* plan) {
  auto labelId = nodeEntry->getTableID();
  auto labelPB = std::make_unique<::common::NameOrId>();
  labelPB->set_id(labelId);
  auto batchVertex = std::make_unique<::physical::BatchInsertVertex>();
  batchVertex->set_allocated_vertex_type(labelPB.release());
  common::alias_id_t columnId = 0;
  for (auto column : columnExprs) {
    if (skipColumn(column->toString())) {
      continue;  // skip internal columns
    }
    batchVertex->mutable_property_mappings()->AddAllocated(
        convertPropMapping(*column, columnId++).release());
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_load_vertex(batchVertex.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

std::string GQueryConvertor::getExtensionName(
    const planner::LogicalCopyTo& copyTo) {
  auto exportFunc = copyTo.getExportFunc();
  if (exportFunc.name == gs::function::ExportCSVFunction::name) {
    return "csv";
  } else {
    throw common::Exception("Unsupported export function: " + exportFunc.name);
  }
}

void GQueryConvertor::convertCopyTo(const planner::LogicalCopyTo& copyTo,
                                    ::physical::QueryPlan* plan) {
  auto exportPB = std::make_unique<::physical::DataExport>();
  auto extensionName = getExtensionName(copyTo);
  exportPB->set_extension_name(extensionName);
  std::string path = copyTo.getBindData()->fileName;
  exportPB->set_file_path(path);
  *exportPB->mutable_options() = std::move(*convertExportOptions(copyTo));
  auto columnNames = copyTo.getBindData()->columnNames;
  // // set column mappings, here assume input column ID is the same as output
  // column ID of the previous operator.
  // // todo: consider about column reordering.
  // size_t inputColumnId = 0;
  if (copyTo.getChildren().empty()) {
    throw common::Exception("COPY TO operator should have at least one child");
  }
  auto child = copyTo.getChild(0);
  auto outputSchema = child->getSchema()->getExpressionsInScope();
  if (outputSchema.size() != columnNames.size()) {
    throw common::Exception(
        "Mismatch between number of output columns (" +
        std::to_string(columnNames.size()) + ") and number of input columns (" +
        std::to_string(outputSchema.size()) + ") in COPY TO operator.");
  }
  size_t pos = 0;
  for (auto& column : columnNames) {
    auto& outputExpr = outputSchema[pos++];
    auto outputAliasId = aliasManager->getAliasId(outputExpr->getUniqueName());
    if (outputAliasId == DEFAULT_ALIAS_ID) {
      throw common::Exception("Invalid alias id in output column: " +
                              outputExpr->toString());
    }
    auto mappingPB = convertPropMapping(column, outputAliasId);
    exportPB->mutable_property_mappings()->AddAllocated(mappingPB.release());
  }
  auto physicalPB = std::make_unique<::physical::PhysicalOpr>();
  auto oprPB = std::make_unique<::physical::PhysicalOpr_Operator>();
  oprPB->set_allocated_data_export(exportPB.release());
  physicalPB->set_allocated_opr(oprPB.release());
  plan->mutable_plan()->AddAllocated(physicalPB.release());
}

std::unique_ptr<::physical::PropertyMapping>
GQueryConvertor::convertPropMapping(const binder::Expression& expr,
                                    common::alias_id_t columnId) {
  auto propertyName = expr.toString();
  auto namePB = std::make_unique<::common::NameOrId>();
  namePB->set_name(propertyName);
  auto propertyPB = std::make_unique<::common::Property>();
  propertyPB->set_allocated_key(namePB.release());
  auto mappingPB = std::make_unique<::physical::PropertyMapping>();
  mappingPB->set_allocated_property(propertyPB.release());
  auto dataPB = exprConvertor->convertVar(columnId);
  mappingPB->set_allocated_data(dataPB.release());
  return mappingPB;
}

std::unique_ptr<::physical::PropertyMapping>
GQueryConvertor::convertPropMapping(const std::string& propertyName,
                                    common::alias_id_t columnId) {
  auto namePB = std::make_unique<::common::NameOrId>();
  namePB->set_name(propertyName);
  auto propertyPB = std::make_unique<::common::Property>();
  propertyPB->set_allocated_key(namePB.release());
  auto mappingPB = std::make_unique<::physical::PropertyMapping>();
  mappingPB->set_allocated_property(propertyPB.release());
  auto dataPB = exprConvertor->convertVar(columnId);
  mappingPB->set_allocated_data(dataPB.release());
  return mappingPB;
}

std::unique_ptr<::physical::PropertyMapping>
GQueryConvertor::convertPropMapping(const std::string& propertyName,
                                    const binder::Expression& data) {
  auto namePB = std::make_unique<::common::NameOrId>();
  namePB->set_name(propertyName);
  auto propertyPB = std::make_unique<::common::Property>();
  propertyPB->set_allocated_key(namePB.release());
  auto mappingPB = std::make_unique<::physical::PropertyMapping>();
  mappingPB->set_allocated_property(propertyPB.release());
  auto dataPB = exprConvertor->convert(data, {});
  mappingPB->set_allocated_data(dataPB.release());
  return mappingPB;
}

}  // namespace gopt
}  // namespace gs