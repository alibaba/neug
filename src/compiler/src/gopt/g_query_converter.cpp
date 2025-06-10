#include "gopt/g_query_converter.h"

#include <google/protobuf/wrappers.pb.h>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>
#include "binder/expression/expression.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/constants.h"
#include "common/exception/exception.h"
#include "common/types/types.h"
#include "gopt/g_alias_manager.h"
#include "gopt/g_ddl_converter.h"
#include "planner/operator/logical_filter.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/logical_plan.h"
#include "planner/operator/logical_projection.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "src/proto_generated_gie/algebra.pb.h"
#include "src/proto_generated_gie/common.pb.h"
#include "src/proto_generated_gie/cypher_dml.pb.h"
#include "src/proto_generated_gie/expr.pb.h"
#include "src/proto_generated_gie/physical.pb.h"

namespace gs {
namespace gopt {

GQueryConvertor::GQueryConvertor(std::shared_ptr<GAliasManager> aliasManager,
                                 gs::catalog::Catalog* catalog)
    : aliasManager(aliasManager),
      catalog(catalog),
      exprConvertor(std::make_unique<GExprConverter>(aliasManager)),
      typeConverter(std::make_unique<GTypeConverter>()) {}

std::unique_ptr<::physical::QueryPlan> GQueryConvertor::convert(
    const planner::LogicalPlan& plan) {
  auto planPB = std::make_unique<::physical::QueryPlan>();
  convertOperator(*plan.getLastOperator(), planPB.get());
  if (plan.getLastOperator()->getOperatorType() ==
      planner::LogicalOperatorType::PROJECTION) {
    // add tail sink
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
                                      ::physical::QueryPlan* plan) {
  for (auto child : op.getChildren()) {
    convertOperator(*child, plan);
  }
  switch (op.getOperatorType()) {
  case planner::LogicalOperatorType::SCAN_NODE_TABLE: {
    auto scanNodeTable = op.constPtrCast<planner::LogicalScanNodeTable>();
    convertScan(*scanNodeTable, plan);
    break;
  }
  case planner::LogicalOperatorType::EXTEND: {
    auto extend = op.constPtrCast<planner::LogicalExtend>();
    convertExtend(*extend, plan);
    break;
  }
  case planner::LogicalOperatorType::GET_V: {
    auto getV = op.constPtrCast<gopt::LogicalGetV>();
    convertGetV(*getV, plan);
    break;
  }
  case planner::LogicalOperatorType::PROJECTION: {
    auto project = op.constPtrCast<planner::LogicalProjection>();
    convertProject(*project, plan);
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
  case planner::LogicalOperatorType::PARTITIONER:
  case planner::LogicalOperatorType::INDEX_LOOK_UP:
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
  scanPB->set_allocated_params(convertParams(scan.getTableIDs()).release());

  auto aliasId = INVALID_ALIAS_ID;
  auto aliasName = scan.getAliasName();
  if (!aliasName.empty()) {
    aliasId = aliasManager->getAliasId(aliasName);
  }

  // set alias Id if valid
  if (aliasId != INVALID_ALIAS_ID) {
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
  // set meta data alias, can be set a INVALID_ALIAS_ID if no alias
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

::physical::GetV::VOpt convertGetVOpt(common::ExtendDirection direction) {
  switch (direction) {
  case common::ExtendDirection::FWD:
    return ::physical::GetV::END;
  case common::ExtendDirection::BWD:
    return ::physical::GetV::START;
  case common::ExtendDirection::BOTH:
    return ::physical::GetV::BOTH;
  default:
    throw common::Exception("Unsupported getV direction: " +
                            std::to_string(static_cast<int>(direction)));
  }
}

void GQueryConvertor::convertExtend(const planner::LogicalExtend& extend,
                                    ::physical::QueryPlan* plan) {
  auto extendPB = std::make_unique<::physical::EdgeExpand>();
  // set expand options
  extendPB->set_expand_opt(
      ::physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE);
  // set direction
  extendPB->set_direction(convertDirection(extend.getDirection()));

  // set v tag
  auto startAlias = extend.getStartAliasName();
  if (!startAlias.empty()) {
    auto aliasId = aliasManager->getAliasId(startAlias);
    auto aliasPB = std::make_unique<::google::protobuf::Int32Value>();
    aliasPB->set_value(aliasId);
    extendPB->set_allocated_v_tag(aliasPB.release());
  }

  // set params
  extendPB->set_allocated_params(convertParams(extend.getLabelIds()).release());

  auto aliasId = INVALID_ALIAS_ID;
  auto aliasName = extend.getAliasName();
  if (!aliasName.empty()) {
    aliasId = aliasManager->getAliasId(aliasName);
  }

  // Set alias ID if valid
  if (aliasId != INVALID_ALIAS_ID) {
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

void GQueryConvertor::convertGetV(const gopt::LogicalGetV& getV,
                                  ::physical::QueryPlan* plan) {
  auto getVPB = std::make_unique<::physical::GetV>();
  // set opt
  getVPB->set_opt(convertGetVOpt(getV.getDirection()));

  // todo: set start v tag

  // Set params
  getVPB->set_allocated_params(convertParams(getV.getTableIDs()).release());

  auto aliasId = INVALID_ALIAS_ID;
  auto aliasName = getV.getAliasName();
  if (!aliasName.empty()) {
    aliasId = aliasManager->getAliasId(aliasName);
  }

  // Set alias ID if valid
  if (aliasId != INVALID_ALIAS_ID) {
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

  // get the last operator in the query plan
  auto lastOpr = plan->mutable_plan()->Mutable(plan->plan_size() - 1);
  if (lastOpr && lastOpr->opr().has_scan()) {
    // set predicates into query params in scan
    auto scan = lastOpr->mutable_opr()->mutable_scan();
    auto predicate = filter.getPredicate();
    auto predicatePB = exprConvertor->convert(*predicate);
    if (!predicatePB) {
      throw common::Exception("Failed to convert predicate: " +
                              predicate->toString());
    }
    scan->mutable_params()->set_allocated_predicate(predicatePB.release());
  } else if (lastOpr && lastOpr->opr().has_edge()) {
    // set predicates into query params in edge expand
    auto edgeExpand = lastOpr->mutable_opr()->mutable_edge();
    auto predicate = filter.getPredicate();
    auto predicatePB = exprConvertor->convert(*predicate);
    if (!predicatePB) {
      throw common::Exception("Failed to convert predicate: " +
                              predicate->toString());
    }
    edgeExpand->mutable_params()->set_allocated_predicate(
        predicatePB.release());
  } else if (lastOpr && lastOpr->opr().has_vertex()) {
    // set predicates into query params in getV
    auto getV = lastOpr->mutable_opr()->mutable_vertex();
    auto predicate = filter.getPredicate();
    auto predicatePB = exprConvertor->convert(*predicate);
    if (!predicatePB) {
      throw common::Exception("Failed to convert predicate: " +
                              predicate->toString());
    }
    getV->mutable_params()->set_allocated_predicate(predicatePB.release());
  } else {
    // todo: add predicated into expand and getV operators, or append a
    // filtering PB into plan
    throw common::Exception(
        "Cannot convert filter, last operator is not a scan operator.");
  }
}

void GQueryConvertor::convertProject(const planner::LogicalProjection& project,
                                     ::physical::QueryPlan* plan) {
  auto projectPB = std::make_unique<::physical::Project>();
  // auto schema = project.getSchema()->getExpressionsInScope();
  std::vector<std::string> aliasNames;
  GAliasManager::extractAliasName(project, aliasNames);
  auto exprs = project.getExpressionsToProject();
  if (exprs.size() != aliasNames.size()) {
    throw common::Exception(
        "Number of expressions to project does not match "
        "the number of schema expressions.");
  }
  // set project mappings
  for (size_t i = 0; i < exprs.size(); i++) {
    auto& expr = exprs[i];
    auto exprPB = exprConvertor->convert(*expr);
    if (!exprPB) {
      throw common::Exception("Failed to convert expression: " +
                              expr->toString());
    }
    auto exprAliasPB = std::make_unique<::physical::Project::ExprAlias>();
    exprAliasPB->set_allocated_expr(exprPB.release());
    auto aliasName = aliasNames[i];
    // the aliasId should have existed in aliasManager
    auto aliasId = aliasManager->getAliasId(aliasName);
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
  if (schema.size() != aliasNames.size()) {
    throw common::Exception(
        "Number of schema expressions does not match the number of alias "
        "names.");
  }
  // Set metadata by enumerating schema and aliasNames
  for (size_t i = 0; i < schema.size(); i++) {
    auto metaPB = std::make_unique<::physical::PhysicalOpr_MetaData>();
    // Convert alias name to ID
    auto aliasId = aliasManager->getAliasId(aliasNames[i]);
    metaPB->set_alias(aliasId);
    auto& type = schema[i]->dataType;

    if (type.getLogicalTypeID() == common::LogicalTypeID::NODE) {}
    // Get type from schema expression
    auto typePB = typeConverter->convertType(schema[i]->dataType, *exprs[i]);
    metaPB->set_allocated_type(typePB.release());
    physicalOpr->mutable_meta_data()->AddAllocated(metaPB.release());
  }

  plan->mutable_plan()->AddAllocated(physicalOpr.release());
}

std::unique_ptr<algebra::QueryParams> GQueryConvertor::convertParams(
    const std::vector<common::table_id_t>& labelIds) {
  auto queryParams = std::make_unique<algebra::QueryParams>();
  for (auto& label : labelIds) {
    auto tableId = std::make_unique<::common::NameOrId>();
    tableId->set_id(label);
    queryParams->mutable_tables()->AddAllocated(tableId.release());
  }
  return queryParams;
}

std::unique_ptr<::physical::DataSource> GQueryConvertor::convertDataSource(
    const common::FileScanInfo& fileInfo) {
  auto options = convertCSVOptions(fileInfo);
  auto csvPB = std::make_unique<::physical::ReadCSV>();
  csvPB->set_allocated_csv_options(options.release());
  csvPB->set_file_path(fileInfo.filePaths[0]);
  auto sourcePB = std::make_unique<::physical::DataSource>();
  sourcePB->set_allocated_read_csv(csvPB.release());
  return sourcePB;
}

std::unique_ptr<::physical::ReadCSV::options>
GQueryConvertor::convertCSVOptions(const common::FileScanInfo& fileInfo) {
  return std::make_unique<::physical::ReadCSV::options>();
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
    auto typePB = typeConverter->convertLogicalType(column->getDataType());
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

}  // namespace gopt
}  // namespace gs