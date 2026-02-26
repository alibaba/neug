/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include <memory>
#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/path_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/rel_table_catalog_entry.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/utils.h"
#include "neug/compiler/function/cast/functions/cast_from_string_functions.h"
#include "neug/compiler/function/gds/rec_joins.h"
#include "neug/compiler/function/rewrite_function.h"
#include "neug/compiler/function/schema/vector_node_rel_functions.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/main/client_context.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::parser;
using namespace neug::catalog;

namespace neug {
namespace binder {

// A graph pattern contains node/rel and a set of key-value pairs associated
// with the variable. We bind node/rel as query graph and key-value pairs as a
// separate collection. This collection is interpreted in two different ways.
//    - In MATCH clause, these are additional predicates to WHERE clause
//    - In UPDATE clause, there are properties to set.
// We do not store key-value pairs in query graph primarily because we will
// merge key-value std::pairs with other predicates specified in WHERE clause.
BoundGraphPattern Binder::bindGraphPattern(
    const std::vector<PatternElement>& graphPattern) {
  auto queryGraphCollection = QueryGraphCollection();
  for (auto& patternElement : graphPattern) {
    queryGraphCollection.addAndMergeQueryGraphIfConnected(
        bindPatternElement(patternElement));
  }
  queryGraphCollection.finalize();
  auto boundPattern = BoundGraphPattern();
  boundPattern.queryGraphCollection = std::move(queryGraphCollection);
  return boundPattern;
}

// Grammar ensures pattern element is always connected and thus can be bound as
// a query graph.
QueryGraph Binder::bindPatternElement(const PatternElement& patternElement) {
  auto queryGraph = QueryGraph();
  expression_vector nodeAndRels;
  auto leftNode =
      bindQueryNode(*patternElement.getFirstNodePattern(), queryGraph);
  nodeAndRels.push_back(leftNode);
  for (auto i = 0u; i < patternElement.getNumPatternElementChains(); ++i) {
    auto patternElementChain = patternElement.getPatternElementChain(i);
    auto rightNode =
        bindQueryNode(*patternElementChain->getNodePattern(), queryGraph);
    auto rel = bindQueryRel(*patternElementChain->getRelPattern(), leftNode,
                            rightNode, queryGraph);
    nodeAndRels.push_back(rel);
    nodeAndRels.push_back(rightNode);
    leftNode = rightNode;
  }
  if (patternElement.hasPathName()) {
    auto pathName = patternElement.getPathName();
    auto pathExpression = createPath(pathName, nodeAndRels);
    addToScope(pathName, pathExpression);
  }
  return queryGraph;
}

static LogicalType getRecursiveRelLogicalType(const LogicalType& nodeType,
                                              const LogicalType& relType) {
  auto nodesType = LogicalType::LIST(nodeType.copy());
  auto relsType = LogicalType::LIST(relType.copy());
  std::vector<StructField> recursiveRelFields;
  recursiveRelFields.emplace_back(InternalKeyword::NODES, std::move(nodesType));
  recursiveRelFields.emplace_back(InternalKeyword::RELS, std::move(relsType));
  return LogicalType::RECURSIVE_REL(
      std::make_unique<common::StructTypeInfo>(std::move(recursiveRelFields)));
}

static void extraFieldFromStructType(const LogicalType& structType,
                                     std::unordered_set<std::string>& nameSet,
                                     std::vector<std::string>& names,
                                     std::vector<LogicalType>& types) {
  for (auto& field : ::StructType::getFields(structType)) {
    if (!nameSet.contains(field.getName())) {
      nameSet.insert(field.getName());
      names.push_back(field.getName());
      types.push_back(field.getType().copy());
    }
  }
}

std::shared_ptr<Expression> Binder::createPath(
    const std::string& pathName, const expression_vector& children) {
  std::unordered_set<std::string> nodeFieldNameSet;
  std::vector<std::string> nodeFieldNames;
  std::vector<LogicalType> nodeFieldTypes;
  std::unordered_set<std::string> relFieldNameSet;
  std::vector<std::string> relFieldNames;
  std::vector<LogicalType> relFieldTypes;
  for (auto& child : children) {
    if (ExpressionUtil::isNodePattern(*child)) {
      auto node = neug_dynamic_cast<NodeExpression*>(child.get());
      extraFieldFromStructType(node->getDataType(), nodeFieldNameSet,
                               nodeFieldNames, nodeFieldTypes);
    } else if (ExpressionUtil::isRelPattern(*child)) {
      auto rel = neug_dynamic_cast<RelExpression*>(child.get());
      extraFieldFromStructType(rel->getDataType(), relFieldNameSet,
                               relFieldNames, relFieldTypes);
    } else if (ExpressionUtil::isRecursiveRelPattern(*child)) {
      auto recursiveRel = neug_dynamic_cast<RelExpression*>(child.get());
      auto recursiveInfo = recursiveRel->getRecursiveInfo();
      extraFieldFromStructType(recursiveInfo->node->getDataType(),
                               nodeFieldNameSet, nodeFieldNames,
                               nodeFieldTypes);
      extraFieldFromStructType(recursiveInfo->rel->getDataType(),
                               relFieldNameSet, relFieldNames, relFieldTypes);
    } else {
      NEUG_UNREACHABLE;
    }
  }
  auto nodeExtraInfo =
      std::make_unique<common::StructTypeInfo>(nodeFieldNames, nodeFieldTypes);
  auto nodeType = LogicalType::NODE(std::move(nodeExtraInfo));
  auto relExtraInfo =
      std::make_unique<common::StructTypeInfo>(relFieldNames, relFieldTypes);
  auto relType = LogicalType::REL(std::move(relExtraInfo));
  auto uniqueName = getUniqueExpressionName(pathName);
  return std::make_shared<PathExpression>(
      getRecursiveRelLogicalType(nodeType, relType), uniqueName, pathName,
      std::move(nodeType), std::move(relType), children);
}

static std::vector<std::string> getPropertyNames(
    const std::vector<TableCatalogEntry*>& entries) {
  std::vector<std::string> result;
  common::case_insensitve_set_t propertyNamesSet;
  for (auto& entry : entries) {
    for (auto& property : entry->getProperties()) {
      if (propertyNamesSet.contains(property.getName())) {
        continue;
      }
      propertyNamesSet.insert(property.getName());
      result.push_back(property.getName());
    }
  }
  return result;
}

static std::unique_ptr<Expression> createPropertyExpression(
    const std::string& propertyName, const std::string& uniqueVariableName,
    const std::string& rawVariableName,
    const std::vector<TableCatalogEntry*>& entries) {
  table_id_map_t<SingleLabelPropertyInfo> infos;
  std::vector<LogicalType> dataTypes;
  for (auto& entry : entries) {
    bool exists = false;
    if (entry->containsProperty(propertyName)) {
      exists = true;
      dataTypes.push_back(entry->getProperty(propertyName).getType().copy());
    }
    // Bind isPrimaryKey
    auto isPrimaryKey = false;
    if (entry->getTableType() == TableType::NODE) {
      auto nodeEntry = entry->constPtrCast<NodeTableCatalogEntry>();
      isPrimaryKey = nodeEntry->getPrimaryKeyName() == propertyName;
    }
    auto info = SingleLabelPropertyInfo(exists, isPrimaryKey);
    infos.insert({entry->getTableID(), std::move(info)});
  }
  // Validate property under the same name has the same type.
  NEUG_ASSERT(!dataTypes.empty());
  for (const auto& type : dataTypes) {
    if (dataTypes[0] != type) {
      THROW_BINDER_EXCEPTION(stringFormat(
          "Expected the same data type for property {} but found {} and {}.",
          propertyName, type.toString(), dataTypes[0].toString()));
    }
  }
  return make_unique<PropertyExpression>(std::move(dataTypes[0]), propertyName,
                                         uniqueVariableName, rawVariableName,
                                         std::move(infos));
}

static std::unique_ptr<Expression> createPropertyExpression(
    const std::string& propertyName, const Expression& pattern,
    const std::vector<TableCatalogEntry*>& entries) {
  return createPropertyExpression(propertyName, pattern.getUniqueName(),
                                  pattern.toString(), entries);
}

static void checkRelDirectionTypeAgainstStorageDirection(
    const RelExpression* rel) {
  switch (rel->getDirectionType()) {
  case RelDirectionType::SINGLE:
    // Directed pattern is in the fwd direction
    if (!containsValue(rel->getExtendDirections(), ExtendDirection::FWD)) {
      THROW_BINDER_EXCEPTION(
          stringFormat("Querying table matched in rel pattern '{}' with "
                       "bwd-only storage direction isn't supported.",
                       rel->toString()));
    }
    break;
  case RelDirectionType::BOTH:
    if (rel->getExtendDirections().size() < NUM_REL_DIRECTIONS) {
      THROW_BINDER_EXCEPTION(stringFormat(
          "Undirected rel pattern '{}' has at least one matched rel table with "
          "storage type 'fwd' or 'bwd'. Undirected rel patterns are only "
          "supported if every matched rel table has storage type 'both'.",
          rel->toString()));
    }
    break;
  default:
    NEUG_UNREACHABLE;
  }
}

std::shared_ptr<RelExpression> Binder::bindQueryRel(
    const RelPattern& relPattern,
    const std::shared_ptr<NodeExpression>& leftNode,
    const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph) {
  auto parsedName = relPattern.getVariableName();
  if (scope.contains(parsedName)) {
    auto prevVariable = scope.getExpression(parsedName);
    auto expectedDataType =
        QueryRelTypeUtils::isRecursive(relPattern.getRelType())
            ? LogicalTypeID::RECURSIVE_REL
            : LogicalTypeID::REL;
    ExpressionUtil::validateDataType(*prevVariable, expectedDataType);
    THROW_BINDER_EXCEPTION("Bind relationship " + parsedName +
                           " to relationship with same name is not supported.");
  }
  auto entries = bindRelTableEntries(relPattern.getTableNames());
  // bind src & dst node
  RelDirectionType directionType = RelDirectionType::UNKNOWN;
  std::shared_ptr<NodeExpression> srcNode;
  std::shared_ptr<NodeExpression> dstNode;
  switch (relPattern.getDirection()) {
  case ArrowDirection::LEFT: {
    srcNode = rightNode;
    dstNode = leftNode;
    directionType = RelDirectionType::SINGLE;
  } break;
  case ArrowDirection::RIGHT: {
    srcNode = leftNode;
    dstNode = rightNode;
    directionType = RelDirectionType::SINGLE;
  } break;
  case ArrowDirection::BOTH: {
    // For both direction, left and right will be written with the same label
    // set. So either one being src will be correct.
    srcNode = leftNode;
    dstNode = rightNode;
    directionType = RelDirectionType::BOTH;
  } break;
  default:
    NEUG_UNREACHABLE;
  }
  // bind variable length
  std::shared_ptr<RelExpression> queryRel;
  if (QueryRelTypeUtils::isRecursive(relPattern.getRelType())) {
    queryRel = createRecursiveQueryRel(relPattern, entries, srcNode, dstNode,
                                       directionType);
  } else {
    queryRel = createNonRecursiveQueryRel(relPattern.getVariableName(), entries,
                                          srcNode, dstNode, directionType);
    for (auto& [propertyName, rhs] : relPattern.getPropertyKeyVals()) {
      auto boundLhs = expressionBinder.bindNodeOrRelPropertyExpression(
          *queryRel, propertyName);
      auto boundRhs = expressionBinder.bindExpression(*rhs);
      boundRhs = expressionBinder.implicitCastIfNecessary(boundRhs,
                                                          boundLhs->dataType);
      queryRel->addPropertyDataExpr(propertyName, std::move(boundRhs));
    }
  }
  queryRel->setLeftNode(leftNode);
  queryRel->setRightNode(rightNode);
  queryRel->setAlias(parsedName);
  if (!parsedName.empty()) {
    addToScope(parsedName, queryRel);
  }
  queryGraph.addQueryRel(queryRel);
  checkRelDirectionTypeAgainstStorageDirection(queryRel.get());
  return queryRel;
}

static std::vector<StructField> getBaseNodeStructFields() {
  std::vector<StructField> fields;
  fields.emplace_back(InternalKeyword::ID, LogicalType::INTERNAL_ID());
  fields.emplace_back(InternalKeyword::LABEL, LogicalType::STRING());
  return fields;
}

static std::vector<StructField> getBaseRelStructFields(
    std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode) {
  std::vector<StructField> fields;
  if (srcNode) {
    fields.emplace_back(InternalKeyword::SRC, srcNode->getDataType().copy());
  } else {
    fields.emplace_back(InternalKeyword::SRC, LogicalType::INTERNAL_ID());
  }
  if (dstNode) {
    fields.emplace_back(InternalKeyword::DST, dstNode->getDataType().copy());
  } else {
    fields.emplace_back(InternalKeyword::DST, LogicalType::INTERNAL_ID());
  }
  fields.emplace_back(InternalKeyword::LABEL, LogicalType::STRING());
  return fields;
}

std::shared_ptr<RelExpression> Binder::createNonRecursiveQueryRel(
    const std::string& parsedName,
    const std::vector<TableCatalogEntry*>& entries,
    std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
  auto queryRel = make_shared<RelExpression>(
      LogicalType(LogicalTypeID::REL), getUniqueExpressionName(parsedName),
      parsedName, entries, srcNode, dstNode, directionType,
      QueryRelType::NON_RECURSIVE);
  if (directionType == RelDirectionType::BOTH) {
    queryRel->setDirectionExpr(expressionBinder.createVariableExpression(
        LogicalType::BOOL(),
        queryRel->getUniqueName() + InternalKeyword::DIRECTION));
  }
  queryRel->setAlias(parsedName);
  bindQueryRelProperties(*queryRel);
  // Bind internal expressions.
  auto input = function::RewriteFunctionBindInput(
      clientContext, &expressionBinder, {queryRel});
  queryRel->setLabelExpression(function::LabelFunction::rewriteFunc(input));
  // Bind properties.
  auto fields = getBaseRelStructFields(srcNode, dstNode);
  for (auto& expression : queryRel->getPropertyExprsRef()) {
    auto& property = expression->constCast<PropertyExpression>();
    fields.emplace_back(property.getPropertyName(),
                        property.getDataType().copy());
  }
  std::vector<catalog::GRelTableCatalogEntry*> relEntries;
  for (auto& entry : entries) {
    relEntries.emplace_back(entry->ptrCast<GRelTableCatalogEntry>());
  }
  auto relType = std::make_unique<gopt::GRelType>(relEntries);
  auto extraInfo = std::make_unique<common::GRelTypeInfo>(std::move(fields),
                                                          std::move(relType));
  queryRel->setExtraTypeInfo(std::move(extraInfo));
  return queryRel;
}

static void bindProjectionListAsStructField(
    const expression_vector& projectionList, std::vector<StructField>& fields) {
  for (auto& expression : projectionList) {
    if (expression->expressionType != ExpressionType::PROPERTY) {
      THROW_BINDER_EXCEPTION(
          stringFormat("Unsupported projection item {} on recursive rel.",
                       expression->toString()));
    }
    auto& property = expression->constCast<PropertyExpression>();
    fields.emplace_back(property.getPropertyName(),
                        property.getDataType().copy());
  }
}

static void checkWeightedShortestPathSupportedType(const LogicalType& type) {
  switch (type.getLogicalTypeID()) {
  case LogicalTypeID::INT8:
  case LogicalTypeID::UINT8:
  case LogicalTypeID::INT16:
  case LogicalTypeID::UINT16:
  case LogicalTypeID::INT32:
  case LogicalTypeID::UINT32:
  case LogicalTypeID::INT64:
  case LogicalTypeID::UINT64:
  case LogicalTypeID::DOUBLE:
  case LogicalTypeID::FLOAT:
    return;
  default:
    break;
  }
  THROW_BINDER_EXCEPTION(stringFormat(
      "{} weight type is not supported for weighted shortest path.",
      type.toString()));
}

std::shared_ptr<RelExpression> Binder::createRecursiveQueryRel(
    const parser::RelPattern& relPattern,
    const std::vector<TableCatalogEntry*>& entries,
    std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
  auto catalog = clientContext->getCatalog();
  auto transaction = clientContext->getTransaction();
  table_catalog_entry_set_t entrySet;
  for (auto entry : entries) {
    auto& relTableEntry = entry->constCast<RelTableCatalogEntry>();
    entrySet.insert(catalog->getTableCatalogEntry(
        transaction, relTableEntry.getSrcTableID()));
    entrySet.insert(catalog->getTableCatalogEntry(
        transaction, relTableEntry.getDstTableID()));
  }
  auto recursivePatternInfo = relPattern.getRecursiveInfo();
  auto prevScope = saveScope();
  scope.clear();
  // Bind intermediate node.
  auto node = createQueryNode(
      recursivePatternInfo->nodeName,
      std::vector<TableCatalogEntry*>{entrySet.begin(), entrySet.end()});
  addToScope(node->toString(), node);
  auto nodeProjectionList =
      bindRecursivePatternNodeProjectionList(*recursivePatternInfo, *node);
  auto nodeCopy = createQueryNode(
      recursivePatternInfo->nodeName,
      std::vector<TableCatalogEntry*>{entrySet.begin(), entrySet.end()});
  // Bind intermediate rel
  auto rel = createNonRecursiveQueryRel(recursivePatternInfo->relName, entries,
                                        srcNode, dstNode, directionType);
  addToScope(rel->toString(), rel);
  auto relProjectionList =
      bindRecursivePatternRelProjectionList(*recursivePatternInfo, *rel);
  // Bind predicates in {}, e.g. [e* {date=1999-01-01}]
  std::shared_ptr<Expression> relPredicate = nullptr;
  for (auto& [propertyName, rhs] : relPattern.getPropertyKeyVals()) {
    auto boundLhs =
        expressionBinder.bindNodeOrRelPropertyExpression(*rel, propertyName);
    auto boundRhs = expressionBinder.bindExpression(*rhs);
    boundRhs =
        expressionBinder.implicitCastIfNecessary(boundRhs, boundLhs->dataType);
    auto predicate =
        expressionBinder.createEqualityComparisonExpression(boundLhs, boundRhs);
    relPredicate = expressionBinder.combineBooleanExpressions(
        ExpressionType::AND, relPredicate, predicate);
  }
  // Bind predicates in (r, n | WHERE )
  bool emptyRecursivePattern = false;
  std::shared_ptr<Expression> nodePredicate = nullptr;
  if (recursivePatternInfo->whereExpression != nullptr) {
    expressionBinder.config.disableLabelFunctionLiteralRewrite = true;
    auto wherePredicate =
        bindWhereExpression(*recursivePatternInfo->whereExpression);
    expressionBinder.config.disableLabelFunctionLiteralRewrite = false;
    for (auto& predicate : wherePredicate->splitOnAND()) {
      auto collector = DependentVarNameCollector();
      collector.visit(predicate);
      auto dependentVariableNames = collector.getVarNames();
      auto dependOnNode =
          dependentVariableNames.contains(node->getUniqueName());
      auto dependOnRel = dependentVariableNames.contains(rel->getUniqueName());
      if (dependOnNode && dependOnRel) {
        THROW_BINDER_EXCEPTION(stringFormat(
            "Cannot evaluate {} because it depends on both {} and {}.",
            predicate->toString(), node->toString(), rel->toString()));
      } else if (dependOnNode) {
        nodePredicate = expressionBinder.combineBooleanExpressions(
            ExpressionType::AND, nodePredicate, predicate);
      } else if (dependOnRel) {
        relPredicate = expressionBinder.combineBooleanExpressions(
            ExpressionType::AND, relPredicate, predicate);
      } else {
        if (!ExpressionUtil::isBoolLiteral(*predicate)) {
          THROW_BINDER_EXCEPTION(stringFormat(
              "Cannot evaluate {} because it does not depend on {} or {}. "
              "Treating it as "
              "a node or relationship predicate is ambiguous.",
              predicate->toString(), node->toString(), rel->toString()));
        }
        // If predicate is true literal, we ignore.
        // If predicate is false literal, we mark this recursive relationship as
        // empty and later in planner we replace it with EmptyResult.
        if (!ExpressionUtil::getLiteralValue<bool>(*predicate)) {
          emptyRecursivePattern = true;
        }
      }
    }
  }
  // Bind rel
  restoreScope(std::move(prevScope));
  auto parsedName = relPattern.getVariableName();
  auto prunedRelEntries = entries;
  if (emptyRecursivePattern) {
    prunedRelEntries.clear();
  }
  auto queryRel = std::make_shared<RelExpression>(
      getRecursiveRelLogicalType(node->getDataType(), rel->getDataType()),
      getUniqueExpressionName(parsedName), parsedName, prunedRelEntries,
      std::move(srcNode), std::move(dstNode), directionType,
      relPattern.getRelType());
  // Bind graph entry.
  auto graphEntry = graph::GraphEntry(node->getEntries(), rel->getEntries());
  graphEntry.setRelPredicate(relPredicate);  // TODO: revisit me

  auto bindData = std::make_unique<function::RJBindData>(graphEntry.copy());
  // Bind lower upper bound.
  auto [lowerBound, upperBound] = bindVariableLengthRelBound(relPattern);
  bindData->lowerBound = lowerBound;
  bindData->upperBound = upperBound;
  // Bind semantic.
  bindData->semantic =
      QueryRelTypeUtils::getPathSemantic(queryRel->getRelType());
  // Bind path related expressions.
  bindData->lengthExpr = PropertyExpression::construct(
      LogicalType::INT64(), InternalKeyword::LENGTH, *queryRel);
  bindData->pathNodeIDsExpr = createInvisibleVariable(
      "pathNodeIDs", LogicalType::LIST(LogicalType::INTERNAL_ID()));
  bindData->pathEdgeIDsExpr = createInvisibleVariable(
      "pathEdgeIDs", LogicalType::LIST(LogicalType::INTERNAL_ID()));
  if (queryRel->getDirectionType() == RelDirectionType::BOTH) {
    bindData->directionExpr = createInvisibleVariable(
        "pathEdgeDirections", LogicalType::LIST(LogicalType::BOOL()));
  }
  // Bind weighted path related expressions.
  if (QueryRelTypeUtils::isWeighted(queryRel->getRelType())) {
    auto propertyExpr = expressionBinder.bindNodeOrRelPropertyExpression(
        *rel, recursivePatternInfo->weightPropertyName);
    checkWeightedShortestPathSupportedType(propertyExpr->getDataType());
    bindData->weightPropertyExpr = propertyExpr;
    bindData->weightOutputExpr = std::make_shared<binder::PropertyExpression>(
        LogicalType::DOUBLE().copy(), "cost", queryRel->getUniqueName(),
        parsedName);
  }

  auto recursiveInfo = std::make_unique<RecursiveInfo>();
  recursiveInfo->node = node;
  recursiveInfo->nodeCopy = nodeCopy;
  recursiveInfo->rel = rel;
  recursiveInfo->nodePredicate = std::move(nodePredicate);
  recursiveInfo->relPredicate = std::move(relPredicate);
  recursiveInfo->nodeProjectionList = std::move(nodeProjectionList);
  recursiveInfo->relProjectionList = std::move(relProjectionList);
  recursiveInfo->function =
      QueryRelTypeUtils::getFunction(queryRel->getRelType());
  recursiveInfo->bindData = std::move(bindData);
  queryRel->setRecursiveInfo(std::move(recursiveInfo));
  return queryRel;
}

expression_vector Binder::bindRecursivePatternNodeProjectionList(
    const RecursiveRelPatternInfo& info, const NodeOrRelExpression& expr) {
  expression_vector result;
  if (!info.hasProjection) {
    for (auto& expression : expr.getPropertyExprsRef()) {
      result.push_back(expression->copy());
    }
  } else {
    for (auto& expression : info.nodeProjectionList) {
      result.push_back(expressionBinder.bindExpression(*expression));
    }
  }
  return result;
}

expression_vector Binder::bindRecursivePatternRelProjectionList(
    const RecursiveRelPatternInfo& info, const NodeOrRelExpression& expr) {
  expression_vector result;
  if (!info.hasProjection) {
    for (auto& expression : expr.getPropertyExprsRef()) {
      if (expression->constCast<PropertyExpression>().isInternalID()) {
        continue;
      }
      result.push_back(expression->copy());
    }
  } else {
    for (auto& expression : info.relProjectionList) {
      result.push_back(expressionBinder.bindExpression(*expression));
    }
  }
  return result;
}

std::pair<uint64_t, uint64_t> Binder::bindVariableLengthRelBound(
    const neug::parser::RelPattern& relPattern) {
  auto recursiveInfo = relPattern.getRecursiveInfo();
  uint32_t lowerBound = 0;
  function::CastString::operation(
      neug_string_t{recursiveInfo->lowerBound.c_str(),
                    recursiveInfo->lowerBound.length()},
      lowerBound);
  auto maxDepth = clientContext->getClientConfig()->varLengthMaxDepth;
  auto upperBound = maxDepth;
  if (!recursiveInfo->upperBound.empty()) {
    function::CastString::operation(
        neug_string_t{recursiveInfo->upperBound.c_str(),
                      recursiveInfo->upperBound.length()},
        upperBound);
  }
  if (lowerBound > upperBound) {
    THROW_BINDER_EXCEPTION(
        stringFormat("Lower bound of rel {} is greater than upperBound.",
                     relPattern.getVariableName()));
  }
  if (upperBound > maxDepth) {
    THROW_BINDER_EXCEPTION(
        stringFormat("Upper bound of rel {} exceeds maximum: {}.",
                     relPattern.getVariableName(), std::to_string(maxDepth)));
  }
  if ((relPattern.getRelType() == QueryRelType::ALL_SHORTEST ||
       relPattern.getRelType() == QueryRelType::SHORTEST) &&
      lowerBound != 1) {
    THROW_BINDER_EXCEPTION(
        "Lower bound of shortest/all_shortest path must be 1.");
  }
  return std::make_pair(lowerBound, upperBound);
}

void Binder::bindQueryRelProperties(RelExpression& rel) {
  if (rel.isEmpty()) {
    auto internalID = PropertyExpression::construct(LogicalType::INTERNAL_ID(),
                                                    InternalKeyword::ID, rel);
    rel.addPropertyExpression(InternalKeyword::ID, std::move(internalID));
    return;
  }
  auto entries = rel.getEntries();
  auto propertyNames = getPropertyNames(entries);
  for (auto& propertyName : propertyNames) {
    auto property = createPropertyExpression(propertyName, rel, entries);
    rel.addPropertyExpression(propertyName, std::move(property));
  }
}

std::shared_ptr<NodeExpression> Binder::bindQueryNode(
    const NodePattern& nodePattern, QueryGraph& queryGraph) {
  auto parsedName = nodePattern.getVariableName();
  std::shared_ptr<NodeExpression> queryNode;
  if (scope.contains(parsedName)) {  // bind to node in scope
    auto prevVariable = scope.getExpression(parsedName);
    if (!ExpressionUtil::isNodePattern(*prevVariable)) {
      if (!scope.hasNodeReplacement(parsedName)) {
        THROW_BINDER_EXCEPTION(
            stringFormat("Cannot bind {} as node pattern.", parsedName));
      }
      queryNode = scope.getNodeReplacement(parsedName);
      queryNode->addPropertyDataExpr(InternalKeyword::ID,
                                     queryNode->getInternalID());
    } else {
      queryNode = std::static_pointer_cast<NodeExpression>(prevVariable);
      // E.g. MATCH (a:person) MATCH (a:organisation)
      // We bind to a single node with both labels

      if (queryNode->getEntries()
              .empty()) {  // handle the special case of unwind node
        std::string preUniqueName = queryNode->getUniqueName();
        queryNode = createQueryNode(nodePattern);
        queryNode->setNodeUniqueName(preUniqueName);
        replaceExpressionInScope(parsedName, parsedName, queryNode);
      } else if (!nodePattern.getTableNames().empty()) {
        auto otherNodeEntries =
            bindNodeTableEntries(nodePattern.getTableNames());
        queryNode->addEntries(otherNodeEntries);
      }
    }
  } else {
    queryNode = createQueryNode(nodePattern);
    if (!parsedName.empty()) {
      addToScope(parsedName, queryNode);
    }
  }
  for (auto& [propertyName, rhs] : nodePattern.getPropertyKeyVals()) {
    auto boundLhs = expressionBinder.bindNodeOrRelPropertyExpression(
        *queryNode, propertyName);
    auto boundRhs = expressionBinder.bindExpression(*rhs);
    boundRhs =
        expressionBinder.implicitCastIfNecessary(boundRhs, boundLhs->dataType);
    queryNode->addPropertyDataExpr(propertyName, std::move(boundRhs));
  }
  queryGraph.addQueryNode(queryNode);
  return queryNode;
}

std::shared_ptr<NodeExpression> Binder::createQueryNode(
    const NodePattern& nodePattern) {
  auto parsedName = nodePattern.getVariableName();
  return createQueryNode(parsedName,
                         bindNodeTableEntries(nodePattern.getTableNames()));
}

std::shared_ptr<NodeExpression> Binder::createQueryNode(
    const std::string& parsedName,
    const std::vector<TableCatalogEntry*>& entries) {
  auto queryNode = make_shared<NodeExpression>(
      LogicalType(LogicalTypeID::NODE), getUniqueExpressionName(parsedName),
      parsedName, entries);
  queryNode->setAlias(parsedName);
  // Bind internal expressions
  queryNode->setInternalID(PropertyExpression::construct(
      LogicalType::INTERNAL_ID(), InternalKeyword::ID, *queryNode));
  auto input = function::RewriteFunctionBindInput(
      clientContext, &expressionBinder, {queryNode});
  queryNode->setLabelExpression(function::LabelFunction::rewriteFunc(input));
  auto structFields = getBaseNodeStructFields();
  // Bind properties.
  bindQueryNodeProperties(*queryNode);
  for (auto& expression : queryNode->getPropertyExprsRef()) {
    auto property = neug_dynamic_cast<PropertyExpression*>(expression.get());
    structFields.emplace_back(property->getPropertyName(),
                              property->getDataType().copy());
  }
  std::vector<catalog::NodeTableCatalogEntry*> nodeEntries;
  for (auto& entry : entries) {
    nodeEntries.emplace_back(entry->ptrCast<catalog::NodeTableCatalogEntry>());
  }
  auto nodeType = std::make_unique<gopt::GNodeType>(nodeEntries);
  auto extraInfo = std::make_unique<common::GNodeTypeInfo>(
      std::move(structFields), std::move(nodeType));
  queryNode->setExtraTypeInfo(std::move(extraInfo));
  return queryNode;
}

void Binder::bindQueryNodeProperties(NodeExpression& node) {
  auto entries = node.getEntries();
  auto propertyNames = getPropertyNames(entries);
  for (auto& propertyName : propertyNames) {
    auto property = createPropertyExpression(propertyName, node, entries);
    node.addPropertyExpression(propertyName, std::move(property));
  }
}

static std::vector<TableCatalogEntry*> sortEntries(
    const table_catalog_entry_set_t& set) {
  std::vector<TableCatalogEntry*> entries;
  for (auto entry : set) {
    entries.push_back(entry);
  }
  std::sort(entries.begin(), entries.end(),
            [](const TableCatalogEntry* a, const TableCatalogEntry* b) {
              return a->getTableID() < b->getTableID();
            });
  return entries;
}

std::vector<TableCatalogEntry*> Binder::bindNodeTableEntries(
    const std::vector<std::string>& tableNames) const {
  auto transaction = clientContext->getTransaction();
  auto catalog = clientContext->getCatalog();
  auto useInternal = clientContext->useInternalCatalogEntry();
  table_catalog_entry_set_t entrySet;
  if (tableNames.empty()) {
    for (auto entry : catalog->getNodeTableEntries(transaction, useInternal)) {
      entrySet.insert(entry);
    }
  } else {
    for (auto& name : tableNames) {
      auto entry = bindNodeTableEntry(name);
      if (entry->getType() != CatalogEntryType::NODE_TABLE_ENTRY) {
        THROW_BINDER_EXCEPTION(stringFormat(
            "Cannot bind {} as a node pattern label.", entry->getName()));
      }
      entrySet.insert(entry);
    }
  }
  return sortEntries(entrySet);
}

TableCatalogEntry* Binder::bindNodeTableEntry(const std::string& name) const {
  auto transaction = clientContext->getTransaction();
  auto catalog = clientContext->getCatalog();
  auto useInternal = clientContext->useInternalCatalogEntry();
  if (!catalog->containsTable(transaction, name, useInternal)) {
    THROW_BINDER_EXCEPTION(stringFormat("Table {} does not exist.", name));
  }
  return catalog->getTableCatalogEntry(transaction, name, useInternal);
}

std::vector<TableCatalogEntry*> Binder::bindRelTableEntries(
    const std::vector<std::string>& tableNames) const {
  auto transaction = clientContext->getTransaction();
  auto catalog = clientContext->getCatalog();
  auto useInternal = clientContext->useInternalCatalogEntry();
  table_catalog_entry_set_t entrySet;
  if (tableNames.empty()) {
    for (auto& entry : catalog->getRelTableEntries(transaction, useInternal)) {
      entrySet.insert(entry);
    }
  } else {
    for (auto& name : tableNames) {
      if (catalog->containsRelGroup(transaction, name)) {
        auto groupEntry = catalog->getRelGroupEntry(transaction, name);
        for (auto& id : groupEntry->getRelTableIDs()) {
          auto relEntry = catalog->getTableCatalogEntry(transaction, id);
          entrySet.insert(relEntry);
        }
      } else if (catalog->containsTable(transaction, name)) {
        auto entry =
            catalog->getTableCatalogEntry(transaction, name, useInternal);
        if (entry->getType() != CatalogEntryType::REL_TABLE_ENTRY) {
          THROW_BINDER_EXCEPTION(
              stringFormat("Cannot bind {} as a relationship pattern label.",
                           entry->getName()));
        }
        entrySet.insert(entry);
      } else {
        THROW_BINDER_EXCEPTION(stringFormat("Table {} does not exist.", name));
      }
    }
  }
  return sortEntries(entrySet);
}

}  // namespace binder
}  // namespace neug
