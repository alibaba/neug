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

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_scan_source.h"
#include "neug/compiler/binder/copy/bound_copy_from.h"
#include "neug/compiler/binder/copy/bound_query_scan_info.h"
#include "neug/compiler/binder/ddl/bound_create_table_info.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/binder/query/bound_regular_query.h"
#include "neug/compiler/binder/query/reading_clause/bound_load_from.h"
#include "neug/compiler/binder/query/return_with_clause/bound_projection_body.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/parser/expression/parsed_literal_expression.h"
#include "neug/compiler/parser/load_as.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::parser;
using namespace neug::catalog;

namespace neug {
namespace binder {

static std::string extractStringOption(const options_t& options,
                                       const std::string& key) {
  auto it = options.find(key);
  if (it == options.end()) {
    return {};
  }
  auto* literal = it->second->constPtrCast<ParsedLiteralExpression>();
  if (!literal) {
    return {};
  }
  return literal->getValue().getValue<std::string>();
}

// Build an ordered list of columns for DDL schema creation.
//
// For NODE:
//   - With RETURN: subset of sourceColumns in source order.
//   - Without RETURN: all sourceColumns.
//
// For REL:
//   ddlColumns[0] and [1] MUST be the src/dst key columns (required by
//   ExtraBoundCopyRelInfo and DDLEdgeInfo). When from_col/to_col options are
//   specified, those columns are placed first regardless of their file
//   position; otherwise columns[0] and [1] are used as keys (consistent
//   with COPY FROM's default behavior).
static expression_vector buildDdlColumns(
    const expression_vector& sourceColumns,
    const std::vector<std::string>& returnCols, bool isRel,
    const options_t& parsingOptions) {
  if (!isRel) {
    // NODE: keep RETURN subset in source order, or all columns if no RETURN.
    if (returnCols.empty()) {
      return sourceColumns;
    }
    expression_vector result;
    for (const auto& c : sourceColumns) {
      for (const auto& ret : returnCols) {
        if (c->rawName() == ret) { result.push_back(c); break; }
      }
    }
    return result;
  }

  // --- REL: ensure ddlColumns[0/1] are the src/dst key columns ---
  auto fromCol = extractStringOption(parsingOptions, "from_col");
  auto toCol = extractStringOption(parsingOptions, "to_col");

  // With RETURN: columns in RETURN-specified order.
  // Without RETURN: reorder so from_col/to_col are at [0/1] if specified,
  // otherwise use file order (keys at [0/1] by convention).
  if (!returnCols.empty()) {
    expression_vector result;
    for (const auto& ret : returnCols) {
      for (const auto& c : sourceColumns) {
        if (c->rawName() == ret) { result.push_back(c); break; }
      }
    }
    return result;
  }

  // No RETURN: reorder if from_col/to_col specify non-[0/1] key columns.
  if (!fromCol.empty() && !toCol.empty()) {
    expression_vector result;
    // Push src key column first.
    for (const auto& c : sourceColumns) {
      if (c->rawName() == fromCol) { result.push_back(c); break; }
    }
    // Push dst key column second.
    for (const auto& c : sourceColumns) {
      if (c->rawName() == toCol) { result.push_back(c); break; }
    }
    // Append remaining columns.
    for (const auto& c : sourceColumns) {
      if (c->rawName() != fromCol && c->rawName() != toCol) {
        result.push_back(c);
      }
    }
    return result;
  }

  // No RETURN, no from_col/to_col: file order (keys at [0/1]).
  return sourceColumns;
}

// When LOAD AS has WHERE and/or RETURN, construct an internal subquery
// equivalent to: LOAD FROM <source> WHERE <pred> RETURN <cols>
// This returns a BoundQueryScanSource that CopyFrom can use via the
// ScanSourceType::QUERY execution path, delegating filter/projection to the
// standard query planner rather than hacking CopyFrom's optimizer.
static std::unique_ptr<BoundBaseScanSource> buildFilteredSubquerySource(
    BoundTableScanInfo scanInfo, std::shared_ptr<Expression> wherePredicate,
    const std::vector<std::string>& projectedColumnNames) {
  // Get columns from the scanInfo that will be registered in the
  // TableFunctionCall's schema. Projection must reference THESE objects.
  expression_vector scanColumns = scanInfo.bindData->columns;

  // Build projection expressions by matching column names.
  expression_vector projExprs;
  for (const auto& name : projectedColumnNames) {
    for (const auto& col : scanColumns) {
      if (col->rawName() == name) {
        projExprs.push_back(col);
        break;
      }
    }
  }

  // 1. Create BoundLoadFrom (the reading clause).
  auto boundLoadFrom = std::make_unique<BoundLoadFrom>(std::move(scanInfo));
  if (wherePredicate) {
    boundLoadFrom->setPredicate(std::move(wherePredicate));
  }

  // 2. Create BoundProjectionBody with the selected columns.
  auto projBody = BoundProjectionBody(/*distinct=*/false);
  projBody.setProjectionExpressions(projExprs);

  // 3. Assemble NormalizedQueryPart.
  auto queryPart = NormalizedQueryPart();
  queryPart.addReadingClause(std::move(boundLoadFrom));
  queryPart.setProjectionBody(std::move(projBody));

  // 4. Assemble NormalizedSingleQuery with statement result.
  auto singleQuery = NormalizedSingleQuery();
  auto stmtResult = BoundStatementResult();
  for (const auto& expr : projExprs) {
    stmtResult.addColumn(expr->rawName(), expr);
  }
  singleQuery.appendQueryPart(std::move(queryPart));
  singleQuery.setStatementResult(std::move(stmtResult));

  // 5. Create BoundRegularQuery.
  auto stmtResultForQuery = singleQuery.getStatementResult()->copy();
  auto boundQuery = std::make_shared<BoundRegularQuery>(
      std::vector<bool>{}, std::move(stmtResultForQuery));
  boundQuery->addSingleQuery(std::move(singleQuery));

  // 6. Wrap in BoundQueryScanSource.
  auto querySourceInfo =
      BoundQueryScanSourceInfo(case_insensitive_map_t<Value>{});
  return std::make_unique<BoundQueryScanSource>(std::move(boundQuery),
                                                std::move(querySourceInfo));
}

std::unique_ptr<BoundStatement> Binder::bindLoadAs(
    const Statement& statement) {
  auto& loadAs = statement.constCast<LoadAs>();
  const auto& parsingOptions = loadAs.getParsingOptions();
  auto boundSource = bindScanSource(loadAs.getSource(),
                                    parsingOptions, {}, {});
  expression_vector columns = boundSource->getColumns();
  auto offset = createInvisibleVariable(
      std::string(InternalKeyword::ROW_OFFSET), LogicalType::INT64());
  const auto& labelName = loadAs.getTargetLabel();

  if (columns.empty()) {
    THROW_BINDER_EXCEPTION(stringFormat(
        "No columns found for LOAD AS `{}`, cannot infer schema", labelName));
  }

  // Validate RETURN columns early.
  std::vector<std::string> validatedReturnCols;
  if (loadAs.hasReturnColumns()) {
    const auto& returnCols = loadAs.getReturnColumns();
    for (const auto& retCol : returnCols) {
      bool found = false;
      for (const auto& srcCol : columns) {
        if (srcCol->rawName() == retCol) {
          found = true;
          break;
        }
      }
      if (!found) {
        THROW_BINDER_EXCEPTION(stringFormat(
            "RETURN column `{}` not found in source data for LOAD AS `{}`",
            retCol, labelName));
      }
    }
    validatedReturnCols = returnCols;
  }

  // Determine whether we need a subquery source (has WHERE or RETURN).
  bool hasFilter = loadAs.hasWherePredicate();
  bool hasProjection = !validatedReturnCols.empty();
  bool useSubquery = hasFilter || hasProjection;

  // Build DDL columns (determines the schema of the temporary table).
  expression_vector ddlColumns = buildDdlColumns(
      columns, validatedReturnCols, loadAs.isRelLoad(), parsingOptions);

  // If from_col/to_col caused ddlColumns to reorder relative to the file's
  // native column order, the direct file-scan path would produce data in
  // file order while ddlColumns expects [srcKey, dstKey, ...].  In that
  // case we must use the subquery path which projects columns in the
  // correct order.
  if (!useSubquery && loadAs.isRelLoad() && ddlColumns.size() >= 2 &&
      columns.size() >= 2 &&
      (ddlColumns[0]->rawName() != columns[0]->rawName() ||
       ddlColumns[1]->rawName() != columns[1]->rawName())) {
    hasProjection = true;
    useSubquery = true;
    // For the subquery's RETURN, use ddlColumns order so the projected
    // output matches what the CopyFrom operator expects.
    validatedReturnCols.clear();
    for (const auto& c : ddlColumns) {
      validatedReturnCols.push_back(c->rawName());
    }
    // Rebuild ddlColumns to match the subquery's projected output order.
    ddlColumns = buildDdlColumns(
        columns, validatedReturnCols, loadAs.isRelLoad(), parsingOptions);
  }

  // Build the data source for CopyFrom.
  std::unique_ptr<BoundBaseScanSource> copySource;
  if (useSubquery) {
    // Extract scan info from the bound source to construct LOAD FROM subquery.
    auto& tableScanSource = boundSource->constCast<BoundTableScanSource>();
    auto scanInfo = tableScanSource.info.copy();

    // Bind WHERE predicate (columns are accessible in current binder state).
    std::shared_ptr<Expression> wherePred;
    if (hasFilter) {
      wherePred = bindWhereExpression(*loadAs.getWherePredicate());
    }

    // Projection column names: ddlColumns names determine what the subquery
    // outputs. The subquery internally matches these against scanInfo's columns.
    std::vector<std::string> projColNames;
    for (const auto& col : ddlColumns) {
      projColNames.push_back(col->rawName());
    }

    copySource = buildFilteredSubquerySource(
        std::move(scanInfo), std::move(wherePred), projColNames);
  } else {
    copySource = std::move(boundSource);
  }

  // --- Build DDL info and ExtraCopyInfo ---
  std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo;
  std::unique_ptr<DDLTableInfo> ddlTableInfo;

  if (!loadAs.isRelLoad()) {
    // --- LOAD NODE TABLE ---
    auto primaryKey = extractStringOption(parsingOptions, "primary_key");
    if (primaryKey.empty()) {
      primaryKey = columns[0]->rawName();
    }
    if (!validatedReturnCols.empty()) {
      bool pkFound = false;
      for (const auto& retCol : validatedReturnCols) {
        if (retCol == primaryKey) { pkFound = true; break; }
      }
      if (!pkFound) {
        THROW_BINDER_EXCEPTION(stringFormat(
            "RETURN must include primary key column `{}` for LOAD NODE "
            "TABLE AS `{}`",
            primaryKey, labelName));
      }
    }
    ddlTableInfo = std::make_unique<DDLVertexInfo>(
        labelName, primaryKey, ddlColumns, expressionBinder,
        /*temporary=*/true);
  } else {
    // --- LOAD REL TABLE ---
    auto fromLabel = extractStringOption(parsingOptions, "from");
    auto toLabel = extractStringOption(parsingOptions, "to");
    if (fromLabel.empty() || toLabel.empty()) {
      THROW_BINDER_EXCEPTION(stringFormat(
          "LOAD REL TABLE requires `from` and `to` options naming "
          "existing vertex types."));
    }
    auto* srcCatalogEntry = bindNodeTableEntry(fromLabel);
    auto* dstCatalogEntry = bindNodeTableEntry(toLabel);
    Binder::validateNodeTableType(srcCatalogEntry);
    Binder::validateNodeTableType(dstCatalogEntry);
    auto* srcNode = srcCatalogEntry->ptrCast<NodeTableCatalogEntry>();
    auto* dstNode = dstCatalogEntry->ptrCast<NodeTableCatalogEntry>();
    auto srcTableID = srcNode->getTableID();
    auto dstTableID = dstNode->getTableID();

    if (columns.size() < 2u) {
      THROW_BINDER_EXCEPTION(stringFormat(
          "Cannot infer edge schema: need at least two columns "
          "(source key, destination key)."));
    }

    // Validate RETURN includes from_col and to_col.
    if (!validatedReturnCols.empty()) {
      auto fromCol = extractStringOption(parsingOptions, "from_col");
      auto toCol = extractStringOption(parsingOptions, "to_col");
      if (!fromCol.empty()) {
        bool found = false;
        for (const auto& r : validatedReturnCols) {
          if (r == fromCol) { found = true; break; }
        }
        if (!found) {
          THROW_BINDER_EXCEPTION(stringFormat(
              "RETURN must include from_col `{}` for LOAD REL TABLE AS `{}`",
              fromCol, labelName));
        }
      }
      if (!toCol.empty()) {
        bool found = false;
        for (const auto& r : validatedReturnCols) {
          if (r == toCol) { found = true; break; }
        }
        if (!found) {
          THROW_BINDER_EXCEPTION(stringFormat(
              "RETURN must include to_col `{}` for LOAD REL TABLE AS `{}`",
              toCol, labelName));
        }
      }
    }

    auto srcOffset = createVariable(std::string(InternalKeyword::SRC_OFFSET),
                                    LogicalType::INT64());
    auto dstOffset = createVariable(std::string(InternalKeyword::DST_OFFSET),
                                    LogicalType::INT64());
    expression_vector warningDataExprs;
    // For edge loads, ddlColumns[0] and [1] are always from_col and to_col
    // (enforced by buildDdlColumns). Use them as lookup keys.
    std::shared_ptr<Expression> srcKey = ddlColumns[0];
    std::shared_ptr<Expression> dstKey = ddlColumns[1];
    auto srcLookUpInfo =
        IndexLookupInfo(srcTableID, srcOffset, srcKey, warningDataExprs);
    auto dstLookUpInfo =
        IndexLookupInfo(dstTableID, dstOffset, dstKey, warningDataExprs);
    auto lookupInfos =
        std::vector<IndexLookupInfo>{srcLookUpInfo, dstLookUpInfo};
    auto internalIDColumnIndices = std::vector<idx_t>{0, 1, 2};
    extraInfo = std::make_unique<ExtraBoundCopyRelInfo>(
        internalIDColumnIndices, lookupInfos);

    ddlTableInfo = std::make_unique<DDLEdgeInfo>(
        labelName, fromLabel, toLabel, srcTableID, dstTableID, ddlColumns,
        expressionBinder, /*temporary=*/true);
  }

  std::vector<ColumnEvaluateType> ddlEvaluateTypes(
      ddlColumns.size(), ColumnEvaluateType::REFERENCE);
  // In the subquery path, offset is not needed: the subquery plan handles
  // row-level tracking internally. Only the direct file scan path uses offset.
  auto copyOffset = useSubquery ? nullptr : std::move(offset);
  auto boundCopyFromInfo =
      BoundCopyFromInfo(std::move(copySource), std::move(copyOffset),
                        std::move(ddlColumns), std::move(ddlEvaluateTypes),
                        std::move(extraInfo), std::move(ddlTableInfo));

  return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

}  // namespace binder
}  // namespace neug
