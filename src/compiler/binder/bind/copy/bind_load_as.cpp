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
#include "neug/compiler/binder/ddl/bound_create_table_info.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression_binder.h"
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

std::unique_ptr<BoundStatement> Binder::bindLoadAs(
    const Statement& statement) {
  auto& loadAs = statement.constCast<LoadAs>();
  const auto& parsingOptions = loadAs.getParsingOptions();
  auto boundSource = bindScanSource(loadAs.getSource(),
                                    parsingOptions, {}, {});
  expression_vector columns = boundSource->getColumns();
  std::vector<ColumnEvaluateType> evaluateTypes(
      columns.size(), ColumnEvaluateType::REFERENCE);
  auto offset = createInvisibleVariable(
      std::string(InternalKeyword::ROW_OFFSET), LogicalType::INT64());
  const auto& labelName = loadAs.getTargetLabel();

  if (columns.empty()) {
    THROW_BINDER_EXCEPTION(stringFormat(
        "No columns found for LOAD AS `{}`, cannot infer schema", labelName));
  }

  // Validate RETURN columns early so they can filter DDL properties.
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

  // When RETURN is specified, only those columns become vertex/edge properties.
  expression_vector ddlColumns = columns;
  if (!validatedReturnCols.empty()) {
    ddlColumns.clear();
    if (loadAs.isRelLoad()) {
      // For edges, from_col and to_col must come first in DDL order.
      auto fromCol = extractStringOption(parsingOptions, "from_col");
      auto toCol = extractStringOption(parsingOptions, "to_col");
      if (!fromCol.empty()) {
        for (const auto& srcCol : columns) {
          if (srcCol->rawName() == fromCol) {
            ddlColumns.push_back(srcCol);
            break;
          }
        }
      }
      if (!toCol.empty()) {
        for (const auto& srcCol : columns) {
          if (srcCol->rawName() == toCol) {
            ddlColumns.push_back(srcCol);
            break;
          }
        }
      }
      // Append remaining return columns.
      for (const auto& retCol : validatedReturnCols) {
        if (retCol == fromCol || retCol == toCol) continue;
        for (const auto& srcCol : columns) {
          if (srcCol->rawName() == retCol) {
            ddlColumns.push_back(srcCol);
            break;
          }
        }
      }
    } else {
      for (const auto& retCol : validatedReturnCols) {
        for (const auto& srcCol : columns) {
          if (srcCol->rawName() == retCol) {
            ddlColumns.push_back(srcCol);
            break;
          }
        }
      }
    }
  }

  std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo;
  std::unique_ptr<DDLTableInfo> ddlTableInfo;

  if (!loadAs.isRelLoad()) {
    // --- LOAD NODE TABLE ---
    auto primaryKey = extractStringOption(parsingOptions, "primary_key");
    if (primaryKey.empty()) {
      primaryKey = columns[0]->rawName();
    }
    // Validate RETURN includes primary key.
    if (!validatedReturnCols.empty()) {
      bool pkFound = false;
      for (const auto& retCol : validatedReturnCols) {
        if (retCol == primaryKey) {
          pkFound = true;
          break;
        }
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
        bool fromFound = false;
        for (const auto& retCol : validatedReturnCols) {
          if (retCol == fromCol) {
            fromFound = true;
            break;
          }
        }
        if (!fromFound) {
          THROW_BINDER_EXCEPTION(stringFormat(
              "RETURN must include from_col `{}` for LOAD REL TABLE AS `{}`",
              fromCol, labelName));
        }
      }
      if (!toCol.empty()) {
        bool toFound = false;
        for (const auto& retCol : validatedReturnCols) {
          if (retCol == toCol) {
            toFound = true;
            break;
          }
        }
        if (!toFound) {
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
    std::shared_ptr<Expression> srcKey = columns[0];
    std::shared_ptr<Expression> dstKey = columns[1];
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

  auto boundCopyFromInfo =
      BoundCopyFromInfo(std::move(boundSource), std::move(offset),
                        std::move(columns), std::move(evaluateTypes),
                        std::move(extraInfo), std::move(ddlTableInfo));

  // Bind optional WHERE predicate for filter pushdown.
  if (loadAs.hasWherePredicate()) {
    auto wherePredicate = bindWhereExpression(*loadAs.getWherePredicate());
    boundCopyFromInfo.wherePredicate = std::move(wherePredicate);
  }

  // Store validated return columns for projection pushdown.
  if (!validatedReturnCols.empty()) {
    boundCopyFromInfo.returnColumns = std::move(validatedReturnCols);
  }

  return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

}  // namespace binder
}  // namespace neug
