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

#pragma once

#include <memory>
#include "index_look_up_info.h"
#include "neug/compiler/binder/bound_scan_source.h"
#include "neug/compiler/binder/ddl/bound_create_table_info.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/enums/column_evaluate_type.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_rel_table_entry.h"

namespace neug {
namespace binder {

struct ExtraBoundCopyFromInfo {
  virtual ~ExtraBoundCopyFromInfo() = default;
  virtual std::unique_ptr<ExtraBoundCopyFromInfo> copy() const = 0;

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
};

// Supplemental DDL for COPY when the target vertex/edge type is inferred from
// scan columns (not parser::ExtraCreateTableInfo — that name lives in parser
// for parsed CREATE TABLE).
struct NEUG_API DDLTableInfo {
  virtual ~DDLTableInfo() = default;
  virtual catalog::TableCatalogEntry* getTableEntry() const = 0;
  virtual binder::BoundCreateTableInfo getCreateInfo() const = 0;
};

struct NEUG_API DDLVertexInfo : public DDLTableInfo {
  // create inner node table entry and table info from the given parameters
  DDLVertexInfo(const std::string& vertexLabelName,
                const std::string& primaryKeyName,
                const expression_vector& columns, ExpressionBinder& binder);

  // return vertex label name
  std::string getVertexLabelName();

  catalog::TableCatalogEntry* getTableEntry() const override;
  binder::BoundCreateTableInfo getCreateInfo() const override;

 private:
  std::unique_ptr<catalog::NodeTableCatalogEntry> nodeTableEntry;
  binder::BoundCreateTableInfo createTableInfo;
};

struct NEUG_API DDLEdgeInfo : public DDLTableInfo {
  DDLEdgeInfo(const std::string& edgeLabelName, const std::string& srcLabelName,
              const std::string& dstLabelName, common::table_id_t srcLabelID,
              common::table_id_t dstLabelID, const expression_vector& columns,
              ExpressionBinder& binder);
  // return edge label name
  std::string getEdgeLabelName();
  std::string getSrcLabelName();
  std::string getDstLabelName();

  catalog::TableCatalogEntry* getTableEntry() const override;
  binder::BoundCreateTableInfo getCreateInfo() const override;

 private:
  std::string srcLabelName_;
  std::string dstLabelName_;
  std::unique_ptr<catalog::GRelTableCatalogEntry> relTableEntry;
  binder::BoundCreateTableInfo createTableInfo;
};

struct NEUG_API BoundCopyFromInfo {
  // Table entry to copy into.
  catalog::TableCatalogEntry* tableEntry;
  // Data source.
  std::unique_ptr<BoundBaseScanSource> source;
  // Row offset.
  std::shared_ptr<Expression> offset;
  expression_vector columnExprs;
  std::vector<common::ColumnEvaluateType> columnEvaluateTypes;
  std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo;
  // extra table info to create vertex or edge type from inferred results
  std::shared_ptr<DDLTableInfo> ddlTableInfo;

  BoundCopyFromInfo(catalog::TableCatalogEntry* tableEntry,
                    std::unique_ptr<BoundBaseScanSource> source,
                    std::shared_ptr<Expression> offset,
                    expression_vector columnExprs,
                    std::vector<common::ColumnEvaluateType> columnEvaluateTypes,
                    std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo)
      : tableEntry{tableEntry},
        source{std::move(source)},
        offset{std::move(offset)},
        columnExprs{std::move(columnExprs)},
        columnEvaluateTypes{std::move(columnEvaluateTypes)},
        extraInfo{std::move(extraInfo)},
        ddlTableInfo{nullptr} {}

  // to support vertex or edge type not exist in catalog
  // tableEntry should not be null, need to set tableEntry with
  // extraTableInfo->getTableEntry()
  BoundCopyFromInfo(std::unique_ptr<BoundBaseScanSource> source,
                    std::shared_ptr<Expression> offset,
                    expression_vector columnExprs,
                    std::vector<common::ColumnEvaluateType> columnEvaluateTypes,
                    std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo,
                    std::unique_ptr<DDLTableInfo> extraTableInfo);

  EXPLICIT_COPY_DEFAULT_MOVE(BoundCopyFromInfo);

  expression_vector getSourceColumns() const {
    return source ? source->getColumns() : expression_vector{};
  }
  common::column_id_t getNumWarningColumns() const {
    return source ? source->getNumWarningDataColumns() : 0;
  }
  bool getIgnoreErrorsOption() const {
    return source ? source->getIgnoreErrorsOption() : false;
  }

 private:
  BoundCopyFromInfo(const BoundCopyFromInfo& other)
      : offset{other.offset},
        columnExprs{other.columnExprs},
        columnEvaluateTypes{other.columnEvaluateTypes} {
    // ref to table entry in catalog or ddl table info
    tableEntry = other.tableEntry;
    // Shadow copy of ddl table info to avoid extra overhead
    ddlTableInfo = other.ddlTableInfo;
    source = other.source ? other.source->copy() : nullptr;
    if (other.extraInfo) {
      extraInfo = other.extraInfo->copy();
    }
  }
};

struct ExtraBoundCopyRelInfo final : ExtraBoundCopyFromInfo {
  // We process internal ID column as offset (INT64) column until partitioner.
  // In partitioner, we need to manually change offset(INT64) type to internal
  // ID type.
  std::vector<common::idx_t> internalIDColumnIndices;
  std::vector<IndexLookupInfo> infos;

  ExtraBoundCopyRelInfo(std::vector<common::idx_t> internalIDColumnIndices,
                        std::vector<IndexLookupInfo> infos)
      : internalIDColumnIndices{std::move(internalIDColumnIndices)},
        infos{std::move(infos)} {}
  ExtraBoundCopyRelInfo(const ExtraBoundCopyRelInfo& other) = default;

  std::unique_ptr<ExtraBoundCopyFromInfo> copy() const override {
    return std::make_unique<ExtraBoundCopyRelInfo>(*this);
  }
};

class BoundCopyFrom final : public BoundStatement {
  static constexpr common::StatementType statementType_ =
      common::StatementType::COPY_FROM;

 public:
  explicit BoundCopyFrom(BoundCopyFromInfo info)
      : BoundStatement{statementType_,
                       BoundStatementResult::createSingleStringColumnResult()},
        info{std::move(info)} {}

  const BoundCopyFromInfo* getInfo() const { return &info; }

 private:
  BoundCopyFromInfo info;
};

}  // namespace binder
}  // namespace neug
