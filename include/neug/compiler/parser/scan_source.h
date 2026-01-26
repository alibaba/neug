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
#include <string>
#include <vector>

#include "expression/parsed_expression.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/enums/scan_source_type.h"
#include "neug/compiler/parser/statement.h"

namespace neug {
namespace parser {

struct BaseScanSource {
  common::ScanSourceType type;

  explicit BaseScanSource(common::ScanSourceType type) : type{type} {}
  virtual ~BaseScanSource() = default;
  DELETE_COPY_AND_MOVE(BaseScanSource);

  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }
  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }
};

struct FileScanSource : public BaseScanSource {
  std::vector<std::string> filePaths;

  explicit FileScanSource(std::vector<std::string> paths)
      : BaseScanSource{common::ScanSourceType::FILE},
        filePaths{std::move(paths)} {}
};

struct ObjectScanSource : public BaseScanSource {
  // If multiple object presents, assuming they have a nested structure.
  // E.g. for postgres.person, objectNames should be [postgres, person]
  std::vector<std::string> objectNames;

  explicit ObjectScanSource(std::vector<std::string> objectNames)
      : BaseScanSource{common::ScanSourceType::OBJECT},
        objectNames{std::move(objectNames)} {}
};

struct QueryScanSource : public BaseScanSource {
  std::unique_ptr<Statement> statement;

  explicit QueryScanSource(std::unique_ptr<Statement> statement)
      : BaseScanSource{common::ScanSourceType::QUERY},
        statement{std::move(statement)} {}
};

struct TableFuncScanSource : public BaseScanSource {
  std::unique_ptr<ParsedExpression> functionExpression = nullptr;

  explicit TableFuncScanSource(
      std::unique_ptr<ParsedExpression> functionExpression)
      : BaseScanSource{common::ScanSourceType::TABLE_FUNC},
        functionExpression{std::move(functionExpression)} {}
};

}  // namespace parser
}  // namespace neug
