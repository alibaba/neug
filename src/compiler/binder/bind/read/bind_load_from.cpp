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

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_scan_source.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/query/reading_clause/bound_load_from.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/parser/query/reading_clause/load_from.h"
#include "neug/compiler/parser/scan_source.h"
#include "neug/utils/exception/exception.h"

using namespace neug::function;
using namespace neug::common;
using namespace neug::parser;
using namespace neug::catalog;

namespace neug {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindLoadFrom(
    const ReadingClause& readingClause) {
  auto& loadFrom = readingClause.constCast<LoadFrom>();
  auto source = loadFrom.getSource();
  std::unique_ptr<BoundLoadFrom> boundLoadFrom;
  std::vector<std::string> columnNames;
  std::vector<LogicalType> columnTypes;
  switch (source->type) {
  case ScanSourceType::OBJECT: {
    auto objectSource = source->ptrCast<ObjectScanSource>();
    auto boundScanSource = bindObjectScanSource(
        *objectSource, loadFrom.getParsingOptions(), columnNames, columnTypes);
    auto& scanInfo = boundScanSource->constCast<BoundTableScanSource>().info;
    boundLoadFrom = std::make_unique<BoundLoadFrom>(scanInfo.copy());
  } break;
  case ScanSourceType::FILE: {
    auto boundScanSource = bindFileScanSource(
        *source, loadFrom.getParsingOptions(), columnNames, columnTypes);
    auto& scanInfo = boundScanSource->constCast<BoundTableScanSource>().info;
    auto& bindData = scanInfo.bindData->cast<function::ScanFileBindData>();
    auto& fileInfo = bindData.fileScanInfo;
    // We support LOAD FROM by ArrowArrayContextColumn with BATCH_READ set to
    // false.
    fileInfo.options.insert({"BATCH_READ", common::Value::createValue(false)});
    boundLoadFrom = std::make_unique<BoundLoadFrom>(scanInfo.copy());
  } break;
  default:
    THROW_BINDER_EXCEPTION(
        stringFormat("LOAD FROM subquery is not supported."));
  }
  if (!columnTypes.empty()) {
    auto info = boundLoadFrom->getInfo();
    for (auto i = 0u; i < columnTypes.size(); ++i) {
      ExpressionUtil::validateDataType(*info->bindData->columns[i],
                                       columnTypes[i]);
    }
  }
  if (loadFrom.hasWherePredicate()) {
    auto wherePredicate = bindWhereExpression(*loadFrom.getWherePredicate());
    boundLoadFrom->setPredicate(std::move(wherePredicate));
  }
  return boundLoadFrom;
}

}  // namespace binder
}  // namespace neug
