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

#pragma once

#ifndef _WIN32
#include <glob.h>
#endif
#include <memory>
#include <string>
#include <vector>
#include "neug/compiler/function/table/table_function.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/reader.h"

namespace neug {
class IDataChunkSupplier;
namespace function {

// Required factory for a single execution's incremental reader.
using read_supplier_func_t = std::function<std::shared_ptr<IDataChunkSupplier>(
    std::shared_ptr<reader::ReadSharedState>)>;

// The function used to sniff/infer file column names and their types from
// external data sources.
using read_sniff_func_t = std::function<std::shared_ptr<reader::EntrySchema>(
    const reader::FileSchema& schema)>;

struct ReadFunction : public TableFunction {
  read_supplier_func_t supplierFunc;
  read_sniff_func_t sniffFunc = nullptr;

  ReadFunction(std::string name, std::vector<common::DataTypeId> inputTypes,
               read_supplier_func_t supplier)
      : TableFunction{std::move(name), std::move(inputTypes)},
        supplierFunc(std::move(supplier)) {
    if (!supplierFunc) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "ReadFunction requires a supplier factory");
    }
  }
};
}  // namespace function
}  // namespace neug
