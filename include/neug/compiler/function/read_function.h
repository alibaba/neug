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

#include <arrow/filesystem/localfs.h>
#include <glob.h>
#include <memory>
#include <string>
#include <vector>
#include "neug/compiler/function/table/table_function.h"
#include "neug/execution/common/context.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"

namespace neug {
namespace function {

// The exec function invoked by data source operators to load data from external
// data sources.
using read_exec_func_t = std::function<execution::Context(
    std::shared_ptr<reader::ReadSharedState> state)>;

// The function used to sniff/infer file column names and their types from
// external data sources.
using read_sniff_func_t = std::function<std::shared_ptr<reader::EntrySchema>(
    const reader::FileSchema& schema)>;

struct ReadFunction : public TableFunction {
  read_exec_func_t execFunc = nullptr;
  read_sniff_func_t sniffFunc = nullptr;

  ReadFunction(std::string name, std::vector<common::LogicalTypeID> inputTypes)
      : TableFunction{std::move(name), std::move(inputTypes)} {}
};
}  // namespace function
}  // namespace neug