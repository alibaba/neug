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

#include "neug/compiler/function/gds/gds_algo_function.h"

namespace neug {
namespace gds {
struct NEUG_API PersonalizedPageRankFunction {
  static constexpr const char* name = "PERSONALIZED_PAGE_RANK";
  static execution::Context PersonalizedPageRankExec(
      execution::Context& ctx, const ::physical::Subgraph& subgraph,
      const function::options_t& options, IStorageInterface& g);

  static function::function_set getFunctionSet() {
    function::function_set funcSet;
    // two input params:
    // 1. subgraph name in string
    // 2. options in map
    std::vector<common::LogicalTypeID> inputTypes = {
        common::LogicalTypeID::STRING, common::LogicalTypeID::ANY};
    // two output columns:
    // 1. node type
    // 2. page rank value in double
    function::call_output_columns outputColumns = {
        {"node", common::LogicalTypeID::NODE},
        {"page_rank", common::LogicalTypeID::DOUBLE}};
    auto function = std::make_unique<function::GDSAlgoFunction>(
        name, inputTypes, outputColumns);
    function->algoExec = PersonalizedPageRankExec;
    funcSet.emplace_back(std::move(function));
    return funcSet;
  }
};
}  // namespace gds
}  // namespace neug
