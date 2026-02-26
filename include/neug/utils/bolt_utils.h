/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <arrow/api.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/util/macros.h>
#include <arrow/util/time.h>
#include <arrow/util/value_parsing.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include "glog/logging.h"

namespace neug {

rapidjson::Value create_bolt_summary(
    const std::string& query_text,
    rapidjson::Document::AllocatorType& allocator);

std::string arrow_table_to_bolt_response(
    const std::shared_ptr<arrow::Table>& table);

std::string arrow_table_to_bolt_response(
    const arrow::Table& table, const std::vector<std::string>& column_names);

rapidjson::Value arrow_path_to_bolt_path(
    std::shared_ptr<arrow::StructArray> path_struct, int64_t index,
    rapidjson::Value& nodes, rapidjson::Value& edges,
    std::unordered_set<int64_t>& node_ids,
    std::unordered_set<int64_t>& edge_ids,
    rapidjson::Document::AllocatorType& allocator);

}  // namespace neug
