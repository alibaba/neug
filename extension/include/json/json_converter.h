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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <rapidjson/document.h>
#include <memory>
#include <vector>

#include "neug/utils/property/types.h"

namespace neug {
namespace extension {

// Create an Arrow Builder corresponding to the given DataTypeId
std::unique_ptr<arrow::ArrayBuilder> createArrowBuilder(DataTypeId type);

// Append a rapidjson Value to an Arrow Builder
// @return true if successful, false if failed (null will be appended)
bool appendJsonValueToBuilder(arrow::ArrayBuilder* builder,
                              const rapidjson::Value& val, DataTypeId type);

// Infer DataTypeId from a JSON value
DataTypeId inferPropertyTypeFromValue(const rapidjson::Value& val);

// Merge two PropertyTypes (choose the more general type)
DataTypeId mergePropertyTypes(DataTypeId a, DataTypeId b);

}  // namespace extension
}  // namespace neug