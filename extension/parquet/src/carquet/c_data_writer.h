/** Copyright 2020 Alibaba Group Holding Limited.
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

#include <carquet/carquet.h>

#include <cstdint>
#include <string>
#include <vector>

#include "neug/generated/proto/response/response.pb.h"
#include "neug/utils/result.h"

namespace neug::parquet {

// Shared by response validation and both scalar/nested write paths.
const std::string& responseArrayValidity(const Array& array);

/**
 * Writes one QueryResponse slice through Carquet's standard C Data ABI.
 *
 * The temporary schema and array trees own every buffer until Carquet consumes
 * them. This bridge uses only the ABI structs declared by Carquet and does not
 * link the Arrow C++ SDK.
 * The caller must validate the complete response (including child lengths,
 * offsets, validity bitmaps and nesting depth) before calling this helper.
 */
Status writeCarquetCDataBatch(carquet_writer_t* writer,
                              const QueryResponse& table, int32_t begin,
                              int32_t count,
                              const std::vector<std::string>& columnNames);

}  // namespace neug::parquet
