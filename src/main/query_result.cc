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

#include "neug/main/query_result.h"

#include <glog/logging.h>
#include <stdint.h>
#include <cstring>
#include <memory>
#include <ostream>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"

#include <arrow/api.h>
#include <arrow/array/concatenate.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/scalar.h>
#include <arrow/type.h>

namespace neug {

std::string QueryResult::ToString() const { return response_.DebugString(); }

QueryResult QueryResult::From(const std::string& serialized_table) {
  return From(std::string(serialized_table));
}

QueryResult QueryResult::From(std::string&& serialized_table) {
  QueryResult result;
  if (!result.response_.ParseFromString(serialized_table)) {
    LOG(ERROR) << "Failed to parse QueryResponse from string";
  }
  return result;
}

std::string QueryResult::Serialize() const {
  std::string serialized_response;
  if (!response_.SerializeToString(&serialized_response)) {
    LOG(ERROR) << "Failed to serialize QueryResponse to string";
    THROW_RUNTIME_ERROR("Failed to serialize QueryResponse to string");
  }
  return serialized_response;
}

}  // namespace neug
