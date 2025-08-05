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
#include "neug/utils/exception/exception.h"

#include <glog/logging.h>

namespace gs {

QueryResult QueryResult::From(results::CollectiveResults&& result) {
  return QueryResult(std::move(result));
}

QueryResult QueryResult::From(const std::string& result_str) {
  results::CollectiveResults result;
  if (!result.ParseFromString(result_str)) {
    throw exception::RuntimeError(
        "Failed to parse CollectiveResults from string");
  }
  return QueryResult(std::move(result));
}

bool QueryResult::hasNext() const {
  return cur_index_ < (size_t) result_.results_size();
}

size_t QueryResult::length() const { return result_.results_size(); }

RecordLine QueryResult::next() {
  if (!hasNext()) {
    LOG(ERROR) << "No more records to return";
    return RecordLine();
  }
  // We just got the pointers, and don't own them.
  std::vector<const results::Entry*> entries;
  size_t cur_ind = cur_index_++;
  auto result = result_.mutable_results(cur_ind)->mutable_record();
  for (int32_t i = 0; i < result->columns_size(); ++i) {
    // Although we use mutable_columns, we don't mutate the data.
    auto ptr = result->mutable_columns(i)->mutable_entry();
    // cast to a const pointer and points to const data
    auto entry = reinterpret_cast<const results::Entry*>(ptr);
    entries.emplace_back(entry);
  }
  return RecordLine(entries);
}

const std::string& QueryResult::get_result_schema() const {
  return result_.result_schema();
}
}  // namespace gs
