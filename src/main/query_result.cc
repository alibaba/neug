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
#include <memory>
#include <ostream>
#include "neug/utils/exception/exception.h"

namespace neug {

QueryResult QueryResult::From(results::CollectiveResults&& result) {
  return QueryResult(std::move(result));
}

QueryResult QueryResult::From(const std::string& result_str) {
  results::CollectiveResults result;
  if (!result.ParseFromString(result_str)) {
    THROW_RUNTIME_ERROR("Failed to parse CollectiveResults from string");
  }
  return QueryResult(std::move(result));
}

bool QueryResult::hasNext() const {
  return cur_index_ < (size_t) result_.results_size();
}

size_t QueryResult::length() const { return result_.results_size(); }

std::string QueryResult::ToString() const {
  std::string result = "QueryResult: [\n";
  for (size_t i = 0; i < length(); ++i) {
    result += "  " + (*this)[i].ToString() + "\n";
  }
  result += "]";
  return result;
}

RecordLine record_to_entries_vec(const results::Record* result) {
  std::vector<const results::Entry*> entries;
  for (int32_t i = 0; i < result->columns_size(); ++i) {
    // auto ptr = result->mutable_columns(i)->mutable_entry();
    auto ptr = &result->columns(i).entry();
    entries.emplace_back(ptr);
  }
  return RecordLine(entries);
}

RecordLine QueryResult::next() {
  if (!hasNext()) {
    LOG(ERROR) << "No more records to return";
    return RecordLine();
  }
  size_t cur_ind = cur_index_++;
  // We just got the pointers, and don't own them.
  auto result = result_.mutable_results(cur_ind)->mutable_record();
  return record_to_entries_vec(result);
}

RecordLine QueryResult::operator[](int index) const {
  if (index < 0 || index >= static_cast<int>(result_.results_size())) {
    THROW_RUNTIME_ERROR("Index out of range");
  }
  return record_to_entries_vec(&result_.results(index).record());
}

const std::string& QueryResult::get_result_schema() const {
  return result_.result_schema();
}
}  // namespace neug
