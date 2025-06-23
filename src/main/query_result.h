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

#ifndef SRC_MAIN_QUERY_RESULT_H_
#define SRC_MAIN_QUERY_RESULT_H_

#include <memory>
#include "src/proto_generated_gie/results.pb.h"
#include "src/utils/result.h"

namespace gs {
/**
 * @brief We use this class to represent the returned result of the query.
 * No extra memory should
 */

// A single record in the result set.
class RecordLine {
 public:
  RecordLine() = default;
  RecordLine(std::vector<const results::Entry*> entries) : entries_(entries) {}

  std::string ToString() const {
    std::string result;
    result += "<";
    for (size_t i = 0; i < entries_.size(); ++i) {
      if (i != 0) {
        result += ", ";
      }
      result += entries_[i]->ShortDebugString();
    }
    result += ">";
    return result;
  }

  const std::vector<const results::Entry*>& entries() const { return entries_; }

 private:
  std::vector<const results::Entry*> entries_;
};

class QueryResult {
 public:
  static QueryResult From(
      std::tuple<results::CollectiveResults, std::string>&& result);

  QueryResult() = default;

  QueryResult(results::CollectiveResults&& res, std::string&& schema)
      : cur_index_(0),
        result_(std::move(res)),
        result_schema_(std::move(schema)) {}
  ~QueryResult() {}

  bool hasNext() const;

  /**
   * @brief Get the next result.
   * @note The result is a shared pointer, so the user should not delete it. We
   * return the value that is stored in the result_, don't allocate new memory.
   * @return std::vector<std::shared_ptr<results::Entry>>
   */
  RecordLine next();

  size_t length() const;

  const std::string& get_result_schema() const;  // YAML string

 private:
  size_t cur_index_;
  results::CollectiveResults result_;
  std::string result_schema_;  // result schema in YAML format
};
}  // namespace gs

#endif  // SRC_MAIN_QUERY_RESULT_H_