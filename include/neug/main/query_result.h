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

#ifndef INCLUDE_NEUG_MAIN_QUERY_RESULT_H_
#define INCLUDE_NEUG_MAIN_QUERY_RESULT_H_

#include <stddef.h>
#include <string>
#include <utility>
#include <vector>
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/results.pb.h"
#else
#include "neug/utils/proto/plan/results.pb.h"
#endif

namespace gs {
/**
 * @brief We use this class to represent the returned result of the query.
 * No extra memory should
 */

// A single record in the result set.
class RecordLine {
 public:
  RecordLine() = default;
  explicit RecordLine(std::vector<const results::Entry*> entries)
      : entries_(entries) {}

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

/**
 * @brief Container for query execution results with iterator-based access.
 *
 * QueryResult wraps a CollectiveResults protobuf message and provides
 * convenient C++ iterator-style access to the result records. It maintains an
 * internal cursor for sequential access via hasNext()/next() pattern.
 *
 * **Internal Structure:**
 * - Wraps results::CollectiveResults protobuf message
 * - Maintains cur_index_ for iteration state
 * - Provides both sequential and random access to records
 *
 * **Memory Model:** Returns pointers to internal data without copying.
 *
 * @since v0.1.0
 */
class QueryResult {
 public:
  /**
   * @brief Create QueryResult from CollectiveResults (move semantics).
   *
   * Factory method that creates a QueryResult by moving a CollectiveResults.
   *
   * @param result CollectiveResults to be moved into the QueryResult
   * @return QueryResult containing the moved results
   *
   * Implementation: Simply calls QueryResult constructor with std::move.
   *
   * @since v0.1.0
   */
  static QueryResult From(results::CollectiveResults&& result);

  /**
   * @brief Create QueryResult by deserializing from a string.
   *
   * Deserializes a CollectiveResults protobuf from string format.
   *
   * @param result_str Serialized CollectiveResults string
   * @return QueryResult containing the deserialized results
   *
   * @throws std::runtime_error if parsing fails
   *
   * Implementation: Calls CollectiveResults::ParseFromString() then moves
   * result.
   *
   * @since v0.1.0
   */
  static QueryResult From(const std::string& result_str);

  /**
   * @brief Default constructor creating empty result.
   *
   * @since v0.1.0
   */
  QueryResult() = default;

  /**
   * @brief Construct QueryResult from CollectiveResults.
   *
   * @param res CollectiveResults to be moved and stored
   *
   * Implementation: Initializes cur_index_ to 0 and moves res to result_.
   *
   * @since v0.1.0
   */
  explicit QueryResult(results::CollectiveResults&& res)
      : cur_index_(0), result_(std::move(res)) {}

  /**
   * @brief Destructor.
   *
   * @since v0.1.0
   */
  ~QueryResult() {}

  /**
   * @brief Check if there are more records to iterate.
   *
   * @return true if cur_index_ < result_.results_size(), false otherwise
   *
   * Implementation: Compares cur_index_ with result_.results_size().
   *
   * @since v0.1.0
   */
  bool hasNext() const;

  /**
   * @brief Get the next result record and advance iterator.
   *
   * Returns a RecordLine containing pointers to Entry objects from the current
   * record. Advances cur_index_ after retrieving the record.
   *
   * @return RecordLine containing const Entry* pointers to record columns
   *
   * @note Returns pointers to internal data - no memory allocation or copying
   * @note Returns empty RecordLine if no more records (logs error)
   * @note Caller should check hasNext() before calling this method
   *
   * Implementation: Gets mutable_results(cur_index_++), extracts Entry pointers
   * via record_to_entries_vec helper function.
   *
   * @since v0.1.0
   */
  RecordLine next();

  /**
   * @brief Get a record by index (random access).
   *
   * @param index Zero-based index of the record to retrieve
   * @return RecordLine containing const Entry* pointers to record columns
   *
   * @note Does not affect iterator state (cur_index_)
   * @note Returns pointers to internal data - no copying
   *
   * Implementation: Gets mutable_results(index), extracts Entry pointers.
   *
   * @since v0.1.0
   */
  RecordLine operator[](int index);

  /**
   * @brief Get total number of records in the result set.
   *
   * @return Total number of result records
   *
   * Implementation: Returns result_.results_size().
   *
   * @since v0.1.0
   */
  size_t length() const;

  /**
   * @brief Get the result schema as a string.
   *
   * @return const std::string& Reference to the schema string from
   * CollectiveResults
   *
   * Implementation: Returns result_.result_schema().
   *
   * @since v0.1.0
   */
  const std::string& get_result_schema() const;

  inline const results::CollectiveResults& get_result() const {
    return result_;
  }

 private:
  size_t cur_index_;
  results::CollectiveResults result_;
};

}  // namespace gs

#endif  // INCLUDE_NEUG_MAIN_QUERY_RESULT_H_
