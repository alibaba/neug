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

#include <stddef.h>
#include <string>
#include <utility>
#include <vector>
#include "neug/generated/proto/plan/results.pb.h"

namespace neug {

/**
 * @brief A single row/record from query results.
 *
 * RecordLine represents one row of query output, containing multiple
 * column values (entries). Each entry corresponds to a RETURN clause
 * expression in the Cypher query.
 *
 * **Usage Example:**
 * @code{.cpp}
 * auto result = conn->Query("MATCH (n:Person) RETURN n.name, n.age", "read");
 * for (auto& record : result.value()) {
 *   // Access entries by index
 *   const auto& entries = record.entries();
 *   // entries[0] = name, entries[1] = age
 *   std::cout << record.ToString() << std::endl;
 * }
 * @endcode
 *
 * @note Entries are pointers to internal data; do not store beyond result lifetime.
 *
 * @see QueryResult For iterating over all records
 *
 * @since v0.1.0
 */
class RecordLine {
 public:
  RecordLine() = default;

  /**
   * @brief Construct RecordLine from entry pointers.
   * @param entries Vector of pointers to result entries
   */
  explicit RecordLine(std::vector<const results::Entry*> entries)
      : entries_(entries) {}

  /**
   * @brief Convert record to string representation.
   * @return String representation of all entries
   */
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

  /**
   * @brief Get all entries (column values) in this record.
   * @return Vector of const pointers to Entry objects
   */
  const std::vector<const results::Entry*>& entries() const { return entries_; }

 private:
  std::vector<const results::Entry*> entries_;
};

/**
 * @brief Container for Cypher query execution results.
 *
 * QueryResult provides convenient access to query results through both
 * iterator-style (hasNext/next) and random access (operator[]) patterns.
 * Results are returned as RecordLine objects containing Entry pointers.
 *
 * **Usage Example:**
 * @code{.cpp}
 * auto result = conn->Query("MATCH (n:Person) RETURN n.name, n.age LIMIT 100");
 *
 * if (result.has_value()) {
 *   QueryResult& qr = result.value();
 *
 *   // Method 1: Range-based for loop
 *   for (auto& record : qr) {
 *     std::cout << record.ToString() << std::endl;
 *   }
 *
 *   // Method 2: Random access by index
 *   for (size_t i = 0; i < qr.length(); ++i) {
 *     RecordLine record = qr[i];
 *   }
 *
 *   // Get result schema
 *   std::cout << "Schema: " << qr.get_result_schema() << std::endl;
 * }
 * @endcode
 *
 * **Memory Model:**
 * - QueryResult owns the underlying protobuf data
 * - RecordLine contains pointers to internal data (no copying)
 * - Do not access records after QueryResult is destroyed
 *
 * @note Thread-safe for read-only access after construction.
 *
 * @see RecordLine For accessing individual record data
 * @see Connection::Query For creating QueryResult objects
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
  RecordLine operator[](int index) const;

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

  /* Convert the QueryResult to a string representation.
   *
   * @return std::string String representation of the QueryResult
   *
   * Implementation: Iterates over all records and calls ToString() on each
   * RecordLine, concatenating results.
   *
   * @since v0.1.0
   */
  std::string ToString() const;

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

}  // namespace neug
