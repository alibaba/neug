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
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "neug/generated/proto/response/response.pb.h"

namespace neug {
/**
 * @brief Lightweight wrapper around protobuf `QueryResponse`.
 *
 * `QueryResult` stores a full query response and exposes utility methods for:
 * - constructing from serialized protobuf bytes (`From()`),
 * - obtaining row count (`length()`),
 * - accessing response schema (`result_schema()`),
 * - serializing/deserializing (`Serialize()` / `From()`),
 * - debugging output (`ToString()`),
 * - cursor-based row traversal via `hasNext()` / `next()`,
 * - typed cell access via `GetInt32()`, `GetString()`, etc.
 */

class QueryResult {
 public:
  static QueryResult From(std::string&& serialized_table);
  static QueryResult From(const std::string& serialized_table);

  QueryResult() : response_(std::make_shared<neug::QueryResponse>()) {}

  QueryResult(QueryResult&& other) noexcept = default;
  QueryResult& operator=(QueryResult&& other) noexcept = default;

  QueryResult(const neug::QueryResponse& response)
      : response_(std::make_shared<neug::QueryResponse>(response)) {}

  QueryResult(const QueryResult& other) = delete;
  QueryResult& operator=(const QueryResult& other) = delete;

  ~QueryResult() {}

  void Swap(QueryResult& other) noexcept {
    response_.swap(other.response_);
    std::swap(current_row_index_, other.current_row_index_);
  }

  void Swap(QueryResult&& other) noexcept {
    response_.swap(other.response_);
    std::swap(current_row_index_, other.current_row_index_);
  }

  // ---------------------------------------------------------------------------
  // Cursor-based row traversal (DuckDB style)
  // ---------------------------------------------------------------------------

  /**
   * @brief Check whether there are more rows to consume.
   */
  bool hasNext() const;

  /**
   * @brief Advance the cursor to the next row.
   *
   * Throws if no more rows are available (check hasNext() first).
   */
  void next();

  /**
   * @brief Reset the internal cursor back to the first row.
   */
  void Reset() { current_row_index_ = 0; }

  /**
   * @brief Return the current cursor position (0-based row index).
   */
  size_t CurrentRowIndex() const { return current_row_index_; }

  // ---------------------------------------------------------------------------
  // Typed value accessors — read from the current cursor row
  // Supports both column index and column name.
  // ---------------------------------------------------------------------------

  /**
   * @brief Check whether the cell at current row is NULL.
   */
  bool IsNull(size_t column_index) const;
  bool IsNull(const std::string& column_name) const;

  int32_t GetInt32(size_t column_index) const;
  int32_t GetInt32(const std::string& column_name) const;
  uint32_t GetUInt32(size_t column_index) const;
  uint32_t GetUInt32(const std::string& column_name) const;
  int64_t GetInt64(size_t column_index) const;
  int64_t GetInt64(const std::string& column_name) const;
  uint64_t GetUInt64(size_t column_index) const;
  uint64_t GetUInt64(const std::string& column_name) const;
  float GetFloat(size_t column_index) const;
  float GetFloat(const std::string& column_name) const;
  double GetDouble(size_t column_index) const;
  double GetDouble(const std::string& column_name) const;
  std::string GetString(size_t column_index) const;
  std::string GetString(const std::string& column_name) const;
  bool GetBool(size_t column_index) const;
  bool GetBool(const std::string& column_name) const;

  /**
   * @brief Get the number of columns.
   */
  size_t ColumnCount() const;

  /**
   * @brief Get column names from schema.
   */
  std::vector<std::string> ColumnNames() const;

  // ---------------------------------------------------------------------------
  // Existing interface (kept for backward compatibility)
  // ---------------------------------------------------------------------------

  /**
   * @brief Convert entire result set to string.
   */
  std::string ToString() const;

  /**
   * @brief Convert the current cursor row to a human-readable string.
   *
   * Produces a comma-separated list of the row's column values (NULL cells are
   * rendered as "null"). Useful for printing rows while iterating with
   * hasNext()/next(). Throws if the cursor is past the end of the result set.
   */
  std::string GetCurrentRowAsString() const;

  /**
   * @brief Get total number of rows.
   */
  size_t length() const { return response_->row_count(); }

  /**
   * @brief Get result schema metadata.
   */
  const neug::MetaDatas& result_schema() const { return response_->schema(); }

  /**
   * @brief Get underlying protobuf response (const reference).
   */
  const neug::QueryResponse& response() const { return *response_; }

  /**
   * @brief Get shared ownership of the underlying protobuf response.
   *
   * Useful when callers need to extend the lifetime of the response beyond
   * the QueryResult (e.g. zero-copy Arrow export).
   */
  std::shared_ptr<const neug::QueryResponse> shared_response() const {
    return response_;
  }

  /**
   * @brief Serialize entire result set to string.
   */
  std::string Serialize() const;

 private:
  void ValidateCursorAccess(size_t column_index) const;
  size_t GetColumnIndex(const std::string& column_name) const;
  const neug::Array& GetColumn(size_t column_index) const;

  std::shared_ptr<neug::QueryResponse> response_;
  size_t current_row_index_ = 0;
};

}  // namespace neug
