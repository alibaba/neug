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
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "neug/generated/proto/response/response.pb.h"

namespace neug {
class RowView {
 public:
  RowView(const neug::QueryResponse* response, size_t row_index)
      : response_(response), row_index_(row_index) {}

  std::string ToString() const;

 private:
  const neug::QueryResponse* response_ = nullptr;
  size_t row_index_ = 0;
};

/**
 * @brief Lightweight wrapper around protobuf `QueryResponse`.
 *
 * `QueryResult` stores a full query response and exposes utility methods for:
 * - constructing from serialized protobuf bytes (`From()`),
 * - obtaining row count (`length()`),
 * - accessing response schema (`result_schema()`),
 * - serializing/deserializing (`Serialize()` / `From()`),
 * - debugging output (`ToString()`),
 * - read-only row traversal via C++ range-for (`begin()/end()`).
 *
 * Note: traversal currently provides row index + column access to raw protobuf
 * arrays through `RowView`, rather than materialized typed cell values.
 */

class QueryResult {
 public:
  class const_iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = RowView;
    using difference_type = std::ptrdiff_t;
    using pointer = void;
    using reference = RowView;

    const_iterator() = default;
    const_iterator(const neug::QueryResponse* response, size_t row_index)
        : response_(response), row_index_(row_index) {}

    value_type operator*() const { return RowView(response_, row_index_); }

    const_iterator& operator++() {
      ++row_index_;
      return *this;
    }

    const_iterator operator++(int) {
      const_iterator tmp(*this);
      ++(*this);
      return tmp;
    }

    bool operator==(const const_iterator& other) const {
      return response_ == other.response_ && row_index_ == other.row_index_;
    }

    bool operator!=(const const_iterator& other) const {
      return !(*this == other);
    }

   private:
    const neug::QueryResponse* response_ = nullptr;
    size_t row_index_ = 0;
  };

  static QueryResult From(std::string&& serialized_table);
  static QueryResult From(const std::string& serialized_table);

  QueryResult() = default;

  QueryResult(QueryResult&& other) noexcept = default;

  QueryResult(const neug::QueryResponse& response) {
    response_.CopyFrom(response);
  }

  QueryResult(const QueryResult& other) = delete;
  QueryResult& operator=(const QueryResult& other) = delete;

  ~QueryResult() {}

  void Swap(QueryResult& other) noexcept { response_.Swap(&other.response_); }

  void Swap(QueryResult&& other) noexcept { response_.Swap(&other.response_); }

  /**
   * @brief Convert entire result set to string.
   */
  std::string ToString() const;

  /**
   * @brief Get total number of rows.
   */
  size_t length() const { return response_.row_count(); }

  /**
   * @brief Get result schema metadata.
   */
  const neug::MetaDatas& result_schema() const { return response_.schema(); }

  /**
   * @brief Get underlying protobuf response.
   */
  const neug::QueryResponse& response() const { return response_; }

  /**
   * @brief Serialize entire result set to string.
   */
  std::string Serialize() const;

  /**
   * @brief Begin iterator for range-for traversal by row index.
   */
  const_iterator begin() const { return const_iterator(&response_, 0); }

  /**
   * @brief End iterator for range-for traversal by row index.
   */
  const_iterator end() const {
    return const_iterator(&response_,
                          static_cast<size_t>(response_.row_count()));
  }

  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }

 private:
  neug::QueryResponse response_;
};

}  // namespace neug
