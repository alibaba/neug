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
 * - debugging output (`ToString()`).
 *
 * It does not provide row-iterator semantics such as `hasNext()/next()`.
 */
class QueryResult {
 public:
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

 private:
  neug::QueryResponse response_;
};

}  // namespace neug
