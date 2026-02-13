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
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types/value.h"
#include "neug/generated/proto/plan/results.pb.h"
#include "neug/main/serialization_protocol.h"

#include <arrow/api.h>

namespace neug {

/**
 * @brief QueryResult purely based on C++ Arrow Table.
 *
 * Provides iterator-style access to query results stored in Arrow format.
 * Supports hasNext()/next() pattern for sequential iteration and random
 * access via operator[].
 *
 * The underlying Arrow Table may have chunked columns. This implementation
 * combines chunks for easier access.
 */
class QueryResult {
 public:
  static QueryResult From(std::string&& serialized_table);
  static QueryResult From(const std::string& serialized_table);

  QueryResult() = default;

  QueryResult(QueryResult&& other) noexcept = default;

  QueryResult(const QueryResult& other) = delete;
  QueryResult& operator=(const QueryResult& other) = delete;

  QueryResult(
      std::vector<std::shared_ptr<arrow::Field>>&& fields,
      std::vector<std::shared_ptr<arrow::Array>>&& arrays,
      std::vector<std::shared_ptr<execution::IContextColumn>>&& columns = {},
      SerializationProtocol protocol = SerializationProtocol::kArrowIPC)
      : fields_(std::move(fields)),
        arrays_(std::move(arrays)),
        columns_(std::move(columns)),
        serialization_protocol_(protocol) {}

  QueryResult(const std::vector<std::shared_ptr<arrow::Field>>& fields,
              const std::vector<std::shared_ptr<arrow::Array>>& arrays,
              const std::vector<std::shared_ptr<execution::IContextColumn>>&
                  columns = {},
              SerializationProtocol protocol = SerializationProtocol::kArrowIPC)
      : fields_(fields),
        arrays_(arrays),
        columns_(columns),
        serialization_protocol_(protocol) {}

  void Swap(QueryResult& other) noexcept {
    fields_.swap(other.fields_);
    arrays_.swap(other.arrays_);
    columns_.swap(other.columns_);
    std::swap(serialization_protocol_, other.serialization_protocol_);
  }

  void Swap(QueryResult&& other) noexcept {
    fields_.swap(other.fields_);
    arrays_.swap(other.arrays_);
    columns_.swap(other.columns_);
    std::swap(serialization_protocol_, other.serialization_protocol_);
  }

  ~QueryResult() {}

  /**
   * @brief Convert entire result set to string.
   */
  std::string ToString() const;

  /**
   * @brief Get total number of rows.
   */
  size_t length() const { return arrays_.empty() ? 0 : arrays_[0]->length(); }

  /**
   * @brief Access underlying Arrow table without copy.
   */
  inline std::shared_ptr<arrow::Table> table() const {
    if (arrays_.empty()) {
      return nullptr;
    }
    return arrow::Table::Make(arrow::schema(fields_), arrays_);
  }

  inline const std::vector<std::shared_ptr<arrow::Field>>& fields() const {
    return fields_;
  }
  inline const std::vector<std::shared_ptr<arrow::Array>>& arrays() const {
    return arrays_;
  }

  std::vector<char> Serialize() const;

  void set_result_schema(const std::string& schema);

  inline void set_serialization_protocol(SerializationProtocol protocol) {
    serialization_protocol_ = protocol;
  }

 private:
  std::vector<std::shared_ptr<arrow::Field>> fields_;
  std::vector<std::shared_ptr<arrow::Array>> arrays_;
  // Holding the primitive context columns, since the corresponding array is
  // wrapped from them
  std::vector<std::shared_ptr<execution::IContextColumn>> columns_;
  SerializationProtocol serialization_protocol_ =
      SerializationProtocol::kArrowIPC;
};

}  // namespace neug
