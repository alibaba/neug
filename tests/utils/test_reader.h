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

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <vector>

#include <arrow/filesystem/localfs.h>
#include <arrow/type.h>
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/utils/reader/expression_converter.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"
#include "neug/utils/reader/type_converter.h"

namespace neug {
namespace test {

static constexpr const char* ARROW_READER_TEST_DIR = "/tmp/arrow_reader_test";

/**
 * @brief Converter class to convert various constant types to ::common::Value
 *
 * This class provides static methods to convert C++ primitive types
 * (int32, int64, double, string, bool) to ::common::Value protobuf messages.
 * It is used to create filter expressions with type-safe constant values.
 */
class ValueConverter {
 public:
  static ::common::Value fromInt32(int32_t value) {
    ::common::Value val;
    val.set_i32(value);
    return val;
  }

  static ::common::Value fromInt64(int64_t value) {
    ::common::Value val;
    val.set_i64(value);
    return val;
  }

  static ::common::Value fromDouble(double value) {
    ::common::Value val;
    val.set_f64(value);
    return val;
  }

  static ::common::Value fromFloat(float value) {
    ::common::Value val;
    val.set_f32(value);
    return val;
  }

  static ::common::Value fromString(const std::string& value) {
    ::common::Value val;
    val.set_str(value);
    return val;
  }

  static ::common::Value fromBool(bool value) {
    ::common::Value val;
    val.set_boolean(value);
    return val;
  }

  static ::common::Value fromUInt32(uint32_t value) {
    ::common::Value val;
    val.set_u32(value);
    return val;
  }

  static ::common::Value fromUInt64(uint64_t value) {
    ::common::Value val;
    val.set_u64(value);
    return val;
  }
};

/**
 * @brief Test fixture for ArrowReader tests
 *
 * This class provides common helper functions for testing ArrowReader
 * functionality, including:
 * - CSV file creation
 * - DataType creation helpers
 * - Expression creation helpers
 * - ReadSharedState creation
 * - Batch row counting utilities
 */
class ReaderTest : public ::testing::Test {
 public:
  void SetUp() override {
    if (std::filesystem::exists(ARROW_READER_TEST_DIR)) {
      std::filesystem::remove_all(ARROW_READER_TEST_DIR);
    }
    std::filesystem::create_directories(ARROW_READER_TEST_DIR);
  }

  void TearDown() override {
    if (std::filesystem::exists(ARROW_READER_TEST_DIR)) {
      std::filesystem::remove_all(ARROW_READER_TEST_DIR);
    }
  }

  // Helper function to create a CSV file
  void createCsvFile(const std::string& filename, const std::string& content) {
    std::ofstream file(std::string(ARROW_READER_TEST_DIR) + "/" + filename);
    file << content;
    file.close();
  }

  // Helper function to create DataType as shared_ptr
  std::shared_ptr<::common::DataType> createInt32Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
    return type;
  }

  std::shared_ptr<::common::DataType> createUInt32Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT32);
    return type;
  }

  std::shared_ptr<::common::DataType> createStringType() {
    auto type = std::make_shared<::common::DataType>();
    auto strType = std::make_unique<::common::String>();
    auto varChar = std::make_unique<::common::String::VarChar>();
    strType->set_allocated_var_char(varChar.release());
    type->set_allocated_string(strType.release());
    return type;
  }

  std::shared_ptr<::common::DataType> createDoubleType() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
    return type;
  }

  std::shared_ptr<::common::DataType> createInt64Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
    return type;
  }

  // Helper function to create a simple expression: column > value
  // Expression in natural order: [var, GT, const]
  std::shared_ptr<::common::Expression> createFilterExpression(
      const std::string& columnName, const ::common::Value& value) {
    auto expr = std::make_shared<::common::Expression>();

    // Variable: column name
    auto var_opr = expr->add_operators();
    auto var = var_opr->mutable_var();
    auto tag = var->mutable_tag();
    tag->set_name(columnName);

    // Comparison: GT
    auto gt_opr = expr->add_operators();
    gt_opr->set_logical(::common::Logical::GT);

    // Constant: value
    auto const_opr = expr->add_operators();
    *const_opr->mutable_const_() = value;

    return expr;
  }

  // Helper function to create AND expression: (leftExpr) AND (rightExpr)
  // Expression in natural order: [leftExpr operators..., AND, rightExpr
  // operators...]
  std::shared_ptr<::common::Expression> createAndExpression(
      std::shared_ptr<::common::Expression> leftExpr,
      std::shared_ptr<::common::Expression> rightExpr) {
    auto expr = std::make_shared<::common::Expression>();

    // Add all operators from left expression
    for (int i = 0; i < leftExpr->operators_size(); ++i) {
      *expr->add_operators() = leftExpr->operators(i);
    }

    // Add AND operator
    auto and_opr = expr->add_operators();
    and_opr->set_logical(::common::Logical::AND);

    // Add all operators from right expression
    for (int i = 0; i < rightExpr->operators_size(); ++i) {
      *expr->add_operators() = rightExpr->operators(i);
    }

    return expr;
  }

  // Helper function to create ReadSharedState
  std::shared_ptr<reader::ReadSharedState> createSharedState(
      const std::string& csvFile, const std::vector<std::string>& columnNames,
      const std::vector<std::shared_ptr<::common::DataType>>& columnTypes,
      const common::case_insensitive_map_t<std::string>& options = {},
      const std::vector<std::string>& skipColumns = {},
      std::shared_ptr<::common::Expression> skipRows = nullptr) {
    auto sharedState = std::make_shared<reader::ReadSharedState>();

    auto entrySchema = std::make_shared<reader::TableEntrySchema>();
    entrySchema->columnNames = columnNames;
    entrySchema->columnTypes = columnTypes;

    // Create FileSchema
    reader::FileSchema fileSchema;
    fileSchema.paths = {std::string(ARROW_READER_TEST_DIR) + "/" + csvFile};
    fileSchema.format = "csv";
    fileSchema.options = options;

    // Create ExternalSchema
    reader::ExternalSchema externalSchema;
    externalSchema.entry = entrySchema;
    externalSchema.file = fileSchema;

    sharedState->schema = std::move(externalSchema);
    sharedState->skipColumns = skipColumns;
    sharedState->skipRows = skipRows;

    return sharedState;
  }

  std::shared_ptr<reader::ArrowReader> createArrowReader(
      const std::shared_ptr<reader::ReadSharedState>& sharedState) {
    auto fileSystem = std::make_shared<arrow::fs::LocalFileSystem>();
    auto optionsBuilder =
        std::make_unique<reader::ArrowCsvOptionsBuilder>(sharedState);
    return std::make_shared<reader::ArrowReader>(
        sharedState, std::move(optionsBuilder), std::move(fileSystem));
  }

  // Helper function to count rows in batch_read mode
  // Extracts the first column from context, casts it to
  // ArrowStreamContextColumn, and counts total rows by iterating through all
  // batches from suppliers
  int64_t count_batch_row_num(const execution::Context& ctx) {
    // Get the first column from context
    if (ctx.columns.empty()) {
      return -1;  // Error: no columns
    }
    auto firstColumn = ctx.columns[0];
    if (!firstColumn) {
      return -1;  // Error: first column is null
    }

    // Cast to ArrowStreamContextColumn
    auto streamColumn =
        std::dynamic_pointer_cast<execution::ArrowStreamContextColumn>(
            firstColumn);
    if (!streamColumn) {
      return -1;  // Error: not ArrowStreamContextColumn
    }

    // Get suppliers from the stream column
    auto suppliers = streamColumn->GetSuppliers();
    if (suppliers.empty()) {
      return -1;  // Error: no suppliers
    }

    // Count total rows by iterating through all batches
    int64_t totalRows = 0;
    for (const auto& supplier : suppliers) {
      if (!supplier) {
        continue;  // Skip null suppliers
      }
      while (true) {
        auto batch = supplier->GetNextBatch();
        if (!batch) {
          break;  // No more batches from this supplier
        }
        totalRows += batch->num_rows();
      }
    }

    return totalRows;
  }
};

}  // namespace test
}  // namespace neug
