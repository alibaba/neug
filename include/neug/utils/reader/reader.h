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

#include <arrow/array/array_base.h>
#include <arrow/compute/expression.h>
#include <arrow/csv/options.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_csv.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/type.h>
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/context.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/schema.h"

namespace common {
class Expression;
}  // namespace common

namespace neug {

namespace execution {
class Context;
}

namespace reader {
class ArrowOptionsBuilder;
class DatasetBuilder;

/**
 * @brief Base class for local read state
 *
 * This struct represents format-specific local state for reading operations.
 * Format-specific readers can extend this struct to add their own local state
 * (e.g., file handles, current position, buffer state). The struct provides
 * type-safe casting methods to convert between base and derived types using
 * dynamic_cast.
 */
struct ReadLocalState {
  virtual ~ReadLocalState() = default;

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }

  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }

  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }
};

/**
 * @brief Shared state for reading operations
 *
 * This struct contains shared state information used across multiple read
 * operations. It includes:
 * - Schema information: external table metadata (column names, types, file
 * info)
 * - Column pruning: list of columns to skip during projection
 * - Filter pushdown: predicate expression for row filtering
 *
 * The columnNum() method calculates the effective number of columns after
 * applying column pruning.
 *
 * Note: schema is stored by value (not shared_ptr) because ExternalSchema
 * now uses value semantics for entry and optional for formatOpts, making it
 * efficient to copy.
 */
struct ReadSharedState {
  ExternalSchema schema;
  std::vector<std::string> skipColumns;
  std::shared_ptr<::common::Expression> skipRows;

  /**
   * @brief Get the number of columns after column pruning
   * @return The number of columns remaining (not skipped) according to
   * skipColumns
   */
  int columnNum() {
    if (!schema.entry) {
      return 0;
    }
    const auto& allColumns = schema.entry->columnNames;
    int total = static_cast<int>(allColumns.size());
    for (const auto& column : allColumns) {
      if (std::find(skipColumns.begin(), skipColumns.end(), column) !=
          skipColumns.end()) {
        --total;
      }
    }
    return total;
  }
};

/**
 * @brief Base Reader class template
 *
 * This template class provides a unified interface for reading data from
 * external files. It is parameterized by FileSystem type to support different
 * filesystem implementations (e.g., local filesystem, S3, HDFS).
 *
 * Derived classes must implement:
 * - read(): reads data from local state and populates the context
 * - makeFileSystem(): creates the appropriate FileSystem instance
 *
 * The class maintains shared state (schema, column pruning, filter pushdown)
 * that is used across all read operations.
 */
template <class FileSystem>
class Reader {
 public:
  explicit Reader(std::shared_ptr<ReadSharedState> sharedState,
                  std::shared_ptr<FileSystem> fileSystem)
      : sharedState(std::move(sharedState)),
        fileSystem(std::move(fileSystem)) {}

  virtual ~Reader() = default;

  /**
   * @brief Read data from local state and populate context
   * @param localState Local read state containing current data block info
   * @param ctx Context to populate with parsed data
   */
  virtual void read(std::shared_ptr<ReadLocalState> localState,
                    execution::Context& ctx) = 0;

 protected:
  std::shared_ptr<ReadSharedState> sharedState;
  std::shared_ptr<FileSystem> fileSystem;
};

/**
 * @brief Builder class for creating DatasetFactory
 *
 * This class provides a unified interface for creating DatasetFactory instances
 * for different file formats.
 *
 * Derived classes must implement:
 * - buildFactory(): creates the appropriate DatasetFactory instance
 */
class DatasetBuilder {
 public:
  explicit DatasetBuilder() = default;
  virtual ~DatasetBuilder() = default;

  virtual std::shared_ptr<arrow::dataset::DatasetFactory> buildFactory(
      std::shared_ptr<ReadSharedState> sharedState,
      std::shared_ptr<arrow::fs::FileSystem> fs,
      std::shared_ptr<arrow::dataset::FileFormat> fileFormat);
};

/**
 * @brief Arrow-based reader implementation for reading data from external files
 *
 * This class provides a concrete implementation of the Reader interface using
 * Apache Arrow's dataset API. It supports reading from multiple files using
 * Arrow's FileSystemDatasetFactory, and can operate in two modes:
 * - Full read: loads entire dataset into memory as a Table
 * - Batch read: streams data in batches using RecordBatchReader
 *
 * The reader uses an ArrowOptionsBuilder to configure scan options including
 * schema, projection (column pruning), and filters (row filtering). It
 * supports various file formats through format-specific option builders.
 */
class ArrowReader : public Reader<arrow::fs::FileSystem> {
 public:
  explicit ArrowReader(std::shared_ptr<ReadSharedState> sharedState,
                       std::unique_ptr<ArrowOptionsBuilder> optionsBuilder,
                       std::shared_ptr<arrow::fs::FileSystem> fileSystem)
      : Reader(std::move(sharedState), std::move(fileSystem)),
        optionsBuilder(std::move(optionsBuilder)),
        datasetBuilder(std::make_shared<DatasetBuilder>()) {}
  explicit ArrowReader(std::shared_ptr<ReadSharedState> sharedState,
                       std::unique_ptr<ArrowOptionsBuilder> optionsBuilder,
                       std::shared_ptr<arrow::fs::FileSystem> fileSystem,
                       std::shared_ptr<DatasetBuilder> datasetBuilder)
      : Reader(std::move(sharedState), std::move(fileSystem)),
        optionsBuilder(std::move(optionsBuilder)),
        datasetBuilder(datasetBuilder) {}
  virtual ~ArrowReader() override = default;

  void read(std::shared_ptr<ReadLocalState> localState,
            execution::Context& ctx) override;

  /**
   * @brief Infer schema from external files using Arrow Dataset API
   *
   * Reads sample data (first block for CSV/JSON) or metadata (for Parquet)
   * from the external files and infers column names and types. Arrow will
   * automatically sample files to determine appropriate types without reading
   * the entire file.
   *
   * @return Arrow Schema containing inferred column names and types
   */
  arrow::Result<std::shared_ptr<arrow::Schema>> inferSchema();

 protected:
  /**
   * @brief Create FileSystemDatasetFactory for schema inference or scanning
   *
   * This helper method creates a FileSystemDatasetFactory using the file paths
   * and file format. It can be used by both inferSchema() and createScanner()
   * to avoid code duplication.
   *
   * @param fs FileSystem instance
   * @param fileFormat File format instance (CSV, Parquet, etc.)
   * @return Arrow Result containing DatasetFactory (which is a
   *         FileSystemDatasetFactory)
   */
  std::shared_ptr<arrow::dataset::DatasetFactory> createFactory(
      std::shared_ptr<arrow::fs::FileSystem> fs,
      std::shared_ptr<arrow::dataset::FileFormat> fileFormat);

  std::shared_ptr<arrow::dataset::Scanner> createScanner(
      std::shared_ptr<arrow::fs::FileSystem> fs);
  void full_read(std::shared_ptr<arrow::dataset::Scanner>,
                 execution::Context& output);
  void batch_read(std::shared_ptr<arrow::dataset::Scanner>,
                  execution::Context& output);

 protected:
  std::unique_ptr<ArrowOptionsBuilder> optionsBuilder;
  std::shared_ptr<DatasetBuilder> datasetBuilder;
};

}  // namespace reader
}  // namespace neug
