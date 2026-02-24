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

#include <memory>
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"
#include "neug/utils/result.h"

namespace neug {
namespace reader {

/**
 * @brief Base class for schema inference from external files
 *
 * Sniffer is responsible for inferring column types and names from external
 * files by reading a sample of the data (first block for CSV/JSON) or metadata
 * (for Parquet). Derived classes implement format-specific sniffing logic:
 * - ArrowSniffer: uses Arrow Dataset API to infer schema
 * - CSVSniffer: reads first block of CSV data
 * - JSONSniffer: reads first block of JSON data
 * - ParquetSniffer: reads Parquet metadata
 *
 * The sniffing process:
 * 1. Reads a sample of data/metadata from the file
 * 2. Infers column names and types
 * 3. Returns an EntrySchema containing the inferred information
 *
 * The adaptiveSniff() method allows merging user-provided schema with
 * inferred schema, giving precedence to user-provided types when available.
 *
 * Note: Each derived Sniffer class manages its own Reader instance if needed.
 * The base Sniffer class does not contain a Reader to keep it simple and
 * flexible.
 */
class Sniffer {
 public:
  virtual ~Sniffer() = default;

  /**
   * @brief Infer schema from external file
   *
   * Reads the first block (for CSV/JSON) or metadata (for Parquet) from the
   * external file and infers column names and types. The inference process
   * samples data to determine appropriate types without reading the entire
   * file.
   *
   * @return EntrySchema containing inferred column names and types with the
   *         specified type.
   */
  virtual result<std::shared_ptr<EntrySchema>> sniff() = 0;
};

/**
 * @brief Arrow-based schema inference implementation
 *
 * ArrowSniffer uses Apache Arrow Dataset API to infer column types and names
 * from external files. It leverages ArrowReader's capabilities to access files
 * and uses Arrow's format-specific inference (Inspect() method) to sample
 * files and determine appropriate types without reading the entire file.
 *
 * The sniffing process:
 * 1. Uses ArrowReader to create a FileSystemDatasetFactory
 * 2. Calls factory->Inspect() to infer schema from sample data/metadata
 * 3. Converts Arrow Schema to EntrySchema (column names and types)
 *
 * The adaptiveSniff() method merges user-provided schema with inferred schema,
 * giving precedence to user-provided types when available.
 */
class ArrowSniffer : public Sniffer {
 public:
  /**
   * @brief Constructs an ArrowSniffer with an ArrowReader shared pointer
   *
   * Takes ownership of the ArrowReader instance and stores it internally.
   * The ArrowReader is used to access files and configuration for schema
   * inference.
   *
   * @param reader ArrowReader instance that provides access to files and
   *               configuration
   */
  explicit ArrowSniffer(std::shared_ptr<ArrowReader> reader)
      : reader_(std::move(reader)) {
    if (!reader_) {
      THROW_RUNTIME_ERROR("ArrowReader cannot be null");
    }
  }

  /**
   * @brief Infer schema from external files using Arrow Dataset API
   *
   * Reads sample data (first block for CSV/JSON) or metadata (for Parquet)
   * from the external files and infers column names and types. Arrow will
   * automatically sample files to determine appropriate types without reading
   * the entire file.
   *
   * @return EntrySchema containing inferred column names and types
   */
  result<std::shared_ptr<EntrySchema>> sniff() override;

 private:
  /**
   * @brief Convert Arrow Schema to EntrySchema
   *
   * Converts Arrow's inferred schema (arrow::Schema) to EntrySchema format
   * by extracting column names and converting Arrow DataTypes to
   * common::DataType (protobuf) using ArrowTypeConverter.
   *
   * @param arrowSchema Arrow schema to convert
   * @param type The type of entry schema to create (TABLE, VERTEX, or EDGE)
   * @return EntrySchema containing column names and types with specified type
   */
  result<std::shared_ptr<EntrySchema>> convertArrowSchemaToEntrySchema(
      const std::shared_ptr<arrow::Schema>& arrowSchema);

  /**
   * @brief ArrowReader instance used for schema inference
   *
   * Stores the ArrowReader instance that provides access to files and
   * configuration. Used to call Arrow-specific methods like inferSchema().
   */
  std::shared_ptr<ArrowReader> reader_;
};

}  // namespace reader
}  // namespace neug
