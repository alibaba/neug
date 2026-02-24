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
#include <string>
#include <vector>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/common/cast.h"
#include "neug/generated/proto/plan/basic_type.pb.h"

namespace neug {
namespace reader {

enum class EntrySchemaType : uint8_t { TABLE = 0, VERTEX = 1, EDGE = 2 };

/**
 * @brief Base entry schema containing column information
 *
 * This struct represents the schema of an external table entry, containing
 * column names and their corresponding logical types. It is used to store
 * inferred or declared types for external data sources.
 */
struct EntrySchema {
  virtual ~EntrySchema() = default;
  virtual EntrySchemaType type() const = 0;
  std::vector<std::string> columnNames;
  std::vector<std::shared_ptr<::common::DataType>> columnTypes;

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

struct TableEntrySchema : public EntrySchema {
  virtual EntrySchemaType type() const override {
    return EntrySchemaType::TABLE;
  }
};

/**
 * @brief Vertex entry schema
 *
 * Extends EntrySchema with vertex-specific metadata including vertex label
 * and primary key column name. Used for representing vertex table schemas
 * in graph databases.
 */
struct VertexEntrySchema : public EntrySchema {
  virtual EntrySchemaType type() const override {
    return EntrySchemaType::VERTEX;
  }
  std::string label;
  std::string primaryCol;
};

/**
 * @brief Edge entry schema
 *
 * Extends EntrySchema with edge-specific metadata including edge label,
 * source/destination vertex labels, and source/destination column names.
 * Used for representing edge table schemas in graph databases.
 */
struct EdgeEntrySchema : public EntrySchema {
  virtual EntrySchemaType type() const override {
    return EntrySchemaType::EDGE;
  }
  std::string label;
  std::string srcLabel;
  std::string dstLabel;
  std::string srcCol;
  std::string dstCol;
};

/**
 * @brief File schema containing file information
 *
 * This class contains all file-related metadata including:
 * - File paths: supports multiple files for batch reading
 * - Format type: identifies the file format (csv, parquet, json)
 * - URL options: configuration for accessing files via various protocols
 * - Read options: common reading configuration (threading, batch size)
 * - Format options: format-specific parsing options (polymorphic)
 */
class FileSchema {
 public:
  std::vector<std::string> paths;
  // file format: csv, json, parquet
  std::string format;
  // file system: local, http, s3
  std::string protocol;
  common::case_insensitive_map_t<std::string> options;
};

/**
 * @brief External schema combining entry and file information
 *
 * This struct combines EntrySchema (column information) and FileSchema
 * (file access information) to provide complete metadata for external tables.
 * It is used for maintaining external table metadata in the Catalog system.
 */
struct ExternalSchema {
  std::shared_ptr<EntrySchema> entry;
  FileSchema file;
};

}  // namespace reader
}  // namespace neug
