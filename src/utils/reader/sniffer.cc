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

#include "neug/utils/reader/sniffer.h"

#include <arrow/type.h>
#include <glog/logging.h>

#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/schema.h"
#include "neug/utils/reader/type_converter.h"

namespace gs {
namespace reader {

std::shared_ptr<EntrySchema> Sniffer::adaptiveSniff(
    const EntrySchema& options) {
  // Infer schema from files with the requested type
  auto inferredSchema = sniff();

  // Merge user-provided options with inferred schema based on type
  if (options.type() == EntrySchemaType::VERTEX) {
    // Create vertex schema from inferred schema
    auto vertexSchema = std::make_shared<VertexEntrySchema>();
    vertexSchema->columnNames = inferredSchema->columnNames;
    vertexSchema->columnTypes = inferredSchema->columnTypes;

    auto& columnNames = vertexSchema->columnNames;

    // Access vertex-specific fields from options
    auto vertexOptions = options.constPtrCast<VertexEntrySchema>();
    if (vertexOptions) {
      if (!vertexOptions->label.empty()) {
        vertexSchema->label = vertexOptions->label;
      }
      if (!vertexOptions->primaryCol.empty()) {
        if (std::find(columnNames.begin(), columnNames.end(),
                      vertexOptions->primaryCol) == columnNames.end()) {
          THROW_NOT_FOUND_EXCEPTION("Primary column " +
                                    vertexOptions->primaryCol +
                                    " not found in schema");
        }
        vertexSchema->primaryCol = vertexOptions->primaryCol;
      }
    }

    // Set default primary column if not specified
    if (vertexSchema->primaryCol.empty() &&
        !vertexSchema->columnNames.empty()) {
      vertexSchema->primaryCol = vertexSchema->columnNames[0];
    }

    return vertexSchema;
  } else if (options.type() == EntrySchemaType::EDGE) {
    // Create edge schema from inferred schema
    auto edgeSchema = std::make_shared<EdgeEntrySchema>();
    edgeSchema->columnNames = inferredSchema->columnNames;
    edgeSchema->columnTypes = inferredSchema->columnTypes;

    auto& columnNames = edgeSchema->columnNames;

    // Access edge-specific fields from options
    auto edgeOptions = options.constPtrCast<EdgeEntrySchema>();
    if (edgeOptions) {
      if (!edgeOptions->label.empty()) {
        edgeSchema->label = edgeOptions->label;
      }
      if (!edgeOptions->srcLabel.empty()) {
        edgeSchema->srcLabel = edgeOptions->srcLabel;
      }
      if (!edgeOptions->dstLabel.empty()) {
        edgeSchema->dstLabel = edgeOptions->dstLabel;
      }
      if (!edgeOptions->srcCol.empty()) {
        if (std::find(columnNames.begin(), columnNames.end(),
                      edgeOptions->srcCol) == columnNames.end()) {
          THROW_NOT_FOUND_EXCEPTION("Source column " + edgeOptions->srcCol +
                                    " not found in schema");
        }
        edgeSchema->srcCol = edgeOptions->srcCol;
      }
      if (!edgeOptions->dstCol.empty()) {
        if (std::find(columnNames.begin(), columnNames.end(),
                      edgeOptions->dstCol) == columnNames.end()) {
          THROW_NOT_FOUND_EXCEPTION("Destination column " +
                                    edgeOptions->dstCol +
                                    " not found in schema");
        }
        edgeSchema->dstCol = edgeOptions->dstCol;
      }
    }

    // Set default source and destination columns if not specified
    if (edgeSchema->srcCol.empty() && edgeSchema->columnNames.size() >= 1) {
      edgeSchema->srcCol = edgeSchema->columnNames[0];
    }
    if (edgeSchema->dstCol.empty() && edgeSchema->columnNames.size() >= 2) {
      edgeSchema->dstCol = edgeSchema->columnNames[1];
    }
    return edgeSchema;
  }

  // For TABLE type, return inferred schema as-is
  return inferredSchema;
}

std::shared_ptr<EntrySchema> ArrowSniffer::sniff() {
  if (!reader_) {
    LOG(ERROR) << "ArrowReader is null";
    THROW_INVALID_ARGUMENT_EXCEPTION("ArrowReader is null");
  }

  // Call ArrowReader's inferSchema() method to get Arrow Schema
  auto arrowSchema = reader_->inferSchema();
  if (!arrowSchema) {
    LOG(ERROR) << "Failed to infer schema from ArrowReader";
    THROW_IO_EXCEPTION("Failed to infer schema from ArrowReader");
  }

  // Convert Arrow Schema to base EntrySchema
  return convertArrowSchemaToEntrySchema(arrowSchema);
}

std::shared_ptr<EntrySchema> ArrowSniffer::convertArrowSchemaToEntrySchema(
    const std::shared_ptr<arrow::Schema>& arrowSchema) {
  if (!arrowSchema) {
    LOG(ERROR) << "Arrow schema is null";
    THROW_INVALID_ARGUMENT_EXCEPTION("Arrow schema is null");
  }

  auto entrySchema = std::make_shared<TableEntrySchema>();
  ArrowTypeConverter converter;
  int numFields = arrowSchema->num_fields();

  entrySchema->columnNames.reserve(numFields);
  entrySchema->columnTypes.reserve(numFields);

  for (int i = 0; i < numFields; ++i) {
    const auto& field = arrowSchema->field(i);
    const std::string& columnName = field->name();

    entrySchema->columnNames.push_back(columnName);

    // Use ArrowTypeConverter to convert Arrow DataType to common::DataType
    auto commonType = converter.convert(*field->type());
    if (!commonType) {
      LOG(ERROR) << "Failed to convert Arrow type for column: " << columnName;
      THROW_CONVERSION_EXCEPTION("Failed to convert Arrow type for column: " +
                                 columnName);
    }
    // Store shared_ptr directly, avoiding copy of protobuf message
    entrySchema->columnTypes.push_back(std::move(commonType));
  }

  return entrySchema;
}

}  // namespace reader
}  // namespace gs