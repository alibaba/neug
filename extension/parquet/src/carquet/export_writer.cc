/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "carquet/export_writer.h"

#include <carquet/carquet.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "carquet/c_data_writer.h"
#include "carquet/output_adapter.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/stream/output_stream.h"

namespace neug::parquet {
namespace {

constexpr int64_t kDefaultRowsPerGroup = 1 << 20;

Status invalid(std::string message) {
  return Status(StatusCode::ERR_INVALID_ARGUMENT, std::move(message));
}

Status ioError(std::string message) {
  return Status(StatusCode::ERR_IO_ERROR, std::move(message));
}

std::string columnName(const QueryResponse& table,
                       const std::shared_ptr<reader::EntrySchema>& schema,
                       int32_t index) {
  if (index < table.schema().name_size()) {
    return table.schema().name(index);
  }
  if (schema && static_cast<size_t>(index) < schema->columnNames.size()) {
    return schema->columnNames[static_cast<size_t>(index)];
  }
  return "col_" + std::to_string(index);
}

int32_t valueCount(const Array& array) {
  switch (array.typed_array_case()) {
  case Array::kInt32Array:
    return array.int32_array().values_size();
  case Array::kUint32Array:
    return array.uint32_array().values_size();
  case Array::kInt64Array:
    return array.int64_array().values_size();
  case Array::kUint64Array:
    return array.uint64_array().values_size();
  case Array::kFloatArray:
    return array.float_array().values_size();
  case Array::kDoubleArray:
    return array.double_array().values_size();
  case Array::kStringArray:
    return array.string_array().values_size();
  case Array::kBoolArray:
    return array.bool_array().values_size();
  case Array::kTimestampArray:
    return array.timestamp_array().values_size();
  case Array::kDateArray:
    return array.date_array().values_size();
  case Array::kIntervalArray:
    return array.interval_array().values_size();
  case Array::kVertexArray:
    return array.vertex_array().values_size();
  case Array::kEdgeArray:
    return array.edge_array().values_size();
  case Array::kPathArray:
    return array.path_array().values_size();
  default:
    return -1;
  }
}

bool isValid(const std::string& bitmap, int32_t row) {
  return bitmap.empty() ||
         ((static_cast<uint8_t>(bitmap[static_cast<size_t>(row) >> 3]) >>
           (row & 7)) &
          1U) != 0;
}

template <typename ProtoArray>
Status validateStrings(const ProtoArray& array, const std::string& name) {
  uint64_t totalBytes = 0;
  for (const auto& value : array.values()) {
    if (value.size() >
        static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      return invalid("Parquet string value is too large: " + name);
    }
    if (value.size() >
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) -
            totalBytes) {
      return invalid("Parquet string offsets overflow: " + name);
    }
    totalBytes += value.size();
  }
  return Status::OK();
}

const ::common::DataType* listElementType(const ::common::DataType* type) {
  if (!type) {
    return nullptr;
  }
  if (type->item_case() == ::common::DataType::kArray) {
    return &type->array().component_type();
  }
  if (type->item_case() == ::common::DataType::kList) {
    return &type->list().component_type();
  }
  return nullptr;
}

Status validateColumn(const Array& array, int32_t rows, const std::string& name,
                      const ::common::DataType* type = nullptr, int depth = 0) {
  if (depth > 64) {
    return invalid("Parquet column nesting exceeds 64 levels: " + name);
  }
  if (rows < 0) {
    return invalid("Parquet column row count is negative: " + name);
  }
  if (type && type->item_case() != ::common::DataType::ITEM_NOT_SET) {
    // Fixed ARRAY and LIST both use list_array in QueryResponse. Check the
    // declared shape before dispatch so scalar responses cannot skip nested
    // dimension/field validation, including at recursive child nodes.
    const bool list = type->item_case() == ::common::DataType::kArray ||
                      type->item_case() == ::common::DataType::kList;
    const bool structure = type->item_case() == ::common::DataType::kTuple;
    if (list != array.has_list_array() ||
        structure != array.has_struct_array()) {
      return invalid("Parquet column shape does not match declared type: " +
                     name);
    }
  }
  const auto& bitmap = responseArrayValidity(array);
  const size_t required = (static_cast<size_t>(rows) + 7) / 8;
  if (!bitmap.empty() && bitmap.size() < required) {
    return invalid("Parquet column validity bitmap is too short: " + name);
  }

  if (array.has_list_array()) {
    const auto& list = array.list_array();
    const int64_t expectedOffsets = static_cast<int64_t>(rows) + 1;
    if (list.offsets_size() != expectedOffsets || list.offsets_size() == 0 ||
        list.offsets(0) != 0) {
      return invalid("Parquet list offsets do not match the row count: " +
                     name);
    }
    const bool fixed = type && type->item_case() == ::common::DataType::kArray;
    const uint32_t fixedLength = fixed ? type->array().fixed_length() : 0;
    if (fixed && fixedLength == 0) {
      return invalid("Parquet fixed array length must be positive: " + name);
    }
    for (int32_t row = 0; row < rows; ++row) {
      const uint32_t begin = list.offsets(row);
      const uint32_t end = list.offsets(row + 1);
      if (end < begin) {
        return invalid("Parquet list offsets are not monotonic: " + name);
      }
      if (fixed) {
        const uint32_t length = end - begin;
        if ((isValid(bitmap, row) && length != fixedLength) ||
            (!isValid(bitmap, row) && length != 0 && length != fixedLength)) {
          return invalid("Parquet fixed array row has the wrong length: " +
                         name);
        }
      }
    }
    const uint32_t elements = list.offsets(rows);
    if (elements > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
      return invalid("Parquet list element count is too large: " + name);
    }
    return validateColumn(list.elements(), static_cast<int32_t>(elements),
                          name + ".element", listElementType(type), depth + 1);
  }

  if (array.has_struct_array()) {
    const auto& structure = array.struct_array();
    if (structure.fields_size() == 0) {
      return invalid("Parquet struct has no fields: " + name);
    }
    const ::common::Tuple* tuple =
        type && type->item_case() == ::common::DataType::kTuple ? &type->tuple()
                                                                : nullptr;
    if (tuple && tuple->component_types_size() != structure.fields_size()) {
      return invalid("Parquet struct schema does not match its fields: " +
                     name);
    }
    for (int32_t field = 0; field < structure.fields_size(); ++field) {
      const auto* fieldType = tuple ? &tuple->component_types(field) : nullptr;
      auto status = validateColumn(structure.fields(field), rows,
                                   name + ".field_" + std::to_string(field),
                                   fieldType, depth + 1);
      if (!status.ok()) {
        return status;
      }
    }
    return Status::OK();
  }

  const int32_t values = valueCount(array);
  if (values < 0) {
    return invalid("Unsupported Parquet column: " + name);
  }
  if (values != rows) {
    return invalid("Parquet column row count mismatch: " + name);
  }
  if (array.has_string_array()) {
    return validateStrings(array.string_array(), name);
  }
  if (array.has_interval_array()) {
    return validateStrings(array.interval_array(), name);
  }
  if (array.has_vertex_array()) {
    return validateStrings(array.vertex_array(), name);
  }
  if (array.has_edge_array()) {
    return validateStrings(array.edge_array(), name);
  }
  if (array.has_path_array()) {
    return validateStrings(array.path_array(), name);
  }
  if (array.has_date_array()) {
    for (int32_t row = 0; row < rows; ++row) {
      if (!isValid(bitmap, row)) {
        continue;
      }
      const int64_t millis = array.date_array().values(row);
      const int64_t days = dateMillisToEpochDays(millis);
      if (days < std::numeric_limits<int32_t>::min() ||
          days > std::numeric_limits<int32_t>::max()) {
        return invalid("Parquet date is outside the DATE range: " + name);
      }
    }
  }
  return Status::OK();
}

carquet_status_t addColumn(carquet_schema_t* schema, const char* name,
                           const Array& array, int32_t parent) {
  carquet_physical_type_t physical;
  carquet_logical_type_t logical{};
  const carquet_logical_type_t* logicalPtr = nullptr;
  switch (array.typed_array_case()) {
  case Array::kInt32Array:
    physical = CARQUET_PHYSICAL_INT32;
    break;
  case Array::kUint32Array:
    physical = CARQUET_PHYSICAL_INT32;
    logical.id = CARQUET_LOGICAL_INTEGER;
    logical.params.integer = {.bit_width = 32, .is_signed = false};
    logicalPtr = &logical;
    break;
  case Array::kInt64Array:
    physical = CARQUET_PHYSICAL_INT64;
    break;
  case Array::kUint64Array:
    physical = CARQUET_PHYSICAL_INT64;
    logical.id = CARQUET_LOGICAL_INTEGER;
    logical.params.integer = {.bit_width = 64, .is_signed = false};
    logicalPtr = &logical;
    break;
  case Array::kFloatArray:
    physical = CARQUET_PHYSICAL_FLOAT;
    break;
  case Array::kDoubleArray:
    physical = CARQUET_PHYSICAL_DOUBLE;
    break;
  case Array::kStringArray:
  case Array::kIntervalArray:
  case Array::kVertexArray:
  case Array::kEdgeArray:
  case Array::kPathArray:
    physical = CARQUET_PHYSICAL_BYTE_ARRAY;
    logical.id = CARQUET_LOGICAL_STRING;
    logicalPtr = &logical;
    break;
  case Array::kBoolArray:
    physical = CARQUET_PHYSICAL_BOOLEAN;
    break;
  case Array::kTimestampArray:
    physical = CARQUET_PHYSICAL_INT64;
    logical.id = CARQUET_LOGICAL_TIMESTAMP;
    // QueryExportWriter sinks DateTime::milli_second into TimestampArray.
    logical.params.timestamp = {.unit = CARQUET_TIME_UNIT_MILLIS,
                                .is_adjusted_to_utc = true};
    logicalPtr = &logical;
    break;
  case Array::kDateArray:
    physical = CARQUET_PHYSICAL_INT32;
    logical.id = CARQUET_LOGICAL_DATE;
    logicalPtr = &logical;
    break;
  default:
    return CARQUET_ERROR_NOT_IMPLEMENTED;
  }
  return carquet_schema_add_column(schema, name, physical, logicalPtr,
                                   CARQUET_REPETITION_OPTIONAL, 0, parent);
}

carquet_status_t addField(carquet_schema_t* schema, const char* name,
                          const Array& array, const ::common::DataType* type,
                          int32_t parent, std::vector<bool>& boolLeaves) {
  if (array.has_list_array()) {
    const int32_t repeated = carquet_schema_add_list_group(
        schema, name, CARQUET_REPETITION_OPTIONAL, parent);
    if (repeated < 0) {
      return CARQUET_ERROR_INTERNAL;
    }
    return addField(schema, "element", array.list_array().elements(),
                    listElementType(type), repeated, boolLeaves);
  }
  if (array.has_struct_array()) {
    const int32_t group = carquet_schema_add_group(
        schema, name, CARQUET_REPETITION_OPTIONAL, parent);
    if (group < 0) {
      return CARQUET_ERROR_INTERNAL;
    }
    const auto& structure = array.struct_array();
    const ::common::Tuple* tuple =
        type && type->item_case() == ::common::DataType::kTuple ? &type->tuple()
                                                                : nullptr;
    for (int32_t field = 0; field < structure.fields_size(); ++field) {
      const auto fieldName = "field_" + std::to_string(field);
      const auto* fieldType = tuple ? &tuple->component_types(field) : nullptr;
      const auto status =
          addField(schema, fieldName.c_str(), structure.fields(field),
                   fieldType, group, boolLeaves);
      if (status != CARQUET_OK) {
        return status;
      }
    }
    return CARQUET_OK;
  }
  const auto status = addColumn(schema, name, array, parent);
  if (status == CARQUET_OK) {
    boolLeaves.push_back(array.has_bool_array());
  }
  return status;
}

Status parseOptions(const reader::options_t& values,
                    carquet_writer_options_t& options, int64_t& rowsPerGroup,
                    bool& dictionary) {
  try {
    const auto compression =
        reader::Option<std::string>::StringOption("COMPRESSION", "snappy");
    const auto rowGroupSize = reader::Option<int64_t>::Int64Option(
        "ROW_GROUP_SIZE", kDefaultRowsPerGroup);
    const auto dictionaryEncoding =
        reader::Option<bool>::BoolOption("DICTIONARY_ENCODING", true);

    std::string codec = compression.get(values);
    std::transform(
        codec.begin(), codec.end(), codec.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (codec == "none" || codec == "uncompressed") {
      options.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
    } else if (codec == "snappy") {
      options.compression = CARQUET_COMPRESSION_SNAPPY;
    } else if (codec == "zlib" || codec == "gzip") {
      options.compression = CARQUET_COMPRESSION_GZIP;
    } else if (codec == "zstd" || codec == "zstandard") {
      options.compression = CARQUET_COMPRESSION_ZSTD;
    } else {
      return invalid("Unsupported compression codec: " + codec +
                     ". Supported: none, snappy, gzip (zlib), zstd");
    }

    rowsPerGroup = rowGroupSize.get(values);
    if (rowsPerGroup <= 0) {
      return invalid("Parquet row_group_size must be positive");
    }
    dictionary = dictionaryEncoding.get(values);
    options.dictionary_encoding =
        dictionary ? CARQUET_ENCODING_RLE_DICTIONARY : CARQUET_ENCODING_PLAIN;
    // ROW_GROUP_SIZE is a row count in NeuG. Disable Carquet's byte-based
    // automatic flush and start each row group explicitly below.
    options.row_group_size = std::numeric_limits<int64_t>::max();
    return Status::OK();
  } catch (const std::exception& e) {
    return invalid("Invalid Parquet export option: " + std::string(e.what()));
  }
}

template <typename Repeated, typename Value>
std::vector<Value> packValues(const Repeated& array, const std::string& bitmap,
                              int32_t begin, int32_t count) {
  std::vector<Value> output;
  output.reserve(static_cast<size_t>(count));
  for (int32_t row = begin; row < begin + count; ++row) {
    if (isValid(bitmap, row)) {
      output.push_back(static_cast<Value>(array.values(row)));
    }
  }
  return output;
}

std::vector<int16_t> definitionLevels(const std::string& bitmap, int32_t begin,
                                      int32_t count) {
  if (bitmap.empty()) {
    return {};
  }
  std::vector<int16_t> levels(static_cast<size_t>(count));
  for (int32_t offset = 0; offset < count; ++offset) {
    levels[static_cast<size_t>(offset)] = isValid(bitmap, begin + offset);
  }
  return levels;
}

template <typename Value>
const void* valuesData(const std::vector<Value>& values) {
  static const Value empty{};
  return values.empty() ? &empty : values.data();
}

template <typename ProtoArray>
carquet_status_t writeStrings(carquet_writer_t* writer, int32_t column,
                              const ProtoArray& input,
                              const std::string& bitmap, int32_t begin,
                              int32_t count, const int16_t* levels) {
  std::vector<carquet_byte_array_t> values;
  values.reserve(static_cast<size_t>(count));
  for (int32_t row = begin; row < begin + count; ++row) {
    if (!isValid(bitmap, row)) {
      continue;
    }
    const auto& value = input.values(row);
    values.push_back({
        .data = reinterpret_cast<uint8_t*>(const_cast<char*>(value.data())),
        .length = static_cast<int32_t>(value.size()),
    });
  }
  return carquet_writer_write_batch(writer, column, valuesData(values), count,
                                    levels, nullptr);
}

carquet_status_t writeColumn(carquet_writer_t* writer, int32_t column,
                             const Array& array, int32_t begin, int32_t count) {
  const auto& bitmap = responseArrayValidity(array);
  auto levels = definitionLevels(bitmap, begin, count);
  const int16_t* levelData = levels.empty() ? nullptr : levels.data();
  switch (array.typed_array_case()) {
  case Array::kInt32Array: {
    auto values = packValues<neug::Int32Array, int32_t>(array.int32_array(),
                                                        bitmap, begin, count);
    return carquet_writer_write_batch(writer, column, valuesData(values), count,
                                      levelData, nullptr);
  }
  case Array::kUint32Array: {
    auto values = packValues<neug::UInt32Array, uint32_t>(array.uint32_array(),
                                                          bitmap, begin, count);
    return carquet_writer_write_batch(writer, column, valuesData(values), count,
                                      levelData, nullptr);
  }
  case Array::kInt64Array: {
    auto values = packValues<neug::Int64Array, int64_t>(array.int64_array(),
                                                        bitmap, begin, count);
    return carquet_writer_write_batch(writer, column, valuesData(values), count,
                                      levelData, nullptr);
  }
  case Array::kUint64Array: {
    auto values = packValues<neug::UInt64Array, uint64_t>(array.uint64_array(),
                                                          bitmap, begin, count);
    return carquet_writer_write_batch(writer, column, valuesData(values), count,
                                      levelData, nullptr);
  }
  case Array::kFloatArray: {
    auto values = packValues<neug::FloatArray, float>(array.float_array(),
                                                      bitmap, begin, count);
    return carquet_writer_write_batch(writer, column, valuesData(values), count,
                                      levelData, nullptr);
  }
  case Array::kDoubleArray: {
    auto values = packValues<neug::DoubleArray, double>(array.double_array(),
                                                        bitmap, begin, count);
    return carquet_writer_write_batch(writer, column, valuesData(values), count,
                                      levelData, nullptr);
  }
  case Array::kBoolArray: {
    auto values = packValues<neug::BoolArray, uint8_t>(array.bool_array(),
                                                       bitmap, begin, count);
    return carquet_writer_write_batch(writer, column, valuesData(values), count,
                                      levelData, nullptr);
  }
  case Array::kStringArray: {
    return writeStrings(writer, column, array.string_array(), bitmap, begin,
                        count, levelData);
  }
  case Array::kTimestampArray: {
    auto values = packValues<neug::TimestampArray, int64_t>(
        array.timestamp_array(), bitmap, begin, count);
    return carquet_writer_write_batch(writer, column, valuesData(values), count,
                                      levelData, nullptr);
  }
  case Array::kDateArray: {
    std::vector<int32_t> values;
    values.reserve(static_cast<size_t>(count));
    for (int32_t row = begin; row < begin + count; ++row) {
      if (isValid(bitmap, row)) {
        values.push_back(static_cast<int32_t>(
            dateMillisToEpochDays(array.date_array().values(row))));
      }
    }
    return carquet_writer_write_batch(writer, column, valuesData(values), count,
                                      levelData, nullptr);
  }
  case Array::kIntervalArray:
    return writeStrings(writer, column, array.interval_array(), bitmap, begin,
                        count, levelData);
  case Array::kVertexArray:
    return writeStrings(writer, column, array.vertex_array(), bitmap, begin,
                        count, levelData);
  case Array::kEdgeArray:
    return writeStrings(writer, column, array.edge_array(), bitmap, begin,
                        count, levelData);
  case Array::kPathArray:
    return writeStrings(writer, column, array.path_array(), bitmap, begin,
                        count, levelData);
  default:
    return CARQUET_ERROR_NOT_IMPLEMENTED;
  }
}

}  // namespace

Status CarquetExportWriter::writeTable(const QueryResponse* table) {
  try {
    if (!table || table->row_count() == 0) {
      return Status::OK();
    }
    if (schema_.paths.empty() || schema_.paths.front().empty()) {
      return invalid("Parquet output path is empty");
    }
    if (table->row_count() < 0 || table->arrays_size() == 0 ||
        table->schema().name_size() > table->arrays_size()) {
      return invalid("Parquet response shape is invalid");
    }

    carquet_writer_options_t options;
    carquet_writer_options_init(&options);
    int64_t rowsPerGroup = 0;
    bool dictionary = false;
    auto optionStatus =
        parseOptions(schema_.options, options, rowsPerGroup, dictionary);
    if (!optionStatus.ok()) {
      return optionStatus;
    }

    carquet_error_t error = CARQUET_ERROR_INIT;
    std::unique_ptr<carquet_schema_t, decltype(&carquet_schema_free)> schema(
        carquet_schema_create(&error), carquet_schema_free);
    if (!schema) {
      return ioError("Failed to create Carquet schema: " +
                     std::string(error.message));
    }

    std::vector<std::string> columnNames;
    columnNames.reserve(static_cast<size_t>(table->arrays_size()));
    std::unordered_set<std::string> uniqueNames;
    std::vector<bool> boolLeaves;
    bool nested = false;
    for (int32_t column = 0; column < table->arrays_size(); ++column) {
      auto name = columnName(*table, entry_schema_, column);
      if (name.empty() || name.find('\0') != std::string::npos) {
        return invalid("Parquet column name is empty or contains a NUL byte");
      }
      if (!uniqueNames.insert(name).second) {
        return invalid("Duplicate Parquet column name: " + name);
      }
      const ::common::DataType* type =
          entry_schema_ && static_cast<size_t>(column) <
                               entry_schema_->columnTypes.size()
              ? entry_schema_->columnTypes[static_cast<size_t>(column)].get()
              : nullptr;
      auto status =
          validateColumn(table->arrays(column), table->row_count(), name, type);
      if (!status.ok()) {
        return status;
      }
      const auto carquetStatus =
          addField(schema.get(), name.c_str(), table->arrays(column), type, 0,
                   boolLeaves);
      if (carquetStatus != CARQUET_OK) {
        return invalid("Failed to create Parquet column " + name + ": " +
                       carquet_status_string(carquetStatus));
      }
      nested = nested || table->arrays(column).has_list_array() ||
               table->arrays(column).has_struct_array();
      columnNames.emplace_back(std::move(name));
    }

    std::unique_ptr<io::OutputStream> output;
    try {
      output = streamOpener_ ? streamOpener_()
                             : io::openLocalOutputStream(schema_.paths.front());
    } catch (const std::exception& e) {
      return ioError("Failed to open Parquet output: " + std::string(e.what()));
    }
    if (!output) {
      return ioError("Failed to open Parquet output");
    }

    std::unique_ptr<carquet_writer_t, decltype(&carquet_writer_abort)> writer(
        createCarquetWriter(std::move(output), schema.get(), &options, &error),
        carquet_writer_abort);
    if (!writer) {
      return ioError("Failed to create Carquet writer: " +
                     std::string(error.message));
    }
    if (dictionary) {
      for (int32_t column = 0; column < static_cast<int32_t>(boolLeaves.size());
           ++column) {
        if (boolLeaves[static_cast<size_t>(column)]) {
          continue;
        }
        const auto status = carquet_writer_set_column_encoding(
            writer.get(), column, CARQUET_ENCODING_RLE_DICTIONARY);
        if (status != CARQUET_OK) {
          return ioError(
              "Failed to enable Parquet dictionary encoding for leaf " +
              std::to_string(column) + ": " + carquet_status_string(status));
        }
      }
    }

    const int32_t totalRows = table->row_count();
    for (int64_t begin = 0; begin < totalRows; begin += rowsPerGroup) {
      const int32_t count = static_cast<int32_t>(
          std::min<int64_t>(rowsPerGroup, totalRows - begin));
      if (nested) {
        auto status = writeCarquetCDataBatch(writer.get(), *table,
                                             static_cast<int32_t>(begin), count,
                                             columnNames);
        if (!status.ok()) {
          return status;
        }
      } else {
        for (int32_t column = 0; column < table->arrays_size(); ++column) {
          const auto status =
              writeColumn(writer.get(), column, table->arrays(column),
                          static_cast<int32_t>(begin), count);
          if (status != CARQUET_OK) {
            return ioError("Failed to write Parquet column " +
                           columnNames[static_cast<size_t>(column)] + ": " +
                           carquet_status_string(status));
          }
        }
      }
      if (begin + count < totalRows) {
        const auto status = carquet_writer_new_row_group(writer.get());
        if (status != CARQUET_OK) {
          return ioError("Failed to finish Parquet row group: " +
                         std::string(carquet_status_string(status)));
        }
      }
    }

    // Closing a callback writer consumes it even when finalization fails.
    const auto closeStatus = carquet_writer_close(writer.release());
    if (closeStatus != CARQUET_OK) {
      return ioError("Failed to close Carquet writer: " +
                     std::string(carquet_status_string(closeStatus)));
    }
    return Status::OK();
  } catch (const std::exception& e) {
    return ioError("Failed to write Parquet table: " + std::string(e.what()));
  } catch (...) { return ioError("Failed to write Parquet table"); }
}

}  // namespace neug::parquet
