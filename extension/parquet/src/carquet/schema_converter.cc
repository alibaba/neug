/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "carquet/schema_converter.h"

#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_set>

#include "carquet/column_converter.h"

namespace neug::parquet {
namespace {

constexpr int kMaxSchemaDepth = 64;
constexpr int64_t kMaxSchemaColumns = 1 << 20;

struct ArrowSchemaOwner : ArrowSchema {
  ArrowSchemaOwner() : ArrowSchema{} {}
  ~ArrowSchemaOwner() {
    if (release) {
      release(this);
    }
  }
};

template <typename T>
result<T> schemaError(const std::string& path, const std::string& message) {
  return tl::unexpected(
      Status(StatusCode::ERR_TYPE_CONVERSION,
             "Invalid Parquet schema at " + path + ": " + message));
}

result<uint32_t> parseFixedLength(std::string_view format,
                                  const std::string& path) {
  constexpr std::string_view prefix = "+w:";
  const std::string_view text = format.substr(prefix.size());
  uint64_t value = 0;
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (text.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != text.data() + text.size() || value == 0 ||
      value > std::numeric_limits<uint32_t>::max()) {
    return schemaError<uint32_t>(
        path, "invalid fixed-size list format \"" + std::string(format) + "\"");
  }
  return static_cast<uint32_t>(value);
}

result<std::shared_ptr<::common::DataType>> convertType(
    const ArrowSchema& schema, const std::string& path, int depth) {
  if (depth > kMaxSchemaDepth) {
    return schemaError<std::shared_ptr<::common::DataType>>(
        path, "nesting exceeds 64 levels");
  }
  if (!schema.format) {
    return schemaError<std::shared_ptr<::common::DataType>>(path,
                                                            "missing format");
  }

  const std::string_view format(schema.format);
  if (format == "+l" || format == "+L" || format.starts_with("+w:")) {
    if (schema.dictionary || schema.n_children != 1 || !schema.children ||
        !schema.children[0]) {
      return schemaError<std::shared_ptr<::common::DataType>>(
          path, "list type must have exactly one child");
    }
    auto child = convertType(*schema.children[0], path + ".element", depth + 1);
    if (!child) {
      return tl::unexpected(child.error());
    }

    auto type = std::make_shared<::common::DataType>();
    if (format.starts_with("+w:")) {
      auto fixedLength = parseFixedLength(format, path);
      if (!fixedLength) {
        return tl::unexpected(fixedLength.error());
      }
      auto* array = type->mutable_array();
      *array->mutable_component_type() = **child;
      array->set_fixed_length(*fixedLength);
    } else {
      *type->mutable_list()->mutable_component_type() = **child;
    }
    return type;
  }
  if (format == "+m") {
    if (schema.dictionary || schema.n_children != 1 || !schema.children ||
        !schema.children[0]) {
      return schemaError<std::shared_ptr<::common::DataType>>(
          path, "MAP must have one entries child");
    }
    const auto& entries = *schema.children[0];
    if (!entries.format || std::string_view(entries.format) != "+s" ||
        entries.n_children != 2 || !entries.children || !entries.children[0] ||
        !entries.children[1]) {
      return schemaError<std::shared_ptr<::common::DataType>>(
          path, "MAP entries must contain key and value fields");
    }
    auto key = convertType(*entries.children[0], path + ".key", depth + 1);
    auto value = convertType(*entries.children[1], path + ".value", depth + 1);
    if (!key || !value) {
      return tl::unexpected(!key ? key.error() : value.error());
    }
    // Preserve schema sniffing without introducing MAP value columns.
    auto type = std::make_shared<::common::DataType>();
    *type->mutable_map()->mutable_key_type() = **key;
    *type->mutable_map()->mutable_value_type() = **value;
    return type;
  }
  if (format == "+s") {
    return schemaError<std::shared_ptr<::common::DataType>>(
        path, "STRUCT fields are not supported");
  }
  return convertArrowCScalarSchemaType(schema, path);
}

}  // namespace

result<std::shared_ptr<::common::DataType>> convertArrowCScalarSchemaType(
    const ArrowSchema& schema, const std::string& path) {
  auto valid = validateArrowCScalarSchema(schema, path);
  if (!valid) {
    return tl::unexpected(valid.error());
  }

  const std::string_view format(schema.format);
  auto type = std::make_shared<::common::DataType>();
  if (format == "b") {
    type->set_primitive_type(::common::PrimitiveType::DT_BOOL);
  } else if (format == "c" || format == "s" || format == "i") {
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
  } else if (format == "C" || format == "S" || format == "I") {
    type->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT32);
  } else if (format == "l") {
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
  } else if (format == "L") {
    type->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT64);
  } else if (format == "f") {
    type->set_primitive_type(::common::PrimitiveType::DT_FLOAT);
  } else if (format == "g") {
    type->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
  } else if (format == "u" || format == "U") {
    type->mutable_string()->mutable_var_char();
  } else if (format == "tdD" || format == "tdm") {
    type->mutable_temporal()->mutable_date();
  } else {
    type->mutable_temporal()->mutable_timestamp();
  }
  return type;
}

result<std::shared_ptr<reader::EntrySchema>> convertArrowCStructSchema(
    const ArrowSchema& schema) {
  if (!schema.format || std::string_view(schema.format) != "+s" ||
      schema.dictionary || schema.n_children < 0 ||
      schema.n_children > kMaxSchemaColumns ||
      (schema.n_children > 0 && !schema.children)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_TYPE_CONVERSION,
                        "Carquet returned an invalid root schema");
  }

  auto entry = std::make_shared<reader::TableEntrySchema>();
  entry->columnNames.reserve(static_cast<size_t>(schema.n_children));
  entry->columnTypes.reserve(static_cast<size_t>(schema.n_children));
  std::unordered_set<std::string> names;
  for (int64_t i = 0; i < schema.n_children; ++i) {
    const ArrowSchema* field = schema.children[i];
    if (!field || !field->name || field->name[0] == '\0') {
      return schemaError<std::shared_ptr<reader::EntrySchema>>(
          "root", "unnamed column at index " + std::to_string(i));
    }
    std::string name(field->name);
    if (!names.emplace(name).second) {
      return schemaError<std::shared_ptr<reader::EntrySchema>>(
          "root", "duplicate column \"" + name + "\"");
    }
    auto converted = convertType(*field, "column \"" + name + "\"", 0);
    if (!converted) {
      return tl::unexpected(converted.error());
    }
    entry->columnNames.emplace_back(std::move(name));
    entry->columnTypes.emplace_back(std::move(*converted));
  }
  return std::static_pointer_cast<reader::EntrySchema>(entry);
}

result<std::shared_ptr<reader::EntrySchema>> convertCarquetSchema(
    const carquet_schema_t& schema) {
  ArrowSchemaOwner exported;
  carquet_error_t error = CARQUET_ERROR_INIT;
  const carquet_status_t status =
      carquet_arrow_export_schema(&schema, &exported, &error);
  if (status != CARQUET_OK) {
    RETURN_STATUS_ERROR(
        StatusCode::ERR_TYPE_CONVERSION,
        "Failed to export Carquet schema: " + std::string(error.message));
  }
  return convertArrowCStructSchema(exported);
}

}  // namespace neug::parquet
