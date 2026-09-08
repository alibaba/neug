/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "schema_converter.h"

#include <string>
#include <string_view>
#include <unordered_set>

#include "column_converter.h"

namespace neug::parquet {
namespace {

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
    auto converted =
        convertArrowCScalarSchemaType(*field, "column \"" + name + "\"");
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
