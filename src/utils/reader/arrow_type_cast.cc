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

#include "neug/utils/reader/arrow_type_cast.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <glog/logging.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

#include "neug/compiler/common/types/timestamp_t.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace reader {

namespace {

// Trim leading/trailing whitespace from a string_view
std::string_view trim(std::string_view sv) {
  while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.front()))) {
    sv.remove_prefix(1);
  }
  while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.back()))) {
    sv.remove_suffix(1);
  }
  return sv;
}

// Get string_view from a string or large_string array at index i
std::string_view getStringView(const arrow::Array* array, int64_t i) {
  if (array->type_id() == arrow::Type::LARGE_STRING) {
    return static_cast<const arrow::LargeStringArray*>(array)->GetView(i);
  }
  return static_cast<const arrow::StringArray*>(array)->GetView(i);
}

// Split a string by delimiter, respecting nested brackets
std::vector<std::string_view> splitElements(std::string_view content) {
  std::vector<std::string_view> elements;
  int depth = 0;
  size_t start = 0;
  for (size_t i = 0; i < content.size(); ++i) {
    char c = content[i];
    if (c == '[') {
      ++depth;
    } else if (c == ']') {
      --depth;
    } else if (c == ',' && depth == 0) {
      elements.push_back(trim(content.substr(start, i - start)));
      start = i + 1;
    }
  }
  auto last = trim(content.substr(start));
  if (!last.empty()) {
    elements.push_back(last);
  }
  return elements;
}

// Parse a single scalar value and append to the appropriate builder
void appendScalarValue(arrow::ArrayBuilder* builder, std::string_view value,
                       const std::shared_ptr<arrow::DataType>& valueType) {
  std::string val_str(value);

  switch (valueType->id()) {
  case arrow::Type::INT8: {
    auto* b = static_cast<arrow::Int8Builder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(static_cast<int8_t>(std::stoi(val_str))));
    break;
  }
  case arrow::Type::INT16: {
    auto* b = static_cast<arrow::Int16Builder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(static_cast<int16_t>(std::stoi(val_str))));
    break;
  }
  case arrow::Type::INT32: {
    auto* b = static_cast<arrow::Int32Builder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(std::stoi(val_str)));
    break;
  }
  case arrow::Type::INT64: {
    auto* b = static_cast<arrow::Int64Builder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(std::stoll(val_str)));
    break;
  }
  case arrow::Type::UINT8: {
    auto* b = static_cast<arrow::UInt8Builder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(static_cast<uint8_t>(std::stoul(val_str))));
    break;
  }
  case arrow::Type::UINT16: {
    auto* b = static_cast<arrow::UInt16Builder*>(builder);
    THROW_IF_ARROW_NOT_OK(
        b->Append(static_cast<uint16_t>(std::stoul(val_str))));
    break;
  }
  case arrow::Type::UINT32: {
    auto* b = static_cast<arrow::UInt32Builder*>(builder);
    THROW_IF_ARROW_NOT_OK(
        b->Append(static_cast<uint32_t>(std::stoul(val_str))));
    break;
  }
  case arrow::Type::UINT64: {
    auto* b = static_cast<arrow::UInt64Builder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(std::stoull(val_str)));
    break;
  }
  case arrow::Type::FLOAT: {
    auto* b = static_cast<arrow::FloatBuilder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(std::stof(val_str)));
    break;
  }
  case arrow::Type::DOUBLE: {
    auto* b = static_cast<arrow::DoubleBuilder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(std::stod(val_str)));
    break;
  }
  case arrow::Type::BOOL: {
    auto* b = static_cast<arrow::BooleanBuilder*>(builder);
    std::string lower = val_str;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    bool bval = (lower == "true" || lower == "1");
    THROW_IF_ARROW_NOT_OK(b->Append(bval));
    break;
  }
  case arrow::Type::STRING: {
    auto* b = static_cast<arrow::StringBuilder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(val_str));
    break;
  }
  case arrow::Type::LARGE_STRING: {
    auto* b = static_cast<arrow::LargeStringBuilder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(val_str));
    break;
  }
  case arrow::Type::DATE32: {
    // Parse ISO date string "YYYY-MM-DD" -> days since epoch
    auto* b = static_cast<arrow::Date32Builder*>(builder);
    auto d = ::neug::common::Date::fromCString(val_str.c_str(), val_str.size());
    THROW_IF_ARROW_NOT_OK(b->Append(d.days));
    break;
  }
  case arrow::Type::DATE64: {
    // Parse ISO date string "YYYY-MM-DD" -> milliseconds since epoch
    auto* b = static_cast<arrow::Date64Builder*>(builder);
    auto d = ::neug::common::Date::fromCString(val_str.c_str(), val_str.size());
    THROW_IF_ARROW_NOT_OK(
        b->Append(static_cast<int64_t>(d.days) * 86400LL * 1000LL));
    break;
  }
  case arrow::Type::TIMESTAMP: {
    // Parse ISO timestamp string "YYYY-MM-DD hh:mm:ss" -> value in
    // microseconds; convert to the target unit.
    auto* b = static_cast<arrow::TimestampBuilder*>(builder);
    auto ts =
        ::neug::common::Timestamp::fromCString(val_str.c_str(), val_str.size());
    auto tsType = std::static_pointer_cast<arrow::TimestampType>(valueType);
    int64_t v;
    switch (tsType->unit()) {
    case arrow::TimeUnit::SECOND:
      v = ts.value / 1000000LL;
      break;
    case arrow::TimeUnit::MILLI:
      v = ts.value / 1000LL;
      break;
    case arrow::TimeUnit::MICRO:
      v = ts.value;
      break;
    case arrow::TimeUnit::NANO:
      v = ts.value * 1000LL;
      break;
    }
    THROW_IF_ARROW_NOT_OK(b->Append(v));
    break;
  }
  case arrow::Type::DURATION: {
    auto* b = static_cast<arrow::DurationBuilder*>(builder);
    THROW_IF_ARROW_NOT_OK(b->Append(std::stoll(val_str)));
    break;
  }
  case arrow::Type::LIST:
  case arrow::Type::LARGE_LIST: {
    // Nested list: directly parse the string into the existing builder
    // without creating temporary arrays, avoiding memory copies.
    auto* listBuilder = static_cast<arrow::ListBuilder*>(builder);
    auto childType =
        std::static_pointer_cast<arrow::BaseListType>(valueType)->value_type();
    auto* childBuilder = listBuilder->value_builder();

    THROW_IF_ARROW_NOT_OK(listBuilder->Append());

    // Parse the inner content: strip brackets and split elements
    std::string_view sv(value);
    sv = trim(sv);
    if (sv.empty() || sv == "[]") {
      break;  // empty list, already appended list start
    }
    if (sv.front() == '[' && sv.back() == ']') {
      sv = sv.substr(1, sv.size() - 2);
    }
    sv = trim(sv);
    if (sv.empty()) {
      break;
    }
    auto elements = splitElements(sv);
    for (const auto& elem : elements) {
      appendScalarValue(childBuilder, elem, childType);
    }
    break;
  }
  case arrow::Type::FIXED_SIZE_LIST: {
    auto* fslBuilder = static_cast<arrow::FixedSizeListBuilder*>(builder);
    auto fslType =
        std::static_pointer_cast<arrow::FixedSizeListType>(valueType);
    auto childType = fslType->value_type();
    int32_t listSize = fslType->list_size();
    auto* childBuilder = fslBuilder->value_builder();

    THROW_IF_ARROW_NOT_OK(fslBuilder->Append());

    std::string_view sv(value);
    sv = trim(sv);
    if (sv.front() == '[' && sv.back() == ']') {
      sv = sv.substr(1, sv.size() - 2);
    }
    sv = trim(sv);
    auto elements = splitElements(sv);
    if (static_cast<int32_t>(elements.size()) != listSize) {
      THROW_CONVERSION_EXCEPTION(
          "FixedSizeList expects " + std::to_string(listSize) +
          " elements but got " + std::to_string(elements.size()));
    }
    for (const auto& elem : elements) {
      appendScalarValue(childBuilder, elem, childType);
    }
    break;
  }
  default:
    THROW_CONVERSION_EXCEPTION(
        "Unsupported list element type for CSV array parsing: " +
        valueType->ToString());
  }
}

// Parse a string array into a ListArray by delegating to appendScalarValue
std::shared_ptr<arrow::Array> parseStringToList(
    const std::shared_ptr<arrow::Array>& stringArray,
    const std::shared_ptr<arrow::DataType>& listType) {
  auto builderResult = arrow::MakeBuilder(listType);
  THROW_IF_ARROW_NOT_OK(builderResult.status());
  auto builder = std::move(builderResult).ValueOrDie();

  int64_t length = stringArray->length();
  for (int64_t i = 0; i < length; ++i) {
    if (stringArray->IsNull(i)) {
      THROW_IF_ARROW_NOT_OK(builder->AppendNull());
      continue;
    }
    auto sv = getStringView(stringArray.get(), i);
    appendScalarValue(builder.get(), sv, listType);
  }

  auto result = builder->Finish();
  THROW_IF_ARROW_NOT_OK(result.status());
  return result.ValueOrDie();
}

// Parse a string array into a FixedSizeListArray by delegating to
// appendScalarValue
std::shared_ptr<arrow::Array> parseStringToFixedSizeList(
    const std::shared_ptr<arrow::Array>& stringArray,
    const std::shared_ptr<arrow::DataType>& listType) {
  auto builderResult = arrow::MakeBuilder(listType);
  THROW_IF_ARROW_NOT_OK(builderResult.status());
  auto builder = std::move(builderResult).ValueOrDie();

  int64_t length = stringArray->length();
  for (int64_t i = 0; i < length; ++i) {
    if (stringArray->IsNull(i)) {
      THROW_IF_ARROW_NOT_OK(builder->AppendNull());
      continue;
    }
    auto sv = getStringView(stringArray.get(), i);
    appendScalarValue(builder.get(), sv, listType);
  }

  auto result = builder->Finish();
  THROW_IF_ARROW_NOT_OK(result.status());
  return result.ValueOrDie();
}

}  // namespace

void ArrowTypeCaster::registerCast(arrow::Type::type from, arrow::Type::type to,
                                   CastFunc func) {
  casters_[{from, to}] = std::move(func);
}

std::shared_ptr<arrow::Array> ArrowTypeCaster::castArray(
    const std::shared_ptr<arrow::Array>& array,
    const std::shared_ptr<arrow::DataType>& targetType) const {
  if (array->type()->Equals(targetType)) {
    return array;
  }

  auto key = std::make_pair(array->type_id(), targetType->id());
  auto it = casters_.find(key);
  if (it == casters_.end()) {
    THROW_CONVERSION_EXCEPTION("No registered cast from " +
                               array->type()->ToString() + " to " +
                               targetType->ToString());
  }
  return it->second(array, targetType);
}

std::shared_ptr<arrow::ChunkedArray> ArrowTypeCaster::castChunkedArray(
    const std::shared_ptr<arrow::ChunkedArray>& array,
    const std::shared_ptr<arrow::DataType>& targetType) const {
  if (array->type()->Equals(targetType)) {
    return array;
  }

  std::vector<std::shared_ptr<arrow::Array>> newChunks;
  newChunks.reserve(array->num_chunks());
  for (int c = 0; c < array->num_chunks(); ++c) {
    newChunks.push_back(castArray(array->chunk(c), targetType));
  }
  return std::make_shared<arrow::ChunkedArray>(std::move(newChunks),
                                               targetType);
}

std::shared_ptr<arrow::Table> ArrowTypeCaster::castTable(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<arrow::Schema>& expectedSchema) const {
  bool needsCast = false;
  for (int i = 0; i < table->num_columns(); ++i) {
    if (!table->column(i)->type()->Equals(expectedSchema->field(i)->type())) {
      needsCast = true;
      break;
    }
  }
  if (!needsCast) {
    return table;
  }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> newColumns;
  newColumns.reserve(table->num_columns());
  for (int i = 0; i < table->num_columns(); ++i) {
    auto expectedType = expectedSchema->field(i)->type();
    newColumns.push_back(castChunkedArray(table->column(i), expectedType));
  }
  return arrow::Table::Make(expectedSchema, std::move(newColumns),
                            table->num_rows());
}

std::shared_ptr<arrow::RecordBatch> ArrowTypeCaster::castBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& expectedSchema) const {
  bool needsCast = false;
  for (int i = 0; i < batch->num_columns(); ++i) {
    if (!batch->column(i)->type()->Equals(expectedSchema->field(i)->type())) {
      needsCast = true;
      break;
    }
  }
  if (!needsCast) {
    return batch;
  }

  std::vector<std::shared_ptr<arrow::Array>> newColumns;
  newColumns.reserve(batch->num_columns());
  for (int i = 0; i < batch->num_columns(); ++i) {
    auto expectedType = expectedSchema->field(i)->type();
    newColumns.push_back(castArray(batch->column(i), expectedType));
  }
  return arrow::RecordBatch::Make(expectedSchema, batch->num_rows(),
                                  std::move(newColumns));
}

// --- LazyTypeCastRecordBatch implementation ---

LazyTypeCastRecordBatch::LazyTypeCastRecordBatch(
    std::shared_ptr<arrow::RecordBatch> inner,
    std::shared_ptr<ArrowTypeCaster> caster,
    std::shared_ptr<arrow::Schema> expectedSchema)
    : RecordBatch(expectedSchema, inner->num_rows()),
      inner_(std::move(inner)),
      caster_(std::move(caster)),
      expectedSchema_(expectedSchema),
      cachedColumns_(expectedSchema->num_fields()),
      materialized_(expectedSchema->num_fields(), false) {}

void LazyTypeCastRecordBatch::materializeColumn(int i) const {
  if (materialized_[i])
    return;
  auto srcCol = inner_->column(i);
  auto expectedType = expectedSchema_->field(i)->type();
  if (!srcCol->type()->Equals(expectedType)) {
    cachedColumns_[i] = caster_->castArray(srcCol, expectedType);
  } else {
    cachedColumns_[i] = srcCol;
  }
  materialized_[i] = true;
}

void LazyTypeCastRecordBatch::materializeAll() const {
  if (allMaterialized_)
    return;
  for (int i = 0; i < num_columns(); ++i) {
    materializeColumn(i);
  }
  allMaterialized_ = true;
}

std::shared_ptr<arrow::Array> LazyTypeCastRecordBatch::column(int i) const {
  materializeColumn(i);
  return cachedColumns_[i];
}

const std::vector<std::shared_ptr<arrow::Array>>&
LazyTypeCastRecordBatch::columns() const {
  materializeAll();
  return cachedColumns_;
}

std::shared_ptr<arrow::ArrayData> LazyTypeCastRecordBatch::column_data(
    int i) const {
  materializeColumn(i);
  return cachedColumns_[i]->data();
}

const arrow::ArrayDataVector& LazyTypeCastRecordBatch::column_data() const {
  materializeAll();
  cachedColumnData_.resize(cachedColumns_.size());
  for (size_t i = 0; i < cachedColumns_.size(); ++i) {
    cachedColumnData_[i] = cachedColumns_[i]->data();
  }
  return cachedColumnData_;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
LazyTypeCastRecordBatch::AddColumn(
    int i, const std::shared_ptr<arrow::Field>& field,
    const std::shared_ptr<arrow::Array>& col) const {
  return inner_->AddColumn(i, field, col);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
LazyTypeCastRecordBatch::SetColumn(
    int i, const std::shared_ptr<arrow::Field>& field,
    const std::shared_ptr<arrow::Array>& col) const {
  return inner_->SetColumn(i, field, col);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
LazyTypeCastRecordBatch::RemoveColumn(int i) const {
  return inner_->RemoveColumn(i);
}

std::shared_ptr<arrow::RecordBatch>
LazyTypeCastRecordBatch::ReplaceSchemaMetadata(
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) const {
  return inner_->ReplaceSchemaMetadata(metadata);
}

std::shared_ptr<arrow::RecordBatch> LazyTypeCastRecordBatch::Slice(
    int64_t offset, int64_t length) const {
  return inner_->Slice(offset, length);
}

const std::shared_ptr<arrow::Device::SyncEvent>&
LazyTypeCastRecordBatch::GetSyncEvent() const {
  return inner_->GetSyncEvent();
}

arrow::DeviceAllocationType LazyTypeCastRecordBatch::device_type() const {
  return inner_->device_type();
}

std::shared_ptr<ArrowTypeCaster> ArrowTypeCaster::createDefault() {
  auto caster = std::make_shared<ArrowTypeCaster>();

  // Register large_string -> list conversion
  caster->registerCast(arrow::Type::LARGE_STRING, arrow::Type::LIST,
                       [](const std::shared_ptr<arrow::Array>& array,
                          const std::shared_ptr<arrow::DataType>& targetType)
                           -> std::shared_ptr<arrow::Array> {
                         return parseStringToList(array, targetType);
                       });

  // Register string -> list conversion
  caster->registerCast(arrow::Type::STRING, arrow::Type::LIST,
                       [](const std::shared_ptr<arrow::Array>& array,
                          const std::shared_ptr<arrow::DataType>& targetType)
                           -> std::shared_ptr<arrow::Array> {
                         return parseStringToList(array, targetType);
                       });

  // Register large_string -> fixed_size_list conversion
  caster->registerCast(arrow::Type::LARGE_STRING, arrow::Type::FIXED_SIZE_LIST,
                       [](const std::shared_ptr<arrow::Array>& array,
                          const std::shared_ptr<arrow::DataType>& targetType)
                           -> std::shared_ptr<arrow::Array> {
                         return parseStringToFixedSizeList(array, targetType);
                       });

  // Register string -> fixed_size_list conversion
  caster->registerCast(arrow::Type::STRING, arrow::Type::FIXED_SIZE_LIST,
                       [](const std::shared_ptr<arrow::Array>& array,
                          const std::shared_ptr<arrow::DataType>& targetType)
                           -> std::shared_ptr<arrow::Array> {
                         return parseStringToFixedSizeList(array, targetType);
                       });

  return caster;
}

}  // namespace reader
}  // namespace neug
