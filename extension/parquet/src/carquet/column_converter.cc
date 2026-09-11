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

#include "carquet/column_converter.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "neug/common/columns/value_columns.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace parquet {
namespace {

template <typename T>
result<T> columnError(const std::string& path, const std::string& message) {
  return tl::unexpected(
      Status(StatusCode::ERR_TYPE_CONVERSION,
             "Invalid Parquet column at " + path + ": " + message));
}

result<uint64_t> validateArray(const ArrowSchema& schema,
                               const ArrowArray& array, const std::string& path,
                               int64_t expectedBuffers) {
  if (!schema.format) {
    return columnError<uint64_t>(path, "missing schema format");
  }
  if (schema.dictionary || array.dictionary) {
    return columnError<uint64_t>(path, "dictionary arrays are not supported");
  }
  if (schema.n_children != 0 || array.n_children != 0) {
    return columnError<uint64_t>(path, "scalar column must not have children");
  }
  if (array.length < 0 || array.offset < 0 || array.null_count < -1 ||
      array.null_count > array.length) {
    return columnError<uint64_t>(path, "invalid length, offset, or null count");
  }
  if (static_cast<uint64_t>(array.length) >
      std::numeric_limits<size_t>::max()) {
    return columnError<uint64_t>(path, "column length exceeds address space");
  }
  if (array.n_buffers != expectedBuffers || !array.buffers) {
    return columnError<uint64_t>(
        path, "expected " + std::to_string(expectedBuffers) + " buffers");
  }
  if (array.offset > std::numeric_limits<int64_t>::max() - array.length) {
    return columnError<uint64_t>(path, "offset plus length overflows");
  }
  if (array.null_count > 0 && !array.buffers[0]) {
    return columnError<uint64_t>(path, "null values require a validity buffer");
  }
  return static_cast<uint64_t>(array.offset);
}

bool isValid(const ArrowArray& array, uint64_t index) {
  if (!array.buffers[0]) {
    return true;
  }
  const auto* validity = static_cast<const uint8_t*>(array.buffers[0]);
  return (validity[index >> 3] & static_cast<uint8_t>(1u << (index & 7))) != 0;
}

template <typename Src, typename Dst>
result<std::shared_ptr<IContextColumn>> convertPrimitive(
    const ArrowSchema& schema, const ArrowArray& array,
    const std::string& path) {
  auto offsetResult = validateArray(schema, array, path, 2);
  if (!offsetResult) {
    return tl::unexpected(offsetResult.error());
  }
  if (array.length > 0 && !array.buffers[1]) {
    return columnError<std::shared_ptr<IContextColumn>>(
        path, "non-empty column has no data buffer");
  }

  ValueColumnBuilder<Dst> builder;
  builder.reserve(static_cast<size_t>(array.length));
  const auto* data = static_cast<const Src*>(array.buffers[1]);
  for (uint64_t i = 0; i < static_cast<uint64_t>(array.length); ++i) {
    const uint64_t sourceIndex = *offsetResult + i;
    if (!isValid(array, sourceIndex)) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(static_cast<Dst>(data[sourceIndex]));
    }
  }
  return builder.finish();
}

result<std::shared_ptr<IContextColumn>> convertBoolean(
    const ArrowSchema& schema, const ArrowArray& array,
    const std::string& path) {
  auto offsetResult = validateArray(schema, array, path, 2);
  if (!offsetResult) {
    return tl::unexpected(offsetResult.error());
  }
  if (array.length > 0 && !array.buffers[1]) {
    return columnError<std::shared_ptr<IContextColumn>>(
        path, "non-empty column has no data buffer");
  }

  ValueColumnBuilder<bool> builder;
  builder.reserve(static_cast<size_t>(array.length));
  const auto* data = static_cast<const uint8_t*>(array.buffers[1]);
  for (uint64_t i = 0; i < static_cast<uint64_t>(array.length); ++i) {
    const uint64_t sourceIndex = *offsetResult + i;
    if (!isValid(array, sourceIndex)) {
      builder.push_back_null();
    } else {
      builder.push_back_opt((data[sourceIndex >> 3] &
                             static_cast<uint8_t>(1u << (sourceIndex & 7))) !=
                            0);
    }
  }
  return builder.finish();
}

template <typename Offset>
result<std::shared_ptr<IContextColumn>> convertString(const ArrowSchema& schema,
                                                      const ArrowArray& array,
                                                      const std::string& path) {
  auto offsetResult = validateArray(schema, array, path, 3);
  if (!offsetResult) {
    return tl::unexpected(offsetResult.error());
  }
  if (!array.buffers[1]) {
    return columnError<std::shared_ptr<IContextColumn>>(
        path, "string column has no offsets buffer");
  }

  const auto* offsets = static_cast<const Offset*>(array.buffers[1]);
  const auto* data = static_cast<const char*>(array.buffers[2]);
  const uint64_t firstIndex = *offsetResult;
  ValueColumnBuilder<std::string> builder;
  builder.reserve(static_cast<size_t>(array.length));
  for (uint64_t i = 0; i < static_cast<uint64_t>(array.length); ++i) {
    const uint64_t sourceIndex = firstIndex + i;
    const Offset begin = offsets[sourceIndex];
    const Offset end = offsets[sourceIndex + 1];
    if (begin < 0 || end < begin) {
      return columnError<std::shared_ptr<IContextColumn>>(
          path, "string offsets are negative or decreasing");
    }
    if (static_cast<uint64_t>(end) >
        static_cast<uint64_t>(std::numeric_limits<std::ptrdiff_t>::max())) {
      return columnError<std::shared_ptr<IContextColumn>>(
          path, "string offset exceeds addressable memory");
    }
    if (end > begin && !data) {
      return columnError<std::shared_ptr<IContextColumn>>(
          path, "non-empty string has no data buffer");
    }
    if (!isValid(array, sourceIndex)) {
      builder.push_back_null();
    } else if (begin == end) {
      builder.push_back_opt(std::string{});
    } else {
      builder.push_back_opt(
          std::string(data + static_cast<uint64_t>(begin),
                      static_cast<size_t>(static_cast<uint64_t>(end) -
                                          static_cast<uint64_t>(begin))));
    }
  }
  return builder.finish();
}

result<std::shared_ptr<IContextColumn>> convertDate32(const ArrowSchema& schema,
                                                      const ArrowArray& array,
                                                      const std::string& path) {
  auto offsetResult = validateArray(schema, array, path, 2);
  if (!offsetResult) {
    return tl::unexpected(offsetResult.error());
  }
  if (array.length > 0 && !array.buffers[1]) {
    return columnError<std::shared_ptr<IContextColumn>>(
        path, "non-empty date column has no data buffer");
  }

  const auto* data = static_cast<const int32_t*>(array.buffers[1]);
  ValueColumnBuilder<Date> builder;
  builder.reserve(static_cast<size_t>(array.length));
  for (uint64_t i = 0; i < static_cast<uint64_t>(array.length); ++i) {
    const uint64_t sourceIndex = *offsetResult + i;
    if (!isValid(array, sourceIndex)) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(Date(data[sourceIndex]));
    }
  }
  return builder.finish();
}

result<std::shared_ptr<IContextColumn>> convertDate64(const ArrowSchema& schema,
                                                      const ArrowArray& array,
                                                      const std::string& path) {
  constexpr int64_t kMillisPerDay = 24LL * 60 * 60 * 1000;
  auto offsetResult = validateArray(schema, array, path, 2);
  if (!offsetResult) {
    return tl::unexpected(offsetResult.error());
  }
  if (array.length > 0 && !array.buffers[1]) {
    return columnError<std::shared_ptr<IContextColumn>>(
        path, "non-empty date column has no data buffer");
  }

  const auto* data = static_cast<const int64_t*>(array.buffers[1]);
  ValueColumnBuilder<Date> builder;
  builder.reserve(static_cast<size_t>(array.length));
  for (uint64_t i = 0; i < static_cast<uint64_t>(array.length); ++i) {
    const uint64_t sourceIndex = *offsetResult + i;
    if (!isValid(array, sourceIndex)) {
      builder.push_back_null();
    } else {
      const int64_t days = data[sourceIndex] / kMillisPerDay;
      if (days < std::numeric_limits<int32_t>::min() ||
          days > std::numeric_limits<int32_t>::max()) {
        return columnError<std::shared_ptr<IContextColumn>>(
            path, "date value is outside NeuG's supported range");
      }
      builder.push_back_opt(Date(static_cast<int32_t>(days)));
    }
  }
  return builder.finish();
}

result<std::shared_ptr<IContextColumn>> convertTimestamp(
    const ArrowSchema& schema, const ArrowArray& array, const std::string& path,
    char unit) {
  auto offsetResult = validateArray(schema, array, path, 2);
  if (!offsetResult) {
    return tl::unexpected(offsetResult.error());
  }
  if (array.length > 0 && !array.buffers[1]) {
    return columnError<std::shared_ptr<IContextColumn>>(
        path, "non-empty timestamp column has no data buffer");
  }

  const auto* data = static_cast<const int64_t*>(array.buffers[1]);
  ValueColumnBuilder<DateTime> builder;
  builder.reserve(static_cast<size_t>(array.length));
  for (uint64_t i = 0; i < static_cast<uint64_t>(array.length); ++i) {
    const uint64_t sourceIndex = *offsetResult + i;
    if (!isValid(array, sourceIndex)) {
      builder.push_back_null();
      continue;
    }
    int64_t millis = data[sourceIndex];
    if (unit == 's') {
      if (millis < std::numeric_limits<int64_t>::min() / 1000 ||
          millis > std::numeric_limits<int64_t>::max() / 1000) {
        return columnError<std::shared_ptr<IContextColumn>>(
            path, "timestamp seconds overflow NeuG milliseconds");
      }
      millis *= 1000;
    } else if (unit == 'u') {
      millis /= 1000;
    } else if (unit == 'n') {
      millis /= 1000000;
    }
    builder.push_back_opt(DateTime(millis));
  }
  return builder.finish();
}

}  // namespace

result<std::shared_ptr<IContextColumn>> convertArrowCScalarColumn(
    const ArrowSchema& schema, const ArrowArray& array,
    const std::string& path) {
  auto schemaStatus = validateArrowCScalarSchema(schema, path);
  if (!schemaStatus) {
    return tl::unexpected(schemaStatus.error());
  }
  const std::string_view format(schema.format);
  if (format == "b") {
    return convertBoolean(schema, array, path);
  }
  if (format == "c") {
    return convertPrimitive<int8_t, int32_t>(schema, array, path);
  }
  if (format == "s") {
    return convertPrimitive<int16_t, int32_t>(schema, array, path);
  }
  if (format == "i") {
    return convertPrimitive<int32_t, int32_t>(schema, array, path);
  }
  if (format == "C") {
    return convertPrimitive<uint8_t, uint32_t>(schema, array, path);
  }
  if (format == "S") {
    return convertPrimitive<uint16_t, uint32_t>(schema, array, path);
  }
  if (format == "I") {
    return convertPrimitive<uint32_t, uint32_t>(schema, array, path);
  }
  if (format == "l") {
    return convertPrimitive<int64_t, int64_t>(schema, array, path);
  }
  if (format == "L") {
    return convertPrimitive<uint64_t, uint64_t>(schema, array, path);
  }
  if (format == "f") {
    return convertPrimitive<float, float>(schema, array, path);
  }
  if (format == "g") {
    return convertPrimitive<double, double>(schema, array, path);
  }
  if (format == "u") {
    return convertString<int32_t>(schema, array, path);
  }
  if (format == "U") {
    return convertString<int64_t>(schema, array, path);
  }
  if (format == "tdD") {
    return convertDate32(schema, array, path);
  }
  if (format == "tdm") {
    return convertDate64(schema, array, path);
  }
  if (format.size() >= 4 && format[0] == 't' && format[1] == 's' &&
      (format[2] == 's' || format[2] == 'm' || format[2] == 'u' ||
       format[2] == 'n') &&
      format[3] == ':') {
    return convertTimestamp(schema, array, path, format[2]);
  }
  return columnError<std::shared_ptr<IContextColumn>>(
      path, "unsupported Arrow C format \"" + std::string(format) + "\"");
}

result<void> validateArrowCScalarSchema(const ArrowSchema& schema,
                                        const std::string& path) {
  if (!schema.format) {
    return columnError<void>(path, "missing schema format");
  }
  if (schema.dictionary) {
    return columnError<void>(path, "dictionary schema is not supported");
  }
  if (schema.n_children != 0) {
    return columnError<void>(path, "scalar column must not have children");
  }
  const std::string_view format(schema.format);
  const bool primitive = format == "b" || format == "c" || format == "s" ||
                         format == "i" || format == "C" || format == "S" ||
                         format == "I" || format == "l" || format == "L" ||
                         format == "f" || format == "g" || format == "u" ||
                         format == "U" || format == "tdD" || format == "tdm";
  const bool timestamp = format.size() >= 4 && format[0] == 't' &&
                         format[1] == 's' &&
                         (format[2] == 's' || format[2] == 'm' ||
                          format[2] == 'u' || format[2] == 'n') &&
                         format[3] == ':';
  if (!primitive && !timestamp) {
    return columnError<void>(
        path, "unsupported Arrow C format \"" + std::string(format) + "\"");
  }
  return {};
}

result<std::shared_ptr<DataChunk>> convertArrowCFlatStruct(
    const ArrowSchema& schema, const ArrowArray& array) {
  if (!schema.format || std::string_view(schema.format) != "+s") {
    return columnError<std::shared_ptr<DataChunk>>(
        "root", "schema must be a top-level struct");
  }
  if (schema.dictionary || array.dictionary) {
    return columnError<std::shared_ptr<DataChunk>>(
        "root", "dictionary root is not supported");
  }
  if (schema.n_children < 0 || array.n_children < 0 ||
      schema.n_children != array.n_children ||
      (schema.n_children > 0 && (!schema.children || !array.children))) {
    return columnError<std::shared_ptr<DataChunk>>(
        "root", "schema and array child counts do not match");
  }
  if (schema.n_children > (1 << 20)) {
    return columnError<std::shared_ptr<DataChunk>>("root",
                                                   "too many struct fields");
  }
  if (array.length < 0 || array.offset != 0 || array.null_count < -1 ||
      array.null_count > array.length || array.n_buffers != 1 ||
      !array.buffers) {
    return columnError<std::shared_ptr<DataChunk>>(
        "root", "invalid top-level struct layout");
  }
  if (array.null_count != 0 || array.buffers[0]) {
    return columnError<std::shared_ptr<DataChunk>>(
        "root", "nullable top-level structs are not supported");
  }

  auto chunk = std::make_shared<DataChunk>();
  for (int64_t i = 0; i < schema.n_children; ++i) {
    if (!schema.children[i] || !array.children[i]) {
      return columnError<std::shared_ptr<DataChunk>>(
          "root", "struct contains a null child");
    }
    if (array.children[i]->length != array.length) {
      return columnError<std::shared_ptr<DataChunk>>(
          "root.field[" + std::to_string(i) + "]",
          "child length does not match the struct");
    }
    auto converted =
        convertArrowCScalarColumn(*schema.children[i], *array.children[i],
                                  "root.field[" + std::to_string(i) + "]");
    if (!converted) {
      return tl::unexpected(converted.error());
    }
    chunk->set(static_cast<int>(i), std::move(*converted));
  }
  return chunk;
}

}  // namespace parquet
}  // namespace neug
