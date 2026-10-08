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

#include "carquet/nested_converter.h"

#include <charconv>
#include <cstdint>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include "carquet/column_converter.h"
#include "neug/common/columns/array_columns.h"
#include "neug/common/columns/list_columns.h"

namespace neug {
namespace parquet {
namespace {

constexpr int kMaxNestedDepth = 64;
constexpr int64_t kMaxChildren = 1 << 20;

template <typename T>
result<T> nestedError(const std::string& path, const std::string& message) {
  return tl::unexpected(
      Status(StatusCode::ERR_TYPE_CONVERSION,
             "Invalid nested Parquet column at " + path + ": " + message));
}

bool isValid(const ArrowArray& array, uint64_t physicalIndex) {
  if (!array.buffers[0]) {
    return true;
  }
  const auto* validity = static_cast<const uint8_t*>(array.buffers[0]);
  return (validity[physicalIndex >> 3] &
          static_cast<uint8_t>(1u << (physicalIndex & 7))) != 0;
}

result<uint64_t> validateLayout(const ArrowSchema& schema,
                                const ArrowArray& array,
                                const std::string& path, uint64_t start,
                                uint64_t count, int64_t expectedBuffers,
                                int64_t expectedChildren) {
  if (!schema.format) {
    return nestedError<uint64_t>(path, "missing schema format");
  }
  if (schema.dictionary || array.dictionary) {
    return nestedError<uint64_t>(path, "dictionary arrays are not supported");
  }
  if (array.length < 0 || array.offset < 0 || array.null_count < -1 ||
      array.null_count > array.length) {
    return nestedError<uint64_t>(path, "invalid length, offset, or null count");
  }
  const auto length = static_cast<uint64_t>(array.length);
  if (length > std::numeric_limits<size_t>::max()) {
    return nestedError<uint64_t>(path, "array length exceeds address space");
  }
  if (start > length || count > length - start) {
    return nestedError<uint64_t>(path,
                                 "requested range exceeds the child array");
  }
  const auto offset = static_cast<uint64_t>(array.offset);
  if (offset >
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - length) {
    return nestedError<uint64_t>(path, "offset plus length overflows");
  }
  if (schema.n_children != expectedChildren ||
      array.n_children != expectedChildren) {
    return nestedError<uint64_t>(
        path, "expected " + std::to_string(expectedChildren) + " children");
  }
  if (expectedChildren > 0 && (!schema.children || !array.children)) {
    return nestedError<uint64_t>(path, "missing child pointer array");
  }
  for (int64_t i = 0; i < expectedChildren; ++i) {
    if (!schema.children[i] || !array.children[i]) {
      return nestedError<uint64_t>(path, "contains a null child pointer");
    }
  }
  if (array.n_buffers != expectedBuffers || !array.buffers) {
    return nestedError<uint64_t>(
        path, "expected " + std::to_string(expectedBuffers) + " buffers");
  }
  if (array.null_count > 0 && !array.buffers[0]) {
    return nestedError<uint64_t>(path, "null values require a validity buffer");
  }
  return offset + start;
}

result<uint64_t> parseFixedLength(std::string_view format,
                                  const std::string& path) {
  constexpr std::string_view prefix = "+w:";
  const auto text = format.substr(prefix.size());
  uint64_t length = 0;
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), length);
  if (text.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != text.data() + text.size() || length == 0 ||
      length > std::numeric_limits<uint32_t>::max()) {
    return nestedError<uint64_t>(
        path, "invalid fixed-size list format \"" + std::string(format) + "\"");
  }
  return length;
}

result<std::shared_ptr<IContextColumn>> convertRange(
    const ArrowSchema& schema, const ArrowArray& array, uint64_t start,
    uint64_t count, const std::string& path, int depth);

template <typename Offset>
result<std::vector<uint64_t>> readOffsets(const ArrowArray& array,
                                          uint64_t physicalStart,
                                          uint64_t count,
                                          const std::string& path) {
  if (!array.buffers[1]) {
    return nestedError<std::vector<uint64_t>>(path, "missing offsets buffer");
  }
  const auto* offsets = static_cast<const Offset*>(array.buffers[1]);
  std::vector<uint64_t> result;
  result.reserve(static_cast<size_t>(count + 1));
  for (uint64_t i = 0; i <= count; ++i) {
    const Offset raw = offsets[physicalStart + i];
    if (raw < 0) {
      return nestedError<std::vector<uint64_t>>(path,
                                                "offsets must be non-negative");
    }
    const auto value = static_cast<uint64_t>(raw);
    if (!result.empty() && value < result.back()) {
      return nestedError<std::vector<uint64_t>>(path,
                                                "offsets must be monotonic");
    }
    result.push_back(value);
  }
  return result;
}

template <typename Offset>
result<std::shared_ptr<IContextColumn>> convertList(
    const ArrowSchema& schema, const ArrowArray& array, uint64_t start,
    uint64_t count, const std::string& path, int depth) {
  auto physicalResult = validateLayout(schema, array, path, start, count, 2, 1);
  if (!physicalResult) {
    return tl::unexpected(physicalResult.error());
  }
  auto offsetsResult = readOffsets<Offset>(array, *physicalResult, count, path);
  if (!offsetsResult) {
    return tl::unexpected(offsetsResult.error());
  }
  const auto& offsets = *offsetsResult;
  const uint64_t first = offsets.front();
  const uint64_t childCount = offsets.back() - first;
  auto childResult =
      convertRange(*schema.children[0], *array.children[0], first, childCount,
                   path + ".element", depth + 1);
  if (!childResult) {
    return tl::unexpected(childResult.error());
  }

  const auto& child = *childResult;
  const auto& childType = child->elem_type();
  ListColumnBuilder builder(childType);
  builder.reserve(static_cast<size_t>(count));
  for (uint64_t row = 0; row < count; ++row) {
    if (!isValid(array, *physicalResult + row)) {
      builder.push_back_null();
      continue;
    }
    std::vector<Value> values;
    const uint64_t localBegin = offsets[row] - first;
    const uint64_t localEnd = offsets[row + 1] - first;
    values.reserve(static_cast<size_t>(localEnd - localBegin));
    for (uint64_t i = localBegin; i < localEnd; ++i) {
      values.emplace_back(child->get_elem(static_cast<size_t>(i)));
    }
    builder.push_back_elem(Value::LIST(childType, std::move(values)));
  }
  return builder.finish();
}

result<std::shared_ptr<IContextColumn>> convertFixedList(
    const ArrowSchema& schema, const ArrowArray& array, uint64_t start,
    uint64_t count, const std::string& path, int depth, uint64_t fixedLength) {
  if (array.n_buffers != 1 && array.n_buffers != 2) {
    return nestedError<std::shared_ptr<IContextColumn>>(
        path,
        "fixed-size list expects one standard buffer or Carquet's "
        "two-buffer compact layout");
  }
  auto physicalResult =
      validateLayout(schema, array, path, start, count, array.n_buffers, 1);
  if (!physicalResult) {
    return tl::unexpected(physicalResult.error());
  }

  std::vector<uint64_t> offsets;
  if (array.n_buffers == 2) {
    auto offsetsResult =
        readOffsets<int32_t>(array, *physicalResult, count, path);
    if (!offsetsResult) {
      return tl::unexpected(offsetsResult.error());
    }
    offsets = std::move(*offsetsResult);
    for (uint64_t row = 0; row < count; ++row) {
      const uint64_t length = offsets[row + 1] - offsets[row];
      const bool valid = isValid(array, *physicalResult + row);
      if ((valid && length != fixedLength) || (!valid && length != 0)) {
        return nestedError<std::shared_ptr<IContextColumn>>(
            path + ".row[" + std::to_string(row) + "]",
            valid ? "value length does not match the fixed-size schema"
                  : "null value contains compact child elements");
      }
    }
  } else {
    if (*physicalResult > std::numeric_limits<uint64_t>::max() / fixedLength ||
        count > std::numeric_limits<uint64_t>::max() / fixedLength -
                    *physicalResult) {
      return nestedError<std::shared_ptr<IContextColumn>>(
          path, "fixed-size child range overflows");
    }
    offsets.reserve(static_cast<size_t>(count + 1));
    const uint64_t first = *physicalResult * fixedLength;
    for (uint64_t row = 0; row <= count; ++row) {
      offsets.push_back(first + row * fixedLength);
    }
  }

  const uint64_t childStart = offsets.front();
  const uint64_t childCount = offsets.back() - childStart;
  auto childResult =
      convertRange(*schema.children[0], *array.children[0], childStart,
                   childCount, path + ".element", depth + 1);
  if (!childResult) {
    return tl::unexpected(childResult.error());
  }

  const auto& child = *childResult;
  const auto arrayType = DataType::Array(child->elem_type(), fixedLength);
  ContextArrayColumnBuilder builder(arrayType);
  builder.reserve(static_cast<size_t>(count));
  for (uint64_t row = 0; row < count; ++row) {
    if (!isValid(array, *physicalResult + row)) {
      builder.push_back_null();
      continue;
    }
    std::vector<Value> values;
    values.reserve(static_cast<size_t>(fixedLength));
    const uint64_t localBegin = offsets[row] - childStart;
    const uint64_t localEnd = offsets[row + 1] - childStart;
    for (uint64_t i = localBegin; i < localEnd; ++i) {
      values.emplace_back(child->get_elem(static_cast<size_t>(i)));
    }
    builder.push_back_elem(Value::ARRAY(arrayType, std::move(values)));
  }
  return builder.finish();
}

result<std::shared_ptr<IContextColumn>> convertRange(
    const ArrowSchema& schema, const ArrowArray& array, uint64_t start,
    uint64_t count, const std::string& path, int depth) {
  if (depth > kMaxNestedDepth) {
    return nestedError<std::shared_ptr<IContextColumn>>(
        path, "nesting exceeds 64 levels");
  }
  if (!schema.format) {
    return nestedError<std::shared_ptr<IContextColumn>>(
        path, "missing schema format");
  }
  const std::string_view format(schema.format);
  if (format == "+l") {
    return convertList<int32_t>(schema, array, start, count, path, depth);
  }
  if (format == "+L") {
    return convertList<int64_t>(schema, array, start, count, path, depth);
  }
  if (format.starts_with("+w:")) {
    auto lengthResult = parseFixedLength(format, path);
    if (!lengthResult) {
      return tl::unexpected(lengthResult.error());
    }
    return convertFixedList(schema, array, start, count, path, depth,
                            *lengthResult);
  }

  auto physicalResult =
      validateLayout(schema, array, path, start, count, array.n_buffers, 0);
  if (!physicalResult) {
    return tl::unexpected(physicalResult.error());
  }
  ArrowArray slice = array;
  slice.length = static_cast<int64_t>(count);
  slice.offset = static_cast<int64_t>(*physicalResult);
  slice.null_count = -1;
  return convertArrowCScalarColumn(schema, slice, path);
}

}  // namespace

result<std::shared_ptr<IContextColumn>> convertArrowCColumn(
    const ArrowSchema& schema, const ArrowArray& array,
    const std::string& path) {
  if (array.length < 0) {
    return nestedError<std::shared_ptr<IContextColumn>>(path,
                                                        "negative length");
  }
  return convertRange(schema, array, 0, static_cast<uint64_t>(array.length),
                      path, 0);
}

result<std::shared_ptr<DataChunk>> convertArrowCStruct(
    const ArrowSchema& schema, const ArrowArray& array) {
  if (!schema.format || std::string_view(schema.format) != "+s") {
    return nestedError<std::shared_ptr<DataChunk>>(
        "root", "schema must be a top-level struct");
  }
  if (schema.n_children < 0 || schema.n_children > kMaxChildren) {
    return nestedError<std::shared_ptr<DataChunk>>("root",
                                                   "invalid field count");
  }
  auto physicalResult =
      validateLayout(schema, array, "root", 0,
                     array.length < 0 ? 0 : static_cast<uint64_t>(array.length),
                     1, schema.n_children);
  if (!physicalResult) {
    return tl::unexpected(physicalResult.error());
  }
  if (array.length > 0 && schema.n_children == 0) {
    return nestedError<std::shared_ptr<DataChunk>>(
        "root", "non-empty record batch must have at least one field");
  }
  for (uint64_t row = 0; row < static_cast<uint64_t>(array.length); ++row) {
    if (!isValid(array, *physicalResult + row)) {
      return nestedError<std::shared_ptr<DataChunk>>(
          "root", "top-level struct rows must not be null");
    }
  }

  auto chunk = std::make_shared<DataChunk>();
  for (int64_t i = 0; i < schema.n_children; ++i) {
    auto converted =
        convertRange(*schema.children[i], *array.children[i], *physicalResult,
                     static_cast<uint64_t>(array.length),
                     "root.field[" + std::to_string(i) + "]", 0);
    if (!converted) {
      return tl::unexpected(converted.error());
    }
    chunk->set(static_cast<int>(i), std::move(*converted));
  }
  return chunk;
}

}  // namespace parquet
}  // namespace neug
