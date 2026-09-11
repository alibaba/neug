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

#include "carquet/c_data_writer.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace neug::parquet {

const std::string& responseArrayValidity(const Array& array) {
  switch (array.typed_array_case()) {
  case Array::kInt32Array:
    return array.int32_array().validity();
  case Array::kUint32Array:
    return array.uint32_array().validity();
  case Array::kInt64Array:
    return array.int64_array().validity();
  case Array::kUint64Array:
    return array.uint64_array().validity();
  case Array::kFloatArray:
    return array.float_array().validity();
  case Array::kDoubleArray:
    return array.double_array().validity();
  case Array::kStringArray:
    return array.string_array().validity();
  case Array::kBoolArray:
    return array.bool_array().validity();
  case Array::kTimestampArray:
    return array.timestamp_array().validity();
  case Array::kDateArray:
    return array.date_array().validity();
  case Array::kIntervalArray:
    return array.interval_array().validity();
  case Array::kListArray:
    return array.list_array().validity();
  case Array::kStructArray:
    return array.struct_array().validity();
  case Array::kVertexArray:
    return array.vertex_array().validity();
  case Array::kEdgeArray:
    return array.edge_array().validity();
  case Array::kPathArray:
    return array.path_array().validity();
  default: {
    static const std::string empty;
    return empty;
  }
  }
}

namespace {

constexpr int64_t kMillisPerDay = 24 * 60 * 60 * 1000;

Status invalid(std::string message) {
  return Status(StatusCode::ERR_INVALID_ARGUMENT, std::move(message));
}

Status ioError(std::string message) {
  return Status(StatusCode::ERR_IO_ERROR, std::move(message));
}

struct SchemaStorage {
  std::string format;
  std::string name;
  std::vector<ArrowSchema*> children;
};

struct ArrayStorage {
  std::vector<std::unique_ptr<std::byte[]>> ownedBuffers;
  std::vector<const void*> buffers;
  std::vector<ArrowArray*> children;
};

template <typename T>
struct CDataDeleter {
  void operator()(T* object) const noexcept {
    if (!object) {
      return;
    }
    if (object->release) {
      object->release(object);
    }
    delete object;
  }
};

template <typename T>
auto makeCDataObject() {
  return std::unique_ptr<T, CDataDeleter<T>>(new T{});
}

void releaseSchema(ArrowSchema* schema) {
  if (!schema || !schema->release) {
    return;
  }
  schema->release = nullptr;
  std::unique_ptr<SchemaStorage> storage(
      static_cast<SchemaStorage*>(schema->private_data));
  schema->private_data = nullptr;
  if (!storage) {
    return;
  }
  for (auto* child : storage->children) {
    if (child && child->release) {
      child->release(child);
    }
    delete child;
  }
}

void releaseArray(ArrowArray* array) {
  if (!array || !array->release) {
    return;
  }
  array->release = nullptr;
  std::unique_ptr<ArrayStorage> storage(
      static_cast<ArrayStorage*>(array->private_data));
  array->private_data = nullptr;
  if (!storage) {
    return;
  }
  for (auto* child : storage->children) {
    if (child && child->release) {
      child->release(child);
    }
    delete child;
  }
}

SchemaStorage& initializeSchema(ArrowSchema& schema, std::string format,
                                std::string name, bool nullable,
                                int64_t children) {
  auto storage = std::make_unique<SchemaStorage>();
  storage->format = std::move(format);
  storage->name = std::move(name);
  storage->children.reserve(static_cast<size_t>(children));
  schema = {};
  schema.format = storage->format.c_str();
  schema.name = storage->name.c_str();
  schema.flags = nullable ? ARROW_FLAG_NULLABLE : 0;
  schema.n_children = children;
  schema.release = releaseSchema;
  schema.private_data = storage.release();
  return *static_cast<SchemaStorage*>(schema.private_data);
}

ArrayStorage& initializeArray(ArrowArray& array, int64_t length,
                              int64_t buffers, int64_t children) {
  auto storage = std::make_unique<ArrayStorage>();
  storage->buffers.resize(static_cast<size_t>(buffers));
  storage->children.reserve(static_cast<size_t>(children));
  array = {};
  array.length = length;
  array.null_count = 0;
  array.n_buffers = buffers;
  array.n_children = children;
  array.release = releaseArray;
  array.private_data = storage.release();
  auto& result = *static_cast<ArrayStorage*>(array.private_data);
  array.buffers = result.buffers.data();
  return result;
}

result<size_t> checkedBytes(size_t count, size_t width,
                            const std::string& name) {
  if (width != 0 && count > std::numeric_limits<size_t>::max() / width) {
    return tl::unexpected(
        invalid("C Data buffer size overflows for Parquet column: " + name));
  }
  return count * width;
}

void* allocateBuffer(ArrayStorage& storage, size_t bytes) {
  auto buffer = std::make_unique<std::byte[]>(std::max<size_t>(bytes, 1));
  void* data = buffer.get();
  storage.ownedBuffers.emplace_back(std::move(buffer));
  return data;
}

bool isValid(const std::string& bitmap, int64_t index) {
  return bitmap.empty() ||
         ((static_cast<uint8_t>(bitmap[static_cast<size_t>(index) >> 3]) >>
           (index & 7)) &
          1U) != 0;
}

void addValidity(ArrayStorage& storage, ArrowArray& output,
                 const std::string& bitmap, int32_t begin, int32_t count) {
  if (bitmap.empty()) {
    storage.buffers[0] = nullptr;
    return;
  }
  const size_t bytes = (static_cast<size_t>(count) + 7) / 8;
  auto* bits = static_cast<uint8_t*>(allocateBuffer(storage, bytes));
  std::memset(bits, 0, bytes);
  for (int32_t offset = 0; offset < count; ++offset) {
    if (isValid(bitmap, begin + offset)) {
      bits[static_cast<size_t>(offset) >> 3] |=
          static_cast<uint8_t>(1U << (offset & 7));
    } else {
      ++output.null_count;
    }
  }
  storage.buffers[0] = bits;
}

template <typename ProtoArray, typename Value>
Status addFixedValues(ArrayStorage& storage, const ProtoArray& input,
                      int32_t begin, int32_t count, const std::string& name) {
  auto bytes = checkedBytes(static_cast<size_t>(count), sizeof(Value), name);
  if (!bytes) {
    return bytes.error();
  }
  auto* values = static_cast<Value*>(allocateBuffer(storage, *bytes));
  for (int32_t offset = 0; offset < count; ++offset) {
    values[offset] = static_cast<Value>(input.values(begin + offset));
  }
  storage.buffers[1] = values;
  return Status::OK();
}

template <typename ProtoArray>
Status addStrings(ArrayStorage& storage, const ProtoArray& input, int32_t begin,
                  int32_t count, const std::string& name) {
  auto offsetBytes =
      checkedBytes(static_cast<size_t>(count) + 1, sizeof(int64_t), name);
  if (!offsetBytes) {
    return offsetBytes.error();
  }
  auto* offsets = static_cast<int64_t*>(allocateBuffer(storage, *offsetBytes));
  offsets[0] = 0;
  size_t bytes = 0;
  for (int32_t offset = 0; offset < count; ++offset) {
    const size_t length = input.values(begin + offset).size();
    if (length > std::numeric_limits<size_t>::max() - bytes ||
        length > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) -
                     bytes) {
      return invalid("C Data string offsets overflow for Parquet column: " +
                     name);
    }
    bytes += length;
    offsets[offset + 1] = static_cast<int64_t>(bytes);
  }
  auto* data = static_cast<char*>(allocateBuffer(storage, bytes));
  size_t position = 0;
  for (int32_t offset = 0; offset < count; ++offset) {
    const auto& value = input.values(begin + offset);
    std::memcpy(data + position, value.data(), value.size());
    position += value.size();
  }
  storage.buffers[1] = offsets;
  storage.buffers[2] = data;
  return Status::OK();
}

Status buildNode(const Array& input, int32_t begin, int32_t count,
                 std::string name, ArrowSchema& schema, ArrowArray& array);

Status buildPrimitive(const Array& input, int32_t begin, int32_t count,
                      std::string name, ArrowSchema& schema,
                      ArrowArray& array) {
  const std::string path = name;
  std::string format;
  int64_t buffers = 2;
  switch (input.typed_array_case()) {
  case Array::kInt32Array:
    format = "i";
    break;
  case Array::kUint32Array:
    format = "I";
    break;
  case Array::kInt64Array:
    format = "l";
    break;
  case Array::kUint64Array:
    format = "L";
    break;
  case Array::kFloatArray:
    format = "f";
    break;
  case Array::kDoubleArray:
    format = "g";
    break;
  case Array::kBoolArray:
    format = "b";
    break;
  case Array::kTimestampArray:
    // TimestampArray stores DateTime::milli_second, including nested leaves.
    format = "tsm:UTC";
    break;
  case Array::kDateArray:
    format = "tdD";
    break;
  case Array::kStringArray:
  case Array::kIntervalArray:
  case Array::kVertexArray:
  case Array::kEdgeArray:
  case Array::kPathArray:
    format = "U";
    buffers = 3;
    break;
  default:
    return invalid("Unsupported Parquet C Data leaf: " + path);
  }

  initializeSchema(schema, std::move(format), std::move(name), true, 0);
  auto& storage = initializeArray(array, count, buffers, 0);
  addValidity(storage, array, responseArrayValidity(input), begin, count);
  switch (input.typed_array_case()) {
  case Array::kInt32Array:
    return addFixedValues<Int32Array, int32_t>(storage, input.int32_array(),
                                               begin, count, path);
  case Array::kUint32Array:
    return addFixedValues<UInt32Array, uint32_t>(storage, input.uint32_array(),
                                                 begin, count, path);
  case Array::kInt64Array:
    return addFixedValues<Int64Array, int64_t>(storage, input.int64_array(),
                                               begin, count, path);
  case Array::kUint64Array:
    return addFixedValues<UInt64Array, uint64_t>(storage, input.uint64_array(),
                                                 begin, count, path);
  case Array::kFloatArray:
    return addFixedValues<FloatArray, float>(storage, input.float_array(),
                                             begin, count, path);
  case Array::kDoubleArray:
    return addFixedValues<DoubleArray, double>(storage, input.double_array(),
                                               begin, count, path);
  case Array::kBoolArray: {
    const size_t bytes = (static_cast<size_t>(count) + 7) / 8;
    auto* bits = static_cast<uint8_t*>(allocateBuffer(storage, bytes));
    std::memset(bits, 0, bytes);
    for (int32_t offset = 0; offset < count; ++offset) {
      if (input.bool_array().values(begin + offset)) {
        bits[static_cast<size_t>(offset) >> 3] |=
            static_cast<uint8_t>(1U << (offset & 7));
      }
    }
    storage.buffers[1] = bits;
    return Status::OK();
  }
  case Array::kTimestampArray:
    return addFixedValues<TimestampArray, int64_t>(
        storage, input.timestamp_array(), begin, count, path);
  case Array::kDateArray: {
    auto bytes =
        checkedBytes(static_cast<size_t>(count), sizeof(int32_t), path);
    if (!bytes) {
      return bytes.error();
    }
    auto* values = static_cast<int32_t*>(allocateBuffer(storage, *bytes));
    for (int32_t offset = 0; offset < count; ++offset) {
      values[offset] = static_cast<int32_t>(
          input.date_array().values(begin + offset) / kMillisPerDay);
    }
    storage.buffers[1] = values;
    return Status::OK();
  }
  case Array::kStringArray:
    return addStrings(storage, input.string_array(), begin, count, path);
  case Array::kIntervalArray:
    return addStrings(storage, input.interval_array(), begin, count, path);
  case Array::kVertexArray:
    return addStrings(storage, input.vertex_array(), begin, count, path);
  case Array::kEdgeArray:
    return addStrings(storage, input.edge_array(), begin, count, path);
  case Array::kPathArray:
    return addStrings(storage, input.path_array(), begin, count, path);
  default:
    return invalid("Unsupported Parquet C Data leaf: " + path);
  }
}

Status buildNode(const Array& input, int32_t begin, int32_t count,
                 std::string name, ArrowSchema& schema, ArrowArray& array) {
  if (input.has_list_array()) {
    const auto& list = input.list_array();
    const std::string path = name;
    // QueryResponse uses list_array for both LIST and fixed ARRAY. Preserve
    // that established export representation instead of adding fixed-list
    // metadata that the previous writer did not emit.
    auto& schemaStorage =
        initializeSchema(schema, "+l", std::move(name), true, 1);
    auto& arrayStorage = initializeArray(array, count, 2, 1);
    addValidity(arrayStorage, array, list.validity(), begin, count);

    auto offsetBytes =
        checkedBytes(static_cast<size_t>(count) + 1, sizeof(int32_t), path);
    if (!offsetBytes) {
      return offsetBytes.error();
    }
    auto* offsets =
        static_cast<int32_t*>(allocateBuffer(arrayStorage, *offsetBytes));
    const uint32_t first = list.offsets(begin);
    for (int64_t offset = 0; offset <= count; ++offset) {
      offsets[offset] = static_cast<int32_t>(
          list.offsets(begin + static_cast<int32_t>(offset)) - first);
    }
    arrayStorage.buffers[1] = offsets;

    auto childSchema = makeCDataObject<ArrowSchema>();
    auto childArray = makeCDataObject<ArrowArray>();
    const uint32_t last = list.offsets(begin + count);
    auto status = buildNode(list.elements(), static_cast<int32_t>(first),
                            static_cast<int32_t>(last - first), "element",
                            *childSchema, *childArray);
    if (!status.ok()) {
      return status;
    }
    schemaStorage.children.push_back(childSchema.release());
    schema.children = schemaStorage.children.data();
    arrayStorage.children.push_back(childArray.release());
    array.children = arrayStorage.children.data();
    return Status::OK();
  }

  if (input.has_struct_array()) {
    const auto& structure = input.struct_array();
    const std::string path = name;
    auto& schemaStorage = initializeSchema(schema, "+s", std::move(name), true,
                                           structure.fields_size());
    auto& arrayStorage =
        initializeArray(array, count, 1, structure.fields_size());
    addValidity(arrayStorage, array, structure.validity(), begin, count);
    for (int32_t field = 0; field < structure.fields_size(); ++field) {
      auto childSchema = makeCDataObject<ArrowSchema>();
      auto childArray = makeCDataObject<ArrowArray>();
      auto status = buildNode(structure.fields(field), begin, count,
                              "field_" + std::to_string(field), *childSchema,
                              *childArray);
      if (!status.ok()) {
        return status;
      }
      schemaStorage.children.push_back(childSchema.release());
      arrayStorage.children.push_back(childArray.release());
    }
    schema.children = schemaStorage.children.data();
    array.children = arrayStorage.children.data();
    return Status::OK();
  }

  return buildPrimitive(input, begin, count, std::move(name), schema, array);
}

class CDataBatchOwner {
 public:
  CDataBatchOwner() = default;
  CDataBatchOwner(const CDataBatchOwner&) = delete;
  CDataBatchOwner& operator=(const CDataBatchOwner&) = delete;

  ~CDataBatchOwner() {
    if (array.release) {
      array.release(&array);
    }
    if (schema.release) {
      schema.release(&schema);
    }
  }

  ArrowSchema schema{};
  ArrowArray array{};
};

}  // namespace

Status writeCarquetCDataBatch(carquet_writer_t* writer,
                              const QueryResponse& table, int32_t begin,
                              int32_t count,
                              const std::vector<std::string>& columnNames) {
  if (!writer || begin < 0 || count < 0 || table.row_count() < 0 ||
      begin > table.row_count() || count > table.row_count() - begin ||
      columnNames.size() != static_cast<size_t>(table.arrays_size())) {
    return invalid("Invalid Parquet C Data write range");
  }

  CDataBatchOwner batch;
  auto& schemaStorage = initializeSchema(batch.schema, "+s", "schema", false,
                                         table.arrays_size());
  auto& arrayStorage =
      initializeArray(batch.array, count, 1, table.arrays_size());
  arrayStorage.buffers[0] = nullptr;

  for (int32_t column = 0; column < table.arrays_size(); ++column) {
    auto childSchema = makeCDataObject<ArrowSchema>();
    auto childArray = makeCDataObject<ArrowArray>();
    auto status = buildNode(table.arrays(column), begin, count,
                            columnNames[static_cast<size_t>(column)],
                            *childSchema, *childArray);
    if (!status.ok()) {
      return status;
    }
    schemaStorage.children.push_back(childSchema.release());
    arrayStorage.children.push_back(childArray.release());
  }
  batch.schema.children = schemaStorage.children.data();
  batch.array.children = arrayStorage.children.data();

  carquet_error_t error = CARQUET_ERROR_INIT;
  const auto status =
      carquet_writer_write_arrow(writer, &batch.array, &batch.schema, &error);
  if (status != CARQUET_OK) {
    std::string message = "Failed to write nested Parquet row group: ";
    message += carquet_status_string(status);
    if (error.message[0] != '\0') {
      message += " (";
      message += error.message;
      message += ")";
    }
    return ioError(std::move(message));
  }
  return Status::OK();
}

}  // namespace neug::parquet
