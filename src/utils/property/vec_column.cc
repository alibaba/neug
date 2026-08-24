/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include "neug/utils/property/vec_column.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

namespace {

constexpr const char* kAccessorRef = "offset_accessor";
constexpr const char* kArrayType = "array_type";
constexpr const char* kDefaultValue = "default_value";
constexpr std::string_view kBase64Alphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string Base64Encode(std::string_view bytes) {
  std::string encoded;
  encoded.reserve(((bytes.size() + 2) / 3) * 4);
  for (size_t i = 0; i < bytes.size(); i += 3) {
    const auto first = static_cast<uint8_t>(bytes[i]);
    const auto second =
        i + 1 < bytes.size() ? static_cast<uint8_t>(bytes[i + 1]) : uint8_t{0};
    const auto third =
        i + 2 < bytes.size() ? static_cast<uint8_t>(bytes[i + 2]) : uint8_t{0};
    const uint32_t value = (static_cast<uint32_t>(first) << 16) |
                           (static_cast<uint32_t>(second) << 8) | third;
    encoded.push_back(kBase64Alphabet[(value >> 18) & 0x3f]);
    encoded.push_back(kBase64Alphabet[(value >> 12) & 0x3f]);
    encoded.push_back(
        i + 1 < bytes.size() ? kBase64Alphabet[(value >> 6) & 0x3f] : '=');
    encoded.push_back(i + 2 < bytes.size() ? kBase64Alphabet[value & 0x3f]
                                           : '=');
  }
  return encoded;
}

uint8_t Base64DecodeChar(char ch) {
  auto pos = kBase64Alphabet.find(ch);
  if (pos == std::string_view::npos) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn::Open: invalid base64 default value");
  }
  return static_cast<uint8_t>(pos);
}

std::string Base64Decode(std::string_view encoded) {
  if (encoded.size() % 4 != 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn::Open: invalid base64 default value length");
  }
  std::string decoded;
  decoded.reserve((encoded.size() / 4) * 3);
  for (size_t i = 0; i < encoded.size(); i += 4) {
    const bool last_group = i + 4 == encoded.size();
    const bool third_padding = encoded[i + 2] == '=';
    const bool fourth_padding = encoded[i + 3] == '=';
    if (encoded[i] == '=' || encoded[i + 1] == '=' ||
        (third_padding && !fourth_padding) ||
        (!last_group && (third_padding || fourth_padding))) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "VecColumn::Open: invalid base64 default value padding");
    }
    const uint32_t value =
        (static_cast<uint32_t>(Base64DecodeChar(encoded[i])) << 18) |
        (static_cast<uint32_t>(Base64DecodeChar(encoded[i + 1])) << 12) |
        (third_padding
             ? 0
             : static_cast<uint32_t>(Base64DecodeChar(encoded[i + 2])) << 6) |
        (fourth_padding ? 0 : Base64DecodeChar(encoded[i + 3]));
    decoded.push_back(static_cast<char>((value >> 16) & 0xff));
    if (!third_padding) {
      decoded.push_back(static_cast<char>((value >> 8) & 0xff));
    }
    if (!fourth_padding) {
      decoded.push_back(static_cast<char>(value & 0xff));
    }
  }
  return decoded;
}

std::string SerializeValue(const Value& value) {
  InArchive archive;
  archive << value;
  return Base64Encode({archive.GetBuffer(), archive.GetSize()});
}

Value DeserializeValue(const std::string& encoded) {
  auto bytes = Base64Decode(encoded);
  OutArchive archive;
  archive.SetSlice(bytes.data(), bytes.size());
  Value value;
  archive >> value;
  return value;
}

template <typename T>
constexpr bool IsPod() {
  return std::is_pod_v<T>;
}

bool IsPodType(DataTypeId type) {
  switch (type) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return IsPod<type>();
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  default:
    return false;
  }
}

size_t ElementSize(DataTypeId type) {
  switch (type) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return sizeof(type);
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  default:
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn requires an array with a POD child type");
  }
}

template <typename T>
void FillBuffer(void* buffer, size_t old_size, size_t new_size,
                size_t array_size, const Value& value) {
  auto* data = static_cast<T*>(buffer);
  const auto& children = ArrayValue::GetChildren(value);
  for (size_t row = old_size; row < new_size; ++row) {
    for (size_t col = 0; col < array_size; ++col) {
      data[row * array_size + col] = children[col].GetValue<T>();
    }
  }
}

template <typename T>
void SetBufferValue(void* buffer, size_t offset, size_t array_size,
                    const Value& value) {
  auto* data = static_cast<T*>(buffer) + offset * array_size;
  const auto& children = ArrayValue::GetChildren(value);
  for (size_t i = 0; i < array_size; ++i) {
    data[i] = children[i].GetValue<T>();
  }
}

template <typename T>
Value GetBufferValue(const void* buffer, size_t offset, size_t array_size,
                     const DataType& array_type) {
  const auto* data = static_cast<const T*>(buffer) + offset * array_size;
  std::vector<Value> values;
  values.reserve(array_size);
  for (size_t i = 0; i < array_size; ++i) {
    values.emplace_back(Value::CreateValue<T>(data[i]));
  }
  return Value::ARRAY(array_type, std::move(values));
}

template <typename T>
Value ReadValue(OutArchive& arc, const DataType& array_type) {
  const auto array_size = ArrayType::GetNumElements(array_type);
  std::vector<Value> values;
  values.reserve(array_size);
  for (size_t i = 0; i < array_size; ++i) {
    T value;
    arc >> value;
    values.emplace_back(Value::CreateValue<T>(value));
  }
  return Value::ARRAY(array_type, std::move(values));
}

}  // namespace

VecColumn::VecColumn()
    : offset_accessor_(std::make_unique<DefaultIndexIDAccessor>()),
      buffer_(nullptr),
      ckp_(nullptr),
      level_(MemoryLevel::kInMemory),
      array_type_(DataType::SQLNULL),
      size_(0),
      default_value_(DataType::SQLNULL) {}

VecColumn::VecColumn(std::shared_ptr<IDataContainer> buffer,
                     std::unique_ptr<IndexIDAccessor> offset_accessor,
                     const DataType& array_type, size_t size,
                     const Value& default_value, Checkpoint& ckp,
                     MemoryLevel level)
    : offset_accessor_(std::move(offset_accessor)),
      buffer_(std::move(buffer)),
      ckp_(&ckp),
      level_(level),
      array_type_(array_type),
      size_(size),
      default_value_(default_value) {
  validateState();
}

void VecColumn::Open(Checkpoint& ckp, const ModuleDescriptor& desc,
                     MemoryLevel level) {
  openInternal(ckp, &ckp.manifest(), desc, level);
}

void VecColumn::Open(Checkpoint& ckp, const CheckpointManifest& manifest,
                     const ModuleDescriptor& desc, MemoryLevel level) {
  openInternal(ckp, &manifest, desc, level);
}

void VecColumn::openInternal(Checkpoint& ckp,
                             const CheckpointManifest* manifest,
                             const ModuleDescriptor& desc, MemoryLevel level) {
  ckp_ = &ckp;
  level_ = level;
  auto array_type_yaml = desc.get(kArrayType);
  if (!array_type_yaml.has_value()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn::Open: missing array_type in module descriptor");
  }
  auto node = YAML::Load(*array_type_yaml);
  if (!YAML::convert<DataType>::decode(node, array_type_)) {
    THROW_RUNTIME_ERROR(
        "VecColumn::Open: failed to parse array_type from descriptor");
  }
  size_ = std::stoull(desc.get("size").value_or("0"));
  auto default_value = desc.get(kDefaultValue);
  if (!default_value.has_value()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn::Open: missing default_value in module descriptor");
  }
  default_value_ = DeserializeValue(*default_value);
  buffer_ = ckp.OpenFile(
      desc.get_path(ModuleDescriptor::kDataPath).value_or(""), level);
  auto accessor_ref = desc.get_ref(kAccessorRef);
  if (!accessor_ref) {
    offset_accessor_->Open(ckp, ModuleDescriptor{}, level);
  } else {
    const auto* resolver = manifest ? manifest : &ckp.manifest();
    const auto* accessor_desc = resolver->FindModule(*accessor_ref);
    if (!accessor_desc) {
      THROW_RUNTIME_ERROR("VecColumn::Open: missing offset accessor module");
    }
    static_cast<Module*>(offset_accessor_.get())
        ->Open(ckp, *resolver, *accessor_desc, level);
  }
  validateState();
}

void VecColumn::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                     const std::string& key) {
  if (!buffer_) {
    THROW_RUNTIME_ERROR("VecColumn::Dump: buffer is not initialized");
  }
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  desc.set(kArrayType,
           YAML::Dump(YAML::convert<DataType>::encode(array_type_)));
  desc.set("size", std::to_string(size_));
  desc.set(kDefaultValue, SerializeValue(default_value_));
  desc.set_path(ModuleDescriptor::kDataPath, ckp.Commit(*buffer_));
  auto accessor_key = key + "/" + kAccessorRef;
  offset_accessor_->Dump(ckp, meta, accessor_key);
  auto* accessor_desc = meta.FindMutableModule(accessor_key);
  if (accessor_desc == nullptr) {
    THROW_RUNTIME_ERROR(
        "VecColumn::Dump: offset accessor did not write module");
  }
  accessor_desc->mark_as_referenced_module();
  desc.set_ref(kAccessorRef, accessor_key);
  meta.SetModule(key, std::move(desc));
}

size_t VecColumn::size() const { return size_; }

void VecColumn::resize(size_t new_size) {
  if (new_size <= size_) {
    return;
  }
  if (!ckp_) {
    THROW_RUNTIME_ERROR(
        "VecColumn::resize requires an initialized checkpoint context");
  }
  const auto bytes_per_vector =
      array_size() * ElementSize(ArrayType::GetChildType(array_type_).id());
  const auto old_logical_bytes = size_ * bytes_per_vector;
  auto replacement =
      ckp_->CreateRuntimeContainer(new_size * bytes_per_vector, level_);
  if (old_logical_bytes != 0) {
    std::memcpy(replacement->GetData(), buffer_->GetData(), old_logical_bytes);
  }
  buffer_ = std::move(replacement);
  size_ = new_size;
}

void VecColumn::resize(size_t new_size, const Value& default_value) {
  validateValue(default_value);
  if (new_size <= size_) {
    return;
  }
  auto old_size = size_;
  resize(new_size);
  switch (ArrayType::GetChildType(array_type_).id()) {
#define TYPE_DISPATCHER(enum_val, type)                                    \
  case DataTypeId::enum_val:                                               \
    FillBuffer<type>(buffer_->GetData(), old_size, new_size, array_size(), \
                     default_value);                                       \
    break;
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  default:
    validatePodType();
  }
  default_value_ = default_value;
}

DataTypeId VecColumn::type() const { return DataTypeId::kArray; }

void VecColumn::set_any(size_t vid, const Value& value, bool insert_safe) {
  if (vid > std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("VecColumn::set_any: invalid vid");
  }
  validatePodType();
  // As with the other property columns, an untyped NULL assignment is stored
  // as the column default. Secondary indexes still receive the original NULL
  // value and remove the row instead of indexing this physical placeholder.
  const Value& normalized = value.IsNull() ? default_value_ : value;
  validateValue(normalized);
  index_id_t next_offset = offset_accessor_->GetNextIndexID();
  if (next_offset >= size_) {
    if (!insert_safe) {
      THROW_STORAGE_EXCEPTION(
          "VecColumn has insufficient capacity and insert_safe is false");
    }
    resize(next_offset < 4096 ? 4096 : next_offset + next_offset / 4,
           default_value_);
  }
  assert(next_offset < size_);
  auto offset = offset_accessor_->UpsertVID(static_cast<vid_t>(vid));
  switch (ArrayType::GetChildType(array_type_).id()) {
#define TYPE_DISPATCHER(enum_val, type)                            \
  case DataTypeId::enum_val:                                       \
    SetBufferValue<type>(buffer_->GetData(), offset, array_size(), \
                         normalized);                              \
    return;
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  default:
    validatePodType();
  }
}

Value VecColumn::get_any(size_t vid) const {
  if (vid > std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("VecColumn::get_any: invalid vid");
  }
  auto offset = offset_accessor_->GetIndexIDByVID(static_cast<vid_t>(vid));
  if (offset == INVALID_OFFSET) {
    THROW_RUNTIME_ERROR("VecColumn::get_any: vid has no allocated offset");
  }
  if (offset >= size_) {
    THROW_RUNTIME_ERROR("VecColumn::get_any: offset out of range");
  }
  switch (ArrayType::GetChildType(array_type_).id()) {
#define TYPE_DISPATCHER(enum_val, type)                                   \
  case DataTypeId::enum_val:                                              \
    return GetBufferValue<type>(buffer_->GetData(), offset, array_size(), \
                                array_type_);
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  default:
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn requires an array with a POD child type");
  }
}

IndexIDAccessor* VecColumn::get_offset_accessor() const {
  return offset_accessor_.get();
}

const void* VecColumn::get_buffer_ptr() const {
  return buffer_ ? buffer_->GetData() : nullptr;
}

uint64_t VecColumn::array_size() const {
  return ArrayType::GetNumElements(array_type_);
}

DataType VecColumn::array_type() const { return array_type_; }

std::unique_ptr<Module> VecColumn::Clone() const {
  auto cloned = std::make_unique<VecColumn>();
  cloned->buffer_ = buffer_;
  auto accessor = offset_accessor_->Clone();
  cloned->offset_accessor_.reset(
      static_cast<IndexIDAccessor*>(accessor.release()));
  cloned->ckp_ = ckp_;
  cloned->level_ = level_;
  cloned->array_type_ = array_type_;
  cloned->size_ = size_;
  cloned->default_value_ = default_value_;
  return cloned;
}

void VecColumn::Detach(Checkpoint& ckp, MemoryLevel level) {
  ckp_ = &ckp;
  level_ = level;
  // Payload storage stays shared intentionally. The detached accessor assigns
  // transaction-private offsets to writes, while resize() allocates a
  // replacement buffer before extending beyond the shared capacity.
  // A consuming checkpoint on the clone can therefore close a payload still
  // owned by the published base graph. Bulk checkpoint commit must fail-stop
  // after consumption begins until a checkpoint-specific payload fork exists.
  if (offset_accessor_) {
    offset_accessor_->Detach(ckp, level);
  }
}

std::string VecColumn::ModuleTypeName() const { return type_name(); }

std::string VecColumn::type_name() { return "column<vector<float>>"; }

void VecColumn::validatePodType() const {
  if (array_type_.id() != DataTypeId::kArray ||
      !IsPodType(ArrayType::GetChildType(array_type_).id())) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn requires an array with a POD child type");
  }
}

void VecColumn::validateState() const {
  validatePodType();
  if (!buffer_ || !offset_accessor_ || array_size() == 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn requires a buffer, accessor, and non-zero array size");
  }
  const auto element_size =
      ElementSize(ArrayType::GetChildType(array_type_).id());
  if (buffer_->GetDataSize() < size_ * array_size() * element_size) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn buffer is smaller than its logical size");
  }
  validateValue(default_value_);
}

void VecColumn::validateValue(const Value& value) const {
  if (value.type() != array_type_) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn value type does not match vector type");
  }
  const auto& children = ArrayValue::GetChildren(value);
  if (children.size() != array_size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn value element count does not match vector dimension");
  }
}

NEUG_REGISTER_MODULE(VecColumn);

}  // namespace neug
