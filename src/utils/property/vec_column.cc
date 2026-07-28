#include "neug/utils/property/vec_column.h"

#include <cstring>
#include <utility>
#include <vector>

#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/serialization/in_archive.h"

namespace neug {

namespace {

constexpr const char* kAccessorRef = "offset_accessor";
constexpr const char* kDefaultValue = "default_value";

std::string SerializeValue(const Value& value) {
  InArchive archive;
  archive << value;
  return {archive.GetBuffer(), archive.GetSize()};
}

Value DeserializeValue(const std::string& bytes) {
  OutArchive archive;
  archive.SetSlice(const_cast<char*>(bytes.data()), bytes.size());
  Value value;
  archive >> value;
  return value;
}

}  // namespace

template <typename T>
VecColumn<T>::VecColumn()
    : offset_accessor_(std::make_unique<DefaultIndexIDAccessor>()),
      buffer_(nullptr),
      ckp_(nullptr),
      level_(MemoryLevel::kInMemory),
      array_size_(0),
      size_(0),
      default_value_(DataType::SQLNULL) {}

template <typename T>
VecColumn<T>::VecColumn(std::shared_ptr<IDataContainer> buffer,
                        std::unique_ptr<IndexIDAccessor> offset_accessor,
                        uint64_t array_size, size_t size,
                        const Value& default_value, Checkpoint& ckp,
                        MemoryLevel level)
    : offset_accessor_(std::move(offset_accessor)),
      buffer_(std::move(buffer)),
      ckp_(&ckp),
      level_(level),
      array_size_(array_size),
      size_(size),
      default_value_(default_value) {
  validateState();
}

template <typename T>
void VecColumn<T>::Open(Checkpoint& ckp, const ModuleDescriptor& desc,
                        MemoryLevel level) {
  openInternal(ckp, &ckp.GetMeta(), desc, level);
}

template <typename T>
void VecColumn<T>::Open(Checkpoint& ckp, const CheckpointManifest& manifest,
                        const ModuleDescriptor& desc, MemoryLevel level) {
  openInternal(ckp, &manifest, desc, level);
}

template <typename T>
void VecColumn<T>::openInternal(Checkpoint& ckp,
                                const CheckpointManifest* manifest,
                                const ModuleDescriptor& desc,
                                MemoryLevel level) {
  ckp_ = &ckp;
  level_ = level;
  array_size_ = std::stoull(desc.get("array_size").value_or("0"));
  size_ = std::stoull(desc.get("size").value_or("0"));
  default_value_ =
      DeserializeValue(desc.get(kDefaultValue).value_or(std::string{}));
  buffer_ = ckp.OpenFile(
      desc.get_path(ModuleDescriptor::kDataPath).value_or(""), level);
  auto accessor_ref = desc.get_ref(kAccessorRef);
  if (!accessor_ref) {
    THROW_RUNTIME_ERROR("VecColumn::Open: missing offset accessor reference");
  }
  const auto* resolver = manifest ? manifest : &ckp.GetMeta();
  auto accessor_desc = resolver->module(*accessor_ref);
  if (!accessor_desc) {
    THROW_RUNTIME_ERROR("VecColumn::Open: missing offset accessor module");
  }
  static_cast<Module*>(offset_accessor_.get())
      ->Open(ckp, *resolver, *accessor_desc, level);
  validateState();
}

template <typename T>
void VecColumn<T>::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                        const std::string& key) {
  if (!buffer_) {
    THROW_RUNTIME_ERROR("VecColumn::Dump: buffer is not initialized");
  }
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  desc.set("array_size", std::to_string(array_size_));
  desc.set("size", std::to_string(size_));
  desc.set(kDefaultValue, SerializeValue(default_value_));
  desc.set_path(ModuleDescriptor::kDataPath, ckp.Commit(*buffer_));
  auto accessor_key = key + "/" + kAccessorRef;
  offset_accessor_->Dump(ckp, meta, accessor_key);
  meta.mutable_modules().at(accessor_key).mark_as_referenced_module();
  desc.set_ref(kAccessorRef, accessor_key);
  meta.set_module(key, std::move(desc));
}

template <typename T>
size_t VecColumn<T>::size() const {
  return size_;
}

template <typename T>
void VecColumn<T>::resize(size_t new_size) {
  if (new_size <= size_) {
    size_ = new_size;
    return;
  }
  if (!ckp_) {
    THROW_RUNTIME_ERROR(
        "VecColumn::resize requires an initialized checkpoint context");
  }
  auto replacement = ckp_->OpenFile(buffer_->GetPath(), level_);
  replacement->Resize(new_size * static_cast<size_t>(array_size_) * sizeof(T));
  if (buffer_->GetDataSize() != 0) {
    std::memcpy(replacement->GetData(), buffer_->GetData(),
                buffer_->GetDataSize());
  }
  buffer_ = std::move(replacement);
  size_ = new_size;
}

template <typename T>
void VecColumn<T>::resize(size_t new_size, const Value& default_value) {
  validateValue(default_value);
  auto old_size = size_;
  resize(new_size);
  if (new_size <= old_size) {
    return;
  }
  auto* data = static_cast<T*>(buffer_->GetData());
  const auto& children = ArrayValue::GetChildren(default_value);
  for (size_t row = old_size; row < new_size; ++row) {
    for (size_t col = 0; col < array_size_; ++col) {
      data[row * array_size_ + col] = children[col].GetValue<T>();
    }
  }
  default_value_ = default_value;
}

template <typename T>
DataTypeId VecColumn<T>::type() const {
  return DataTypeId::kArray;
}

template <typename T>
void VecColumn<T>::set_any(size_t vid, const Value& value, bool) {
  if (vid > std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("VecColumn::set_any: invalid vid");
  }
  validateValue(value);
  auto offset = offset_accessor_->GetIndexIDByVID(static_cast<vid_t>(vid));
  if (offset == INVALID_OFFSET) {
    offset = offset_accessor_->UpsertVID(static_cast<vid_t>(vid));
  }
  if (offset >= size_) {
    resize(offset < 4096 ? 4096 : offset + offset / 4, default_value_);
  }
  auto* data = static_cast<T*>(buffer_->GetData()) +
               static_cast<size_t>(offset) * array_size_;
  const auto& children = ArrayValue::GetChildren(value);
  for (size_t i = 0; i < array_size_; ++i) {
    data[i] = children[i].GetValue<T>();
  }
}

template <typename T>
Value VecColumn<T>::get_any(size_t vid) const {
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
  const auto* data = static_cast<const T*>(buffer_->GetData()) +
                     static_cast<size_t>(offset) * array_size_;
  std::vector<Value> values;
  values.reserve(array_size_);
  for (size_t i = 0; i < array_size_; ++i) {
    values.emplace_back(Value::CreateValue<T>(data[i]));
  }
  return Value::ARRAY(arrayType(), std::move(values));
}

template <typename T>
void VecColumn<T>::ingest(uint32_t vid, OutArchive& arc) {
  std::vector<Value> values;
  values.reserve(array_size_);
  for (size_t i = 0; i < array_size_; ++i) {
    T value;
    arc >> value;
    values.emplace_back(Value::CreateValue<T>(value));
  }
  set_any(vid, Value::ARRAY(arrayType(), std::move(values)), true);
}

template <typename T>
IndexIDAccessor* VecColumn<T>::get_offset_accessor() {
  return offset_accessor_.get();
}

template <typename T>
const void* VecColumn<T>::get_buffer_ptr() const {
  return buffer_ ? buffer_->GetData() : nullptr;
}

template <typename T>
uint64_t VecColumn<T>::array_size() const {
  return array_size_;
}

template <typename T>
DataType VecColumn<T>::array_type() const {
  return arrayType();
}

template <typename T>
std::unique_ptr<Module> VecColumn<T>::Clone() const {
  auto cloned = std::make_unique<VecColumn<T>>();
  cloned->buffer_ = buffer_;
  auto accessor = offset_accessor_->Clone();
  cloned->offset_accessor_.reset(
      static_cast<IndexIDAccessor*>(accessor.release()));
  cloned->ckp_ = ckp_;
  cloned->level_ = level_;
  cloned->array_size_ = array_size_;
  cloned->size_ = size_;
  cloned->default_value_ = default_value_;
  return cloned;
}

template <typename T>
void VecColumn<T>::Detach(Checkpoint& ckp, MemoryLevel level) {
  ckp_ = &ckp;
  level_ = level;
  if (offset_accessor_) {
    offset_accessor_->Detach(ckp, level);
  }
}

template <typename T>
std::string VecColumn<T>::ModuleTypeName() const {
  return type_name();
}

template <typename T>
std::string VecColumn<T>::type_name() {
  return "column<vector<" + ValueConverter<T>::name() + ">>";
}

template <typename T>
DataType VecColumn<T>::arrayType() const {
  return DataType::Array(ValueConverter<T>::type(), array_size_);
}

template <typename T>
void VecColumn<T>::validateState() const {
  if (!buffer_ || !offset_accessor_ || array_size_ == 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn requires a buffer, accessor, and non-zero array size");
  }
  if (buffer_->GetDataSize() < size_ * array_size_ * sizeof(T)) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn buffer is smaller than its logical size");
  }
  validateValue(default_value_);
}

template <typename T>
void VecColumn<T>::validateValue(const Value& value) const {
  if (value.type() != arrayType()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "VecColumn value type does not match vector type");
  }
}

template class VecColumn<float>;

NEUG_REGISTER_TEMPLATE_MODULE(VecColumn, float);

}  // namespace neug
