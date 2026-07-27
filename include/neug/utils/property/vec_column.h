#pragma once

#include <limits>
#include <memory>
#include <string>

#include "neug/common/extra_type_info.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/index/index_id_accessor.h"
#include "neug/utils/property/array_column.h"

namespace neug {

inline constexpr index_id_t INVALID_OFFSET = INVALID_INDEX_ID;

template <typename T>
class VecColumn : public ColumnBase {
 public:
  VecColumn();
  VecColumn(std::shared_ptr<IDataContainer> buffer,
            std::unique_ptr<IndexIDAccessor> offset_accessor,
            uint64_t array_size, size_t size, const Value& default_value);

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override;
  void Open(Checkpoint& ckp, const CheckpointManifest& manifest,
            const ModuleDescriptor& desc, MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;

  size_t size() const override;
  void resize(size_t size) override;
  void resize(size_t size, const Value& default_value) override;
  DataTypeId type() const override;
  void set_any(size_t vid, const Value& value, bool insert_safe) override;
  Value get_any(size_t vid) const override;
  void ingest(uint32_t vid, OutArchive& arc) override;

  const IndexIDAccessor* get_offset_accessor() const;
  const void* get_buffer_ptr() const;
  std::shared_ptr<IDataContainer> TakeBuffer();
  uint64_t array_size() const;
  DataType array_type() const;

  std::unique_ptr<Module> Clone() const override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;

  std::string ModuleTypeName() const override;
  static std::string type_name();

 private:
  DataType arrayType() const;
  void openInternal(Checkpoint& ckp, const CheckpointManifest* manifest,
                    const ModuleDescriptor& desc, MemoryLevel level);
  void validateState() const;
  void validateValue(const Value& value) const;

  std::unique_ptr<IndexIDAccessor> offset_accessor_;
  std::shared_ptr<IDataContainer> buffer_;
  Checkpoint* ckp_;
  MemoryLevel level_;
  uint64_t array_size_;
  size_t size_;
  Value default_value_;
};

extern template class VecColumn<float>;
extern template class VecColumn<double>;

template <typename T>
class VecRefColumn : public RefColumnBase {
 public:
  explicit VecRefColumn(const VecColumn<T>& column) : column_(column) {}
  Value get_any(size_t index) const override { return column_.get_any(index); }
  DataTypeId type() const override { return DataTypeId::kArray; }
  ColType col_type() const override { return ColType::kInternal; }

 private:
  const VecColumn<T>& column_;
};

}  // namespace neug
