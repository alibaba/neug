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
#pragma once

#include <memory>
#include <string>

#include "neug/common/types.h"
#include "neug/common/types/value.h"
#include "neug/utils/property/column.h"

namespace neug {

class ListPropertyColumn : public ColumnBase {
 public:
  ListPropertyColumn() : size_(0) {}
  explicit ListPropertyColumn(const DataType& list_type);
  ~ListPropertyColumn() override = default;

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override;
  void Open(Checkpoint& ckp, const CheckpointManifest& manifest,
            const ModuleDescriptor& desc, MemoryLevel level) override;

  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;

  size_t size() const override { return size_; }
  void resize(size_t size) override;
  void resize(size_t size, const Value& default_value) override;

  DataTypeId type() const override { return DataTypeId::kList; }
  void set_any(size_t index, const Value& value, bool insert_safe) override;
  Value get_any(size_t index) const override;

  std::unique_ptr<Module> Clone() const override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;

  std::string ModuleTypeName() const override { return type_name(); }
  static std::string type_name() { return "column<list>"; }

  const DataType& list_type() const { return list_type_; }
  const DataType& child_type() const { return child_type_; }

 private:
  void openInternal(Checkpoint& ckp, const CheckpointManifest* manifest,
                    const ModuleDescriptor& desc, MemoryLevel level);
  ModuleDescriptor dumpSelfDescriptor() const;

  DataType list_type_;
  DataType child_type_;
  size_t size_;
  std::unique_ptr<ULongColumn> offsets_;
  std::unique_ptr<ULongColumn> lengths_;
  std::unique_ptr<ColumnBase> elements_;
};

class ListPropertyRefColumn : public RefColumnBase {
 public:
  explicit ListPropertyRefColumn(const ListPropertyColumn& column)
      : column_(column) {}

  Value get_any(size_t index) const override { return column_.get_any(index); }
  DataTypeId type() const override { return DataTypeId::kList; }
  ColType col_type() const override { return ColType::kInternal; }

 private:
  const ListPropertyColumn& column_;
};

}  // namespace neug
