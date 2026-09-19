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
#include <vector>

#include "neug/common/types.h"
#include "neug/common/types/value.h"
#include "neug/utils/property/column.h"

namespace neug {

// A struct property column stores one child column per field. Every child
// holds exactly one slot per row, so the layout is dense and Dump needs no
// compaction (unlike ListPropertyColumn). Nested structs, LIST/ARRAY fields
// and LIST<STRUCT> fall out naturally because each child column is created
// recursively via CreateColumn.
//
// Null semantics (normalized on write, same as ListPropertyColumn): a null
// struct value is stored as all-field defaults; a null field value is stored
// as that field's default (handled by the child column itself). No validity
// bitmap is kept, which keeps storage compact and Dump simple.
class StructPropertyColumn : public ColumnBase {
 public:
  StructPropertyColumn() = default;
  explicit StructPropertyColumn(const DataType& struct_type);
  ~StructPropertyColumn() override = default;

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override;
  void Open(Checkpoint& ckp, const CheckpointManifest& manifest,
            const ModuleDescriptor& desc, MemoryLevel level) override;

  // Dump writes this column's descriptor (struct type + row count) and lets
  // each field column dump itself under key/<field_name>. Children are dense
  // with one slot per row, so a single sequential pass with no row-wise data
  // movement suffices.
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;

  // Row count is derived from the first field column: field columns size
  // exactly (one slot per row), so no separate size_ member is needed.
  size_t size() const override {
    return fields_.empty() ? 0 : fields_.front()->size();
  }
  void resize(size_t size) override;
  void resize(size_t size, const Value& default_value) override;

  DataTypeId type() const override { return DataTypeId::kStruct; }
  void set_any(size_t index, const Value& value, bool insert_safe) override;
  Value get_any(size_t index) const override;

  std::unique_ptr<Module> Clone() const override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;

  std::string ModuleTypeName() const override { return type_name(); }
  static std::string type_name() { return "column<struct>"; }

  const DataType& struct_type() const { return struct_type_; }
  size_t num_fields() const { return fields_.size(); }
  const ColumnBase& field_column(size_t idx) const { return *fields_[idx]; }
  size_t field_idx(const std::string& name) const {
    return StructType::GetFieldIdx(struct_type_, name);
  }

 private:
  void openInternal(Checkpoint& ckp, const CheckpointManifest* manifest,
                    const ModuleDescriptor& desc, MemoryLevel level);
  ModuleDescriptor dumpSelfDescriptor() const;

  DataType struct_type_;
  std::vector<std::unique_ptr<ColumnBase>> fields_;
};

class StructPropertyRefColumn : public RefColumnBase {
 public:
  explicit StructPropertyRefColumn(const StructPropertyColumn& column);

  Value get_any(size_t index) const override;
  DataTypeId type() const override { return DataTypeId::kStruct; }
  ColType col_type() const override { return ColType::kInternal; }

  // Exposes the child ref column for field-level access: a struct field
  // expression can bind only the relevant child column instead of assembling
  // the whole struct. Nested structs recurse through further
  // StructPropertyRefColumn children.
  const RefColumnBase& field_ref(size_t idx) const { return *fields_[idx]; }
  // Shares ownership of the child ref column so a struct field expression can
  // bind only that column (projection pushdown).
  const std::shared_ptr<RefColumnBase>& field_ref_ptr(size_t idx) const {
    return fields_[idx];
  }
  size_t field_idx(const std::string& name) const {
    return StructType::GetFieldIdx(struct_type_, name);
  }
  const DataType& struct_type() const { return struct_type_; }

 private:
  DataType struct_type_;
  std::vector<std::shared_ptr<RefColumnBase>> fields_;
};

}  // namespace neug
