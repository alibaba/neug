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

#include "neug/utils/property/nest_column.h"

#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module/module_broker.h"
#include "neug/storages/module/module_factory.h"

namespace neug {

NEUG_REGISTER_MODULE(ListColumn);

void ListColumn::DumpTo(Checkpoint& ckp, CheckpointManifest& meta,
                        const std::string& key) {
  meta.set_module(key, Dump(ckp));
  if (child_column_) {
    const std::string child_key = key + "/child";
    child_column_->DumpTo(ckp, meta, child_key);
  }
}

void ListColumn::RestoreChildren(ModuleBroker& store, const std::string& key) {
  const std::string child_key = key + "/child";
  if (!store.Contains(child_key)) {
    return;
  }
  child_column_ = store.TakeModule<ColumnBase>(child_key);

  // Propagate child type info for nested lists before recursing.
  if (list_type_.id() == DataTypeId::kList) {
    auto child_type = ListType::GetChildType(list_type_);
    if (child_type.id() == DataTypeId::kList) {
      auto* child_list = dynamic_cast<ListColumn*>(child_column_.get());
      if (child_list) {
        child_list->SetListType(child_type);
      }
    }
  }

  child_column_->RestoreChildren(store, child_key);
}

}  // namespace neug
