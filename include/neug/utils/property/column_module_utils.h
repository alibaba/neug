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

#include <optional>
#include <string>

#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module_descriptor.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace column_module {

// Helpers shared by composite property columns (list, struct) whose Dump/Open
// wire child columns into the manifest through named refs.
// `context` identifies the calling column operation in error messages, e.g.
// "ListPropertyColumn::Dump".

inline std::string ChildModuleKey(const std::string& parent,
                                  const std::string& role) {
  return parent + "/" + role;
}

inline void MarkReferenced(CheckpointManifest& meta, const std::string& key,
                           const char* context) {
  auto* desc = meta.FindMutableModule(key);
  if (desc == nullptr) {
    THROW_RUNTIME_ERROR(std::string(context) +
                        ": child column did not write module '" + key + "'");
  }
  desc->mark_as_referenced_module();
}

inline const ModuleDescriptor& ResolveChild(
    const CheckpointManifest& manifest, const ModuleDescriptor& parent,
    const std::string& role, std::optional<ModuleDescriptor>& storage,
    const char* context) {
  auto ref = parent.get_ref(role);
  if (!ref.has_value()) {
    THROW_RUNTIME_ERROR(std::string(context) + ": missing '" + role + "' ref");
  }
  const auto* child = manifest.FindModule(*ref);
  if (child == nullptr) {
    THROW_RUNTIME_ERROR(std::string(context) + ": missing child module '" +
                        *ref + "'");
  }
  storage.emplace(*child);
  return *storage;
}

}  // namespace column_module
}  // namespace neug
