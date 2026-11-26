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

/**
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License.
 */

#include "neug/storages/index/art_tree/const_prefix_handle.h"

#include "neug/storages/index/art_tree/art.h"
#include "neug/storages/index/art_tree/node.h"

namespace neug::art {

namespace {
// Tree-style branch characters
constexpr const char* const PREFIX_BRANCH_END = "└── ";
constexpr const char* const PREFIX_SPACE = "    ";

// ASCII printable character range
constexpr uint8_t PREFIX_ASCII_PRINTABLE_MIN = 32;
constexpr uint8_t PREFIX_ASCII_PRINTABLE_MAX = 126;
}  // namespace

string ConstPrefixHandle::ToString(ArtTree& art, const NodePtr& node,
                                   const ToStringOptions& options) {
  auto format_byte = [&](const uint8_t byte) {
    if (!options.inside_gate && options.display_ascii &&
        byte >= PREFIX_ASCII_PRINTABLE_MIN &&
        byte <= PREFIX_ASCII_PRINTABLE_MAX) {
      return string(1, static_cast<char>(byte));
    }
    return to_string(byte);
  };

  // Print prefix bytes (single branch, child follows on next line)
  auto str = StringUtil::Format("%s%sPrefix: |", options.tree_prefix,
                                PREFIX_BRANCH_END);
  auto child_options = options;
  auto tail = Iterator(art, node, true,
                       [&](const ConstNodeHandle& handle, const_data_ptr_t data,
                           const NodePtr& child) {
                         for (idx_t i = 0; i < data[art.PrefixCount()]; i++) {
                           str +=
                               StringUtil::Format("%s|", format_byte(data[i]));
                           if (options.key_path) {
                             child_options.key_depth++;
                           }
                         }
                       });
  str += "\n";

  // Child is printed indented under the prefix (not as a sibling)
  child_options.tree_prefix =
      StringUtil::Format("%s%s", options.tree_prefix, PREFIX_SPACE);
  str += tail.ToString(art, child_options);
  return str;
}

void ConstPrefixHandle::Verify(ArtTree& art, const NodePtr& node) {
  auto tail = Iterator(art, node, true,
                       [&](const ConstNodeHandle& handle, const_data_ptr_t data,
                           const NodePtr& child) {
                         D_ASSERT(data[art.PrefixCount()] != 0);
                         D_ASSERT(data[art.PrefixCount()] <= art.PrefixCount());
                       });

  tail.Verify(art);
}

}  // namespace neug::art
