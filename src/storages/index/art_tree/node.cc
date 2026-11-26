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

#include "neug/storages/index/art_tree/node.h"

#include "neug/storages/index/art_tree/art.h"
#include "neug/storages/index/art_tree/art_key.h"
#include "neug/storages/index/art_tree/art_scanner.h"
#include "neug/storages/index/art_tree/base_leaf.h"
#include "neug/storages/index/art_tree/base_node.h"
#include "neug/storages/index/art_tree/common.h"
#include "neug/storages/index/art_tree/const_prefix_handle.h"
#include "neug/storages/index/art_tree/node256.h"
#include "neug/storages/index/art_tree/node256_leaf.h"
#include "neug/storages/index/art_tree/node48.h"
#include "neug/storages/index/art_tree/prefix.h"

namespace neug::art {
void NodePtr::New(ArtTree& art, NodePtr& node, NType type) {
  switch (type) {
  case NType::NODE_7_LEAF:
    Node7Leaf::New(art, node);
    break;
  case NType::NODE_15_LEAF:
    Node15Leaf::New(art, node);
    break;
  case NType::NODE_256_LEAF:
    Node256Leaf::New(art, node);
    break;
  case NType::NODE_4:
    Node4::New(art, node);
    break;
  case NType::NODE_16:
    Node16::New(art, node);
    break;
  case NType::NODE_48:
    Node48::New(art, node);
    break;
  case NType::NODE_256:
    Node256::New(art, node);
    break;
  default:
    throw InternalException("Invalid node type for New: %d.", type);
  }
}

NType NodePtr::GetInternalNodeType(idx_t count) {
  if (count <= Node4::CAPACITY) {
    return NType::NODE_4;
  } else if (count <= Node16::CAPACITY) {
    return NType::NODE_16;
  } else if (count <= Node48::CAPACITY) {
    return NType::NODE_48;
  }
  return NType::NODE_256;
}

void NodePtr::FreeNode(ArtTree& art, NodePtr& node) {
  D_ASSERT(node.HasMetadata());
  GetAllocator(art, node.GetType()).Free(node);
  node.Clear();
}

//===--------------------------------------------------------------------===//
// Allocators
//===--------------------------------------------------------------------===//

FixedSizeAllocator& NodePtr::GetAllocator(const ArtTree& art,
                                          const NType type) {
  return *(*art.allocators)[GetAllocatorIdx(type)];
}

uint8_t NodePtr::GetAllocatorIdx(const NType type) {
  switch (type) {
  case NType::PREFIX:
    return 0;
  case NType::NODE_4:
    return 1;
  case NType::NODE_16:
    return 2;
  case NType::NODE_48:
    return 3;
  case NType::NODE_256:
    return 4;
  case NType::NODE_7_LEAF:
    return 5;
  case NType::NODE_15_LEAF:
    return 6;
  case NType::NODE_256_LEAF:
    return 7;
  default:
    throw InternalException("Invalid node type for GetAllocatorIdx: %d.", type);
  }
}

void NodePtr::InsertChild(ArtTree& art, NodePtr& node, const uint8_t byte,
                          const NodePtr child) {
  D_ASSERT(node.HasMetadata());

  auto type = node.GetType();
  switch (type) {
  case NType::NODE_4:
    return Node4::InsertChild(art, node, byte, child);
  case NType::NODE_16:
    return Node16::InsertChild(art, node, byte, child);
  case NType::NODE_48:
    return Node48::InsertChild(art, node, byte, child);
  case NType::NODE_256:
    return Node256::InsertChild(art, node, byte, child);
  case NType::NODE_7_LEAF:
    return Node7Leaf::InsertByte(art, node, byte);
  case NType::NODE_15_LEAF:
    return Node15Leaf::InsertByte(art, node, byte);
  case NType::NODE_256_LEAF:
    return Node256Leaf::InsertByte(art, node, byte);
  default:
    throw InternalException("Invalid node type for InsertChild: %d.", type);
  }
}

//===--------------------------------------------------------------------===//
// Get child and byte.
//===--------------------------------------------------------------------===//

template <class NODE>
static unsafe_optional_ptr<NodePtr> GetChildInternal(ArtTree& art, NODE& node,
                                                     const uint8_t byte,
                                                     const bool unsafe) {
  D_ASSERT(node.HasMetadata());

  auto type = node.GetType();
  switch (type) {
  case NType::NODE_4:
    return Node4::GetChild(NodePtr::Ref<Node4>(art, node, type), byte, unsafe);
  case NType::NODE_16:
    return Node16::GetChild(NodePtr::Ref<Node16>(art, node, type), byte,
                            unsafe);
  case NType::NODE_48:
    return Node48::GetChild(NodePtr::Ref<Node48>(art, node, type), byte,
                            unsafe);
  case NType::NODE_256: {
    return Node256::GetChild(NodePtr::Ref<Node256>(art, node, type), byte,
                             unsafe);
  }
  default:
    throw InternalException("Invalid node type for GetChildInternal: %d.",
                            type);
  }
}

unsafe_optional_ptr<NodePtr> NodePtr::GetChildMutable(ArtTree& art,
                                                      const uint8_t byte,
                                                      const bool unsafe) const {
  return GetChildInternal(art, *this, byte, unsafe);
}

OptionalNodePtr NodePtr::GetChildNode(const ArtTree& art,
                                      const uint8_t byte) const {
  D_ASSERT(HasMetadata());
  auto type = GetType();
  ConstNodeHandle handle(art, *this);
  switch (type) {
  case NType::NODE_4:
    return Node4::GetChildNode(handle.Get<Node4>(), byte);
  case NType::NODE_16:
    return Node16::GetChildNode(handle.Get<Node16>(), byte);
  case NType::NODE_48:
    return Node48::GetChildNode(handle.Get<Node48>(), byte);
  case NType::NODE_256:
    return Node256::GetChildNode(handle.Get<Node256>(), byte);
  default:
    throw InternalException("Invalid node type for GetChildNode: %d.", type);
  }
}

OptionalNodePtr NodePtr::GetNextChildNode(const ArtTree& art,
                                          uint8_t& byte) const {
  D_ASSERT(HasMetadata());
  auto type = GetType();
  ConstNodeHandle handle(art, *this);
  switch (type) {
  case NType::NODE_4:
    return Node4::GetNextChildNode(handle.Get<Node4>(), byte);
  case NType::NODE_16:
    return Node16::GetNextChildNode(handle.Get<Node16>(), byte);
  case NType::NODE_48:
    return Node48::GetNextChildNode(handle.Get<Node48>(), byte);
  case NType::NODE_256:
    return Node256::GetNextChildNode(handle.Get<Node256>(), byte);
  default:
    throw InternalException("Invalid node type for GetNextChildNode: %d.",
                            type);
  }
}

template <class NODE>
unsafe_optional_ptr<NodePtr> GetNextChildInternal(ArtTree& art, NODE& node,
                                                  uint8_t& byte) {
  D_ASSERT(node.HasMetadata());

  auto type = node.GetType();
  switch (type) {
  case NType::NODE_4:
    return Node4::GetNextChild(NodePtr::Ref<Node4>(art, node, type), byte);
  case NType::NODE_16:
    return Node16::GetNextChild(NodePtr::Ref<Node16>(art, node, type), byte);
  case NType::NODE_48:
    return Node48::GetNextChild(NodePtr::Ref<Node48>(art, node, type), byte);
  case NType::NODE_256:
    return Node256::GetNextChild(NodePtr::Ref<Node256>(art, node, type), byte);
  default:
    throw InternalException("Invalid node type for GetNextChildInternal: %d.",
                            type);
  }
}

const unsafe_optional_ptr<NodePtr> NodePtr::GetNextChild(ArtTree& art,
                                                         uint8_t& byte) const {
  return GetNextChildInternal(art, *this, byte);
}

bool NodePtr::HasByte(const ArtTree& art, const uint8_t byte) const {
  D_ASSERT(HasMetadata());

  auto type = GetType();
  switch (type) {
  case NType::NODE_7_LEAF:
    return Ref<const Node7Leaf>(art, *this, NType::NODE_7_LEAF).HasByte(byte);
  case NType::NODE_15_LEAF:
    return Ref<const Node15Leaf>(art, *this, NType::NODE_15_LEAF).HasByte(byte);
  case NType::NODE_256_LEAF:
    return Ref<const Node256Leaf>(art, *this, NType::NODE_256_LEAF)
        .HasByte(byte);
  default:
    throw InternalException("Invalid node type for GetNextByte: %d.", type);
  }
}

bool NodePtr::GetNextByte(const ArtTree& art, uint8_t& byte) const {
  D_ASSERT(HasMetadata());

  auto type = GetType();
  switch (type) {
  case NType::NODE_7_LEAF:
    return Ref<const Node7Leaf>(art, *this, NType::NODE_7_LEAF)
        .GetNextByte(byte);
  case NType::NODE_15_LEAF:
    return Ref<const Node15Leaf>(art, *this, NType::NODE_15_LEAF)
        .GetNextByte(byte);
  case NType::NODE_256_LEAF:
    return Ref<const Node256Leaf>(art, *this, NType::NODE_256_LEAF)
        .GetNextByte(byte);
  default:
    throw InternalException("Invalid node type for GetNextByte: %d.", type);
  }
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

idx_t GetCapacity(NType type) {
  switch (type) {
  case NType::NODE_4:
    return Node4::CAPACITY;
  case NType::NODE_7_LEAF:
    return Node7Leaf::CAPACITY;
  case NType::NODE_15_LEAF:
    return Node15Leaf::CAPACITY;
  case NType::NODE_16:
    return Node16::CAPACITY;
  case NType::NODE_48:
    return Node48::CAPACITY;
  case NType::NODE_256_LEAF:
    return Node256::CAPACITY;
  case NType::NODE_256:
    return Node256::CAPACITY;
  default:
    throw InternalException("Invalid node type for GetCapacity: %d.", type);
  }
}

bool NodePtr::IsInternalNode() const {
  switch (GetType()) {
  case NType::NODE_4:
  case NType::NODE_16:
  case NType::NODE_48:
  case NType::NODE_256:
    return true;
  default:
    return false;
  }
}

bool NodePtr::IsNestedLeaf() const {
  switch (GetType()) {
  case NType::NODE_7_LEAF:
  case NType::NODE_15_LEAF:
  case NType::NODE_256_LEAF:
    return true;
  default:
    return false;
  }
}

bool NodePtr::IsAnyLeaf() const {
  if (IsNestedLeaf()) {
    return true;
  }

  switch (GetType()) {
  case NType::LEAF_INLINED:
    return true;
  default:
    return false;
  }
}

//===--------------------------------------------------------------------===//
// Verification
//===--------------------------------------------------------------------===//

void NodePtr::Verify(ArtTree& art) const {
  D_ASSERT(HasMetadata());

  auto type = GetType();
  switch (type) {
  case NType::LEAF_INLINED:
    return;
  case NType::PREFIX: {
    ConstPrefixHandle::Verify(art, *this);
    return;
  }
  default:
    break;
  }

  if (!IsNestedLeaf()) {
    uint8_t byte = 0;
    auto child = GetNextChildNode(art, byte);
    while (child) {
      child.Get().Verify(art);
      if (byte == ARTNumericLimits<uint8_t>::Maximum()) {
        break;
      }
      byte++;
      child = GetNextChildNode(art, byte);
    }
  }
}

void NodePtr::VerifyAllocations(
    ArtTree& art, unordered_map<uint8_t, idx_t>& node_counts) const {
  D_ASSERT(HasMetadata());

  auto child_handler = [&](const NodePtr& child) -> OptionalNodePtr {
    D_ASSERT(child.HasMetadata());
    auto type = child.GetType();
    switch (type) {
    case NType::LEAF_INLINED:
      return OptionalNodePtr();

    case NType::NODE_7_LEAF:
    case NType::NODE_15_LEAF:
    case NType::NODE_256_LEAF:
      node_counts[GetAllocatorIdx(type)]++;
      return OptionalNodePtr();
    case NType::PREFIX:
    case NType::NODE_4:
    case NType::NODE_16:
    case NType::NODE_48:
    case NType::NODE_256:
      node_counts[GetAllocatorIdx(type)]++;
      return child;
    default:
      throw InternalException("invalid node type for VerifyAllocations: %d",
                              type);
    }
  };

  ARTConstScanPreorder(art, *this, child_handler);
}

//===--------------------------------------------------------------------===//
// Printing
//===--------------------------------------------------------------------===//

namespace {
// Tree-style branch characters
constexpr const char* const NODE_BRANCH_MID = "├── ";
constexpr const char* const NODE_BRANCH_END = "└── ";
constexpr const char* const NODE_VERTICAL = "│   ";
constexpr const char* const NODE_SPACE = "    ";

// ASCII printable character range
constexpr uint8_t NODE_ASCII_PRINTABLE_MIN = 32;
constexpr uint8_t NODE_ASCII_PRINTABLE_MAX = 126;
}  // namespace

string NodePtr::ToStringChildren(ArtTree& art,
                                 const ToStringOptions& options) const {
  auto format_byte = [&](const uint8_t byte) {
    if (!options.inside_gate && options.display_ascii &&
        byte >= NODE_ASCII_PRINTABLE_MIN && byte <= NODE_ASCII_PRINTABLE_MAX) {
      return string(1, static_cast<char>(byte));
    }
    return to_string(byte);
  };

  auto is_gate = GetGateStatus() == GateStatus::GATE_SET;
  auto propagate_gate = options.inside_gate || is_gate;
  auto print_full_tree =
      propagate_gate || !options.key_path || options.expand_after_n_levels == 0;

  string str;

  if (IsNestedLeaf()) {
    str +=
        StringUtil::Format("%s%sLeaf |", options.tree_prefix, NODE_BRANCH_END);
    uint8_t byte = 0;
    auto has_byte = GetNextByte(art, byte);
    while (has_byte) {
      str += StringUtil::Format("%s|", format_byte(byte));
      if (byte == ARTNumericLimits<uint8_t>::Maximum()) {
        break;
      }
      byte++;
      has_byte = GetNextByte(art, byte);
    }
    str += "\n";
  } else if (IsInternalNode()) {
    // Collect all children first to know which is last
    vector<pair<uint8_t, const NodePtr*>> children;
    uint8_t byte = 0;
    auto child = GetNextChild(art, byte);
    while (child) {
      children.emplace_back(byte, child.get());
      if (byte == ARTNumericLimits<uint8_t>::Maximum()) {
        break;
      }
      byte++;
      child = GetNextChild(art, byte);
    }

    uint8_t expected_byte = 0;
    auto has_expected_byte = false;
    if (options.key_path && !print_full_tree &&
        options.key_depth < options.key_path->len) {
      expected_byte = (*options.key_path)[options.key_depth];
      has_expected_byte = true;
    }

    for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
      auto& child_entry = children[child_idx];
      auto child_byte = child_entry.first;
      auto& child_slot = child_entry.second;
      auto is_last = (child_idx == children.size() - 1);
      auto on_path = !has_expected_byte ||
                     (has_expected_byte && child_byte == expected_byte);

      // When structure_only is true and we're following a path, treat on-path
      // child as last (since we're hiding off-path siblings, the branch should
      // end here)
      auto effective_last =
          is_last || (options.structure_only && has_expected_byte && on_path);
      auto branch = effective_last ? NODE_BRANCH_END : NODE_BRANCH_MID;
      auto child_prefix =
          StringUtil::Format("%s%s", options.tree_prefix,
                             effective_last ? NODE_SPACE : NODE_VERTICAL);

      if (on_path) {
        str += StringUtil::Format("%s%s%s\n", options.tree_prefix, branch,
                                  format_byte(child_byte));

        auto child_options = options;
        child_options.inside_gate = propagate_gate;
        child_options.key_depth =
            has_expected_byte ? options.key_depth + 1 : options.key_depth;
        child_options.expand_after_n_levels =
            (options.expand_after_n_levels > 0)
                ? options.expand_after_n_levels - 1
                : 0;

        auto child_type = child_slot->GetType();
        auto is_internal =
            (child_type == NType::NODE_4 || child_type == NType::NODE_16 ||
             child_type == NType::NODE_48 || child_type == NType::NODE_256);
        if (is_internal) {
          str +=
              StringUtil::Format("%s%sNode%s\n", child_prefix, NODE_BRANCH_END,
                                 to_string(GetCapacity(child_type)));
          child_options.tree_prefix =
              StringUtil::Format("%s%s", child_prefix, NODE_SPACE);
          str += child_slot->ToStringChildren(art, child_options);
        } else {
          child_options.tree_prefix = child_prefix;
          str += child_slot->ToString(art, child_options);
        }
      } else {
        if (!options.structure_only) {
          str += StringUtil::Format("%s%s%s ...\n", options.tree_prefix, branch,
                                    format_byte(child_byte));
        }
      }
    }
  }
  return str;
}

string NodePtr::ToString(ArtTree& art, const ToStringOptions& options) const {
  auto type = GetType();
  auto is_gate = GetGateStatus() == GateStatus::GATE_SET;
  auto propagate_gate = options.inside_gate || is_gate;

  switch (type) {
  case NType::LEAF_INLINED: {
    return options.tree_prefix +
           "Inlined Leaf [row ID: " + to_string(GetRowId()) + "]\n";
  }
  case NType::PREFIX: {
    auto prefix_options = options;
    prefix_options.inside_gate = propagate_gate;
    auto str = ConstPrefixHandle::ToString(art, *this, prefix_options);
    if (is_gate) {
      str = options.tree_prefix + "Gate\n" + str;
    }
    return str;
  }
  default:
    break;
  }

  // For internal nodes: print header then children
  auto str = options.tree_prefix + "Node" + to_string(GetCapacity(type)) + "\n";
  str += ToStringChildren(art, options);

  if (is_gate && type != NType::PREFIX) {
    str = options.tree_prefix + "Gate\n" + str;
  }
  return str;
}

}  // namespace neug::art
