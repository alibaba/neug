/**
 * Copyright 2020 Alibaba Group Holding Limited.
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
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <string>
#include <unordered_map>

#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/types/types.h"

namespace neug {
namespace planner {
class Schema;
}
namespace graph {

struct GraphEntryTableInfo {
  std::string tableName;
  std::string predicate;

  GraphEntryTableInfo(std::string tableName, std::string predicate)
      : tableName{std::move(tableName)}, predicate{std::move(predicate)} {}

  std::string toString() const;
};

struct ParsedGraphEntry {
  std::vector<GraphEntryTableInfo> nodeInfos;
  std::vector<GraphEntryTableInfo> relInfos;
};

struct BoundGraphEntryTableInfo {
  catalog::TableCatalogEntry* entry;

  std::shared_ptr<binder::Expression> nodeOrRel;
  std::shared_ptr<binder::Expression> predicate;

  explicit BoundGraphEntryTableInfo(catalog::TableCatalogEntry* entry)
      : entry{entry} {}
  BoundGraphEntryTableInfo(catalog::TableCatalogEntry* entry,
                           std::shared_ptr<binder::Expression> nodeOrRel,
                           std::shared_ptr<binder::Expression> predicate)
      : entry{entry},
        nodeOrRel{std::move(nodeOrRel)},
        predicate{std::move(predicate)} {}
};

// Organize projected graph similar to CatalogEntry. When we want to share
// projected graph across statements, we need to migrate this class to catalog
// (or client context).
struct NEUG_API GraphEntry {
  std::vector<BoundGraphEntryTableInfo> nodeInfos;
  std::vector<BoundGraphEntryTableInfo> relInfos;

  GraphEntry() = default;
  GraphEntry(std::vector<catalog::TableCatalogEntry*> nodeEntries,
             std::vector<catalog::TableCatalogEntry*> relEntries);
  EXPLICIT_COPY_DEFAULT_MOVE(GraphEntry);

  bool isEmpty() const { return nodeInfos.empty() && relInfos.empty(); }

  std::vector<common::table_id_t> getNodeTableIDs() const;
  std::vector<common::table_id_t> getRelTableIDs() const;
  std::vector<catalog::TableCatalogEntry*> getNodeEntries() const;
  std::vector<catalog::TableCatalogEntry*> getRelEntries() const;

  const BoundGraphEntryTableInfo& getRelInfo(common::table_id_t tableID) const;

  void setRelPredicate(std::shared_ptr<binder::Expression> predicate);

  std::string toString() const {
    std::string result = "GraphEntry{";
    result += "nodeInfos=[";
    for (auto& nodeInfo : nodeInfos) {
      result += nodeInfo.entry->getName() + ", ";
    }
    result += "], relInfos=[";
    for (auto& relInfo : relInfos) {
      result += relInfo.entry->getName() + ", ";
    }
    result += "]}";
    return result;
  }

 private:
  GraphEntry(const GraphEntry& other)
      : nodeInfos{other.nodeInfos}, relInfos{other.relInfos} {}
};

class GraphEntrySet {
 public:
  bool hasGraph(const std::string& name) const {
    return nameToEntry.contains(name);
  }
  const ParsedGraphEntry& getEntry(const std::string& name) const {
    NEUG_ASSERT(hasGraph(name));
    return nameToEntry.at(name);
  }
  void addGraph(const std::string& name, const ParsedGraphEntry& entry) {
    nameToEntry.insert({name, entry});
  }
  void dropGraph(const std::string& name) { nameToEntry.erase(name); }

  using iterator = std::unordered_map<std::string, ParsedGraphEntry>::iterator;
  using const_iterator =
      std::unordered_map<std::string, ParsedGraphEntry>::const_iterator;

  iterator begin() { return nameToEntry.begin(); }
  iterator end() { return nameToEntry.end(); }
  const_iterator begin() const { return nameToEntry.begin(); }
  const_iterator end() const { return nameToEntry.end(); }
  const_iterator cbegin() const { return nameToEntry.cbegin(); }
  const_iterator cend() const { return nameToEntry.cend(); }

 private:
  std::unordered_map<std::string, ParsedGraphEntry> nameToEntry;
};

}  // namespace graph
}  // namespace neug
