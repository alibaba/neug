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

#include "neug/compiler/binder/query/query_graph_label_analyzer.h"

#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/rel_table_catalog_entry.h"
#include "neug/compiler/common/string_format.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::catalog;
using namespace neug::transaction;

namespace neug {
namespace binder {

// NOLINTNEXTLINE(readability-non-const-parameter): graph is supposed to be
// modified.
void QueryGraphLabelAnalyzer::pruneLabel(QueryGraph& graph) const {
  for (auto i = 0u; i < graph.getNumQueryNodes(); ++i) {
    pruneNode(graph, *graph.getQueryNode(i));
  }
  for (auto i = 0u; i < graph.getNumQueryRels(); ++i) {
    pruneRel(*graph.getQueryRel(i));
  }
}

void QueryGraphLabelAnalyzer::pruneNode(const QueryGraph& graph,
                                        NodeExpression& node) const {
  auto catalog = clientContext.getCatalog();
  for (auto i = 0u; i < graph.getNumQueryRels(); ++i) {
    auto queryRel = graph.getQueryRel(i);
    if (queryRel->isRecursive()) {
      continue;
    }
    common::table_id_set_t candidates;
    std::unordered_set<std::string> candidateNamesSet;
    auto isSrcConnect = *queryRel->getSrcNode() == node;
    auto isDstConnect = *queryRel->getDstNode() == node;
    auto tx = clientContext.getTransaction();
    if (queryRel->getDirectionType() == RelDirectionType::BOTH) {
      if (isSrcConnect || isDstConnect) {
        for (auto entry : queryRel->getEntries()) {
          auto& relEntry = entry->constCast<RelTableCatalogEntry>();
          auto srcTableID = relEntry.getSrcTableID();
          auto dstTableID = relEntry.getDstTableID();
          candidates.insert(srcTableID);
          candidates.insert(dstTableID);
          auto srcEntry = catalog->getTableCatalogEntry(tx, srcTableID);
          auto dstEntry = catalog->getTableCatalogEntry(tx, dstTableID);
          candidateNamesSet.insert(srcEntry->getName());
          candidateNamesSet.insert(dstEntry->getName());
        }
      }
    } else {
      if (isSrcConnect) {
        for (auto entry : queryRel->getEntries()) {
          auto& relEntry = entry->constCast<RelTableCatalogEntry>();
          auto srcTableID = relEntry.getSrcTableID();
          candidates.insert(srcTableID);
          auto srcEntry = catalog->getTableCatalogEntry(tx, srcTableID);
          candidateNamesSet.insert(srcEntry->getName());
        }
      } else if (isDstConnect) {
        for (auto entry : queryRel->getEntries()) {
          auto& relEntry = entry->constCast<RelTableCatalogEntry>();
          auto dstTableID = relEntry.getDstTableID();
          candidates.insert(dstTableID);
          auto dstEntry = catalog->getTableCatalogEntry(tx, dstTableID);
          candidateNamesSet.insert(dstEntry->getName());
        }
      }
    }
    if (candidates.empty()) {  // No need to prune.
      continue;
    }
    std::vector<TableCatalogEntry*> prunedEntries;
    for (auto entry : node.getEntries()) {
      if (!candidates.contains(entry->getTableID())) {
        continue;
      }
      prunedEntries.push_back(entry);
    }
    node.setEntries(prunedEntries);
    if (prunedEntries.empty()) {
      if (throwOnViolate) {
        auto candidateNames = std::vector<std::string>{
            candidateNamesSet.begin(), candidateNamesSet.end()};
        auto candidateStr = candidateNames[0];
        for (auto j = 1u; j < candidateNames.size(); ++j) {
          candidateStr += ", " + candidateNames[j];
        }
        THROW_BINDER_EXCEPTION(stringFormat(
            "Query node {} violates schema. Expected labels are {}.",
            node.toString(), candidateStr));
      }
    }
  }
}

void QueryGraphLabelAnalyzer::pruneRel(RelExpression& rel) const {
  if (rel.isRecursive()) {
    return;
  }
  std::vector<TableCatalogEntry*> prunedEntries;
  if (rel.getDirectionType() == RelDirectionType::BOTH) {
    table_id_set_t srcBoundTableIDSet;
    table_id_set_t dstBoundTableIDSet;
    for (auto entry : rel.getSrcNode()->getEntries()) {
      srcBoundTableIDSet.insert(entry->getTableID());
    }
    for (auto entry : rel.getDstNode()->getEntries()) {
      dstBoundTableIDSet.insert(entry->getTableID());
    }
    for (auto& entry : rel.getEntries()) {
      auto& relEntry = entry->constCast<RelTableCatalogEntry>();
      auto srcTableID = relEntry.getSrcTableID();
      auto dstTableID = relEntry.getDstTableID();
      if ((srcBoundTableIDSet.contains(srcTableID) &&
           dstBoundTableIDSet.contains(dstTableID)) ||
          (dstBoundTableIDSet.contains(srcTableID) &&
           srcBoundTableIDSet.contains(dstTableID))) {
        prunedEntries.push_back(entry);
      }
    }
  } else {
    auto srcTableIDSet = rel.getSrcNode()->getTableIDsSet();
    auto dstTableIDSet = rel.getDstNode()->getTableIDsSet();
    for (auto& entry : rel.getEntries()) {
      auto& relEntry = entry->constCast<RelTableCatalogEntry>();
      auto srcTableID = relEntry.getSrcTableID();
      auto dstTableID = relEntry.getDstTableID();
      if (!srcTableIDSet.contains(srcTableID) ||
          !dstTableIDSet.contains(dstTableID)) {
        continue;
      }
      prunedEntries.push_back(entry);
    }
  }
  rel.setEntries(prunedEntries);
  // Note the pruning for node should guarantee the following exception won't be
  // triggered. For safety (and consistency) reason, we still write the check
  // but skip coverage check. LCOV_EXCL_START
  if (prunedEntries.empty()) {
    if (throwOnViolate) {
      THROW_BINDER_EXCEPTION(
          stringFormat("Cannot find a label for relationship {} that "
                       "connects to all of its neighbour nodes.",
                       rel.toString()));
    }
  }
  // LCOV_EXCL_STOP
}

}  // namespace binder
}  // namespace neug
