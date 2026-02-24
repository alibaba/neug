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

#include <cstddef>

#include "graph.h"
#include "graph_entry.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/data_chunk/sel_vector.h"
#include "neug/compiler/common/enums/extend_direction.h"
#include "neug/compiler/common/enums/path_semantic.h"
#include "neug/compiler/common/enums/rel_direction.h"
#include "neug/compiler/common/mask.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/storage/store/node_table.h"
#include "neug/compiler/storage/store/rel_table.h"

namespace neug {
namespace storage {
class MemoryManager;
}

namespace graph {

class OnDiskGraphNbrScanState : public NbrScanState {
  friend class OnDiskGraph;

 public:
  OnDiskGraphNbrScanState(main::ClientContext* context,
                          catalog::TableCatalogEntry* tableEntry,
                          std::shared_ptr<binder::Expression> predicate);
  OnDiskGraphNbrScanState(main::ClientContext* context,
                          catalog::TableCatalogEntry* tableEntry,
                          std::shared_ptr<binder::Expression> predicate,
                          std::vector<std::string> relProperties,
                          bool randomLookup = false);

  Chunk getChunk() override {
    std::vector<common::ValueVector*> vectors;
    for (auto& propertyVector : propertyVectors) {
      vectors.push_back(propertyVector.get());
    }
    common::SelectionVector selVector;
    return createChunk(std::span<const common::nodeID_t>(), selVector, vectors);
  }
  bool next() override { return false; }

  void startScan(common::RelDataDirection direction) {}

 private:
  std::unique_ptr<common::ValueVector> srcNodeIDVector;
  std::unique_ptr<common::ValueVector> dstNodeIDVector;
  std::vector<std::unique_ptr<common::ValueVector>> propertyVectors;

  common::SemiMask* nbrNodeMask = nullptr;
};

class OnDiskGraphVertexScanState final : public VertexScanState {
 public:
  OnDiskGraphVertexScanState(main::ClientContext& context,
                             const catalog::TableCatalogEntry* tableEntry,
                             const std::vector<std::string>& propertyNames);

  void startScan(common::offset_t beginOffset,
                 common::offset_t endOffsetExclusive);

  bool next() override;
  Chunk getChunk() override {
    return createChunk(std::span(&nodeIDVector->getValue<common::nodeID_t>(0),
                                 numNodesScanned),
                       std::span(propertyVectors.valueVectors));
  }

 private:
  const main::ClientContext& context;
  const storage::NodeTable& nodeTable;

  common::DataChunk propertyVectors;
  std::unique_ptr<common::ValueVector> nodeIDVector;

  common::offset_t numNodesScanned;
  common::offset_t currentOffset;
  common::offset_t endOffsetExclusive;
};

class NEUG_API OnDiskGraph final : public Graph {
 public:
  OnDiskGraph(main::ClientContext* context, GraphEntry entry);

  GraphEntry* getGraphEntry() override { return &graphEntry; }

  void setNodeOffsetMask(common::NodeOffsetMaskMap* maskMap) {
    nodeOffsetMaskMap = maskMap;
  }

  std::vector<common::table_id_t> getNodeTableIDs() const override {
    return graphEntry.getNodeTableIDs();
  }
  std::vector<common::table_id_t> getRelTableIDs() const override {
    return graphEntry.getRelTableIDs();
  }

  common::table_id_map_t<common::offset_t> getMaxOffsetMap(
      transaction::Transaction* transaction) const override;

  common::offset_t getMaxOffset(transaction::Transaction* transaction,
                                common::table_id_t id) const override;

  common::offset_t getNumNodes(
      transaction::Transaction* transaction) const override;

  std::vector<NbrTableInfo> getForwardNbrTableInfos(
      common::table_id_t srcNodeTableID) override;

  std::unique_ptr<NbrScanState> prepareRelScan(
      catalog::TableCatalogEntry* tableEntry,
      catalog::TableCatalogEntry* nbrNodeEntry,
      std::vector<std::string> relProperties) override;
  std::unique_ptr<NbrScanState> prepareRelScan(
      catalog::TableCatalogEntry* tableEntry) const;

  EdgeIterator scanFwd(common::nodeID_t nodeID, NbrScanState& state) override;
  EdgeIterator scanBwd(common::nodeID_t nodeID, NbrScanState& state) override;

  std::unique_ptr<VertexScanState> prepareVertexScan(
      catalog::TableCatalogEntry* tableEntry,
      const std::vector<std::string>& propertiesToScan) override;
  VertexIterator scanVertices(common::offset_t beginOffset,
                              common::offset_t endOffsetExclusive,
                              VertexScanState& state) override;

 private:
  main::ClientContext* context;
  GraphEntry graphEntry;
  common::NodeOffsetMaskMap* nodeOffsetMaskMap = nullptr;
  common::table_id_map_t<storage::NodeTable*> nodeIDToNodeTable;
  common::table_id_map_t<std::vector<NbrTableInfo>> nodeIDToNbrTableInfos;
};

}  // namespace graph
}  // namespace neug
