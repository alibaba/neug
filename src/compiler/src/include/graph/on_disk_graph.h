#pragma once

#include <cstddef>

#include "binder/expression/expression.h"
#include "common/assert.h"
#include "common/copy_constructors.h"
#include "common/data_chunk/sel_vector.h"
#include "common/enums/extend_direction.h"
#include "common/enums/path_semantic.h"
#include "common/enums/rel_direction.h"
#include "common/mask.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "function/gds/gds_state.h"
#include "graph.h"
#include "graph_entry.h"
#include "main/client_context.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
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

class KUZU_API OnDiskGraph final : public Graph {
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
}  // namespace kuzu
