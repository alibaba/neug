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

#include "neug/compiler/graph/on_disk_graph.h"

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/data_chunk/data_chunk_state.h"
#include "neug/compiler/common/enums/rel_direction.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/graph/graph.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/planner/operator/schema.h"
#include "neug/compiler/processor/result/result_set.h"
#include "neug/compiler/storage/stats_manager.h"

using namespace neug::catalog;
using namespace neug::storage;
using namespace neug::main;
using namespace neug::common;
using namespace neug::planner;
using namespace neug::binder;

namespace neug {
namespace graph {

static std::vector<column_id_t> getColumnIDs(
    const expression_vector& propertyExprs, const TableCatalogEntry& relEntry,
    const std::vector<column_id_t>& propertyColumnIDs) {}

static expression_vector getProperties(std::shared_ptr<Expression> expr) {
  if (expr == nullptr) {
    return expression_vector{};
  }
  auto collector = PropertyExprCollector();
  collector.visit(std::move(expr));
  return ExpressionUtil::removeDuplication(collector.getPropertyExprs());
}

static Schema getSchema(const expression_vector& exprs) {
  auto schema = Schema();
  schema.createGroup();
  for (auto expr : exprs) {
    schema.insertToGroupAndScope(expr, 0);
  }
  return schema;
}

static neug::processor::ResultSet getResultSet(Schema* schema,
                                             MemoryManager* mm) {
  throw new std::runtime_error(
      "getResultSet is not implemented, remove dependency of processor module");
}

static std::unique_ptr<ValueVector> getValueVector(
    const LogicalType& type, MemoryManager* mm,
    std::shared_ptr<DataChunkState> state) {
  auto vector = std::make_unique<ValueVector>(type.copy(), mm);
  vector->state = std::move(state);
  return vector;
}

OnDiskGraphNbrScanState::OnDiskGraphNbrScanState(
    ClientContext* context, TableCatalogEntry* tableEntry,
    std::shared_ptr<Expression> predicate)
    : OnDiskGraphNbrScanState{context, tableEntry, std::move(predicate), {}} {}

OnDiskGraphNbrScanState::OnDiskGraphNbrScanState(
    ClientContext* context, TableCatalogEntry* tableEntry,
    std::shared_ptr<Expression> predicate,
    std::vector<std::string> relProperties, bool randomLookup) {}

OnDiskGraph::OnDiskGraph(ClientContext* context, GraphEntry entry)
    : context{context}, graphEntry{std::move(entry)} {}

table_id_map_t<offset_t> OnDiskGraph::getMaxOffsetMap(
    transaction::Transaction* transaction) const {
  table_id_map_t<offset_t> result;
  for (auto tableID : getNodeTableIDs()) {
    result[tableID] = getMaxOffset(transaction, tableID);
  }
  return result;
}

offset_t OnDiskGraph::getMaxOffset(transaction::Transaction* transaction,
                                   table_id_t id) const {
  NEUG_ASSERT(nodeIDToNodeTable.contains(id));
  return nodeIDToNodeTable.at(id)->getNumTotalRows(transaction);
}

offset_t OnDiskGraph::getNumNodes(transaction::Transaction* transaction) const {
  offset_t numNodes = 0u;
  for (auto id : getNodeTableIDs()) {
    if (nodeOffsetMaskMap != nullptr &&
        nodeOffsetMaskMap->containsTableID(id)) {
      numNodes += nodeOffsetMaskMap->getOffsetMask(id)->getNumMaskedNodes();
    } else {
      numNodes += getMaxOffset(transaction, id);
    }
  }
  return numNodes;
}

std::vector<NbrTableInfo> OnDiskGraph::getForwardNbrTableInfos(
    table_id_t srcNodeTableID) {
  NEUG_ASSERT(nodeIDToNbrTableInfos.contains(srcNodeTableID));
  return nodeIDToNbrTableInfos.at(srcNodeTableID);
}

std::unique_ptr<NbrScanState> OnDiskGraph::prepareRelScan(
    TableCatalogEntry* tableEntry, TableCatalogEntry* nbrNodeEntry,
    std::vector<std::string> relProperties) {
  auto& info = graphEntry.getRelInfo(tableEntry->getTableID());
  auto state = std::make_unique<OnDiskGraphNbrScanState>(
      context, tableEntry, info.predicate, relProperties,
      true /*randomLookup*/);
  if (nodeOffsetMaskMap != nullptr &&
      nodeOffsetMaskMap->containsTableID(nbrNodeEntry->getTableID())) {
    state->nbrNodeMask =
        nodeOffsetMaskMap->getOffsetMask(nbrNodeEntry->getTableID());
  }
  return state;
}

std::unique_ptr<NbrScanState> OnDiskGraph::prepareRelScan(
    TableCatalogEntry* tableEntry) const {
  auto& info = graphEntry.getRelInfo(tableEntry->getTableID());
  std::vector<std::string> properties;
  return std::make_unique<OnDiskGraphNbrScanState>(
      context, tableEntry, info.predicate, properties, true /*randomLookup*/);
}

Graph::EdgeIterator OnDiskGraph::scanFwd(nodeID_t nodeID, NbrScanState& state) {
  auto& onDiskScanState = neug_dynamic_cast<OnDiskGraphNbrScanState&>(state);
  onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
  onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
  onDiskScanState.startScan(RelDataDirection::FWD);
  return EdgeIterator(&onDiskScanState);
}

Graph::EdgeIterator OnDiskGraph::scanBwd(nodeID_t nodeID, NbrScanState& state) {
  auto& onDiskScanState = neug_dynamic_cast<OnDiskGraphNbrScanState&>(state);
  onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
  onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
  onDiskScanState.startScan(RelDataDirection::BWD);
  return EdgeIterator(&onDiskScanState);
}

Graph::VertexIterator OnDiskGraph::scanVertices(offset_t beginOffset,
                                                offset_t endOffsetExclusive,
                                                VertexScanState& state) {
  auto& onDiskVertexScanState =
      neug_dynamic_cast<OnDiskGraphVertexScanState&>(state);
  onDiskVertexScanState.startScan(beginOffset, endOffsetExclusive);
  return VertexIterator(&state);
}

std::unique_ptr<VertexScanState> OnDiskGraph::prepareVertexScan(
    TableCatalogEntry* tableEntry,
    const std::vector<std::string>& propertiesToScan) {
  return std::make_unique<OnDiskGraphVertexScanState>(*context, tableEntry,
                                                      propertiesToScan);
}

OnDiskGraphVertexScanState::OnDiskGraphVertexScanState(
    ClientContext& context, const TableCatalogEntry* tableEntry,
    const std::vector<std::string>& propertyNames)
    : context{context},
      nodeTable{neug_dynamic_cast<const NodeTable&>(
          *context.getStatsManager()->getTable(tableEntry->getTableID()))},
      numNodesScanned{0},
      currentOffset{0},
      endOffsetExclusive{0} {}

void OnDiskGraphVertexScanState::startScan(offset_t beginOffset,
                                           offset_t endOffsetExclusive) {}

bool OnDiskGraphVertexScanState::next() {}

}  // namespace graph
}  // namespace neug
