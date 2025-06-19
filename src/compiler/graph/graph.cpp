#include "src/include/graph/graph.h"

#include "src/compiler/common/system_config.h"

namespace gs::graph {
NbrScanState::Chunk::Chunk(std::span<const common::nodeID_t> nbrNodes,
                           common::SelectionVector& selVector,
                           std::vector<common::ValueVector*> propertyVectors)
    : nbrNodes{nbrNodes},
      selVector{selVector},
      propertyVectors{std::move(propertyVectors)} {
  KU_ASSERT(nbrNodes.size() == common::DEFAULT_VECTOR_CAPACITY);
}

VertexScanState::Chunk::Chunk(
    std::span<const common::nodeID_t> nodeIDs,
    std::span<const std::shared_ptr<common::ValueVector>> propertyVectors)
    : nodeIDs{nodeIDs}, propertyVectors{propertyVectors} {
  KU_ASSERT(nodeIDs.size() <= common::DEFAULT_VECTOR_CAPACITY);
}
}  // namespace gs::graph
