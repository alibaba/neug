#include "neug/compiler/common/mask.h"

namespace gs {
namespace common {

offset_t NodeOffsetMaskMap::getNumMaskedNode() const {
  offset_t numNodes = 0;
  for (auto& [tableID, mask] : maskMap) {
    numNodes += mask->getNumMaskedNodes();
  }
  return numNodes;
}

}  // namespace common
}  // namespace gs
