#pragma once

#include "graph_element.hpp"

namespace gbi {

class GraphStorage {
 public:
  virtual ~GraphStorage() = default;
  virtual bool HasEdge(EIdType source, EIdType target) = 0;
  virtual bool HasEdge(EIdType source, EIdType target, int label) = 0;
};

}  // namespace gbi
