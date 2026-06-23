#ifndef BACKTRACK_HELPER_H_
#define BACKTRACK_HELPER_H_

#include <boost/dynamic_bitset.hpp>

#include "../global/global.h"

namespace daf {
enum MappingState { UNMAPPED, MAPPED, RESERVED };

struct SearchTreeNode {
  bool initialized;
  Vertex u;
  Vertex v;
  Size v_idx;
  boost::dynamic_bitset<> failing_set;
  bool embedding_founded;
};

class BacktrackHelper {
 public:
  inline BacktrackHelper();
  inline ~BacktrackHelper();

  BacktrackHelper &operator=(const BacktrackHelper &) = delete;
  BacktrackHelper(const BacktrackHelper &) = delete;

  inline void Initialize(Size num_query_vtx, Size degree, Size cs_size,
                         Vertex u);

  void AddMapping(Vertex u) {
    num_mapped_neighbors_ += 1;
    num_extendable_indices_[num_mapped_neighbors_] = 0;
    num_unmapped_extendable_indices_[num_mapped_neighbors_] = 0;
    ancestors_[num_mapped_neighbors_] = ancestors_[num_mapped_neighbors_ - 1];
    last_mapped_neighbor_[num_mapped_neighbors_] = u;
  }
  void RemoveMapping() { num_mapped_neighbors_ -= 1; }

  inline boost::dynamic_bitset<> &GetAncestor() {
    return ancestors_[num_mapped_neighbors_];
  }
  inline Size &GetNumExtendable() {
    return num_extendable_indices_[num_mapped_neighbors_];
  }
  inline Size &GetNumUnmappedExtendable() {
    return num_unmapped_extendable_indices_[num_mapped_neighbors_];
  }
  inline Size *GetExtendableIndices() {
    return extendable_indices_[num_mapped_neighbors_ - 1];
  }
  inline Size GetNumPrevExtendable() {
    return num_extendable_indices_[num_mapped_neighbors_ - 1];
  }
  inline Size *GetPrevExtendableIndices() {
    return extendable_indices_[num_mapped_neighbors_ - 2];
  }
  inline Size GetExtendableIndex(Size i) {
    if (num_mapped_neighbors_ == 0)
      return i;
    else
      return extendable_indices_[num_mapped_neighbors_ - 1][i];
  }
  inline MappingState &GetMappingState() { return mapping_state_; }
  inline Size GetNumMappedNeighbors() { return num_mapped_neighbors_; }
  inline Vertex GetLastMappedNeighbor() {
    return last_mapped_neighbor_[num_mapped_neighbors_];
  }

 private:
  Size num_mapped_neighbors_;
  boost::dynamic_bitset<> *ancestors_;
  Size **extendable_indices_;
  Size *num_extendable_indices_;
  Size *num_unmapped_extendable_indices_;
  Vertex *last_mapped_neighbor_;
  MappingState mapping_state_;

  Size degree_;
};

inline BacktrackHelper::BacktrackHelper() { degree_ = INVALID_SZ; }

inline BacktrackHelper::~BacktrackHelper() {
  if (degree_ != INVALID_SZ) {
    delete[] ancestors_;
    delete[] num_extendable_indices_;
    delete[] num_unmapped_extendable_indices_;

    for (Size i = 0; i < degree_; ++i) {
      delete[] extendable_indices_[i];
    }
    delete[] extendable_indices_;
    delete[] last_mapped_neighbor_;
  }
}

inline void BacktrackHelper::Initialize(Size num_query_vtx, Size degree,
                                        Size cs_size, Vertex u) {
  num_mapped_neighbors_ = 0;
  ancestors_ = new boost::dynamic_bitset<>[degree + 1];
  extendable_indices_ = new Size *[degree];
  num_extendable_indices_ = new Size[degree + 1];
  num_unmapped_extendable_indices_ = new Size[degree + 1];
  last_mapped_neighbor_ = new Vertex[degree + 1];
  mapping_state_ = UNMAPPED;

  ancestors_[0].resize(num_query_vtx);
  ancestors_[0].set(u);
  num_extendable_indices_[0] = cs_size;
  num_unmapped_extendable_indices_[0] = cs_size;
  last_mapped_neighbor_[0] = INVALID_VTX;

  for (Size i = 0; i < degree; ++i) {
    extendable_indices_[i] = new Size[cs_size];
  }

  degree_ = degree;
}

}  // namespace daf

#endif  // BACKTRACK_HELPER_H_
