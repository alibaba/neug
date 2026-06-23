#ifndef DAG_H_
#define DAG_H_

#include <algorithm>

#include "../global/global.h"
#include "../include/data_graph.h"
#include "../include/query_graph.h"

namespace daf {
class DAG {
 public:
  DAG(const DataGraph &data, const QueryGraph &query);
  ~DAG();

  DAG &operator=(const DAG &) = delete;
  DAG(const DAG &) = delete;

  void BuildDAG();

  inline Vertex GetRoot() const;
  inline Size GetNumChildren(Vertex v) const;
  inline Size GetNumParents(Vertex v) const;
  inline Size GetChild(Vertex v, Size i) const;
  inline Size GetParent(Vertex v, Size i) const;
  inline Size GetInitCandSize(Vertex v) const;
  inline Vertex GetVertexOrderedByBFS(Size i) const;

 private:
  const DataGraph &data_;
  const QueryGraph &query_;

  Vertex *bfs_sequence_;
  Size *num_children_;
  Size *num_parents_;
  Vertex **children_;
  Vertex **parents_;
  Size *init_cand_size_;

  Vertex SelectRootVertex();
};

inline Vertex DAG::GetRoot() const { return bfs_sequence_[0]; }
inline Size DAG::GetNumChildren(Vertex v) const { return num_children_[v]; }
inline Size DAG::GetNumParents(Vertex v) const { return num_parents_[v]; }
inline Size DAG::GetChild(Vertex v, Size i) const { return children_[v][i]; }
inline Size DAG::GetParent(Vertex v, Size i) const { return parents_[v][i]; }
inline Size DAG::GetInitCandSize(Vertex v) const { return init_cand_size_[v]; }
inline Vertex DAG::GetVertexOrderedByBFS(Size i) const {
  return bfs_sequence_[i];
}

}  // namespace daf

#endif  // DAG_H_
