#include "include/dag.h"

namespace daf {
DAG::DAG(const DataGraph &data, const QueryGraph &query)
    : data_(data), query_(query) {
  // initialize
  bfs_sequence_ = new Vertex[query_.GetNumVertices()];

  num_children_ = new Size[query.GetNumVertices()];
  num_parents_ = new Size[query.GetNumVertices()];

  children_ = new Vertex *[query_.GetNumVertices()];
  parents_ = new Vertex *[query_.GetNumVertices()];

  for (Size i = 0; i < query_.GetNumVertices(); ++i) {
    children_[i] = new Vertex[query_.GetNumVertices()];
    parents_[i] = new Vertex[query_.GetNumVertices()];
  }

  init_cand_size_ = new Size[query_.GetNumVertices()];

  std::fill(num_children_, num_children_ + query.GetNumVertices(), 0);
  std::fill(num_parents_, num_parents_ + query.GetNumVertices(), 0);
}

DAG::~DAG() {
  delete[] bfs_sequence_;

  for (Size i = 0; i < query_.GetNumVertices(); ++i) {
    delete[] children_[i];
    delete[] parents_[i];
  }
  delete[] children_;
  delete[] parents_;

  delete[] num_children_;
  delete[] num_parents_;

  delete[] init_cand_size_;
}

void DAG::BuildDAG() {
  bool *visit = new bool[query_.GetNumVertices()];
  bool *popped = new bool[query_.GetNumVertices()];

  std::fill(visit, visit + query_.GetNumVertices(), false);
  std::fill(popped, popped + query_.GetNumVertices(), false);

  // used as queue (topologically ordered)
  bfs_sequence_[0] = SelectRootVertex();
  visit[bfs_sequence_[0]] = true;

  Size begin = 0;
  Size end = 1;

  while (begin < end) {
    std::sort(bfs_sequence_ + begin, bfs_sequence_ + end,
              [this](Vertex v1, Vertex v2) -> bool {
                Size d1 = query_.GetDegree(v1);
                Size d2 = query_.GetDegree(v2);
                return d1 > d2;
              });
    std::stable_sort(bfs_sequence_ + begin, bfs_sequence_ + end,
                     [this](Vertex v1, Vertex v2) -> bool {
                       Label l1 = query_.GetLabel(v1);
                       Label l2 = query_.GetLabel(v2);
                       Size lf1 = data_.GetLabelFrequency(l1);
                       Size lf2 = data_.GetLabelFrequency(l2);
                       if (lf1 != lf2) return lf1 < lf2;
                       return l1 < l2;
                     });

    Size cur_level_end = end;
    while (begin < cur_level_end) {
      Vertex parent = bfs_sequence_[begin];

      begin += 1;
      popped[parent] = true;

      for (Size i = query_.GetStartOffset(parent);
           i < query_.GetEndOffset(parent); ++i) {
        Vertex child = query_.GetNeighbor(i);

        if (!popped[child]) {
          // build edge from parent to child
          children_[parent][num_children_[parent]] = child;
          parents_[child][num_parents_[child]] = parent;

          num_children_[parent] += 1;
          num_parents_[child] += 1;

          if (!visit[child]) {
            visit[child] = true;

            bfs_sequence_[end] = child;
            end += 1;
          }
        }
      }
    }
  }

  delete[] visit;
  delete[] popped;
}

Vertex DAG::SelectRootVertex() {
  Vertex root = 0;
  double min_rank = std::numeric_limits<double>::max();
  for (Vertex v = 0; v < query_.GetNumVertices(); ++v) {
    Label l = query_.GetLabel(v);
    Size d = query_.GetDegree(v);
    init_cand_size_[v] = data_.GetInitCandSize(l, d);

    if (query_.GetCoreNum(v) < 2 && !query_.IsTree()) continue;

    double rank =
        static_cast<double>(init_cand_size_[v]) / static_cast<double>(d);

    if (rank < min_rank) {
      root = v;
      min_rank = rank;
    }
  }

  return root;
}
}  // namespace daf
