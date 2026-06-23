#ifndef ORDERING_H_
#define ORDERING_H_

#include "../global/global.h"
#include "../include/candidate_space.h"
#include "../include/query_graph.h"

namespace daf {
class Ordering {
 public:
  explicit Ordering(Size num_query_vertices);
  ~Ordering();

  Ordering &operator=(const Ordering &) = delete;
  Ordering(const Ordering &) = delete;

  void Insert(Vertex u, Size weight);
  void UpdateWeight(Vertex u, Size weight);
  void Remove(Vertex u);
  bool Exists(Vertex u);
  Vertex PopMinWeight();

 private:
  Size num_query_vertices_;
  Vertex *extendable_queue_;
  Size extendable_queue_size_;
  Size *weights_;
};
}  // namespace daf

#endif  // ORDERING_H_
