#include "include/ordering.h"

namespace daf {
Ordering::Ordering(Size num_query_vertices)
    : num_query_vertices_(num_query_vertices) {
  extendable_queue_ = new Vertex[num_query_vertices_];
  extendable_queue_size_ = 0;
  weights_ = new Size[num_query_vertices_];
  std::fill(weights_, weights_ + num_query_vertices_,
            std::numeric_limits<Size>::max());
}

Ordering::~Ordering() {
  delete[] extendable_queue_;
  delete[] weights_;
}

void Ordering::Insert(Vertex u, Size weight) {
  extendable_queue_[extendable_queue_size_] = u;
  extendable_queue_size_ += 1;
  weights_[u] = weight;
}

void Ordering::UpdateWeight(Vertex u, Size weight) { weights_[u] = weight; }

void Ordering::Remove(Vertex u) {
  for (Size i = 0; i < extendable_queue_size_; ++i) {
    if (extendable_queue_[i] == u) {
      weights_[u] = std::numeric_limits<Size>::max();
      std::swap(extendable_queue_[i],
                extendable_queue_[extendable_queue_size_ - 1]);
      extendable_queue_size_ -= 1;
      return;
    }
  }
}

bool Ordering::Exists(Vertex u) {
  return weights_[u] != std::numeric_limits<Size>::max();
}

Vertex Ordering::PopMinWeight() {
  Size popped_idx = 0;

  for (Size i = 1; i < extendable_queue_size_; ++i) {
    if (weights_[extendable_queue_[i]] <
        weights_[extendable_queue_[popped_idx]])
      popped_idx = i;
  }

  Vertex popped_vtx = extendable_queue_[popped_idx];

  weights_[popped_vtx] = std::numeric_limits<Size>::max();
  std::swap(extendable_queue_[popped_idx],
            extendable_queue_[extendable_queue_size_ - 1]);
  extendable_queue_size_ -= 1;

  return popped_vtx;
}
}  // namespace daf
