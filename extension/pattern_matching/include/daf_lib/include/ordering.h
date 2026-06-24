/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the DAF project
 * (https://github.com/SNUCSE-CTA/DAF) Licensed under the MIT License.
 * Modified by Yunkai Lou and Shunyang Li in 2025 to support Neug-specific
 * features.
 */

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

  Ordering& operator=(const Ordering&) = delete;
  Ordering(const Ordering&) = delete;

  void Insert(Vertex u, Size weight);
  void UpdateWeight(Vertex u, Size weight);
  void Remove(Vertex u);
  bool Exists(Vertex u);
  Vertex PopMinWeight();

 private:
  Size num_query_vertices_;
  Vertex* extendable_queue_;
  Size extendable_queue_size_;
  Size* weights_;
};
}  // namespace daf

#endif  // ORDERING_H_
