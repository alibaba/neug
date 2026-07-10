/** Copyright 2020 Alibaba Group Holding Limited.
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

#include "neug/execution/utils/opr_timer.h"

namespace neug {

namespace execution {

OprTimer& OprTimer::operator+=(const OprTimer& other) {
  this->time_ += other.time_;
  this->numTuples_ += other.numTuples_;
  for (size_t i = 0; i < children_.size(); ++i) {
    *children_[i] += *other.children_[i];
  }
  if (other.next_) {
    *(this->next_) += *(other.next_);
  }
  return *this;
}

ProfileResult OprTimer::ToProfileResult(OprTimer* root) {
  ProfileResult result;

  if (!root) {
    result.set_total_elapsed_ms(0.0);
    result.set_total_output_rows(0);
    return result;
  }

  // DFS traverse OprTimer tree, collect metrics
  std::vector<ProfileResult::OperatorMetrics*> metrics_list;
  std::function<void(OprTimer*, int64_t)> traverse = [&](OprTimer* node,
                                                         int64_t parent_id) {
    if (!node)
      return;

    auto metrics = result.add_operators();
    metrics->set_operator_id(node->id());
    metrics->set_parent_id(parent_id);
    metrics->set_operator_name(node->name());
    metrics->set_elapsed_ms(node->elapsed() * 1000.0);
    metrics->set_output_rows(node->get_num_tuples());

    // traverse children
    for (size_t i = 0; i < node->get_children_count(); ++i) {
      metrics->add_child_ids(node->get_child(i)->id());
      traverse(node->get_child(i), node->id());
    }

    // traverse next
    if (node->next()) {
      traverse(node->next(), parent_id);
    }
  };

  traverse(root, -1);

  // Calculate total elapsed time and total output rows
  double total_elapsed = root->elapsed();
  result.set_total_elapsed_ms(total_elapsed * 1000.0);

  // Find the last node in the main chain to get total output rows
  OprTimer* last = root;
  while (last && last->next()) {
    last = last->next();
  }
  if (last) {
    result.set_total_output_rows(last->get_num_tuples());
  }

  return result;
}

}  // namespace execution

}  // namespace neug
