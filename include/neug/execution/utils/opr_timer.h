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
#pragma once

#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/generated/proto/response/response.pb.h"

namespace neug {

namespace execution {

class TimerUnit {
 public:
  TimerUnit() : start_() {}
  ~TimerUnit() = default;

  void start() { start_ = std::chrono::high_resolution_clock::now(); }

  double elapsed() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double>(end - start_).count();
  }

 private:
  std::chrono::high_resolution_clock::time_point start_;
};

class OprTimer {
 public:
  OprTimer()
      : children_(), next_(nullptr), name_(""), time_(0.0), numTuples_(0) {
    id_ = reinterpret_cast<int64_t>(this) & 0xFFFFFFFF;
  }

  void set_name(const std::string& name) { name_ = name; }

  const std::string& name() const { return name_; }

  int64_t id() const { return id_; }

  void add_child(std::unique_ptr<OprTimer>&& child) {
    children_.emplace_back(std::move(child));
  }

  void set_next(std::unique_ptr<OprTimer>&& next) { next_ = std::move(next); }

  void record(const TimerUnit& tu) { time_ += tu.elapsed(); }

  void add_num_tuples(uint64_t num) { numTuples_ += num; }

  ~OprTimer() = default;

  OprTimer* next() { return next_.get(); }

  size_t get_children_count() const { return children_.size(); }

  OprTimer* get_child(size_t index) const {
    if (index < children_.size()) {
      return children_[index].get();
    }
    return nullptr;
  }

  uint64_t get_num_tuples() const { return numTuples_; }

  void output(const std::string& prefix, std::ostream& os) const {
    os << prefix << name_ << " elapsed: " << time_ << " s, " << numTuples_
       << " tuples" << std::endl;
    int idx = 0;
    std::string child_prefix = prefix + "         ";
    for (const auto& child : children_) {
      os << child_prefix << "child " << idx++ << ":" << std::endl;
      child->output(child_prefix, os);
    }
    if (next_) {
      next_->output(prefix, os);
    }
  }

  OprTimer& operator+=(const OprTimer& other);

  double elapsed() const {
    double time = time_;
    auto next = next_.get();
    while (next) {
      time += next->time_;
      next = next->next_.get();
    }
    return time;
  }

 private:
  std::vector<std::unique_ptr<OprTimer>> children_;
  std::unique_ptr<OprTimer> next_;
  std::string name_;
  double time_ = 0.0;
  int64_t id_;
  uint64_t numTuples_;
};

inline ProfileResult OprTimerToProfileResult(OprTimer* root) {
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
