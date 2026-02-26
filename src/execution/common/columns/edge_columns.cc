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

#include "neug/execution/common/columns/edge_columns.h"

namespace neug {

namespace execution {

std::shared_ptr<IContextColumn> SDSLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  SDSLEdgeColumnBuilder builder(dir_, label_);
  builder.reserve(offsets.size());
  if (is_optional_) {
    for (auto offset : offsets) {
      auto& e = edges_[offset];
      if (std::get<0>(e) == std::numeric_limits<vid_t>::max() ||
          std::get<1>(e) == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e));
      }
    }
  } else {
    for (auto offset : offsets) {
      auto& e = edges_[offset];
      builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e));
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> SDSLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  SDSLEdgeColumnBuilder builder(dir_, label_);
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      auto& e = edges_[offset];
      if (std::get<0>(e) == std::numeric_limits<vid_t>::max() ||
          std::get<1>(e) == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e));
      }
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> SDSLEdgeColumnBuilder::finish() {
  auto col = std::make_shared<SDSLEdgeColumn>(dir_, label_);
  col->edges_ = std::move(edges_);
  col->is_optional_ = is_optional_;
  return col;
}

std::shared_ptr<IContextColumn> MSEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  if (labels_.size() == 1) {
    BDSLEdgeColumnBuilder builder(labels_[0]);
    builder.reserve(offsets.size());
    if (is_optional_) {
      for (auto offset : offsets) {
        for (auto& seg_tuple : edges_) {
          auto& seg = std::get<2>(seg_tuple);
          if (offset < seg.size()) {
            auto& e = seg[offset];
            if (std::get<0>(e) == std::numeric_limits<vid_t>::max() ||
                std::get<1>(e) == std::numeric_limits<vid_t>::max()) {
              builder.push_back_null();
            } else {
              builder.push_back_opt(std::get<0>(e), std::get<1>(e),
                                    std::get<2>(e), std::get<1>(seg_tuple));
            }
            break;
          } else {
            offset -= seg.size();
          }
        }
      }
    } else {
      for (auto offset : offsets) {
        for (auto& seg_tuple : edges_) {
          auto& seg = std::get<2>(seg_tuple);
          if (offset < seg.size()) {
            auto& e = seg[offset];
            builder.push_back_opt(std::get<0>(e), std::get<1>(e),
                                  std::get<2>(e), std::get<1>(seg_tuple));
            break;
          } else {
            offset -= seg.size();
          }
        }
      }
    }
    return builder.finish();
  } else {
    BDMLEdgeColumnBuilder builder(labels_);
    builder.reserve(offsets.size());
    if (is_optional_) {
      for (auto offset : offsets) {
        for (auto& seg_tuple : edges_) {
          auto& seg = std::get<2>(seg_tuple);
          if (offset < seg.size()) {
            auto& e = seg[offset];
            if (std::get<0>(e) == std::numeric_limits<vid_t>::max() ||
                std::get<1>(e) == std::numeric_limits<vid_t>::max()) {
              builder.push_back_null();
            } else {
              builder.push_back_opt(std::get<0>(seg_tuple), std::get<0>(e),
                                    std::get<1>(e), std::get<2>(e),
                                    std::get<1>(seg_tuple));
            }
            break;
          } else {
            offset -= seg.size();
          }
        }
      }
    } else {
      for (auto offset : offsets) {
        for (auto& seg_tuple : edges_) {
          auto& seg = std::get<2>(seg_tuple);
          if (offset < seg.size()) {
            auto& e = seg[offset];
            builder.push_back_opt(std::get<0>(seg_tuple), std::get<0>(e),
                                  std::get<1>(e), std::get<2>(e),
                                  std::get<1>(seg_tuple));
            break;
          } else {
            offset -= seg.size();
          }
        }
      }
    }
    return builder.finish();
  }
}

std::shared_ptr<IContextColumn> MSEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  if (labels_.size() == 1) {
    BDSLEdgeColumnBuilder builder(labels_[0]);
    builder.reserve(offsets.size());

    for (auto offset : offsets) {
      if (offset == std::numeric_limits<size_t>::max()) {
        builder.push_back_null();
        continue;
      }
      for (auto& seg_tuple : edges_) {
        auto& seg = std::get<2>(seg_tuple);
        if (offset < seg.size()) {
          auto& e = seg[offset];
          if (std::get<0>(e) == std::numeric_limits<vid_t>::max() ||
              std::get<1>(e) == std::numeric_limits<vid_t>::max()) {
            builder.push_back_null();
          } else {
            builder.push_back_opt(std::get<0>(e), std::get<1>(e),
                                  std::get<2>(e), std::get<1>(seg_tuple));
          }
          break;
        } else {
          offset -= seg.size();
        }
      }
    }

    return builder.finish();
  } else {
    BDMLEdgeColumnBuilder builder(labels_);
    builder.reserve(offsets.size());
    for (auto offset : offsets) {
      if (offset == std::numeric_limits<size_t>::max()) {
        builder.push_back_null();
        continue;
      }
      for (auto& seg_tuple : edges_) {
        auto& seg = std::get<2>(seg_tuple);
        if (offset < seg.size()) {
          auto& e = seg[offset];
          if (std::get<0>(e) == std::numeric_limits<vid_t>::max() ||
              std::get<1>(e) == std::numeric_limits<vid_t>::max()) {
            builder.push_back_null();
          } else {
            builder.push_back_opt(std::get<0>(seg_tuple), std::get<0>(e),
                                  std::get<1>(e), std::get<2>(e),
                                  std::get<1>(seg_tuple));
          }
          break;
        } else {
          offset -= seg.size();
        }
      }
    }

    return builder.finish();
  }
}

std::shared_ptr<IContextColumn> BDSLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  BDSLEdgeColumnBuilder builder(label_);
  builder.reserve(offsets.size());
  if (is_optional_) {
    for (auto offset : offsets) {
      auto& e = edges_[offset];
      if (std::get<0>(e) == std::numeric_limits<vid_t>::max() ||
          std::get<1>(e) == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e),
                              std::get<3>(e));
      }
    }
  } else {
    for (auto offset : offsets) {
      auto& e = edges_[offset];
      builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e),
                            std::get<3>(e));
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> BDSLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  BDSLEdgeColumnBuilder builder(label_);
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      auto& e = edges_[offset];
      if (std::get<0>(e) == std::numeric_limits<vid_t>::max() ||
          std::get<1>(e) == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e),
                              std::get<3>(e));
      }
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> SDMLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  SDMLEdgeColumnBuilder builder(dir_, labels_);
  builder.reserve(offsets.size());
  if (is_optional_) {
    for (auto offset : offsets) {
      auto& e = edges_[offset];
      if (std::get<1>(e) == std::numeric_limits<vid_t>::max() ||
          std::get<2>(e) == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e),
                              std::get<3>(e));
      }
    }
  } else {
    for (auto offset : offsets) {
      auto& e = edges_[offset];
      builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e),
                            std::get<3>(e));
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> SDMLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  SDMLEdgeColumnBuilder builder(dir_, labels_);
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      auto& e = edges_[offset];
      if (std::get<1>(e) == std::numeric_limits<vid_t>::max() ||
          std::get<2>(e) == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e),
                              std::get<3>(e));
      }
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> BDMLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  BDMLEdgeColumnBuilder builder(labels_);
  builder.reserve(offsets.size());
  if (is_optional_) {
    for (auto offset : offsets) {
      auto& e = edges_[offset];
      if (std::get<1>(e) == std::numeric_limits<vid_t>::max() ||
          std::get<2>(e) == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e),
                              std::get<3>(e), std::get<4>(e));
      }
    }
  } else {
    for (auto offset : offsets) {
      auto& e = edges_[offset];
      builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e),
                            std::get<3>(e), std::get<4>(e));
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> BDMLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  BDMLEdgeColumnBuilder builder(labels_);
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      auto& e = edges_[offset];
      if (std::get<1>(e) == std::numeric_limits<vid_t>::max() ||
          std::get<2>(e) == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(std::get<0>(e), std::get<1>(e), std::get<2>(e),
                              std::get<3>(e), std::get<4>(e));
      }
    }
  }
  return builder.finish();
}

}  // namespace execution

}  // namespace neug
