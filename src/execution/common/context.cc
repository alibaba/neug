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

#include "neug/execution/common/context.h"

#include <iostream>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {

namespace execution {

Context::Context() : head(nullptr) {}

void Context::clear() {
  columns.clear();
  head.reset();
  tag_ids.clear();
}

void Context::set(int alias, std::shared_ptr<IContextColumn> col) {
  head = col;
  if (alias >= 0) {
    if (columns.size() <= static_cast<size_t>(alias)) {
      columns.resize(alias + 1, nullptr);
    }
    if (columns[alias] != nullptr) {
      THROW_RUNTIME_ERROR("column already set: " + std::to_string(alias));
    }
    columns[alias] = col;
  }
}

void Context::set_with_reshuffle(int alias, std::shared_ptr<IContextColumn> col,
                                 const std::vector<size_t>& offsets) {
  head.reset();
  head = nullptr;

  if (alias >= 0) {
    if (columns.size() > static_cast<size_t>(alias) &&
        columns[alias] != nullptr) {
      columns[alias].reset();
      columns[alias] = nullptr;
    }
  }

  reshuffle(offsets);
  set(alias, col);
}

void Context::reshuffle(const std::vector<size_t>& offsets) {
  bool head_shuffled = false;
  std::vector<std::shared_ptr<IContextColumn>> new_cols;

  for (auto col : columns) {
    if (col == nullptr) {
      new_cols.push_back(nullptr);

      continue;
    }
    if (col == head) {
      head = col->shuffle(offsets);
      new_cols.push_back(head);
      head_shuffled = true;
    } else {
      new_cols.push_back(col->shuffle(offsets));
    }
  }
  if (!head_shuffled && head != nullptr) {
    head = head->shuffle(offsets);
  }
  std::swap(new_cols, columns);
}

void Context::optional_reshuffle(const std::vector<size_t>& offsets) {
  bool head_shuffled = false;
  std::vector<std::shared_ptr<IContextColumn>> new_cols;

  for (auto col : columns) {
    if (col == nullptr) {
      new_cols.push_back(nullptr);

      continue;
    }
    if (col == head) {
      head = col->optional_shuffle(offsets);
      new_cols.push_back(head);
      head_shuffled = true;
    } else {
      new_cols.push_back(col->optional_shuffle(offsets));
    }
  }
  if (!head_shuffled && head != nullptr) {
    head = head->optional_shuffle(offsets);
  }
  std::swap(new_cols, columns);
}

std::shared_ptr<IContextColumn> Context::get(int alias) {
  if (alias == -1) {
    return head;
  }
  if (alias >= static_cast<int>(columns.size())) {
    THROW_INTERNAL_EXCEPTION("alias out of range: " + std::to_string(alias) +
                             " >= " + std::to_string(columns.size()));
  }
  return columns[alias];
}

const std::shared_ptr<IContextColumn> Context::get(int alias) const {
  if (alias == -1) {
    assert(head != nullptr);
    return head;
  }
  if (alias >= static_cast<int>(columns.size())) {
    THROW_INTERNAL_EXCEPTION("alias out of range: " + std::to_string(alias) +
                             " >= " + std::to_string(columns.size()));
  }
  // return nullptr if the column is not set
  return columns[alias];
}

void Context::remove(int alias) {
  if (alias == -1) {
    for (auto& col : columns) {
      if (col == head) {
        col = nullptr;
      }
    }
    head = nullptr;
  } else if (static_cast<size_t>(alias) < columns.size() && alias >= 0) {
    if (head == columns[alias]) {
      head = nullptr;
    }
    columns[alias] = nullptr;
  }
}

size_t Context::row_num() const {
  for (auto col : columns) {
    if (col != nullptr) {
      return col->size();
    }
  }
  if (head != nullptr) {
    return head->size();
  }
  return 0;
}

bool Context::exist(int alias) const {
  if (alias == -1 && head != nullptr) {
    return true;
  }
  if (static_cast<size_t>(alias) >= columns.size()) {
    return false;
  }
  return columns[alias] != nullptr;
}

void Context::desc(const std::string& info) const {
  if (!info.empty()) {
    LOG(INFO) << info;
  }
  for (size_t col_i = 0; col_i < col_num(); ++col_i) {
    if (columns[col_i] != nullptr) {
      LOG(INFO) << "\tcol-" << col_i << ": " << columns[col_i]->column_info();
    }
  }
  LOG(INFO) << "\thead: " << ((head == nullptr) ? "NULL" : head->column_info());
}

void Context::show(const StorageReadInterface& graph) const {
  size_t rn = row_num();
  size_t cn = col_num();
  for (size_t ri = 0; ri < rn; ++ri) {
    std::string line;
    for (size_t ci = 0; ci < cn; ++ci) {
      if (columns[ci] != nullptr &&
          columns[ci]->column_type() == ContextColumnType::kVertex) {
        auto v = std::dynamic_pointer_cast<IVertexColumn>(columns[ci])
                     ->get_vertex(ri);
        if (v.vid_ == std::numeric_limits<vid_t>::max()) {
          line += "(null)";
        } else {
          line += graph.GetVertexId(v.label_, v.vid_).to_string();
        }
        line += ", ";
      } else if (columns[ci] != nullptr) {
        line += columns[ci]->get_elem(ri).to_string();
        line += ", ";
      }
    }
    LOG(INFO) << line;
  }
}

Context Context::union_ctx(const Context& other) const {
  Context ctx;
  CHECK(columns.size() == other.columns.size());
  for (size_t i = 0; i < col_num(); ++i) {
    if (columns[i] != nullptr) {
      if (head == columns[i]) {
        auto col = columns[i]->union_col(other.get(i));
        ctx.set(i, col);
        ctx.head = col;
      } else {
        ctx.set(i, columns[i]->union_col(other.get(i)));
      }
    }
  }
  return ctx;
}

size_t Context::col_num() const { return columns.size(); }

void ContextMeta::desc() const {
  std::cout << "===============================================" << std::endl;
  for (const auto& col : alias_set_) {
    std::cout << "col - " << col.first << std::endl;
  }
}

}  // namespace execution

}  // namespace neug
