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

#include "neug/engines/graph_db/runtime/common/columns/edge_columns.h"

namespace gs {

namespace runtime {

void set_edge_data(PropertyType col_ele_type, EdgePropVecBase* col, size_t idx,
                   int32_t col_id, const Any& edge_data) {
  if (col_ele_type != PropertyType::kRecordView) {
    if (col_id != 0) {
      throw std::runtime_error(
          "set_edge_data: col_id should be 0 for non-record edge property");
    }
    if (edge_data.type == PropertyType::kEmpty) {
      return;
    } else if (edge_data.type == PropertyType::kInt64) {
      dynamic_cast<EdgePropVec<int64_t>*>(col)->set(idx, edge_data.AsInt64());
    } else if (edge_data.type == PropertyType::kInt32) {
      dynamic_cast<EdgePropVec<int32_t>*>(col)->set(idx, edge_data.AsInt32());
    } else if (edge_data.type == PropertyType::kDouble) {
      dynamic_cast<EdgePropVec<double>*>(col)->set(idx, edge_data.AsDouble());
    } else if (edge_data.type == PropertyType::kBool) {
      dynamic_cast<EdgePropVec<bool>*>(col)->set(idx, edge_data.AsBool());
    } else if (edge_data.type == PropertyType::kStringView) {
      auto str_view = edge_data.AsStringView();
      dynamic_cast<EdgePropVec<std::string_view>*>(col)->set(
          idx, std::string_view(str_view.data(), str_view.size()));
    } else if (edge_data.type == PropertyType::kDateTime) {
      dynamic_cast<EdgePropVec<DateTime>*>(col)->set(idx,
                                                     edge_data.AsDateTime());
    } else if (edge_data.type == PropertyType::kDate) {
      dynamic_cast<EdgePropVec<Date>*>(col)->set(idx, edge_data.AsDate());
    } else {
      throw std::runtime_error("set_edge_data: unsupported property type: " +
                               edge_data.type.ToString());
    }
  } else {
    if (col_id == -1 && col_ele_type != edge_data.type) {
      throw std::runtime_error(
          "set_edge_data: edge_data type does not match the column type: " +
          edge_data.type.ToString() + " vs " + col_ele_type.ToString());
    }
    auto ptr = dynamic_cast<EdgePropVec<RecordView>*>(col);
    if (ptr == nullptr) {
      throw std::runtime_error("set_edge_data: cast failed");
    }
    auto record_view = col->get(idx).as<RecordView>();
    if (col_id >= 0 && col_id >= (int32_t) record_view.size()) {
      throw std::runtime_error(
          "set_edge_data: col_id out of range for record view: " +
          std::to_string(col_id) +
          ", size = " + std::to_string(record_view.size()));
    }
    // No need to call set, since RecordView is just a view, the updated value
    // will be visible in the EdgePropVec<RecordView> after this function
    // returns.
  }
}

std::shared_ptr<IContextColumn> SDSLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  SDSLEdgeColumnBuilder builder(dir_, label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);

  if (prop_type_ == PropertyType::kEmpty) {
    for (auto off : offsets) {
      const auto& e = edges_[off];
      builder.push_back_endpoints(e.first, e.second);
    }
  } else {
    auto& ret_props = *builder.prop_col_;
    ret_props.resize(new_row_num);
    for (size_t idx = 0; idx < new_row_num; ++idx) {
      size_t off = offsets[idx];
      const auto& e = edges_[off];
      builder.push_back_endpoints(e.first, e.second);
      ret_props.set_any(idx, prop_col_.get(), off);
    }
  }

  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> SDSLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  CHECK(prop_type_ != PropertyType::kRecordView);
  auto builder =
      SDSLEdgeColumnBuilder::optional_builder(dir_, label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);

  if (prop_type_ == PropertyType::kEmpty) {
    for (auto off : offsets) {
      if (off == std::numeric_limits<size_t>::max()) {
        builder.push_back_null();
      } else {
        const auto& e = edges_[off];
        builder.push_back_endpoints(e.first, e.second);
      }
    }
  } else {
    auto& ret_props = *builder.prop_col_;
    ret_props.resize(new_row_num);
    for (size_t idx = 0; idx < new_row_num; ++idx) {
      size_t off = offsets[idx];
      if (off == std::numeric_limits<size_t>::max()) {
        builder.push_back_null();
      } else {
        const auto& e = edges_[off];
        builder.push_back_endpoints(e.first, e.second);
        ret_props.set_any(idx, prop_col_.get(), off);
      }
    }
  }

  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> SDSLEdgeColumnBuilder::finish(
    const std::shared_ptr<Arena>& arena) {
  if (!is_optional_) {
    auto ret = std::make_shared<SDSLEdgeColumn>(dir_, label_, prop_type_);
    ret->edges_.swap(edges_);
    // shrink to fit
    prop_col_->resize(ret->edges_.size());
    ret->prop_col_ = prop_col_;
    ret->set_arena(arena);
    return ret;
  } else {
    auto ret =
        std::make_shared<OptionalSDSLEdgeColumn>(dir_, label_, prop_type_);
    ret->column_.edges_.swap(edges_);
    prop_col_->resize(ret->column_.edges_.size());
    ret->column_.prop_col_ = prop_col_;
    ret->set_arena(arena);
    return ret;
  }
}

std::shared_ptr<IContextColumn> BDSLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  auto builder = BDSLEdgeColumnBuilder::builder(label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);

  auto& ret_props = *builder.prop_col_;
  ret_props.resize(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = edges_[off];
    builder.push_back_endpoints(std::get<0>(e), std::get<1>(e), std::get<2>(e));
    ret_props.set_any(idx, prop_col_.get(), off);
  }

  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> BDSLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  auto builder = BDSLEdgeColumnBuilder::optional_builder(label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);

  auto& ret_props = *builder.prop_col_;
  ret_props.resize(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    if (off == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      const auto& e = edges_[off];
      builder.push_back_endpoints(std::get<0>(e), std::get<1>(e),
                                  std::get<2>(e));
      ret_props.set_any(idx, prop_col_.get(), off);
    }
  }

  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> BDSLEdgeColumnBuilder::finish(
    const std::shared_ptr<Arena>& arena) {
  if (!is_optional_) {
    auto ret = std::make_shared<BDSLEdgeColumn>(label_, prop_type_);
    ret->set_arena(arena);
    prop_col_->resize(edges_.size());
    ret->edges_.swap(edges_);
    ret->prop_col_ = prop_col_;
    return ret;
  } else {
    auto ret = std::make_shared<OptionalBDSLEdgeColumn>(label_, prop_type_);
    ret->set_arena(arena);
    prop_col_->resize(edges_.size());
    ret->column_.edges_.swap(edges_);
    ret->column_.prop_col_ = prop_col_;
    return ret;
  }
}

std::shared_ptr<IContextColumn> SDMLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  SDMLEdgeColumnBuilder builder(dir_, edge_labels_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = edges_[off];
    auto index = std::get<0>(e);
    auto offset = std::get<3>(e);
    builder.push_back_opt(index, std::get<1>(e), std::get<2>(e),
                          prop_cols_[index]->get(offset));
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> SDMLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  auto builder = SDMLEdgeColumnBuilder::optional_builder(dir_, edge_labels_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    if (off == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      const auto& e = edges_[off];
      auto index = std::get<0>(e);
      auto offset = std::get<3>(e);
      builder.push_back_opt(index, std::get<1>(e), std::get<2>(e),
                            prop_cols_[index]->get(offset));
    }
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> OptionalSDMLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  auto builder = SDMLEdgeColumnBuilder::optional_builder(column_.dir_,
                                                         column_.edge_labels_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = column_.edges_[off];
    auto index = std::get<0>(e);
    auto offset = std::get<3>(e);
    builder.push_back_opt(index, std::get<1>(e), std::get<2>(e),
                          column_.prop_cols_[index]->get(offset));
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> SDMLEdgeColumnBuilder::finish(
    const std::shared_ptr<Arena>& arena) {
  if (!is_optional_) {
    auto ret = std::make_shared<SDMLEdgeColumn>(dir_, edge_labels_);
    ret->set_arena(arena);
    ret->edges_.swap(edges_);
    ret->prop_cols_.swap(prop_cols_);
    return ret;
  } else {
    auto ret = std::make_shared<OptionalSDMLEdgeColumn>(dir_, edge_labels_);
    ret->set_arena(arena);
    ret->column_.edges_.swap(edges_);
    ret->column_.prop_cols_.swap(prop_cols_);
    return ret;
  }
}

std::shared_ptr<IContextColumn> BDMLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  BDMLEdgeColumnBuilder builder(labels_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = edges_[off];
    auto index = std::get<0>(e);
    auto offset = std::get<3>(e);
    auto dir = std::get<4>(e) ? Direction::kOut : Direction::kIn;
    builder.push_back_opt(index, std::get<1>(e), std::get<2>(e),
                          prop_cols_[index]->get(offset), dir);
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> OptionalBDMLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  auto builder = BDMLEdgeColumnBuilder::optional_builder(column_.labels_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = column_.edges_[off];
    auto index = std::get<0>(e);
    auto offset = std::get<3>(e);
    auto dir = std::get<4>(e) ? Direction::kOut : Direction::kIn;
    builder.push_back_opt(index, std::get<1>(e), std::get<2>(e),
                          column_.prop_cols_[index]->get(offset), dir);
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> BDMLEdgeColumnBuilder::finish(
    const std::shared_ptr<Arena>& arena) {
  if (!is_optional_) {
    auto ret = std::make_shared<BDMLEdgeColumn>(labels_);
    ret->set_arena(arena);
    ret->edges_.swap(edges_);
    ret->prop_cols_.swap(prop_cols_);
    return ret;
  } else {
    auto ret = std::make_shared<OptionalBDMLEdgeColumn>(labels_);
    ret->set_arena(arena);
    ret->column_.edges_.swap(edges_);
    ret->column_.prop_cols_.swap(prop_cols_);
    return ret;
  }
}

std::shared_ptr<IContextColumn> OptionalBDSLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  auto builder = BDSLEdgeColumnBuilder::optional_builder(column_.label_,
                                                         column_.prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = get_edge(off);
    builder.push_back_opt(e.src_, e.dst_, e.prop_, e.dir_);
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> OptionalBDSLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  auto builder = BDSLEdgeColumnBuilder::optional_builder(column_.label_,
                                                         column_.prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    if (off == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
      continue;
    }
    const auto& e = get_edge(off);
    builder.push_back_opt(e.src_, e.dst_, e.prop_, e.dir_);
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> OptionalSDSLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  auto builder = SDSLEdgeColumnBuilder::optional_builder(
      column_.dir_, column_.label_, column_.prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = get_edge(off);
    builder.push_back_opt(e.src_, e.dst_, e.prop_);
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> BDMLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  auto builder = BDMLEdgeColumnBuilder::optional_builder(labels_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    if (off == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
      continue;
    }
    const auto& e = edges_[off];
    auto index = std::get<0>(e);
    auto offset = std::get<3>(e);
    auto dir = std::get<4>(e) ? Direction::kOut : Direction::kIn;
    builder.push_back_opt(index, std::get<1>(e), std::get<2>(e),
                          prop_cols_[index]->get(offset), dir);
  }
  return builder.finish(this->get_arena());
}

}  // namespace runtime

}  // namespace gs
