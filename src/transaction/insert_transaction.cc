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

#include "neug/transaction/insert_transaction.h"

#include <glog/logging.h>
#include <chrono>

#include <limits>
#include <ostream>
#include <thread>

#include "libgrape-lite/grape/serialization/in_archive.h"
#include "libgrape-lite/grape/serialization/out_archive.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/allocators.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace gs {

InsertTransaction::InsertTransaction(PropertyGraph& graph, Allocator& alloc,
                                     IWalWriter& logger, IVersionManager& vm,
                                     timestamp_t timestamp)

    : graph_(graph),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      timestamp_(timestamp) {
  arc_.Resize(sizeof(WalHeader));
}

InsertTransaction::~InsertTransaction() { Abort(); }

bool InsertTransaction::AddVertex(label_t label, const Property& id,
                                  const std::vector<Property>& props) {
  size_t arc_size = arc_.GetSize();
  arc_ << static_cast<uint8_t>(0) << label;
  serialize_field(arc_, id);
  const std::vector<PropertyType>& types =
      graph_.schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    arc_.Resize(arc_size);
    std::string label_name = graph_.schema().get_vertex_label_name(label);
    LOG(ERROR) << "Vertex [" << label_name
               << "] properties size not match, expected " << types.size()
               << ", but got " << props.size();
    return false;
  }
  int col_num = props.size();
  arc_ << static_cast<uint32_t>(col_num);
  for (int col_i = 0; col_i != col_num; ++col_i) {
    auto& prop = props[col_i];
    if (prop.type() != types[col_i]) {
      if (prop.type().type_enum == impl::PropertyTypeImpl::kStringView) {
      } else {
        arc_.Resize(arc_size);
        std::string label_name = graph_.schema().get_vertex_label_name(label);
        LOG(ERROR) << "Vertex [" << label_name << "][" << col_i
                   << "] property type not match, expected " << types[col_i]
                   << ", but got " << prop.type().ToString();
        return false;
      }
    }
    arc_ << static_cast<uint32_t>(col_i);
    serialize_property(arc_, prop);
  }
  added_vertices_.emplace(label, id);
  return true;
}

bool InsertTransaction::AddEdge(label_t src_label, const Property& src,
                                label_t dst_label, const Property& dst,
                                label_t edge_label,
                                const std::vector<Property>& properties) {
  vid_t lid;
  if (!graph_.get_lid(src_label, src, lid, timestamp_)) {
    if (added_vertices_.find(std::make_pair(src_label, src)) ==
        added_vertices_.end()) {
      std::string label_name = graph_.schema().get_vertex_label_name(src_label);
      VLOG(1) << "Source vertex " << label_name << "[" << src.to_string()
              << "] not found...";
      return false;
    }
  }
  if (!graph_.get_lid(dst_label, dst, lid, timestamp_)) {
    if (added_vertices_.find(std::make_pair(dst_label, dst)) ==
        added_vertices_.end()) {
      std::string label_name = graph_.schema().get_vertex_label_name(dst_label);
      VLOG(1) << "Destination vertex " << label_name << "[" << dst.to_string()
              << "] not found...";
      return false;
    }
  }
  const auto& types =
      graph_.schema().get_edge_properties(src_label, dst_label, edge_label);
  if (properties.size() != types.size()) {
    std::string label_name = graph_.schema().get_edge_label_name(edge_label);
    LOG(ERROR) << "Edge property size not match for edge " << label_name
               << ", expected " << types.size() << ", got "
               << properties.size();
    return false;
  }
  for (size_t i = 0; i < properties.size(); ++i) {
    if (properties[i].type() != types[i]) {
      std::string label_name = graph_.schema().get_edge_label_name(edge_label);
      LOG(ERROR) << "Edge property " << label_name
                 << " type not match, expected " << types[i] << ", got "
                 << properties[i].type().ToString();
      return false;
    }
  }
  arc_ << static_cast<uint8_t>(1) << src_label;
  serialize_field(arc_, src);
  arc_ << dst_label;
  serialize_field(arc_, dst);
  arc_ << edge_label;
  arc_ << static_cast<uint32_t>(properties.size());
  for (size_t col_id = 0; col_id < properties.size(); ++col_id) {
    arc_ << static_cast<uint32_t>(col_id);
    serialize_property(arc_, properties[col_id]);
  }
  return true;
}

bool InsertTransaction::Commit() {
  if (timestamp_ == INVALID_TIMESTAMP) {
    return true;
  }
  if (arc_.GetSize() == sizeof(WalHeader)) {
    vm_.release_insert_timestamp(timestamp_);
    clear();
    return true;
  }
  auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
  header->length = arc_.GetSize() - sizeof(WalHeader);
  header->type = 0;
  header->timestamp = timestamp_;

  if (!logger_.append(arc_.GetBuffer(), arc_.GetSize())) {
    LOG(ERROR) << "Failed to append wal log";
    Abort();
    return false;
  }
  IngestWal(graph_, timestamp_, arc_.GetBuffer() + sizeof(WalHeader),
            header->length, alloc_);

  vm_.release_insert_timestamp(timestamp_);
  clear();
  return true;
}

void InsertTransaction::Abort() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    LOG(ERROR) << "aborting " << timestamp_ << "-th transaction (insert)";
    vm_.release_insert_timestamp(timestamp_);
    clear();
  }
}

timestamp_t InsertTransaction::timestamp() const { return timestamp_; }

void InsertTransaction::IngestWal(PropertyGraph& graph, uint32_t timestamp,
                                  char* data, size_t length, Allocator& alloc) {
  grape::OutArchive arc;
  arc.SetSlice(data, length);
  while (!arc.Empty()) {
    uint8_t op_type;
    arc >> op_type;
    if (op_type == 0) {
      label_t label;
      Property id;
      label = deserialize_oid(graph, arc, id);
      vid_t lid = graph.add_vertex(label, id, timestamp);
      // Ignore the cases that the vertex already exists.
      graph.get_vertex_table(label).ingest(lid, arc);
    } else if (op_type == 1) {
      label_t src_label, dst_label, edge_label;
      Property src, dst;
      vid_t src_lid, dst_lid;
      src_label = deserialize_oid(graph, arc, src);
      dst_label = deserialize_oid(graph, arc, dst);
      arc >> edge_label;

      CHECK(get_vertex_with_retries(graph, src_label, src, src_lid, timestamp));
      CHECK(get_vertex_with_retries(graph, dst_label, dst, dst_lid, timestamp));

      graph.IngestEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                       timestamp, arc, alloc);
    } else {
      LOG(FATAL) << "Unexpected op-" << static_cast<int>(op_type);
    }
  }
}

void InsertTransaction::clear() {
  arc_.Clear();
  arc_.Resize(sizeof(WalHeader));
  added_vertices_.clear();

  timestamp_ = INVALID_TIMESTAMP;
}

const Schema& InsertTransaction::schema() const { return graph_.schema(); }

bool InsertTransaction::get_vertex_with_retries(PropertyGraph& graph,
                                                label_t label,
                                                const Property& oid, vid_t& lid,
                                                timestamp_t timestamp) {
  if (NEUG_LIKELY(graph.get_lid(label, oid, lid, timestamp))) {
    return true;
  }
  for (int i = 0; i < 10; ++i) {
    std::this_thread::sleep_for(std::chrono::microseconds(1000000));
    if (NEUG_LIKELY(graph.get_lid(label, oid, lid, timestamp))) {
      return true;
    }
  }

  LOG(ERROR) << "get_vertex [" << oid.to_string() << "] failed";
  return false;
}

}  // namespace gs
