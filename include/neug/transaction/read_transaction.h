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

#ifndef INCLUDE_NEUG_TRANSACTION_READ_TRANSACTION_H_
#define INCLUDE_NEUG_TRANSACTION_READ_TRANSACTION_H_

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "neug/storages/csr/immutable_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/csr/nbr.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace gs {

class PropertyGraph;
class IVersionManager;
template <typename EDATA_T>
class TypedMutableCsrBase;

/**
 * @brief Read-only transaction for consistent snapshot access to graph data.
 *
 * ReadTransaction provides read access to graph data at a specific timestamp,
 * implementing snapshot isolation. It stores references to the session, graph,
 * version manager, and the snapshot timestamp.
 *
 * **Implementation Details:**
 * - Stores const reference to PropertyGraph for read-only access
 * - Maintains timestamp for consistent snapshot view
 * - Calls release() in destructor for cleanup
 * - Commit() simply calls release() and returns true
 *
 * **Thread Safety:** Read operations are safe for concurrent access.
 *
 * @since v0.1.0
 */
class ReadTransaction {
 public:
  /**
   * @brief Construct a ReadTransaction.
   *
   * @param session Reference to the database session
   * @param graph Const reference to the property graph
   * @param vm Reference to version manager
   * @param timestamp Snapshot timestamp for this transaction
   *
   * Implementation: Stores all parameters as member references/values.
   *
   * @since v0.1.0
   */
  ReadTransaction(const PropertyGraph& graph, IVersionManager& vm,
                  timestamp_t timestamp);

  /**
   * @brief Destructor that calls release().
   *
   * Implementation: Calls release() to clean up resources.
   *
   * @since v0.1.0
   */
  ~ReadTransaction();

  timestamp_t timestamp() const;

  bool Commit();

  void Abort();

  const PropertyGraph& graph() const;

  /*
   * @brief Get the handle of the vertex property column, only for non-primary
   * key columns.
   */
  const std::shared_ptr<ColumnBase> GetVertexPropertyColumn(
      uint8_t label, const std::string& col_name) const {
    return graph_.get_vertex_table(label).get_properties_table().get_column(
        col_name);
  }

  /**
   * @brief Get the handle of the vertex property column, including the primary
   * key.
   * @tparam T The type of the column.
   * @param label The label of the vertex.
   * @param col_name The name of the column.
   */
  template <typename T>
  std::shared_ptr<TypedRefColumn<T>> get_vertex_ref_property_column(
      uint8_t label, const std::string& col_name) const {
    if (label >= graph_.schema().vertex_label_num()) {
      LOG(WARNING) << "Invalid label: " << (int32_t) label;
      return nullptr;
    }
    auto pk = graph_.schema().get_vertex_primary_key(label);
    CHECK(pk.size() == 1) << "Only support single primary key";
    if (col_name == std::get<1>(pk[0])) {
      return std::dynamic_pointer_cast<TypedRefColumn<T>>(
          graph_.GetVertexIdColumn(label));
    } else {
      auto ptr =
          graph_.get_vertex_table(label).get_properties_table().get_column(
              col_name);
      if (ptr) {
        return std::dynamic_pointer_cast<TypedRefColumn<T>>(
            CreateRefColumn(ptr));
      } else {
        return nullptr;
      }
    }
  }

  bool GetVertexIndex(label_t label, const Property& id, vid_t& index) const;

  vid_t GetVertexNum(label_t label) const;

  VertexSet GetVertexSet(label_t label) const;

  bool IsValidVertex(label_t label, vid_t index) const;

  Property GetVertexId(label_t label, vid_t index) const;

  GenericView GetGenericOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return graph_.GetGenericOutgoingGraphView(v_label, neighbor_label,
                                              edge_label, timestamp_);
  }

  GenericView GetGenericIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return graph_.GetGenericIncomingGraphView(v_label, neighbor_label,
                                              edge_label, timestamp_);
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return graph_.GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                      prop_id);
  }

  size_t GetOutDegree(label_t label, vid_t u, label_t neighbor_label,
                      label_t edge_label) const;

  size_t GetInDegree(label_t label, vid_t u, label_t neighbor_label,
                     label_t edge_label) const;

  inline const Schema& schema() const { return graph_.schema(); }

 private:
  void release();
  const PropertyGraph& graph_;
  IVersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace gs

#endif  // INCLUDE_NEUG_TRANSACTION_READ_TRANSACTION_H_
