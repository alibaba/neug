
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_MUTABLE_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_MUTABLE_FRAGMENT_LOADER_H_

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>
#include <variant>
#include <vector>

#include "neug/storages/rt_mutable_graph/loader/i_fragment_loader.h"
#include "neug/storages/rt_mutable_graph/loading_config.h"
#include "neug/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "neug/storages/rt_mutable_graph/schema.h"
#include "neug/storages/rt_mutable_graph/types.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace gs {
class IRecordBatchSupplier;

// LoadFragment for csv files.
class MutableFragmentLoader : public IFragmentLoader {
 public:
  MutableFragmentLoader(const std::string& work_dir, const Schema& schema,
                        const LoadingConfig& loading_config);

  static std::shared_ptr<IFragmentLoader> Make(
      const std::string& work_dir, const Schema& schema,
      const LoadingConfig& loading_config);

  ~MutableFragmentLoader() {}

  Result<bool> LoadFragment() override;

 private:
  void createVertices();

  void createEdges();

  void loadVertices();

  void loadEdges();

  template <typename EDATA_T>
  void addEdges(label_t src_label, label_t dst_label, label_t edge_label,
                std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers) {
    if constexpr (std::is_same_v<EDATA_T, RecordView>) {
      addEdges<EDATA_T, std::vector<std::tuple<vid_t, vid_t, size_t>>>(
          src_label, dst_label, edge_label, suppliers);
    } else {
      addEdges<EDATA_T, std::vector<std::tuple<vid_t, vid_t, EDATA_T>>>(
          src_label, dst_label, edge_label, suppliers);
    }
  }

  template <typename EDATA_T, typename VECTOR_T>
  void addEdges(label_t src_label, label_t dst_label, label_t edge_label,
                std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers) {
    auto src_pk_type =
        std::get<0>(schema_.get_vertex_primary_key(src_label)[0]);
    if (src_pk_type == PropertyType::kInt32) {
      addEdges<int32_t, EDATA_T, VECTOR_T>(src_label, dst_label, edge_label,
                                           suppliers);
    } else if (src_pk_type == PropertyType::kInt64) {
      addEdges<int64_t, EDATA_T, VECTOR_T>(src_label, dst_label, edge_label,
                                           suppliers);
    } else if (src_pk_type == PropertyType::kUInt32) {
      addEdges<uint32_t, EDATA_T, VECTOR_T>(src_label, dst_label, edge_label,
                                            suppliers);
    } else if (src_pk_type == PropertyType::kUInt64) {
      addEdges<uint64_t, EDATA_T, VECTOR_T>(src_label, dst_label, edge_label,
                                            suppliers);
    } else if (src_pk_type.type_enum == impl::PropertyTypeImpl::kVarChar ||
               src_pk_type.type_enum == impl::PropertyTypeImpl::kStringView) {
      addEdges<std::string_view, EDATA_T, VECTOR_T>(src_label, dst_label,
                                                    edge_label, suppliers);
    } else {
      LOG(FATAL) << "Unsupported primary type";
    }
  }

  template <typename SRC_PK_T, typename EDATA_T, typename VECTOR_T>
  void addEdges(label_t src_label, label_t dst_label, label_t edge_label,
                std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers) {
    auto dst_pk_type =
        std::get<0>(schema_.get_vertex_primary_key(dst_label)[0]);
    if (dst_pk_type == PropertyType::kInt32) {
      graph_.batch_load_edges<SRC_PK_T, int32_t, EDATA_T, VECTOR_T>(
          src_label, dst_label, edge_label, suppliers);
    } else if (dst_pk_type == PropertyType::kInt64) {
      graph_.batch_load_edges<SRC_PK_T, int64_t, EDATA_T, VECTOR_T>(
          src_label, dst_label, edge_label, suppliers);
    } else if (dst_pk_type == PropertyType::kUInt32) {
      graph_.batch_load_edges<SRC_PK_T, uint32_t, EDATA_T, VECTOR_T>(
          src_label, dst_label, edge_label, suppliers);
    } else if (dst_pk_type == PropertyType::kUInt64) {
      graph_.batch_load_edges<SRC_PK_T, uint64_t, EDATA_T, VECTOR_T>(
          src_label, dst_label, edge_label, suppliers);
    } else if (dst_pk_type.type_enum == impl::PropertyTypeImpl::kVarChar ||
               dst_pk_type.type_enum == impl::PropertyTypeImpl::kStringView) {
      graph_.batch_load_edges<SRC_PK_T, std::string_view, EDATA_T, VECTOR_T>(
          src_label, dst_label, edge_label, suppliers);
    } else {
      LOG(FATAL) << "Unsupported primary type";
    }
  }

  MutablePropertyFragment graph_;
  Schema schema_;
  LoadingConfig loading_config_;

  static const bool registered_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_MUTABLE_FRAGMENT_LOADER_H_