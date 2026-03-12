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

#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/loader/i_fragment_loader.h"
#include "neug/storages/loader/loading_config.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {
class AbstractPropertyGraphLoader : public IFragmentLoader {
 public:
  AbstractPropertyGraphLoader(const std::string& work_dir, const Schema& schema,
                              const LoadingConfig& loading_config)
      : work_dir_(work_dir),
        schema_(schema),
        loading_config_(loading_config),
        thread_num_(loading_config_.GetParallelism()) {
    graph_.Open(schema_, work_dir_, 0);
  }

  virtual ~AbstractPropertyGraphLoader() = default;

  result<bool> LoadFragment() override;

 protected:
  virtual std::vector<std::shared_ptr<IRecordBatchSupplier>>
  createVertexRecordBatchSupplier(label_t v_label,
                                  const std::string& v_label_name,
                                  const std::string& v_file, DataType pk_type,
                                  const std::string& pk_name, int pk_ind,
                                  const LoadingConfig& loading_config,
                                  int thread_id) const = 0;
  virtual std::vector<std::shared_ptr<IRecordBatchSupplier>>
  createEdgeRecordBatchSupplier(label_t src_label, label_t dst_label,
                                label_t e_label, const std::string& e_file,
                                const LoadingConfig& loading_config,
                                int thread_id) const = 0;

 private:
  void loadVertices();
  void addVertices(label_t v_label_id, const std::vector<std::string>& v_files);
  void addVerticesToVertexTable(label_t v_label_id,
                                const std::string& label_name, DataType pk_type,
                                const std::string& pk_name, int pk_ind,
                                const std::vector<std::string>& v_files);

  void loadEdges();
  void addEdges(label_t src_label_id, label_t dst_label_id, label_t e_label_id,
                const std::vector<std::string>& e_files);
  void addEdgesToEdgeTable(label_t src_label_id, label_t dst_label_id,
                           label_t e_label_id,
                           const std::vector<std::string>& e_files);

 protected:
  std::string work_dir_;
  const Schema& schema_;
  const LoadingConfig& loading_config_;
  int thread_num_ = 1;

  PropertyGraph graph_;
};

}  // namespace neug
