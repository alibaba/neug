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

#include "neug/storages/loader/abstract_property_graph_loader.h"
#include "neug/storages/loader/i_fragment_loader.h"

namespace neug {

class CSVPropertyGraphLoader : public AbstractPropertyGraphLoader {
 public:
  CSVPropertyGraphLoader(const std::string& work_dir, const Schema& schema,
                         const LoadingConfig& loading_config)
      : AbstractPropertyGraphLoader(work_dir, schema, loading_config) {}

  static std::shared_ptr<IFragmentLoader> Make(
      const std::string& work_dir, const Schema& schema,
      const LoadingConfig& loading_config);

 protected:
  std::vector<std::shared_ptr<IRecordBatchSupplier>>
  createVertexRecordBatchSupplier(label_t v_label,
                                  const std::string& v_label_name,
                                  const std::string& v_file, DataType pk_type,
                                  const std::string& pk_name, int pk_ind,
                                  const LoadingConfig& loading_config,
                                  int thread_id) const override;

  std::vector<std::shared_ptr<IRecordBatchSupplier>>
  createEdgeRecordBatchSupplier(label_t src_label, label_t dst_label,
                                label_t e_label, const std::string& e_file,
                                const LoadingConfig& loading_config,
                                int thread_id) const override;
  static const bool registered_;
};

}  // namespace neug
