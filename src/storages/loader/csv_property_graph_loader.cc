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

#include "neug/storages/loader/csv_property_graph_loader.h"
#include "neug/storages/loader/loader_factory.h"
#include "neug/storages/loader/loader_utils.h"

namespace neug {

std::shared_ptr<IFragmentLoader> CSVPropertyGraphLoader::Make(
    const std::string& work_dir, const Schema& schema,
    const LoadingConfig& loading_config) {
  return std::shared_ptr<IFragmentLoader>(
      new CSVPropertyGraphLoader(work_dir, schema, loading_config));
}

std::vector<std::shared_ptr<IRecordBatchSupplier>>
CSVPropertyGraphLoader::createVertexRecordBatchSupplier(
    label_t v_label, const std::string& v_label_name, const std::string& v_file,
    DataType pk_type, const std::string& pk_name, int pk_ind,
    const LoadingConfig& loading_config, int thread_id) const {
  auto vertex_property_names = schema_.get_vertex_property_names(v_label);
  auto vertex_property_types = schema_.get_vertex_properties_id(v_label);

  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;
  fillVertexReaderMeta(v_label, v_label_name, v_file, loading_config,
                       vertex_property_names, vertex_property_types,
                       pk_type.id(), pk_name, pk_ind, read_options,
                       parse_options, convert_options);
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  if (loading_config.GetIsBatchReader()) {
    auto res = std::make_shared<CSVStreamRecordBatchSupplier>(
        v_file, convert_options, read_options, parse_options);
    suppliers.emplace_back(
        std::dynamic_pointer_cast<IRecordBatchSupplier>(res));
  } else {
    auto res = std::make_shared<CSVTableRecordBatchSupplier>(
        v_file, convert_options, read_options, parse_options);
    suppliers.emplace_back(
        std::dynamic_pointer_cast<IRecordBatchSupplier>(res));
  }
  return suppliers;
}

std::vector<std::shared_ptr<IRecordBatchSupplier>>
CSVPropertyGraphLoader::createEdgeRecordBatchSupplier(
    label_t src_label_id, label_t dst_label_id, label_t e_label_id,
    const std::string& e_file, const LoadingConfig& loading_config,
    int thread_id) const {
  auto edge_property_names =
      schema_.get_edge_property_names(src_label_id, dst_label_id, e_label_id);
  auto edge_property_types =
      schema_.get_edge_properties_id(src_label_id, dst_label_id, e_label_id);
  auto src_pk_type =
      std::get<0>(schema_.get_vertex_primary_key(src_label_id)[0]);
  auto dst_pk_type =
      std::get<0>(schema_.get_vertex_primary_key(dst_label_id)[0]);
  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;
  fillEdgeReaderMeta(src_label_id, dst_label_id, e_label_id,
                     schema_.get_edge_label_name(e_label_id), e_file,
                     loading_config_, edge_property_names, edge_property_types,
                     src_pk_type.id(), dst_pk_type.id(), read_options,
                     parse_options, convert_options);
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  if (loading_config.GetIsBatchReader()) {
    auto res = std::make_shared<CSVStreamRecordBatchSupplier>(
        e_file, convert_options, read_options, parse_options);
    suppliers.emplace_back(
        std::dynamic_pointer_cast<IRecordBatchSupplier>(res));
  } else {
    auto res = std::make_shared<CSVTableRecordBatchSupplier>(
        e_file, convert_options, read_options, parse_options);
    suppliers.emplace_back(
        std::dynamic_pointer_cast<IRecordBatchSupplier>(res));
  }
  return suppliers;
}

const bool CSVPropertyGraphLoader::registered_ =
    LoaderFactory::Register("file", "csv",
                            static_cast<LoaderFactory::loader_initializer_t>(
                                &CSVPropertyGraphLoader::Make));

}  // namespace neug