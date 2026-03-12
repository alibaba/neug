
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

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>

#include <charconv>

#include "arrow/util/value_parsing.h"
#include "common/configuration.h"  // odps/include
#include "httplib.h"
#include "neug/storages/loader/abstract_property_fragment_loader.h"
#include "neug/storages/loader/loader_factory.h"
#include "neug/storages/loader/loading_config.h"
#include "storage_api.hpp"
#include "storage_api_arrow.hpp"

using apsara::odps::sdk::AliyunAccount;
using apsara::odps::sdk::Configuration;
using apsara::odps::sdk::storage_api::ReadRowsReq;
using apsara::odps::sdk::storage_api::SessionReq;
using apsara::odps::sdk::storage_api::SessionStatus;
using apsara::odps::sdk::storage_api::SplitOptions;
using apsara::odps::sdk::storage_api::TableBatchScanReq;
using apsara::odps::sdk::storage_api::TableBatchScanResp;
using apsara::odps::sdk::storage_api::TableBatchWriteReq;
using apsara::odps::sdk::storage_api::TableBatchWriteResp;
using apsara::odps::sdk::storage_api::TableIdentifier;
using apsara::odps::sdk::storage_api::WriteRowsReq;
using apsara::odps::sdk::storage_api::arrow_adapter::ArrowClient;
using apsara::odps::sdk::storage_api::arrow_adapter::Reader;

namespace neug {

class ODPSReadClient {
 public:
  static constexpr const int CONNECTION_TIMEOUT = 5;
  static constexpr const int READ_WRITE_TIMEOUT = 10;
  static constexpr const size_t MAX_RETRY = 10;
  ODPSReadClient();

  ~ODPSReadClient();

  void init();

  void CreateReadSession(std::string* session_id, int* split_count,
                         const TableIdentifier& table_identifier,
                         const std::vector<std::string>& selected_cols,
                         const std::vector<std::string>& partition_cols,
                         const std::vector<std::string>& selected_partitions);

  std::shared_ptr<arrow::Table> ReadTable(const std::string& session_id,
                                          int split_count,
                                          const TableIdentifier& table_id,
                                          int thread_num) const;

  std::shared_ptr<ArrowClient> GetArrowClient() const;

 private:
  TableBatchScanResp createReadSession(
      const TableIdentifier& table_identifier,
      const std::vector<std::string>& selected_cols,
      const std::vector<std::string>& partition_cols,
      const std::vector<std::string>& selected_partitions);

  TableBatchScanResp getReadSession(std::string session_id,
                                    const TableIdentifier& table_identifier);

  void getReadSessionStatus(const std::string& session_id, int* split_count,
                            const TableIdentifier& table_identifier);

  void producerRoutine(
      const std::string& session_id, const TableIdentifier& table_identifier,
      std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&
          all_batches_,
      std::vector<int>&& indices) const;

  bool readRows(std::string session_id, const TableIdentifier& table_identifier,
                std::vector<std::shared_ptr<arrow::RecordBatch>>& res_batches,
                int split_index) const;

 private:
  // odps table related
  std::string access_id_;
  std::string access_key_;
  std::string odps_endpoint_;
  std::string tunnel_endpoint_;
  std::string output_directory_;
  std::shared_ptr<ArrowClient> arrow_client_ptr_;
  size_t MAX_PRODUCER_NUM = 8;
};

class ODPSStreamRecordBatchSupplier : public IRecordBatchSupplier {
 public:
  ODPSStreamRecordBatchSupplier(label_t label_id, const std::string& file_path,
                                const ODPSReadClient& odps_table_reader,
                                const std::string& session_id, int split_count,
                                TableIdentifier table_identifier, int worker_id,
                                int worker_num);

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  std::string file_path_;
  const ODPSReadClient& odps_read_client_;
  std::string session_id_;
  int split_count_;

  TableIdentifier table_identifier_;

  int32_t cur_split_index_;
  int worker_num_;
  ReadRowsReq read_rows_req_;
  std::shared_ptr<Reader> cur_batch_reader_;
};

class ODPSTableRecordBatchSupplier : public IRecordBatchSupplier {
 public:
  ODPSTableRecordBatchSupplier(label_t label_id, const std::string& file_path,
                               const ODPSReadClient& odps_table_reader,
                               const std::string& session_id, int split_count,
                               TableIdentifier table_identifier,
                               int thread_num);

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  std::string file_path_;
  const ODPSReadClient& odps_read_client_;
  std::string session_id_;
  TableIdentifier table_identifier_;

  std::shared_ptr<arrow::Table> table_;
  std::shared_ptr<arrow::TableBatchReader> reader_;
};

/*
 * ODPSFragmentLoader is used to load graph data from ODPS Table.
 * It fetch the data via ODPS tunnel/halo API.
 * You need to set the following environment variables:
 * 1. ODPS_ACCESS_ID
 * 2. ODPS_ACCESS_KEY
 * 3. ODPS_ENDPOINT
 * 4. ODPS_TUNNEL_ENDPOINT(optional)
 */
class ODPSFragmentLoader : public AbstractPropertyGraphLoader {
 public:
  ODPSFragmentLoader(const std::string& work_dir, const Schema& schema,
                     const LoadingConfig& loading_config)
      : AbstractPropertyGraphLoader(work_dir, schema, loading_config) {
    odps_read_client_.init();
  }

  static std::shared_ptr<IFragmentLoader> Make(
      const std::string& work_dir, const Schema& schema,
      const LoadingConfig& loading_config);

  ~ODPSFragmentLoader() {}

 protected:
  virtual std::vector<std::shared_ptr<IRecordBatchSupplier>>
  createVertexRecordBatchSupplier(
      label_t v_label, const std::string& v_label_name,
      const std::string& v_file, DataType pk_type,
      const std::shared_ptr<ExtraTypeInfo>& extra_type_info,
      const std::string& pk_name, int pk_ind,
      const LoadingConfig& loading_config, int thread_id) const = 0;
  virtual std::vector<std::shared_ptr<IRecordBatchSupplier>>
  createEdgeRecordBatchSupplier(label_t src_label, label_t dst_label,
                                label_t e_label, const std::string& e_file,
                                const LoadingConfig& loading_config,
                                int thread_id) const = 0;

  result<bool> LoadFragment() override;

 private:
  ODPSReadClient odps_read_client_;
  static const bool registered_;
};

}  // namespace neug
