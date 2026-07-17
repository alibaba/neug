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

#include <glog/logging.h>
#include <stddef.h>

#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "neug/common/types/data_chunk.h"
#include "neug/common/types/i_context_column.h"
#include "neug/storages/loader/loading_config.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/csv/csv_read_config.h"
#include "neug/utils/string_utils.h"

namespace neug {

class ColumnBase;

struct CsvSupplierRuntime;

void printDiskRemaining(const std::string& path);

void put_boolean_option(CsvReadConfig& config);

void put_delimiter_option(const std::string& delimiter_str,
                          CsvReadConfig& config);

std::string process_header_row_token(const std::string& token, bool is_quoting,
                                     char quote_char, bool is_escaping,
                                     char escape_char);

std::vector<std::string> read_header(const std::string& file_name,
                                     const CsvReadConfig& config);

std::vector<std::string> columnMappingsToSelectedCols(
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings);

void put_column_names_option(bool header_row, const std::string& file_path,
                             CsvReadConfig& config, size_t len);

void set_column_types_on_config(const std::vector<DataType>& column_types,
                                const std::vector<std::string>& column_names,
                                CsvReadConfig& config);

/// Build a CsvReadConfig from batch-import style CSV option strings.
CsvReadConfig build_csv_read_config(
    const std::string& file_path,
    const std::unordered_map<std::string, std::string>& csv_options,
    const std::vector<DataType>& column_types);

class IDataChunkSource;

class IDataChunkSupplier {
 public:
  virtual ~IDataChunkSupplier() = default;
  virtual std::shared_ptr<DataChunk> GetNextChunk() = 0;
  virtual int64_t RowNum() const = 0;

  /// Whether GetNextChunk() may be called concurrently by several consumers.
  virtual bool SupportsConcurrentGetNext() const { return false; }

  /// Stops any background producers and wakes blocked GetNextChunk() calls.
  virtual void Cancel() {}

  /// Returns the repeatable source backing this supplier, when available.
  /// Storage uses this hint to select staged bulk build internally.
  virtual std::shared_ptr<IDataChunkSource> RepeatableSource() const {
    return nullptr;
  }
};

struct ChunkSourceOptions {
  /// Zero selects the source's serial supplier. Positive values request that
  /// many background parsing workers when input order need not be preserved.
  int32_t producer_count = 0;
  int32_t consumer_count = 1;
  size_t queue_capacity = 2;
  bool preserve_order = true;

  /// Zero-based columns in the source's logical output to materialize. An
  /// empty list means all columns. Sources must preserve the requested order.
  std::vector<int32_t> projected_columns;
};

/// A repeatable source of data chunks.
///
/// Keeping source setup separate from the supplier cursor lets storage consume
/// the input directly without making execution::Context stateful or lazy.
class IDataChunkSource {
 public:
  virtual ~IDataChunkSource() = default;

  /// Opens a new supplier positioned at the beginning of the source using the
  /// requested projection and concurrency settings.
  virtual std::shared_ptr<IDataChunkSupplier> Open(
      const ChunkSourceOptions& options = {}) const = 0;

  /// Returns the source size when cheaply known, otherwise -1.
  virtual int64_t EstimatedBytes() const { return -1; }

  /// Whether the source options permit parallel parsing and consumption.
  virtual bool ParallelEnabled() const { return true; }
};

std::shared_ptr<IDataChunkSupplier> make_data_chunk_supplier(
    std::shared_ptr<IDataChunkSource> source);

inline constexpr int64_t kUnknownRowNum = -1;

enum class CsvRowCountMode {
  kCountOnOpen,
  kUnknown,
};

/// csv-parser based supplier. Reads CSV in chunks and yields ValueColumns.
class CSVChunkSupplier : public IDataChunkSupplier {
 public:
  CSVChunkSupplier(
      const std::string& file_path, CsvReadConfig config,
      CsvRowCountMode row_count_mode = CsvRowCountMode::kCountOnOpen);

  ~CSVChunkSupplier() override;

  std::shared_ptr<DataChunk> GetNextChunk() override;

  int64_t RowNum() const override;

 private:
  std::string file_path_;
  std::unique_ptr<CsvSupplierRuntime> runtime_;
};

struct CsvPartitionPlanCache;

/// Reopens the public CSV parser for each pass. Parallel suppliers share a
/// cached record-aligned partition plan so repeated passes do not repeat the
/// raw row-count scan.
class CSVChunkSource final : public IDataChunkSource {
 public:
  CSVChunkSource(std::vector<std::string> file_paths, CsvReadConfig config,
                 std::vector<int32_t> projected_columns = {});

  std::shared_ptr<IDataChunkSupplier> Open(
      const ChunkSourceOptions& options = {}) const override;
  int64_t EstimatedBytes() const override;
  bool ParallelEnabled() const override { return config_.use_threads; }

 private:
  std::vector<std::string> file_paths_;
  CsvReadConfig config_;
  std::vector<int32_t> projected_columns_;
  // Source-local cache: file paths and partition-relevant config stay fixed
  // across Open() calls; producer count selects the cached plan.
  std::shared_ptr<CsvPartitionPlanCache> partition_plan_cache_;
};

void fillVertexReaderMeta(label_t v_label, const std::string& v_label_name,
                          const std::string& v_file,
                          const LoadingConfig& loading_config,
                          const std::vector<std::string>& vertex_property_names,
                          const std::vector<DataTypeId>& vertex_property_types,
                          DataTypeId pk_type, const std::string& pk_name,
                          size_t pk_ind, CsvReadConfig& config);

void fillEdgeReaderMeta(label_t src_label_id, label_t dst_label_id,
                        label_t label_id, const std::string& edge_label_name,
                        const std::string& e_file,
                        const LoadingConfig& loading_config,
                        const std::vector<std::string>& edge_property_names,
                        const std::vector<DataTypeId>& edge_property_types,
                        DataTypeId src_pk_type, DataTypeId dst_pk_type,
                        CsvReadConfig& config);

void set_properties_from_context_column(
    ColumnBase* col, const std::shared_ptr<IContextColumn>& ctx_col,
    const std::vector<vid_t>& vids);

}  // namespace neug
