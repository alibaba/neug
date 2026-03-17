/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include "parquet_options.h"
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <parquet/properties.h>
#include <parquet/arrow/reader.h>
#include <glog/logging.h>
#include <memory>
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"

namespace neug {
namespace reader {

ArrowOptions ArrowParquetOptionsBuilder::build() const {
  if (!state) {
    THROW_INVALID_ARGUMENT_EXCEPTION("State is null");
  }

  auto scanOptions = std::make_shared<arrow::dataset::ScanOptions>();

  // Build format-specific fragment scan options
  auto fragment_scan_options = buildFragmentOptions();
  scanOptions->fragment_scan_options = fragment_scan_options;
  if (!state->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }
  scanOptions->dataset_schema = createSchema(*state->schema.entry);

  // Build file format using scan options
  auto fileFormat = buildFileFormat(*scanOptions);

  // Create ArrowOptions with both scanOptions and fileFormat
  ArrowOptions arrowOptions;
  arrowOptions.scanOptions = scanOptions;
  arrowOptions.fileFormat = fileFormat;
  return arrowOptions;
}

std::shared_ptr<arrow::dataset::FragmentScanOptions>
ArrowParquetOptionsBuilder::buildFragmentOptions() const {
  if (!state) {
    THROW_INVALID_ARGUMENT_EXCEPTION("State is null");
  }
  if (!state->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }

  auto fragment_scan_options =
      std::make_shared<arrow::dataset::ParquetFragmentScanOptions>();

  const FileSchema& fileSchema = state->schema.file;
  auto& options = fileSchema.options;
  ParquetParseOptions parquetOpts;
  ReadOptions readOpts;

  // Configure Parquet-specific reader properties
  auto reader_properties = std::make_shared<parquet::ReaderProperties>();
  
  // Enable buffered stream if configured
  if (parquetOpts.buffered_stream.get(options)) {
    reader_properties->enable_buffered_stream();
  }
  
  // Set I/O buffer size in bytes
  int64_t buffer_size = readOpts.batch_size.get(options);
  reader_properties->set_buffer_size(buffer_size);
  
  fragment_scan_options->reader_properties = reader_properties;
  
  // Configure Arrow-specific reader properties
  auto arrow_reader_properties = std::make_shared<parquet::ArrowReaderProperties>();
  
  // Set Arrow row batch size (number of rows per batch)
  int64_t row_batch_size = parquetOpts.row_batch_size.get(options);
  arrow_reader_properties->set_batch_size(row_batch_size);
  
  // Use threads setting from general read options
  arrow_reader_properties->set_use_threads(readOpts.use_threads.get(options));
  
  // Configure pre-buffering for high-latency filesystems
  arrow_reader_properties->set_pre_buffer(parquetOpts.pre_buffer.get(options));
  
  // Configure caching via Arrow I/O coalescing (hole-filling cache).
  // When enable_io_coalescing=true (default), use lazy coalescing which only
  // loads explicitly-requested byte ranges (CacheOptions::LazyDefaults).
  // When false, use eager coalescing which pre-fetches data more aggressively
  // (CacheOptions::Defaults).
  if (parquetOpts.enable_io_coalescing.get(options)) {
    arrow_reader_properties->set_cache_options(
        arrow::io::CacheOptions::LazyDefaults());
  } else {
    arrow_reader_properties->set_cache_options(
        arrow::io::CacheOptions::Defaults());
  }
  
  fragment_scan_options->arrow_reader_properties = arrow_reader_properties;

  return fragment_scan_options;
}

std::shared_ptr<arrow::dataset::FileFormat>
ArrowParquetOptionsBuilder::buildFileFormat(
    const arrow::dataset::ScanOptions& options) const {
  auto fileFormat = std::make_shared<arrow::dataset::ParquetFileFormat>();
  auto fragmentOpts = options.fragment_scan_options;
  if (!fragmentOpts) {
    LOG(WARNING)
        << "fragment_scan_options is null in ScanOptions, Parquet reader "
           "will use default configuration";
    return fileFormat;
  }

  auto parquetFragmentOpts =
      std::dynamic_pointer_cast<arrow::dataset::ParquetFragmentScanOptions>(
          fragmentOpts);
  if (!parquetFragmentOpts) {
    LOG(WARNING) << "fragment_scan_options is not ParquetFragmentScanOptions, "
                    "reader will use default configuration";
    return fileFormat;
  }

  fileFormat->default_fragment_scan_options = options.fragment_scan_options;
  return fileFormat;
}

}  // namespace reader
}  // namespace neug
