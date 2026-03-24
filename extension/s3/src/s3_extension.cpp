/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <glog/logging.h>
#include <arrow/filesystem/s3fs.h>
#include <cstdlib>
#include "neug/compiler/function/filesystem_registry.h"
#include "neug/utils/exception/exception.h"
#include "s3_filesystem.h"
#include "http_filesystem.h"

namespace neug {
namespace extension {
namespace s3 {

// Statically initialize S3 provider when extension loads
// This follows the DuckDB extension pattern
static void RegisterS3Provider() {
  auto registry = function::FileSystemProviderRegistry::getInstance();
  auto provider = std::make_shared<S3FileSystemProvider>();
  
  // Register for both s3:// and oss:// schemes
  registry->registerProvider("s3", provider);
  registry->registerProvider("oss", provider);
  
  LOG(INFO) << "[s3 extension] S3FileSystemProvider registered for schemes: s3, oss";
}

// Register HTTP/HTTPS provider
static void RegisterHTTPProvider() {
  auto registry = function::FileSystemProviderRegistry::getInstance();
  auto provider = std::make_shared<http::HTTPFileSystemProvider>();
  
  // Register for both http:// and https:// schemes
  registry->registerProvider("http", provider);
  registry->registerProvider("https", provider);
  
  LOG(INFO) << "[s3 extension] HTTPFileSystemProvider registered for schemes: http, https";
}

// Cleanup function to finalize Arrow S3 (called automatically)
static void CleanupS3() {
  LOG(INFO) << "[s3 extension] automatic cleanup triggered";
  
  try {
    auto status = arrow::fs::FinalizeS3();
    if (!status.ok()) {
      LOG(WARNING) << "[s3 extension] Failed to finalize Arrow S3: " 
                   << status.ToString();
    } else {
      LOG(INFO) << "[s3 extension] Arrow S3 finalized successfully";
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "[s3 extension] cleanup failed: " << e.what();
  } catch (...) {
    LOG(ERROR) << "[s3 extension] cleanup failed: unknown exception";
  }
}

}  // namespace s3
}  // namespace extension
}  // namespace neug

// Extension entry points (extern "C" for dynamic loading)
extern "C" {

/**
 * Init function - called when extension is loaded
 * This is the main entry point for the S3 extension
 */
void Init() {
  LOG(INFO) << "[s3 extension] init called";
  
  try {
    // Register S3 filesystem provider in the global registry
    neug::extension::s3::RegisterS3Provider();
    
    // Register HTTP/HTTPS filesystem provider
    neug::extension::s3::RegisterHTTPProvider();
    
    // Register cleanup function to be called at program exit
    // This ensures Arrow S3 is properly finalized to prevent core dumps
    std::atexit(neug::extension::s3::CleanupS3);
    
    LOG(INFO) << "[s3 extension] initialization completed successfully";
    LOG(INFO) << "[s3 extension] S3, OSS, HTTP, and HTTPS filesystem support is now available";
    LOG(INFO) << "[s3 extension] Usage:";
    LOG(INFO) << "[s3 extension]   - S3: JSON_SCAN('s3://bucket/file.json')";
    LOG(INFO) << "[s3 extension]   - OSS: PARQUET_SCAN('oss://bucket/file.parquet')";
    LOG(INFO) << "[s3 extension]   - HTTP: CSV_SCAN('https://example.com/data.csv')";
  } catch (const std::exception& e) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[s3 extension] initialization failed: " + std::string(e.what()));
  } catch (...) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[s3 extension] initialization failed: unknown exception");
  }
}

/**
 * Name function - returns the extension name
 */
const char* Name() {
  return "S3";
}

}  // extern "C"
