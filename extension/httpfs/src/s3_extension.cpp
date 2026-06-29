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

#include <cstdio>
#include <cstdlib>
#include <glog/logging.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/vfs/file_system.h"
#include "s3_filesystem.h"
#include "http_filesystem.h"
#include "s3_options.h"

namespace neug {
namespace extension {
namespace httpfs {

// Register S3/OSS filesystem factories in the global VFS registry
static void RegisterS3Provider() {
  auto* vfs = neug::main::MetadataRegistry::getVFS();

  // Register for both s3:// and oss:// schemes
  vfs->Register("s3", neug::extension::s3::CreateS3FileSystem);
  vfs->Register("oss", neug::extension::s3::CreateS3FileSystem);

  LOG(INFO) << "[httpfs extension] S3FileSystem registered for schemes: s3, oss";
}

// Register HTTP/HTTPS filesystem factories in the global VFS registry
static void RegisterHTTPProvider() {
  auto* vfs = neug::main::MetadataRegistry::getVFS();

  // Register for both http:// and https:// schemes
  vfs->Register("http", neug::extension::http::CreateHTTPFileSystem);
  vfs->Register("https", neug::extension::http::CreateHTTPFileSystem);

  LOG(INFO)
      << "[httpfs extension] HTTPFileSystem registered for schemes: http, https";
}

// Finalize Arrow S3 to prevent exit crash (called at process exit)
static void FinalizeS3OnExit() {
  try {
    auto status = arrow::fs::FinalizeS3();
    if (!status.ok()) {
      LOG(WARNING) << "[s3 extension] Failed to finalize Arrow S3: "
                   << status.ToString();
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "[s3 extension] cleanup failed: " << e.what();
  }
}

}  // namespace httpfs
}  // namespace extension
}  // namespace neug

// Extension entry points (extern "C" for dynamic loading)
extern "C" {

/**
 * Init function - called when extension is loaded
 * This is the main entry point for the HTTPFS extension
 */
void Init() {
  try {
    // Initialize Arrow TLS/CA-bundle options eagerly at extension load time.
    // This MUST happen before any Arrow filesystem object is created (even
    // LocalFileSystem), because arrow::fs::Initialize(opts) is a one-shot
    // global configuration call.  If deferred to the first S3FileSystem ctor,
    // a prior Arrow init (e.g. triggered by parquet reading a local file)
    // could race and leave TLS unconfigured, resulting in curlCode 77.
    neug::extension::s3::InitializeArrowTlsOptions();

    // Register S3 filesystem provider in the global registry
    neug::extension::httpfs::RegisterS3Provider();

    // Register HTTP/HTTPS filesystem provider
    neug::extension::httpfs::RegisterHTTPProvider();

    // Register atexit handler to finalize S3 on process exit
    std::atexit(neug::extension::httpfs::FinalizeS3OnExit);

    LOG(INFO) << "[httpfs extension] initialized (s3, oss, http, https)";
  } catch (const std::exception& e) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[httpfs extension] initialization failed: " + std::string(e.what()));
  } catch (...) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[httpfs extension] initialization failed: unknown exception");
  }
}

/**
 * Name function - returns the extension name
 */
const char* Name() {
  return "HTTPFS";
}

}  // extern "C"
