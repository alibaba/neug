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
#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_sys/file_system.h"
#include "s3_filesystem.h"
#include "http_filesystem.h"

namespace neug {
namespace extension {
namespace s3 {

// Register S3/OSS filesystem factories in the global VFS registry
static void RegisterS3Provider() {
  auto* vfs = neug::main::MetadataRegistry::getVFS();

  // Register for both s3:// and oss:// schemes
  vfs->Register("s3", neug::extension::s3::CreateS3FileSystem);
  vfs->Register("oss", neug::extension::s3::CreateS3FileSystem);

  LOG(INFO) << "[s3 extension] S3FileSystem registered for schemes: s3, oss";
}

// Register HTTP/HTTPS filesystem factories in the global VFS registry
static void RegisterHTTPProvider() {
  auto* vfs = neug::main::MetadataRegistry::getVFS();

  // Register for both http:// and https:// schemes
  vfs->Register("http", neug::extension::http::CreateHTTPFileSystem);
  vfs->Register("https", neug::extension::http::CreateHTTPFileSystem);

  LOG(INFO)
      << "[s3 extension] HTTPFileSystem registered for schemes: http, https";
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
  try {
    // Register S3 filesystem provider in the global registry
    neug::extension::s3::RegisterS3Provider();

    // Register HTTP/HTTPS filesystem provider
    neug::extension::s3::RegisterHTTPProvider();

    LOG(INFO) << "[s3 extension] initialized (s3, oss, http, https)";
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
