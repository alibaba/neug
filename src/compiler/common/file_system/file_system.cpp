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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/common/file_system/file_system.h"

#include "neug/compiler/common/string_utils.h"

namespace neug {
namespace common {

void FileSystem::overwriteFile(const std::string& /*from*/,
                               const std::string& /*to*/) {
  NEUG_UNREACHABLE;
}

void FileSystem::copyFile(const std::string& /*from*/,
                          const std::string& /*to*/) {
  NEUG_UNREACHABLE;
}

void FileSystem::createDir(const std::string& /*dir*/) const {
  NEUG_UNREACHABLE;
}

void FileSystem::removeFileIfExists(const std::string& /*path*/) {
  NEUG_UNREACHABLE;
}

bool FileSystem::fileOrPathExists(const std::string& /*path*/,
                                  main::ClientContext* /*context*/) {
  NEUG_UNREACHABLE;
}

std::string FileSystem::expandPath(main::ClientContext* /*context*/,
                                   const std::string& path) const {
  return path;
}

std::string FileSystem::joinPath(const std::string& base,
                                 const std::string& part) {
  return base + "/" + part;
}

std::string FileSystem::getFileExtension(const std::filesystem::path& path) {
  auto extension = path.extension();
  if (isCompressedFile(path)) {
    extension = path.stem().extension();
  }
  return extension.string();
}

bool FileSystem::isCompressedFile(const std::filesystem::path& path) {
  return isGZIPCompressed(path);
}

std::string FileSystem::getFileName(const std::filesystem::path& path) {
  return path.filename().string();
}

void FileSystem::writeFile(FileInfo& /*fileInfo*/, const uint8_t* /*buffer*/,
                           uint64_t /*numBytes*/, uint64_t /*offset*/) const {
  NEUG_UNREACHABLE;
}

void FileSystem::truncate(FileInfo& /*fileInfo*/, uint64_t /*size*/) const {
  NEUG_UNREACHABLE;
}

void FileSystem::reset(neug::common::FileInfo& fileInfo) {
  fileInfo.seek(0, SEEK_SET);
}

bool FileSystem::isGZIPCompressed(const std::filesystem::path& path) {
  return StringUtils::getLower(path.extension().string()) == ".gz";
}

}  // namespace common
}  // namespace neug
