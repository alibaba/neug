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

#include "neug/compiler/common/file_system/virtual_file_system.h"

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/file_system/local_file_system.h"
#include "neug/compiler/main/client_context.h"

namespace neug {
namespace common {

VirtualFileSystem::VirtualFileSystem() : VirtualFileSystem{""} {}

VirtualFileSystem::VirtualFileSystem(std::string homeDir) {
  defaultFS = std::make_unique<LocalFileSystem>(homeDir);
}

VirtualFileSystem::~VirtualFileSystem() = default;

void VirtualFileSystem::registerFileSystem(
    std::unique_ptr<FileSystem> fileSystem) {
  subSystems.push_back(std::move(fileSystem));
}

FileCompressionType VirtualFileSystem::autoDetectCompressionType(
    const std::string& path) const {
  if (isGZIPCompressed(path)) {
    return FileCompressionType::GZIP;
  }
  return FileCompressionType::UNCOMPRESSED;
}

std::unique_ptr<FileInfo> VirtualFileSystem::openFile(
    const std::string& path, FileOpenFlags flags,
    main::ClientContext* context) {
  auto compressionType = flags.compressionType;
  if (compressionType == FileCompressionType::AUTO_DETECT) {
    compressionType = autoDetectCompressionType(path);
  }
  auto fileHandle = findFileSystem(path)->openFile(path, flags, context);
  if (compressionType == FileCompressionType::UNCOMPRESSED) {
    return fileHandle;
  }
  return compressedFileSystem.at(compressionType)
      ->openCompressedFile(std::move(fileHandle));
}

std::vector<std::string> VirtualFileSystem::glob(
    main::ClientContext* context, const std::string& path) const {
  return findFileSystem(path)->glob(context, path);
}

void VirtualFileSystem::overwriteFile(const std::string& from,
                                      const std::string& to) {
  findFileSystem(from)->overwriteFile(from, to);
}

void VirtualFileSystem::createDir(const std::string& dir) const {
  findFileSystem(dir)->createDir(dir);
}

void VirtualFileSystem::removeFileIfExists(const std::string& path) {
  findFileSystem(path)->removeFileIfExists(path);
}

bool VirtualFileSystem::fileOrPathExists(const std::string& path,
                                         main::ClientContext* context) {
  return findFileSystem(path)->fileOrPathExists(path, context);
}

std::string VirtualFileSystem::expandPath(main::ClientContext* context,
                                          const std::string& path) const {
  return findFileSystem(path)->expandPath(context, path);
}

void VirtualFileSystem::readFromFile(FileInfo& /*fileInfo*/, void* /*buffer*/,
                                     uint64_t /*numBytes*/,
                                     uint64_t /*position*/) const {
  NEUG_UNREACHABLE;
}

int64_t VirtualFileSystem::readFile(FileInfo& /*fileInfo*/, void* /*buf*/,
                                    size_t /*nbyte*/) const {
  NEUG_UNREACHABLE;
}

void VirtualFileSystem::writeFile(FileInfo& /*fileInfo*/,
                                  const uint8_t* /*buffer*/,
                                  uint64_t /*numBytes*/,
                                  uint64_t /*offset*/) const {
  NEUG_UNREACHABLE;
}

void VirtualFileSystem::syncFile(const FileInfo& fileInfo) const {
  findFileSystem(fileInfo.path)->syncFile(fileInfo);
}

void VirtualFileSystem::cleanUP(main::ClientContext* context) {
  for (auto& subSystem : subSystems) {
    subSystem->cleanUP(context);
  }
  defaultFS->cleanUP(context);
}

int64_t VirtualFileSystem::seek(FileInfo& /*fileInfo*/, uint64_t /*offset*/,
                                int /*whence*/) const {
  NEUG_UNREACHABLE;
}

void VirtualFileSystem::truncate(FileInfo& /*fileInfo*/,
                                 uint64_t /*size*/) const {
  NEUG_UNREACHABLE;
}

uint64_t VirtualFileSystem::getFileSize(const FileInfo& /*fileInfo*/) const {
  NEUG_UNREACHABLE;
}

FileSystem* VirtualFileSystem::findFileSystem(const std::string& path) const {
  for (auto& subSystem : subSystems) {
    if (subSystem->canHandleFile(path)) {
      return subSystem.get();
    }
  }
  return defaultFS.get();
}

}  // namespace common
}  // namespace neug
