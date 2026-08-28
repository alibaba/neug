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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/utils/io/read/common/read_state.h"
#include "neug/utils/result.h"

namespace neug {
namespace fsys {

/// Arrow-agnostic sequential/random-access read stream over a remote object
/// (e.g. an HTTP(S) file or an S3/OSS object).
class RandomAccessStream {
 public:
  virtual ~RandomAccessStream() = default;

  /// Read up to nbytes bytes from the current position.
  /// Returns the number of bytes actually read.
  virtual result<int64_t> Read(int64_t nbytes, void* out) = 0;

  /// Read up to nbytes bytes at the given absolute position without moving
  /// the current position. Returns the number of bytes actually read.
  virtual result<int64_t> ReadAt(int64_t position, int64_t nbytes,
                                 void* out) = 0;

  /// Total size of the underlying object in bytes.
  virtual result<int64_t> GetSize() = 0;

  /// Release the stream. Safe to call multiple times.
  virtual void Close() = 0;
};

/// Arrow-agnostic sequential write stream over a remote object.
class OutputStream {
 public:
  virtual ~OutputStream() = default;

  /// Append bytes to the object.
  virtual result<void> Write(const void* data, int64_t nbytes) = 0;

  /// Finalize the object (flush buffers / complete multipart upload).
  virtual result<void> Close() = 0;

  /// Discard the object instead of publishing it (e.g. abort a dangling
  /// multipart upload). Called on write failures so a partial object is
  /// never visible to readers. Best-effort default for backends without
  /// staged writes.
  virtual result<void> Abort() { return {}; }
};

/// Arrow-agnostic handle to a remote file system (HTTP/HTTPS, S3/OSS, ...).
/// Implementations live in extensions; the core only defines the contract.
class RemoteFileSystem {
 public:
  virtual ~RemoteFileSystem() = default;

  /// Open a read stream for the given path.
  virtual result<std::shared_ptr<RandomAccessStream>> openInputStream(
      const std::string& path) = 0;

  /// Open a write stream for the given path.
  virtual result<std::shared_ptr<OutputStream>> openOutputStream(
      const std::string& path) = 0;

  /// Whether an object exists at the given path.
  virtual result<bool> exists(const std::string& path) = 0;

  /// Size in bytes of the object at the given path.
  virtual result<int64_t> getSize(const std::string& path) = 0;
};

// Unified FileSystem interface for different protocols: local, http, s3, oss
class FileSystem {
 public:
  virtual ~FileSystem() = default;
  // to support path regex patterns, i.e. /path/to/*.csv
  virtual std::vector<std::string> glob(const std::string& path) = 0;
  /// Remote IO handle for extension readers/writers (parquet over httpfs).
  /// Returns nullptr when the protocol has no remote backend (local paths).
  virtual std::shared_ptr<RemoteFileSystem> getRemoteFileSystem() const {
    return nullptr;
  }
};

using FileSystemFactory =
    std::function<std::unique_ptr<FileSystem>(const reader::FileSchema&)>;

class FileSystemRegistry {
 public:
  FileSystemRegistry();
  ~FileSystemRegistry() = default;

  // Returns true when the factory is inserted. An existing protocol is left
  // unchanged so extension initialization can be replayed safely.
  bool Register(const std::string& protocol, FileSystemFactory factory);

  std::unique_ptr<FileSystem> Provide(const reader::FileSchema& schema);

 private:
  std::shared_mutex mtx;
  std::unordered_map<std::string, FileSystemFactory> factories_;
};
}  // namespace fsys
}  // namespace neug
