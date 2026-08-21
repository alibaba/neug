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

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <memory>
#include <string>
#include "neug/utils/io/vfs/file_system.h"

namespace neug {
namespace parquet {

/**
 * Adapts an Arrow-agnostic fsys::RandomAccessStream to Arrow's
 * arrow::io::RandomAccessFile interface so the Parquet/Dataset machinery
 * (which is inherently Arrow-based) can read through it.
 */
class StreamRandomAccessFile : public arrow::io::RandomAccessFile {
 public:
  explicit StreamRandomAccessFile(
      std::shared_ptr<fsys::RandomAccessStream> stream);
  ~StreamRandomAccessFile() override;

  arrow::Status Close() override;
  bool closed() const override;
  arrow::Result<int64_t> Tell() const override;
  arrow::Status Seek(int64_t position) override;

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

  arrow::Result<int64_t> GetSize() override;
  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes,
                                void* out) override;
  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                       int64_t nbytes) override;

 private:
  std::shared_ptr<fsys::RandomAccessStream> stream_;
  int64_t position_ = 0;
  bool closed_ = false;
};

/**
 * Adapts an Arrow-agnostic fsys::OutputStream to arrow::io::OutputStream so
 * Parquet export can write through remote filesystems (e.g. s3://, oss://).
 */
class StreamOutputStream : public arrow::io::OutputStream {
 public:
  explicit StreamOutputStream(std::shared_ptr<fsys::OutputStream> stream);
  ~StreamOutputStream() override;

  arrow::Status Close() override;
  bool closed() const override;
  arrow::Result<int64_t> Tell() const override;
  arrow::Status Write(const void* data, int64_t nbytes) override;

 private:
  std::shared_ptr<fsys::OutputStream> stream_;
  int64_t position_ = 0;
  bool closed_ = false;
  bool failed_ = false;
};

/**
 * Adapts an Arrow-agnostic fsys::RemoteFileSystem to arrow::fs::FileSystem.
 *
 * Only the operations the Parquet read/write paths actually need are
 * implemented (GetFileInfo on explicit file paths, OpenInputFile,
 * OpenInputStream, OpenOutputStream). Directory listing and mutating
 * operations are rejected with NotImplemented — glob expansion happens in
 * the neug VFS layer before paths reach this adapter.
 */
class RemoteFsArrowAdapter : public arrow::fs::FileSystem {
 public:
  explicit RemoteFsArrowAdapter(std::shared_ptr<fsys::RemoteFileSystem> remote);
  ~RemoteFsArrowAdapter() override = default;

  std::string type_name() const override;
  bool Equals(const arrow::fs::FileSystem& other) const override;

  arrow::Result<arrow::fs::FileInfo> GetFileInfo(
      const std::string& path) override;
  arrow::Result<arrow::fs::FileInfoVector> GetFileInfo(
      const arrow::fs::FileSelector& select) override;

  arrow::Status CreateDir(const std::string& path, bool recursive) override;
  arrow::Status DeleteDir(const std::string& path) override;
  arrow::Status DeleteDirContents(const std::string& path,
                                  bool missing_dir_ok) override;
  arrow::Status DeleteRootDirContents() override;
  arrow::Status DeleteFile(const std::string& path) override;
  arrow::Status Move(const std::string& src, const std::string& dest) override;
  arrow::Status CopyFile(const std::string& src,
                         const std::string& dest) override;

  arrow::Result<std::shared_ptr<arrow::io::InputStream>> OpenInputStream(
      const std::string& path) override;
  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;
  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) override;
  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) override;

 private:
  std::shared_ptr<fsys::RemoteFileSystem> remote_;
};

}  // namespace parquet
}  // namespace neug
