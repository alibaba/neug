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

#include "parquet/arrow_fs_adapter.h"

#include <arrow/buffer.h>
#include <arrow/status.h>
#include <utility>

namespace neug {
namespace parquet {

namespace {

// Convert a neug::Status into an arrow::Status (always an IOError — these
// failures originate from remote I/O).
arrow::Status ToArrowStatus(const Status& status) {
  return arrow::Status::IOError(status.ToString());
}

}  // namespace

// ============================================================================
// StreamRandomAccessFile
// ============================================================================

StreamRandomAccessFile::StreamRandomAccessFile(
    std::shared_ptr<fsys::RandomAccessStream> stream)
    : stream_(std::move(stream)) {}

StreamRandomAccessFile::~StreamRandomAccessFile() {
  if (!closed_) {
    stream_->Close();
    closed_ = true;
  }
}

arrow::Status StreamRandomAccessFile::Close() {
  if (!closed_) {
    stream_->Close();
    closed_ = true;
  }
  return arrow::Status::OK();
}

bool StreamRandomAccessFile::closed() const { return closed_; }

arrow::Result<int64_t> StreamRandomAccessFile::Tell() const {
  return position_;
}

arrow::Status StreamRandomAccessFile::Seek(int64_t position) {
  position_ = position;
  return arrow::Status::OK();
}

arrow::Result<int64_t> StreamRandomAccessFile::Read(int64_t nbytes, void* out) {
  // Sequential reads go through ReadAt so they respect Seek(): the
  // underlying stream maintains its own position which Seek() does not
  // touch.
  auto r = stream_->ReadAt(position_, nbytes, out);
  if (!r.has_value()) {
    return ToArrowStatus(r.error());
  }
  position_ += *r;
  return *r;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> StreamRandomAccessFile::Read(
    int64_t nbytes) {
  ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
  auto r = stream_->ReadAt(position_, nbytes, buffer->mutable_data());
  if (!r.has_value()) {
    return ToArrowStatus(r.error());
  }
  ARROW_RETURN_NOT_OK(buffer->Resize(*r));
  position_ += *r;
  return std::shared_ptr<arrow::Buffer>(std::move(buffer));
}

arrow::Result<int64_t> StreamRandomAccessFile::GetSize() {
  auto r = stream_->GetSize();
  if (!r.has_value()) {
    return ToArrowStatus(r.error());
  }
  return *r;
}

arrow::Result<int64_t> StreamRandomAccessFile::ReadAt(int64_t position,
                                                      int64_t nbytes,
                                                      void* out) {
  auto r = stream_->ReadAt(position, nbytes, out);
  if (!r.has_value()) {
    return ToArrowStatus(r.error());
  }
  return *r;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> StreamRandomAccessFile::ReadAt(
    int64_t position, int64_t nbytes) {
  ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
  auto r = stream_->ReadAt(position, nbytes, buffer->mutable_data());
  if (!r.has_value()) {
    return ToArrowStatus(r.error());
  }
  ARROW_RETURN_NOT_OK(buffer->Resize(*r));
  return std::shared_ptr<arrow::Buffer>(std::move(buffer));
}

// ============================================================================
// StreamOutputStream
// ============================================================================

StreamOutputStream::StreamOutputStream(
    std::shared_ptr<fsys::OutputStream> stream)
    : stream_(std::move(stream)) {}

StreamOutputStream::~StreamOutputStream() {
  if (!closed_) {
    // Best-effort finalization at destruction time. If any write failed,
    // abort instead of publishing a partial remote object; otherwise
    // finalize. Errors are unrecoverable at destruction time.
    if (failed_) {
      (void) stream_->Abort();
    } else {
      (void) stream_->Close();
    }
    closed_ = true;
  }
}

arrow::Status StreamOutputStream::Close() {
  if (closed_) {
    return arrow::Status::OK();
  }
  closed_ = true;
  if (failed_) {
    // A prior write failed: abort instead of finalizing, otherwise a
    // partial remote object would be published.
    auto ar = stream_->Abort();
    if (!ar.has_value()) {
      return ToArrowStatus(ar.error());
    }
    return arrow::Status::IOError(
        "Stream aborted due to a prior write failure");
  }
  auto r = stream_->Close();
  if (!r.has_value()) {
    return ToArrowStatus(r.error());
  }
  return arrow::Status::OK();
}

bool StreamOutputStream::closed() const { return closed_; }

arrow::Result<int64_t> StreamOutputStream::Tell() const { return position_; }

arrow::Status StreamOutputStream::Write(const void* data, int64_t nbytes) {
  auto r = stream_->Write(data, nbytes);
  if (!r.has_value()) {
    failed_ = true;
    return ToArrowStatus(r.error());
  }
  position_ += nbytes;
  return arrow::Status::OK();
}

// ============================================================================
// RemoteFsArrowAdapter
// ============================================================================

RemoteFsArrowAdapter::RemoteFsArrowAdapter(
    std::shared_ptr<fsys::RemoteFileSystem> remote)
    : remote_(std::move(remote)) {}

std::string RemoteFsArrowAdapter::type_name() const { return "neug-remote"; }

bool RemoteFsArrowAdapter::Equals(const arrow::fs::FileSystem& other) const {
  return this == &other;
}

arrow::Result<arrow::fs::FileInfo> RemoteFsArrowAdapter::GetFileInfo(
    const std::string& path) {
  arrow::fs::FileInfo info;
  info.set_path(path);

  auto exists_result = remote_->exists(path);
  if (!exists_result.has_value()) {
    return ToArrowStatus(exists_result.error());
  }
  if (!*exists_result) {
    info.set_type(arrow::fs::FileType::NotFound);
    return info;
  }

  auto size_result = remote_->getSize(path);
  if (!size_result.has_value()) {
    return ToArrowStatus(size_result.error());
  }
  info.set_type(arrow::fs::FileType::File);
  info.set_size(*size_result);
  return info;
}

arrow::Result<arrow::fs::FileInfoVector> RemoteFsArrowAdapter::GetFileInfo(
    const arrow::fs::FileSelector& select) {
  return arrow::Status::NotImplemented(
      "RemoteFsArrowAdapter does not support directory listing; "
      "glob expansion happens in the neug VFS layer");
}

arrow::Status RemoteFsArrowAdapter::CreateDir(const std::string& path,
                                              bool recursive) {
  return arrow::Status::NotImplemented(
      "RemoteFsArrowAdapter does not support CreateDir: " + path);
}

arrow::Status RemoteFsArrowAdapter::DeleteDir(const std::string& path) {
  return arrow::Status::NotImplemented(
      "RemoteFsArrowAdapter does not support DeleteDir: " + path);
}

arrow::Status RemoteFsArrowAdapter::DeleteDirContents(const std::string& path,
                                                      bool missing_dir_ok) {
  return arrow::Status::NotImplemented(
      "RemoteFsArrowAdapter does not support DeleteDirContents: " + path);
}

arrow::Status RemoteFsArrowAdapter::DeleteRootDirContents() {
  return arrow::Status::NotImplemented(
      "RemoteFsArrowAdapter does not support DeleteRootDirContents");
}

arrow::Status RemoteFsArrowAdapter::DeleteFile(const std::string& path) {
  return arrow::Status::NotImplemented(
      "RemoteFsArrowAdapter does not support DeleteFile: " + path);
}

arrow::Status RemoteFsArrowAdapter::Move(const std::string& src,
                                         const std::string& dest) {
  return arrow::Status::NotImplemented(
      "RemoteFsArrowAdapter does not support Move: " + src + " -> " + dest);
}

arrow::Status RemoteFsArrowAdapter::CopyFile(const std::string& src,
                                             const std::string& dest) {
  return arrow::Status::NotImplemented(
      "RemoteFsArrowAdapter does not support CopyFile: " + src + " -> " + dest);
}

arrow::Result<std::shared_ptr<arrow::io::InputStream>>
RemoteFsArrowAdapter::OpenInputStream(const std::string& path) {
  auto r = remote_->openInputStream(path);
  if (!r.has_value()) {
    return ToArrowStatus(r.error());
  }
  return std::static_pointer_cast<arrow::io::InputStream>(
      std::make_shared<StreamRandomAccessFile>(std::move(*r)));
}

arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
RemoteFsArrowAdapter::OpenInputFile(const std::string& path) {
  auto r = remote_->openInputStream(path);
  if (!r.has_value()) {
    return ToArrowStatus(r.error());
  }
  return std::make_shared<StreamRandomAccessFile>(std::move(*r));
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
RemoteFsArrowAdapter::OpenOutputStream(
    const std::string& path,
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
  auto r = remote_->openOutputStream(path);
  if (!r.has_value()) {
    return ToArrowStatus(r.error());
  }
  return std::make_shared<StreamOutputStream>(std::move(*r));
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
RemoteFsArrowAdapter::OpenAppendStream(
    const std::string& path,
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
  return arrow::Status::NotImplemented(
      "RemoteFsArrowAdapter does not support OpenAppendStream: " + path);
}

}  // namespace parquet
}  // namespace neug
