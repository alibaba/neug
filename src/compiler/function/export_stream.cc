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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include "neug/compiler/function/export/export_stream.h"

#include <utility>

#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/vfs/file_system.h"

namespace neug {
namespace function {
namespace {

/// Adapts a fsys::OutputStream (remote, e.g. S3/OSS/HTTP) to the
/// io::OutputStream interface used by the string-format export writers.
class RemoteOutputStreamAdapter : public io::OutputStream {
 public:
  explicit RemoteOutputStreamAdapter(std::shared_ptr<fsys::OutputStream> inner)
      : inner_(std::move(inner)) {}

  ~RemoteOutputStreamAdapter() override {
    // Only an explicit successful Close publishes a remote object.
    Abort();
  }

  neug::Status Write(const uint8_t* data, int64_t nbytes) override {
    if (state_ != State::OPEN) {
      return neug::Status(StatusCode::ERR_IO_ERROR,
                          "Cannot write to a finalized remote output");
    }
    // Keep failures sticky even if a remote implementation throws.
    const bool previouslyFailed = failed_;
    failed_ = true;
    auto r = inner_->Write(data, nbytes);
    if (!r) {
      failed_ = true;
      return r.error();
    }
    failed_ = previouslyFailed;
    return neug::Status::OK();
  }

  neug::Status Close() override {
    if (state_ != State::OPEN) {
      return neug::Status::OK();
    }
    if (failed_) {
      Abort();
      return neug::Status(StatusCode::ERR_IO_ERROR,
                          "Remote output discarded after a write failure");
    }
    try {
      auto r = inner_->Close();
      if (!r) {
        Abort();
        return r.error();
      }
      state_ = State::CLOSED;
      return neug::Status::OK();
    } catch (...) {
      Abort();
      throw;
    }
  }

  void Abort() override {
    if (state_ != State::OPEN) {
      return;
    }
    state_ = State::ABORTED;
    try {
      (void) inner_->Abort();
    } catch (...) {
      // Abort is best effort, including during stack unwinding. Never retry a
      // throwing backend from the destructor or replace the original error.
    }
  }

 private:
  enum class State { OPEN, CLOSED, ABORTED };

  std::shared_ptr<fsys::OutputStream> inner_;
  State state_ = State::OPEN;
  bool failed_ = false;
};

}  // namespace

std::unique_ptr<io::OutputStream> openExportOutputStream(
    const reader::FileSchema& schema) {
  if (schema.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Schema paths is empty");
  }
  const auto& vfs = neug::main::MetadataRegistry::getVFS();
  const auto& fs = vfs->Provide(schema);
  if (auto remote = fs->getRemoteFileSystem()) {
    auto r = remote->openOutputStream(schema.paths[0]);
    if (!r) {
      THROW_IO_EXCEPTION("Failed to open remote output stream for " +
                         schema.paths[0] + ": " + r.error().error_message());
    }
    return std::make_unique<RemoteOutputStreamAdapter>(std::move(*r));
  }
  return io::openLocalOutputStream(schema.paths[0]);
}

}  // namespace function
}  // namespace neug
