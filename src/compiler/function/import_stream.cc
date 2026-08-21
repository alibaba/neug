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

#include "neug/compiler/function/import/import_stream.h"

#include <utility>

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/vfs/file_system.h"

namespace neug {
namespace function {
namespace {

/// Adapts a fsys::RandomAccessStream (remote, e.g. S3/OSS/HTTP) to the
/// io::InputStream interface used by the string-format readers.
class RemoteInputStreamAdapter : public io::InputStream {
 public:
  explicit RemoteInputStreamAdapter(
      std::shared_ptr<fsys::RandomAccessStream> inner)
      : inner_(std::move(inner)) {}

  ~RemoteInputStreamAdapter() override { Close(); }

  result<int64_t> Read(void* out, int64_t nbytes) override {
    auto r = inner_->Read(nbytes, out);
    if (!r) {
      return tl::unexpected(r.error());
    }
    return *r;
  }

  result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    auto r = inner_->ReadAt(position, nbytes, out);
    if (!r) {
      return tl::unexpected(r.error());
    }
    return *r;
  }

  result<int64_t> GetSize() override {
    auto r = inner_->GetSize();
    if (!r) {
      return tl::unexpected(r.error());
    }
    return *r;
  }

  void Close() override {
    if (inner_) {
      inner_->Close();
      inner_ = nullptr;
    }
  }

 private:
  std::shared_ptr<fsys::RandomAccessStream> inner_;
};

}  // namespace

io::InputStreamOpener makeImportStreamOpener(const fsys::FileSystem& fs) {
  auto remote = fs.getRemoteFileSystem();
  if (!remote) {
    return nullptr;
  }
  return [remote](const std::string& path) -> std::unique_ptr<io::InputStream> {
    auto r = remote->openInputStream(path);
    if (!r) {
      THROW_IO_EXCEPTION("Failed to open remote input stream for " + path +
                         ": " + r.error().error_message());
    }
    return std::make_unique<RemoteInputStreamAdapter>(std::move(*r));
  };
}

}  // namespace function
}  // namespace neug
