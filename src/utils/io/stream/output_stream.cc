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

#include "neug/utils/io/stream/output_stream.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>

#include "neug/utils/exception/exception.h"

namespace neug {
namespace io {
namespace {

std::string normalizeLocalPath(const std::string& path) {
  constexpr const char* kFilePrefix = "file://";
  if (path.starts_with(kFilePrefix)) {
    std::string local_path = path.substr(strlen(kFilePrefix));
    if (local_path.empty() || local_path[0] != '/') {
      local_path = "/" + local_path;
    }
    return local_path;
  }
  return path;
}

class FileOutputStream : public OutputStream {
 public:
  explicit FileOutputStream(const std::string& path)
      : path_(path), stream_(path_, std::ios::binary | std::ios::trunc) {
    if (!stream_) {
      if (errno == EACCES || errno == EPERM) {
        THROW_PERMISSION_DENIED("Failed to open output file: " + path);
      }
      THROW_IO_EXCEPTION("Failed to open output file: " + path);
    }
  }

  ~FileOutputStream() override {
    if (state_ == State::OPEN) {
      Abort();
    }
  }

  neug::Status Write(const uint8_t* data, int64_t nbytes) override {
    if (state_ != State::OPEN) {
      return neug::Status(StatusCode::ERR_IO_ERROR,
                          "Cannot write to a finalized output file: " + path_);
    }
    if (nbytes <= 0) {
      return neug::Status::OK();
    }
    if (!data) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Cannot write a null buffer to: " + path_);
    }
    if (static_cast<uint64_t>(nbytes) >
        static_cast<uint64_t>(std::numeric_limits<std::streamsize>::max())) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Write size exceeds stream limit for: " + path_);
    }
    errno = 0;
    stream_.write(reinterpret_cast<const char*>(data),
                  static_cast<std::streamsize>(nbytes));
    if (!stream_) {
      failed_ = true;
      return ioError("write");
    }
    return neug::Status::OK();
  }

  neug::Status Close() override {
    if (state_ != State::OPEN) {
      return neug::Status::OK();
    }
    if (failed_) {
      Abort();
      return neug::Status(StatusCode::ERR_IO_ERROR,
                          "Output discarded after a write failure: " + path_);
    }
    errno = 0;
    stream_.flush();
    stream_.close();
    if (stream_.fail()) {
      auto status = ioError("close");
      Abort();
      return status;
    }
    state_ = State::CLOSED;
    return neug::Status::OK();
  }

  void Abort() override {
    if (state_ != State::OPEN) {
      return;
    }
    if (stream_.is_open()) {
      stream_.close();
    }
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
    state_ = State::ABORTED;
  }

 private:
  enum class State { OPEN, CLOSED, ABORTED };

  neug::Status ioError(const char* operation) const {
    std::string message = "Failed to ";
    message += operation;
    message += " output file ";
    message += path_;
    if (errno != 0) {
      message += ": ";
      message += std::strerror(errno);
    }
    return neug::Status(StatusCode::ERR_IO_ERROR, std::move(message));
  }

  std::string path_;
  std::ofstream stream_;
  State state_ = State::OPEN;
  bool failed_ = false;
};

}  // namespace

std::unique_ptr<OutputStream> openLocalOutputStream(const std::string& path) {
  return std::make_unique<FileOutputStream>(normalizeLocalPath(path));
}

}  // namespace io
}  // namespace neug
