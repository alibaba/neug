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

#include "neug/utils/io/stream/input_stream.h"

#include <cerrno>
#include <cstring>
#include <fstream>
#include <utility>

#include "neug/utils/exception/exception.h"

namespace neug {
namespace io {
namespace {

constexpr int64_t kStreamBufSize = 1 << 20;  // 1 MB

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

/// InputStream over a local file. Both Read() and ReadAt() go through
/// an explicit logical position so the two never interfere.
class FileInputStream : public InputStream {
 public:
  explicit FileInputStream(const std::string& path) {
    // errno is only meaningful for this open; reset it first because a
    // stale value from a previous syscall could misclassify the failure.
    errno = 0;
    stream_.open(path, std::ios::binary);
    if (!stream_) {
      if (errno == EACCES || errno == EPERM) {
        THROW_PERMISSION_DENIED("Failed to open input file: " + path);
      }
      THROW_IO_EXCEPTION("Failed to open input file: " + path);
    }
  }

  result<int64_t> Read(void* out, int64_t nbytes) override {
    if (nbytes <= 0) {
      return int64_t{0};
    }
    stream_.clear();
    stream_.seekg(static_cast<std::streamoff>(position_));
    stream_.read(static_cast<char*>(out), static_cast<std::streamsize>(nbytes));
    auto got = static_cast<int64_t>(stream_.gcount());
    if (got == 0 && stream_.bad()) {
      return tl::unexpected(
          neug::Status(neug::StatusCode::ERR_IO_ERROR,
                       "Failed to read from input file (position " +
                           std::to_string(position_) + ")"));
    }
    position_ += got;
    return got;
  }

  result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    if (nbytes <= 0) {
      return int64_t{0};
    }
    stream_.clear();
    stream_.seekg(static_cast<std::streamoff>(position));
    stream_.read(static_cast<char*>(out), static_cast<std::streamsize>(nbytes));
    auto got = static_cast<int64_t>(stream_.gcount());
    if (got == 0 && stream_.bad()) {
      return tl::unexpected(
          neug::Status(neug::StatusCode::ERR_IO_ERROR,
                       "Failed to read from input file (position " +
                           std::to_string(position) + ")"));
    }
    return got;
  }

  result<int64_t> GetSize() override {
    auto current = stream_.tellg();
    stream_.clear();
    stream_.seekg(0, std::ios::end);
    auto size = static_cast<int64_t>(stream_.tellg());
    stream_.clear();
    stream_.seekg(current);
    if (size < 0) {
      return tl::unexpected(neug::Status(neug::StatusCode::ERR_IO_ERROR,
                                         "Failed to get input file size"));
    }
    return size;
  }

  void Close() override { stream_.close(); }

 private:
  std::ifstream stream_;
  int64_t position_ = 0;
};

}  // namespace

std::unique_ptr<InputStream> openLocalInputStream(const std::string& path) {
  return std::make_unique<FileInputStream>(normalizeLocalPath(path));
}

InputStreamFactory bindInputStream(const InputStreamOpener& opener,
                                   const std::string& path) {
  if (!opener) {
    return nullptr;
  }
  return [opener, path]() { return opener(path); };
}

std::string readFirstLine(const InputStreamFactory& factory) {
  if (!factory) {
    THROW_IO_EXCEPTION("readFirstLine requires a non-null stream factory");
  }
  auto stream = factory();
  std::string line;
  constexpr int64_t kBufSize = 64 * 1024;
  std::vector<char> buffer(kBufSize);
  while (true) {
    auto r = stream->Read(buffer.data(), kBufSize);
    if (!r) {
      THROW_IO_EXCEPTION("Failed to read first line: " +
                         r.error().error_message());
    }
    if (*r == 0) {
      break;
    }
    const char* begin = buffer.data();
    const char* end = buffer.data() + *r;
    const char* nl =
        static_cast<const char*>(std::memchr(begin, '\n', end - begin));
    if (nl) {
      line.append(begin, nl - begin);
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      return line;
    }
    line.append(begin, end - begin);
  }
  if (line.empty()) {
    THROW_IO_EXCEPTION("Failed to read first line: empty source");
  }
  if (line.back() == '\r') {
    line.pop_back();
  }
  return line;
}

IoStreamBuf::IoStreamBuf(std::unique_ptr<InputStream> input)
    : input_(std::move(input)), buffer_(kStreamBufSize) {
  // Start with an empty get area; underflow() fills it on demand.
  setg(buffer_.data(), buffer_.data(), buffer_.data());
}

bool IoStreamBuf::fillBuffer() {
  auto r = input_->ReadAt(position_, static_cast<int64_t>(buffer_.size()),
                          buffer_.data());
  if (!r) {
    // A real IO error (network failure, timeout, ...); never disguise it
    // as end of stream — callers must not silently accept a truncated
    // read as a complete source.
    THROW_IO_EXCEPTION("Failed to read from input stream: " +
                       r.error().error_message());
  }
  if (*r == 0) {
    setg(buffer_.data(), buffer_.data(), buffer_.data());
    return false;
  }
  auto got = static_cast<std::streamsize>(*r);
  setg(buffer_.data(), buffer_.data(), buffer_.data() + got);
  position_ += *r;
  return true;
}

IoStreamBuf::int_type IoStreamBuf::underflow() {
  if (gptr() < egptr()) {
    return traits_type::to_int_type(*gptr());
  }
  if (!fillBuffer()) {
    return traits_type::eof();
  }
  return traits_type::to_int_type(*gptr());
}

std::streamsize IoStreamBuf::xsgetn(char* s, std::streamsize n) {
  std::streamsize total = 0;
  while (total < n) {
    if (gptr() >= egptr()) {
      // Bypass the internal buffer for large remaining requests.
      const auto remaining = static_cast<int64_t>(n - total);
      if (remaining >= static_cast<int64_t>(buffer_.size())) {
        auto r = input_->ReadAt(position_, remaining, s + total);
        if (!r) {
          // IO error: propagate instead of pretending the stream ended.
          THROW_IO_EXCEPTION("Failed to read from input stream: " +
                             r.error().error_message());
        }
        if (*r == 0) {
          break;
        }
        position_ += *r;
        total += static_cast<std::streamsize>(*r);
        setg(buffer_.data(), buffer_.data(), buffer_.data());
        if (*r < remaining) {
          break;  // end of stream
        }
        continue;
      }
      if (!fillBuffer()) {
        break;
      }
    }
    const auto avail = static_cast<std::streamsize>(egptr() - gptr());
    const auto take = std::min(avail, n - total);
    std::memcpy(s + total, gptr(), static_cast<size_t>(take));
    gbump(static_cast<int>(take));
    total += take;
  }
  return total;
}

IoStreamBuf::pos_type IoStreamBuf::seekoff(off_type off,
                                           std::ios_base::seekdir dir,
                                           std::ios_base::openmode which) {
  if (which & std::ios_base::out) {
    return pos_type(off_type(-1));
  }
  int64_t base = 0;
  if (dir == std::ios_base::cur) {
    // The logical position already points past the buffered bytes;
    // compensate for the unconsumed part of the get area.
    base = position_ - static_cast<int64_t>(egptr() - gptr());
  } else if (dir == std::ios_base::end) {
    if (size_ < 0) {
      auto r = input_->GetSize();
      if (!r) {
        return pos_type(off_type(-1));
      }
      size_ = *r;
    }
    base = size_;
  }
  const int64_t target = base + static_cast<int64_t>(off);
  if (target < 0) {
    return pos_type(off_type(-1));
  }
  position_ = target;
  setg(buffer_.data(), buffer_.data(), buffer_.data());
  return pos_type(static_cast<off_type>(target));
}

IoStreamBuf::pos_type IoStreamBuf::seekpos(pos_type pos,
                                           std::ios_base::openmode which) {
  return seekoff(off_type(pos), std::ios_base::beg, which);
}

namespace {

/// Owns the streambuf and the istream together; bases are initialized
/// in declaration order, so the buffer is ready when istream binds it.
/// badbit exceptions are enabled so IO errors raised by the streambuf
/// propagate to callers instead of being swallowed into stream state
/// (which would make a truncated remote read look like end of stream).
class IoIStream : private IoStreamBuf, public std::istream {
 public:
  explicit IoIStream(std::unique_ptr<InputStream> input)
      : IoStreamBuf(std::move(input)), std::istream(this) {
    exceptions(std::ios::badbit);
  }
};

}  // namespace

std::unique_ptr<std::istream> makeIoStream(std::unique_ptr<InputStream> input) {
  return std::make_unique<IoIStream>(std::move(input));
}

}  // namespace io
}  // namespace neug
