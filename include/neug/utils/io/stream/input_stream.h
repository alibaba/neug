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
#pragma once

#include <cstdint>
#include <functional>
#include <istream>
#include <memory>
#include <streambuf>
#include <string>
#include <vector>

#include "neug/utils/result.h"

namespace neug {
namespace io {

/// Sequential/random-access read stream abstraction, mirroring
/// fsys::RandomAccessStream so that remote streams can be adapted into
/// this interface without the consumers depending on the VFS layer.
class InputStream {
 public:
  virtual ~InputStream() = default;

  /// Read up to nbytes bytes from the current position, advancing it.
  /// Returns the number of bytes actually read (0 means end of stream).
  virtual result<int64_t> Read(void* out, int64_t nbytes) = 0;

  /// Read up to nbytes bytes at the given absolute position without
  /// moving the current position. Returns the number of bytes actually
  /// read.
  virtual result<int64_t> ReadAt(int64_t position, int64_t nbytes,
                                 void* out) = 0;

  /// Total size of the underlying source in bytes.
  virtual result<int64_t> GetSize() = 0;

  /// Release the stream. Safe to call multiple times.
  virtual void Close() = 0;
};

/// Opens a fresh InputStream. Readers may need several independent
/// streams over the same source (e.g. a row-count pass and a parse
/// pass), so they hold a factory instead of a single stream.
using InputStreamFactory = std::function<std::unique_ptr<InputStream>()>;

/// Opens a fresh InputStream for a given (already resolved) path. Used
/// when one resolver serves multiple files, e.g. after glob expansion.
using InputStreamOpener =
    std::function<std::unique_ptr<InputStream>(const std::string& path)>;

/// Opens a local file for reading (strips optional file:// prefix).
std::unique_ptr<InputStream> openLocalInputStream(const std::string& path);

/// Binds an opener to a specific path, yielding a path-less factory.
/// Returns a null factory when the opener is null (local files).
InputStreamFactory bindInputStream(const InputStreamOpener& opener,
                                   const std::string& path);

/// Reads the first line (up to '\n', terminator stripped) from a fresh
/// stream obtained from the factory. Throws an IO exception on error
/// or when the source is empty.
std::string readFirstLine(const InputStreamFactory& factory);

/// A std::streambuf backed by an InputStream (via ReadAt), so that
/// istream-based third-party parsers (e.g. csv::CSVReader) can consume
/// any source. Reads are buffered; seeking is supported.
class IoStreamBuf : public std::streambuf {
 public:
  explicit IoStreamBuf(std::unique_ptr<InputStream> input);

 protected:
  int_type underflow() override;
  std::streamsize xsgetn(char* s, std::streamsize n) override;
  pos_type seekoff(off_type off, std::ios_base::seekdir dir,
                   std::ios_base::openmode which) override;
  pos_type seekpos(pos_type pos, std::ios_base::openmode which) override;

 private:
  /// Fill the internal buffer at the current logical position.
  /// Returns false on end of stream; throws an IO exception when the
  /// underlying read fails (an IO error must never be reported as EOF).
  bool fillBuffer();

  std::unique_ptr<InputStream> input_;
  std::vector<char> buffer_;
  int64_t position_ = 0;  // logical read position in the source
  int64_t size_ = -1;     // lazily resolved source size
};

/// An std::istream owning an InputStream through an IoStreamBuf.
std::unique_ptr<std::istream> makeIoStream(std::unique_ptr<InputStream> input);

}  // namespace io
}  // namespace neug
