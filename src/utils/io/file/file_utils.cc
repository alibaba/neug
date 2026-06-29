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

#include "neug/utils/io/file/file_utils.h"

#include <glog/logging.h>

#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>
#ifdef __linux__
#include <linux/fs.h>
#include <sys/syscall.h>
#endif
#ifdef __APPLE__
#include <sys/clonefile.h>
#endif
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <memory>
#include <stdexcept>

#include "neug/utils/exception/exception.h"

namespace neug {

namespace file_utils {

/**
 * @brief Copy file metadata (permissions, timestamps).
 */
static void copy_metadata(const struct stat& src_stat,
                          const std::string& dst_path) {
  // Copy permissions
  ::chmod(dst_path.c_str(), src_stat.st_mode);

  // Copy access and modification times
  struct timespec times[2];
#ifdef __linux__
  times[0] = src_stat.st_atim;  // Access time
  times[1] = src_stat.st_mtim;  // Modification time
#else
  times[0].tv_sec = src_stat.st_atime;
  times[0].tv_nsec = 0;
  times[1].tv_sec = src_stat.st_mtime;
  times[1].tv_nsec = 0;
#endif
  ::utimensat(AT_FDCWD, dst_path.c_str(), times, 0);
}

/**
 * @brief Try to create an O(1) COW clone of the file.
 *
 * Platform-specific instant-clone primitives:
 *   - Linux: ioctl(FICLONE) on Btrfs, XFS (reflink=1), OCFS2
 *   - macOS: clonefile(2) on APFS (preserves sparseness exactly)
 *
 * On success the destination shares its underlying storage with the
 * source via copy-on-write, with no data copied.
 */
static bool try_reflink(const std::string& src_path,
                        const std::string& dst_path,
                        const struct stat& src_stat) {
#if defined(__APPLE__)
  // clonefile() requires dst to not exist. The overwrite policy was
  // already enforced by the caller; remove any leftover dst here.
  ::unlink(dst_path.c_str());
  if (::clonefile(src_path.c_str(), dst_path.c_str(), 0) == 0) {
    // clonefile preserves mode/timestamps/ACLs by default; no need to
    // re-apply metadata.
    return true;
  }
  // Not on APFS or unsupported — leave no partial file behind.
  ::unlink(dst_path.c_str());
  return false;
#elif defined(FICLONE)
  int src_fd = ::open(src_path.c_str(), O_RDONLY);
  if (src_fd < 0) {
    return false;
  }

  int dst_fd =
      ::open(dst_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, src_stat.st_mode);
  if (dst_fd < 0) {
    ::close(src_fd);
    return false;
  }

  int ret = ::ioctl(dst_fd, FICLONE, src_fd);
  ::close(src_fd);
  ::close(dst_fd);

  if (ret == 0) {
    copy_metadata(src_stat, dst_path);
    return true;
  }

  ::unlink(dst_path.c_str());
  return false;
#else
  (void) src_path;
  (void) dst_path;
  (void) src_stat;
  return false;
#endif
}

/**
 * @brief Is the file sparse? True iff allocated blocks < logical size.
 *
 * `st_blocks` is always in 512-byte units regardless of FS block size.
 */
static bool is_sparse(const struct stat& st) {
  return static_cast<off_t>(st.st_blocks) * 512 < st.st_size;
}

/**
 * @brief Try to use copy_file_range() syscall.
 *
 * Available on Linux 4.5+. May utilize COW on supported filesystems.
 * Performs server-side copy without data passing through userspace.
 *
 * WARNING: on non-COW filesystems (e.g. ext4) the kernel materializes
 * source holes into physical zero blocks. Callers MUST skip this path
 * for sparse sources — use is_sparse() — and fall through to the
 * SEEK_HOLE/SEEK_DATA-aware fallback_copy instead.
 */
static bool try_copy_file_range(const std::string& src_path,
                                const std::string& dst_path,
                                const struct stat& src_stat) {
#ifdef SYS_copy_file_range
  int src_fd = ::open(src_path.c_str(), O_RDONLY);
  if (src_fd < 0) {
    return false;
  }

  int dst_fd =
      ::open(dst_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, src_stat.st_mode);
  if (dst_fd < 0) {
    ::close(src_fd);
    return false;
  }

  off_t offset = 0;
  size_t remaining = src_stat.st_size;
  bool success = true;

  // copy_file_range may require multiple calls for large files
  while (remaining > 0) {
    ssize_t copied = syscall(SYS_copy_file_range, src_fd, &offset, dst_fd,
                             nullptr, remaining, 0);

    if (copied <= 0) {
      success = false;
      break;
    }

    remaining -= copied;
  }

  ::close(src_fd);
  ::close(dst_fd);

  if (success) {
    copy_metadata(src_stat, dst_path);
    return true;
  }

  // Failed - remove incomplete file
  ::unlink(dst_path.c_str());
  return false;
#else
  (void) src_path;
  (void) dst_path;
  (void) src_stat;
  return false;
#endif
}

constexpr size_t COPY_BUFFER_SIZE = 64 * 1024;

static bool is_block_zero(const char* buf, size_t n) {
  const uint64_t* q = reinterpret_cast<const uint64_t*>(buf);
  size_t qcount = n / sizeof(uint64_t);
  for (size_t i = 0; i < qcount; ++i) {
    if (q[i] != 0)
      return false;
  }
  for (size_t i = qcount * sizeof(uint64_t); i < n; ++i) {
    if (buf[i] != 0)
      return false;
  }
  return true;
}

// pwrite exact n bytes at off; throws on short write / error.
static void pwrite_all(int fd, const char* buf, size_t n, off_t off,
                       const std::string& path) {
  while (n > 0) {
    ssize_t w = ::pwrite(fd, buf, n, off);
    if (w <= 0) {
      throw std::runtime_error("pwrite failed on " + path + ": " +
                               std::strerror(errno));
    }
    buf += w;
    off += w;
    n -= static_cast<size_t>(w);
  }
}

// Walk source's data extents via SEEK_DATA/SEEK_HOLE; pwrite each extent
// at the same offset in dst (dst has already been ftruncate'd to the
// final size, so holes are pre-allocated). Returns false iff the FS
// doesn't implement SEEK_HOLE — caller should fall back.
static bool sparse_copy_seek_hole(int src_fd, int dst_fd, off_t size,
                                  const std::string& src,
                                  const std::string& dst) {
#ifdef SEEK_HOLE
  auto buf = std::make_unique<char[]>(COPY_BUFFER_SIZE);
  off_t off = 0;
  while (off < size) {
    off_t data = ::lseek(src_fd, off, SEEK_DATA);
    if (data == -1) {
      if (errno == ENXIO)
        break;  // remainder is hole
      if (errno == EINVAL || errno == ENOTSUP)
        return false;
      throw std::runtime_error("SEEK_DATA failed on " + src + ": " +
                               std::strerror(errno));
    }
    off_t hole = ::lseek(src_fd, data, SEEK_HOLE);
    if (hole == -1)
      hole = size;

    for (off_t cur = data; cur < hole;) {
      size_t to_read =
          std::min(static_cast<size_t>(hole - cur), COPY_BUFFER_SIZE);
      ssize_t r = ::pread(src_fd, buf.get(), to_read, cur);
      if (r <= 0) {
        throw std::runtime_error("pread failed on " + src + ": " +
                                 std::strerror(errno));
      }
      pwrite_all(dst_fd, buf.get(), static_cast<size_t>(r), cur, dst);
      cur += r;
    }
    off = hole;
  }
  return true;
#else
  (void) src_fd;
  (void) dst_fd;
  (void) size;
  (void) src;
  (void) dst;
  return false;
#endif
}

// Fallback for FS without SEEK_HOLE: read every block linearly and pwrite
// only the non-zero ones; zero blocks stay as the dst's pre-allocated hole.
static void sparse_copy_zero_detect(int src_fd, int dst_fd, off_t size,
                                    const std::string& src,
                                    const std::string& dst) {
  auto buf = std::make_unique<char[]>(COPY_BUFFER_SIZE);
  for (off_t off = 0; off < size;) {
    size_t to_read =
        std::min(static_cast<size_t>(size - off), COPY_BUFFER_SIZE);
    ssize_t r = ::pread(src_fd, buf.get(), to_read, off);
    if (r <= 0) {
      throw std::runtime_error("pread failed on " + src + ": " +
                               std::strerror(errno));
    }
    if (!is_block_zero(buf.get(), static_cast<size_t>(r))) {
      pwrite_all(dst_fd, buf.get(), static_cast<size_t>(r), off, dst);
    }
    off += r;
  }
}

/**
 * @brief Fallback file copy that preserves sparseness.
 *
 * Anchors dst at the final size with ftruncate, then pwrites only the
 * source's allocated extents into the corresponding offsets — holes in
 * the source stay as holes in the destination. Tries SEEK_DATA/SEEK_HOLE
 * first; on filesystems that don't implement them, falls back to reading
 * every block and pwriting only the non-zero ones.
 *
 * Non-static so tests can call it directly (clonefile/FICLONE fast paths
 * normally shadow this helper on supported filesystems).
 */
void fallback_copy(const std::string& src_path, const std::string& dst_path,
                   const struct stat& src_stat) {
  int src_fd = ::open(src_path.c_str(), O_RDONLY);
  if (src_fd < 0) {
    throw std::runtime_error("Failed to open source file: " + src_path);
  }
  int dst_fd =
      ::open(dst_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, src_stat.st_mode);
  if (dst_fd < 0) {
    ::close(src_fd);
    throw std::runtime_error("Failed to create destination file: " + dst_path);
  }

#ifdef POSIX_FADV_SEQUENTIAL
  ::posix_fadvise(src_fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif

  try {
    // Anchor dst size up front. Empty / all-hole / trailing-hole cases all
    // fall out naturally: the inner loops simply don't run for hole regions.
    if (::ftruncate(dst_fd, src_stat.st_size) < 0) {
      throw std::runtime_error("ftruncate failed on " + dst_path);
    }
    if (!sparse_copy_seek_hole(src_fd, dst_fd, src_stat.st_size, src_path,
                               dst_path)) {
      sparse_copy_zero_detect(src_fd, dst_fd, src_stat.st_size, src_path,
                              dst_path);
    }
  } catch (...) {
    ::close(src_fd);
    ::close(dst_fd);
    ::unlink(dst_path.c_str());
    throw;
  }

  ::close(src_fd);
  ::close(dst_fd);
  copy_metadata(src_stat, dst_path);
}

CopyResult copy_file(const std::string& src_path, const std::string& dst_path,
                     bool overwrite) {
  // Verify source file exists
  struct stat src_stat;
  if (stat(src_path.c_str(), &src_stat) < 0) {
    throw std::runtime_error("Source file does not exist: " + src_path);
  }

  // Check if destination exists
  if (!overwrite) {
    struct stat dst_stat;
    if (stat(dst_path.c_str(), &dst_stat) == 0) {
      throw std::runtime_error("Destination file already exists: " + dst_path);
    }
  }

  // Try reflink (COW) first - fastest if supported
  if (try_reflink(src_path, dst_path, src_stat)) {
    return CopyResult::Reflink;
  }

  // copy_file_range() materializes holes into zero blocks on non-COW
  // filesystems (e.g. ext4). For sparse sources we MUST use the
  // SEEK_HOLE/SEEK_DATA-aware fallback to avoid bloating dst from a few
  // KB of real data into the full ftruncate-reserved logical size.
  if (!is_sparse(src_stat) &&
      try_copy_file_range(src_path, dst_path, src_stat)) {
    return CopyResult::CopyFileRange;
  }

  fallback_copy(src_path, dst_path, src_stat);
  return CopyResult::FallbackCopy;
}

// ---------------------------------------------------------------------------
// AtomicFileWriter
// ---------------------------------------------------------------------------

AtomicFileWriter::AtomicFileWriter(const std::string& target_path)
    : target_path_(target_path), tmp_path_(target_path + ".tmp") {
  fd_ = ::open(tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd_ < 0) {
    THROW_IO_EXCEPTION("AtomicFileWriter: cannot open " + tmp_path_ + ": " +
                       std::strerror(errno));
  }
}

AtomicFileWriter::~AtomicFileWriter() {
  if (!committed_) {
    Abort();
  }
}

AtomicFileWriter::AtomicFileWriter(AtomicFileWriter&& other) noexcept
    : target_path_(std::move(other.target_path_)),
      tmp_path_(std::move(other.tmp_path_)),
      fd_(other.fd_),
      committed_(other.committed_),
      file_(other.file_),
      ostream_(std::move(other.ostream_)) {
  other.fd_ = -1;
  other.file_ = nullptr;
  other.committed_ = true;  // prevent double-abort
}

AtomicFileWriter& AtomicFileWriter::operator=(
    AtomicFileWriter&& other) noexcept {
  if (this != &other) {
    if (!committed_) {
      Abort();
    }
    target_path_ = std::move(other.target_path_);
    tmp_path_ = std::move(other.tmp_path_);
    fd_ = other.fd_;
    committed_ = other.committed_;
    file_ = other.file_;
    ostream_ = std::move(other.ostream_);
    other.fd_ = -1;
    other.file_ = nullptr;
    other.committed_ = true;
  }
  return *this;
}

std::ostream& AtomicFileWriter::stream() {
  if (!ostream_) {
    // Wrap fd_ directly (no dup!) so that fflush(FILE*) pushes data to the
    // same kernel fd that Commit() will fsync.  Because fclose() would close
    // fd_ out from under us, we must NOT fclose the FILE* — instead we
    // fflush in Commit/Abort and let the fd be closed by ::close(fd_).
    file_ = ::fdopen(fd_, "wb");
    if (!file_) {
      THROW_IO_EXCEPTION("AtomicFileWriter::stream: fdopen failed: " +
                         std::string(std::strerror(errno)));
    }
    // Simple FILE*-backed streambuf.  Overflow writes through the FILE*.
    struct FileBuf : public std::streambuf {
      FILE* fp;
      explicit FileBuf(FILE* f) : fp(f) {}
      int_type overflow(int_type ch) override {
        if (ch != traits_type::eof()) {
          if (std::fputc(ch, fp) == EOF) {
            return traits_type::eof();
          }
        }
        return ch;
      }
      std::streamsize xsputn(const char* s, std::streamsize n) override {
        return static_cast<std::streamsize>(
            std::fwrite(s, 1, static_cast<size_t>(n), fp));
      }
      int sync() override { return std::fflush(fp) == 0 ? 0 : -1; }
    };
    // The streambuf must live as long as the ostream, so we allocate a
    // combined object.
    struct OStreamWithBuf : public std::ostream {
      FileBuf buf;
      explicit OStreamWithBuf(FILE* f) : std::ostream(&buf), buf(f) {}
    };
    ostream_.reset(new OStreamWithBuf(file_));
  }
  return *ostream_;
}

void AtomicFileWriter::Commit() {
  if (committed_) {
    THROW_IO_EXCEPTION("AtomicFileWriter::Commit: already committed");
  }
  committed_ = true;

  // Step 1: If an ostream was used, flush its user-space buffers through the
  // FILE* and into the kernel page cache.
  if (ostream_) {
    ostream_->flush();
    if (!ostream_->good()) {
      Abort();
      THROW_IO_EXCEPTION("AtomicFileWriter::Commit: stream write failed for " +
                         tmp_path_);
    }
    ostream_.reset();
  }
  if (file_) {
    std::fflush(file_);
    // Do NOT fclose here yet — fclose would close fd_ (no dup), and we
    // still need fd_ for fsync below.
  }

  // Step 2: fsync the data to durable storage.  Without this, a crash after
  // rename could leave a zero-length or corrupt file.
  if (::fsync(fd_) != 0) {
    int err = errno;
    // fclose will close the underlying fd_, so clear fd_ to prevent
    // double-close in Abort.
    if (file_) {
      std::fclose(file_);
      file_ = nullptr;
    } else {
      ::close(fd_);
    }
    fd_ = -1;
    std::error_code ec;
    std::filesystem::remove(tmp_path_, ec);
    THROW_IO_EXCEPTION("AtomicFileWriter::Commit: fsync failed for " +
                       tmp_path_ + ": " + std::strerror(err));
  }

  // Step 3: Close the fd.  If FILE* owns fd_, use fclose; otherwise close
  // directly.  After this point fd_ is invalid.
  if (file_) {
    std::fclose(file_);  // closes fd_ internally
    file_ = nullptr;
  } else {
    ::close(fd_);
  }
  fd_ = -1;

  // Step 4: Atomic rename — POSIX guarantees this is atomic on the same FS.
  std::error_code ec;
  std::filesystem::rename(tmp_path_, target_path_, ec);
  if (ec) {
    std::filesystem::remove(tmp_path_, ec);
    THROW_IO_EXCEPTION("AtomicFileWriter::Commit: rename " + tmp_path_ +
                       " -> " + target_path_ + " failed: " + ec.message());
  }

  // Step 5: fsync the parent directory so the directory entry is durable.
  auto parent_dir = std::filesystem::path(target_path_).parent_path().string();
  int dir_fd = ::open(parent_dir.c_str(), O_RDONLY);
  if (dir_fd >= 0) {
    ::fsync(dir_fd);
    ::close(dir_fd);
  }
}

void AtomicFileWriter::Abort() noexcept {
  ostream_.reset();
  // fclose closes the underlying fd_ (no dup), so we must not ::close(fd_)
  // again afterwards.
  if (file_) {
    std::fclose(file_);
    file_ = nullptr;
    fd_ = -1;  // already closed by fclose
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  if (!tmp_path_.empty()) {
    std::error_code ec;
    std::filesystem::remove(tmp_path_, ec);
  }
  committed_ = true;
}

bool link_or_copy(const std::string& src, const std::string& dst) {
  std::error_code ec;
  std::filesystem::create_hard_link(src, dst, ec);
  if (!ec) {
    return true;
  }
  // Cross-device or unsupported FS – fall back to a regular file copy.
  copy_file(src, dst, /*overwrite=*/false);
  return false;
}

void create_file(const std::string& path, size_t size) {
  // get dir
  std::filesystem::path dir = std::filesystem::path(path).parent_path();
  if (!dir.empty() && !std::filesystem::exists(dir)) {
    std::filesystem::create_directories(dir);
  }
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    throw std::runtime_error("Failed to create file: " + path);
  }
  int ret = ftruncate(fd, size);
  if (ret < 0) {
    ::close(fd);
    throw std::runtime_error("Failed to truncate file: " + path);
  }
  ::close(fd);
}

}  // namespace file_utils

void ensure_directory_exists(const std::string& dir_path) {
  if (dir_path.empty()) {
    LOG(ERROR) << "Error: Directory path is empty.";
    return;
  }
  std::filesystem::path dir(dir_path);
  if (!std::filesystem::exists(dir)) {
    std::filesystem::create_directories(dir);
    LOG(INFO) << "Directory created: " << dir_path;
  } else {
    LOG(INFO) << "Directory already exists: " << dir_path;
  }
}

bool read_string_from_file(const std::string& file_path, std::string& content) {
  std::ifstream inputFile(file_path);

  if (!inputFile.is_open()) {
    LOG(ERROR) << "Error: Could not open the file " << file_path;
    return false;
  }
  std::ostringstream buffer;
  buffer << inputFile.rdbuf();
  content = buffer.str();
  return true;
}

bool write_string_to_file(const std::string& content,
                          const std::string& file_path) {
  std::ofstream outputFile(file_path, std::ios::out | std::ios::trunc);

  if (!outputFile.is_open()) {
    LOG(ERROR) << "Error: Could not open the file " << file_path;
    return false;
  }
  outputFile << content;
  return true;
}

void copy_directory(const std::string& src, const std::string& dst,
                    bool overwrite, bool recursive) {
  if (!std::filesystem::exists(src)) {
    LOG(ERROR) << "Source file does not exist: " << src << std::endl;
    return;
  }
  if (overwrite && std::filesystem::exists(dst)) {
    std::filesystem::remove_all(dst);
  }
  std::filesystem::create_directory(dst);

  for (const auto& entry : std::filesystem::directory_iterator(src)) {
    const auto& path = entry.path();
    auto dest = std::filesystem::path(dst) / path.filename();
    if (std::filesystem::is_directory(path)) {
      if (recursive) {
        copy_directory(path.string(), dest.string(), overwrite, recursive);
      }
    } else if (std::filesystem::is_regular_file(path)) {
      std::error_code errorCode;
      std::filesystem::create_hard_link(path, dest, errorCode);
      if (errorCode) {
        LOG(ERROR) << "Failed to create hard link from " << path << " to "
                   << dest << " " << errorCode.message() << std::endl;
        THROW_IO_EXCEPTION("Failed to create hard link from " + path.string() +
                           " to " + dest.string() + " " + errorCode.message());
      }
    }
  }
}

void remove_directory(const std::string& dir_path) {
  if (std::filesystem::exists(dir_path)) {
    std::error_code errorCode;
    std::filesystem::remove_all(dir_path, errorCode);
    if (errorCode == std::errc::no_such_file_or_directory) {
      return;
    }
    if (errorCode) {
      LOG(ERROR) << "Failed to remove directory: " << dir_path << ", "
                 << errorCode.message();
      THROW_IO_EXCEPTION("Failed to remove directory: " + dir_path + ", " +
                         errorCode.message());
    }
  }
}

void read_file(const std::string& filename, void* buffer, size_t size,
               size_t num) {
  FILE* fin = fopen(filename.c_str(), "rb");
  if (fin == nullptr) {
    std::stringstream ss;
    ss << "Failed to open file " << filename << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  size_t ret_len = 0;
  if ((ret_len = fread(buffer, size, num, fin)) != num) {
    std::stringstream ss;
    ss << "Failed to read file " << filename << ", expected " << num << ", got "
       << ret_len << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  int ret = 0;
  if ((ret = fclose(fin)) != 0) {
    std::stringstream ss;
    ss << "Failed to close file " << filename << ", error code: " << ret << " "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
}

void write_file(const std::string& filename, const void* buffer, size_t size,
                size_t num) {
  FILE* fout = fopen(filename.c_str(), "wb");
  if (fout == nullptr) {
    std::stringstream ss;
    ss << "Failed to open file " << filename << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  size_t ret_len = 0;
  if ((ret_len = fwrite(buffer, size, num, fout)) != num) {
    std::stringstream ss;
    ss << "Failed to write file " << filename << ", expected " << num
       << ", got " << ret_len << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  int ret = 0;
  if ((ret = fclose(fout)) != 0) {
    std::stringstream ss;
    ss << "Failed to close file " << filename << ", error code: " << ret << " "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
}

void write_statistic_file(const std::string& filename, size_t capacity,
                          size_t size) {
  size_t buffer[2] = {capacity, size};
  write_file(filename, buffer, sizeof(size_t), 2);
}

void read_statistic_file(const std::string& filename, size_t& capacity,
                         size_t& size) {
  size_t buffer[2];
  read_file(filename, buffer, sizeof(size_t), 2);
  capacity = buffer[0];
  size = buffer[1];
}

}  // namespace neug
