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

#include <memory>
#include <ostream>
#include <string>

namespace neug {

namespace file_utils {

/**
 * @brief RAII wrapper for crash-safe atomic file writes.
 *
 * Writes go to a temporary file (`target + ".tmp"`) in the same directory
 * (same filesystem) so the final rename is guaranteed to be POSIX-atomic.
 *
 * Lifecycle:
 *   1. Construct with target path — opens the tmp file, acquires fd.
 *   2. Write via `fd()` (low-level) or `stream()` (C++ ostream).
 *   3. Call `Commit()` — flushes stream, fsync(fd), closes fd,
 *      rename(tmp → target), fsync(parent dir).
 *   4. If the writer is destroyed without `Commit()`, the tmp file is
 *      removed automatically (abort semantics).
 *
 * Thread safety: not thread-safe; a single writer per instance.
 */
class AtomicFileWriter {
 public:
  explicit AtomicFileWriter(const std::string& target_path);
  ~AtomicFileWriter();

  AtomicFileWriter(const AtomicFileWriter&) = delete;
  AtomicFileWriter& operator=(const AtomicFileWriter&) = delete;
  AtomicFileWriter(AtomicFileWriter&& other) noexcept;
  AtomicFileWriter& operator=(AtomicFileWriter&& other) noexcept;

  /// Raw POSIX file descriptor — valid until Commit() or destruction.
  int fd() const { return fd_; }

  /// C++ ostream backed by the same fd.  Lazily created on first call.
  std::ostream& stream();

  /// Flush + fsync(fd) + close + rename(tmp → target) + fsync(parent dir).
  /// Throws on any I/O failure.  After Commit() the writer is spent.
  void Commit();

 private:
  void Abort() noexcept;

  std::string target_path_;
  std::string tmp_path_;
  int fd_ = -1;
  bool committed_ = false;

  // Lazily created ostream that wraps fd_ via a FILE*.
  FILE* file_ = nullptr;
  struct OStreamDeleter {
    void operator()(std::ostream* os) const { delete os; }
  };
  std::unique_ptr<std::ostream, OStreamDeleter> ostream_;
};

/**
 * @brief File copy utility with Copy-on-Write (COW) optimization.
 *
 * This utility automatically selects the best copy method based on OS and
 * filesystem support:
 *   1. Reflink (FICLONE) - Instant COW clone, zero physical copy
 *   2. copy_file_range() - Server-side copy, may use COW on supported FS
 *   3. Traditional copy - Fallback using read/write with buffer
 *
 * ============================================================================
 * HOW TO CHECK IF A FILESYSTEM SUPPORTS REFLINK (COW):
 * ============================================================================
 *
 * 1. Check filesystem type of a directory:
 *    $ df -T /path/to/directory
 *    $ stat -f -c %T /path/to/directory
 *
 * 2. Filesystems with reflink support:
 *    - Btrfs: Full reflink support (enabled by default)
 *    - XFS: Requires reflink=1 at mkfs time (Linux 4.16+)
 *      Check: $ xfs_info /mount/point | grep reflink
 *    - OCFS2: Supports reflink
 *    - ext4: Does NOT support reflink
 *    - tmpfs: Does NOT support reflink
 *
 * 3. Quick test if reflink works on a directory:
 *    $ cp --reflink=always /path/to/testfile /path/to/testfile.clone
 *    If it succeeds, reflink is supported.
 *    If it fails with "Operation not supported", reflink is not available.
 *
 * 4. Create a test to verify COW behavior:
 *    $ filefrag -v original_file
 *    $ filefrag -v cloned_file
 *    If they share the same physical extents, COW is working.
 *
 * 5. Check XFS reflink status:
 *    $ xfs_info /mount/point | grep -i reflink
 *    Output "reflink=1" means reflink is enabled.
 *
 * 6. For Btrfs, check if files share extents:
 *    $ btrfs filesystem du /path/to/files
 *    The "Shared" column shows COW-shared data.
 *
 * ============================================================================
 */
enum class CopyResult {
  Reflink,        // Used reflink (FICLONE) - true COW
  CopyFileRange,  // Used copy_file_range() - may use COW on some FS
  FallbackCopy    // Used traditional read/write copy
};
CopyResult copy_file(const std::string& src_path, const std::string& dst_path,
                     bool overwrite);

void create_file(const std::string& path, size_t size);

}  // namespace file_utils

void ensure_directory_exists(const std::string& dir_path);

bool read_string_from_file(const std::string& file_path, std::string& content);

bool write_string_to_file(const std::string& content,
                          const std::string& file_path);

/**
 * Copy file from src to dst.
 * @param src source file path
 * @param dst destination file path
 * @param overwrite whether to clean up the dst file if it already exists
 * @param recursive whether to copy directories recursively
 */
void copy_directory(const std::string& src, const std::string& dst,
                    bool overwrite = false, bool recursive = true);

void remove_directory(const std::string& dir_path);

void write_file(const std::string& filename, const void* buffer, size_t size,
                size_t num);

void read_file(const std::string& filename, void* buffer, size_t size,
               size_t num);

void write_statistic_file(const std::string& file_path, size_t capacity,
                          size_t size);

void read_statistic_file(const std::string& file_path, size_t& capacity,
                         size_t& size);

}  // namespace neug
