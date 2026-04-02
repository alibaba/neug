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

#include "neug/utils/file_utils.h"

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
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
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
 * @brief Try to use FICLONE ioctl for reflink copy.
 *
 * Supported filesystems: Btrfs, XFS (with reflink=1), OCFS2
 * This is the most efficient method - creates instant COW clone.
 */
static bool try_reflink(const std::string& src_path,
                        const std::string& dst_path,
                        const struct stat& src_stat) {
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

// FICLONE: Clone entire file using COW
#ifdef FICLONE
  int ret = ::ioctl(dst_fd, FICLONE, src_fd);
#else
  int ret = -1;  // FICLONE not supported
#endif

  ::close(src_fd);
  ::close(dst_fd);

  if (ret == 0) {
    copy_metadata(src_stat, dst_path);
    return true;
  }

  // Failed - remove partially created file
  ::unlink(dst_path.c_str());
  return false;
}

/**
 * @brief Try to use copy_file_range() syscall.
 *
 * Available on Linux 4.5+. May utilize COW on supported filesystems.
 * Performs server-side copy without data passing through userspace.
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

/**
 * @brief Traditional file copy using read/write with buffer.
 *
 * Fallback method that works on all filesystems.
 * Uses 64KB buffer for reasonable performance.
 */
static void fallback_copy(const std::string& src_path,
                          const std::string& dst_path,
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

  constexpr size_t BUFFER_SIZE = 64 * 1024;  // 64KB buffer
  std::unique_ptr<char[]> buffer(new char[BUFFER_SIZE]);

  ssize_t bytes_read;
  while ((bytes_read = ::read(src_fd, buffer.get(), BUFFER_SIZE)) > 0) {
    ssize_t bytes_written = 0;
    while (bytes_written < bytes_read) {
      ssize_t written = ::write(dst_fd, buffer.get() + bytes_written,
                                bytes_read - bytes_written);
      if (written < 0) {
        ::close(src_fd);
        ::close(dst_fd);
        ::unlink(dst_path.c_str());
        throw std::runtime_error("Failed to write to destination file: " +
                                 dst_path);
      }
      bytes_written += written;
    }
  }

  ::close(src_fd);
  ::close(dst_fd);

  if (bytes_read < 0) {
    ::unlink(dst_path.c_str());
    throw std::runtime_error("Failed to read from source file: " + src_path);
  }

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

  // Try copy_file_range - may use server-side COW
  if (try_copy_file_range(src_path, dst_path, src_stat)) {
    return CopyResult::CopyFileRange;
  }

  // Fallback to traditional copy
  fallback_copy(src_path, dst_path, src_stat);
  return CopyResult::FallbackCopy;
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
