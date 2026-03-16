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

#include <assert.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"

#ifdef __ia64__
#define ADDR (void*) (0x8000000000000000UL)
#ifdef MAP_HUGETLB
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_FIXED)
#else
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED)
#endif
#else
#define ADDR (void*) (0x0UL)
#ifdef MAP_HUGETLB
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB)
#else
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS)
#endif
#endif

#define PROTECTION (PROT_READ | PROT_WRITE)

#define HUGEPAGE_SIZE (2UL * 1024 * 1024)
#define HUGEPAGE_MASK (2UL * 1024 * 1024 - 1UL)
#define ROUND_UP(size) (((size) + HUGEPAGE_MASK) & (~HUGEPAGE_MASK))

inline void* allocate_hugepages(size_t size) {
  return mmap(ADDR, ROUND_UP(size), PROTECTION, FLAGS, -1, 0);
}

inline size_t hugepage_round_up(size_t size) { return ROUND_UP(size); }

#undef ADDR
#undef FLAGS
#undef HUGEPAGE_SIZE
#undef HUGEPAGE_MASK
#undef ROUND_UP

namespace neug {

enum class MemoryStrategy {
  kSyncToFile,
  kMemoryOnly,
  kHugepagePrefered,
};

template <typename T>
class mmap_array {
 public:
  mmap_array()
      : filename_(""),
        fd_(-1),
        data_(NULL),
        size_(0),
        mmap_size_(0),
        sync_to_file_(false),
        hugepage_prefered_(false) {}

  mmap_array(const mmap_array<T>& rhs) : fd_(-1) {
    resize(rhs.size_);
    memcpy(data_, rhs.data_, size_ * sizeof(T));
  }

  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
  ~mmap_array() { reset(); }

  void reset(bool remove_file = true) {
    if (data_ != NULL && mmap_size_ != 0) {
      if (munmap(data_, mmap_size_) != 0) {
        std::stringstream ss;
        ss << "Failed to mummap file [ " << filename_ << " ] "
           << strerror(errno);
        LOG(ERROR) << ss.str();
        THROW_RUNTIME_ERROR(ss.str());
      }
    }
    data_ = NULL;
    size_ = 0;
    mmap_size_ = 0;
    if (fd_ != -1) {
      if (close(fd_) != 0) {
        std::stringstream ss;
        ss << "Failed to close file [ " << filename_ << " ] "
           << strerror(errno);
        LOG(ERROR) << ss.str();
        THROW_RUNTIME_ERROR(ss.str());
      }
      fd_ = -1;
    }
    if (sync_to_file_ && remove_file && !filename_.empty()) {
      std::error_code errorCode;
      std::filesystem::remove(filename_, errorCode);
      if (errorCode) {
        LOG(ERROR) << "Failed to remove file: " << filename_ << ", "
                   << errorCode.message();
      }
      filename_ = "";
      sync_to_file_ = false;
    }
  }

  void set_hugepage_prefered(bool val) {
    hugepage_prefered_ = (val && !sync_to_file_);
  }

  void open(const std::string& filename, bool sync_to_file = false,
            bool is_writable = true) {
    reset();
    filename_ = filename;
    sync_to_file_ = sync_to_file;
    is_writable_ = is_writable;
    hugepage_prefered_ = false;
    if (sync_to_file_) {
      bool creat = !std::filesystem::exists(filename_);
      fd_ = ::open(filename_.c_str(), O_RDWR | O_CREAT, 0777);
      if (fd_ == -1) {
        std::stringstream ss;
        ss << "Failed to open file [" << filename_ << "], " << strerror(errno);
        LOG(ERROR) << ss.str();
        THROW_RUNTIME_ERROR(ss.str());
      }
      if (creat) {
        std::filesystem::perms readWritePermission =
            std::filesystem::perms::owner_read |
            std::filesystem::perms::owner_write;
        std::error_code errorCode;
        std::filesystem::permissions(filename, readWritePermission,
                                     std::filesystem::perm_options::add,
                                     errorCode);
        if (errorCode) {
          std::stringstream ss;
          ss << "Failed to set read/write permission for file: " << filename
             << " " << errorCode.message() << std::endl;
          LOG(ERROR) << ss.str();
          THROW_RUNTIME_ERROR(ss.str());
        }
      }

      size_t file_size = std::filesystem::file_size(filename_);
      size_ = file_size / sizeof(T);
      mmap_size_ = file_size;
      if (mmap_size_ == 0) {
        data_ = NULL;
      } else {
        if (is_writable_) {
          data_ = reinterpret_cast<T*>(mmap(
              NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        } else {
          data_ = reinterpret_cast<T*>(mmap(
              NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd_, 0));
        }
        if (data_ == MAP_FAILED) {
          std::stringstream ss;
          ss << "Failed to mmap file [" << filename_ << "], "
             << strerror(errno);
          LOG(ERROR) << ss.str();
          THROW_RUNTIME_ERROR(ss.str());
        }
        int rt = madvise(data_, mmap_size_, MADV_RANDOM | MADV_WILLNEED);
        if (rt != 0) {
          std::stringstream ss;
          ss << "Failed to madvise file [" << filename_ << "], "
             << strerror(errno);
          LOG(ERROR) << ss.str();
          THROW_RUNTIME_ERROR(ss.str());
        }
      }
    } else {
      if (!filename_.empty() && std::filesystem::exists(filename_)) {
        size_t file_size = std::filesystem::file_size(filename_);
        fd_ = ::open(filename_.c_str(), O_RDWR, 0777);
        if (fd_ == -1) {
          std::stringstream ss;
          ss << "Failed to open file [" << filename_ << "], "
             << strerror(errno);
          LOG(ERROR) << ss.str();
          THROW_RUNTIME_ERROR(ss.str());
        }
        size_ = file_size / sizeof(T);
        mmap_size_ = file_size;
        if (mmap_size_ == 0) {
          data_ = NULL;
        } else {
          data_ = reinterpret_cast<T*>(mmap(
              NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd_, 0));
          if (data_ == MAP_FAILED) {
            std::stringstream ss;
            ss << "Failed to mmap file [" << filename_ << "], "
               << strerror(errno);
            LOG(ERROR) << ss.str();
            THROW_RUNTIME_ERROR(ss.str());
          }
        }
      }
    }
  }

  void open_with_hugepages(const std::string& filename) {
    reset();
    hugepage_prefered_ = true;
    is_writable_ = true;
    if (!filename.empty() && std::filesystem::exists(filename)) {
      size_t file_size = std::filesystem::file_size(filename);
      size_ = file_size / sizeof(T);
      if (size_ != 0) {
        mmap_size_ = hugepage_round_up(size_ * sizeof(T));
        data_ = static_cast<T*>(allocate_hugepages(mmap_size_));
        if (data_ != MAP_FAILED) {
          FILE* fin = fopen(filename.c_str(), "rb");
          if (fin == NULL) {
            std::stringstream ss;
            ss << "Failed to open file [ " << filename << " ], "
               << strerror(errno);
            LOG(ERROR) << ss.str();
            THROW_RUNTIME_ERROR(ss.str());
          }
          if (fread(data_, sizeof(T), size_, fin) != size_) {
            std::stringstream ss;
            ss << "Failed to fread file [ " << filename << " ], "
               << strerror(errno);
            LOG(ERROR) << ss.str();
            THROW_RUNTIME_ERROR(ss.str());
          }
          if (fclose(fin) != 0) {
            std::stringstream ss;
            ss << "Failed to fclose file [ " << filename << " ], "
               << strerror(errno);
            LOG(ERROR) << ss.str();
            THROW_RUNTIME_ERROR(ss.str());
          }
        } else {
          LOG(ERROR) << "allocating hugepage failed, " << strerror(errno)
                     << ", try with normal pages";
          data_ = NULL;
          open(filename, false);
        }
      } else {
        mmap_size_ = 0;
      }
    }
  }

  void dump(const std::string& filename) {
    if (sync_to_file_) {
      std::string old_filename = filename_;
      reset(false);
      std::error_code errorCode;
      std::filesystem::create_hard_link(old_filename, filename, errorCode);
      if (errorCode) {
        std::stringstream ss;
        ss << "Failed to create hard link from " << old_filename << " to "
           << filename << " " << errorCode.message() << std::endl;
        LOG(ERROR) << ss.str();
        THROW_RUNTIME_ERROR(ss.str());
      }
    } else {
      FILE* fout = fopen(filename.c_str(), "wb");
      if (fout == NULL) {
        std::stringstream ss;
        ss << "Failed to open file [ " << filename << " ], " << strerror(errno);
        LOG(ERROR) << ss.str();
        THROW_RUNTIME_ERROR(ss.str());
      }
      if (fwrite(data_, sizeof(T), size_, fout) != size_) {
        std::stringstream ss;
        ss << "Failed to fwrite file [ " << filename << " ], "
           << strerror(errno);
        LOG(ERROR) << ss.str();
        THROW_RUNTIME_ERROR(ss.str());
      }
      if (fflush(fout) != 0) {
        std::stringstream ss;
        ss << "Failed to fflush file [ " << filename << " ], "
           << strerror(errno);
        LOG(ERROR) << ss.str();
        THROW_RUNTIME_ERROR(ss.str());
      }
      if (fclose(fout) != 0) {
        std::stringstream ss;
        ss << "Failed to fclose file [ " << filename << " ], "
           << strerror(errno);
        LOG(ERROR) << ss.str();
        THROW_RUNTIME_ERROR(ss.str());
      }
      reset();
    }

    std::filesystem::perms readPermission = std::filesystem::perms::owner_read;

    std::error_code errorCode;
    std::filesystem::permissions(filename, readPermission,
                                 std::filesystem::perm_options::add, errorCode);

    if (errorCode) {
      std::stringstream ss;
      ss << "Failed to set read permission for file: " << filename << " "
         << errorCode.message() << std::endl;
      LOG(ERROR) << ss.str();
      THROW_RUNTIME_ERROR(ss.str());
    }
  }

  void resize(size_t size) {
    if (size == size_) {
      return;
    }

    if (sync_to_file_) {
      if (data_ != NULL && mmap_size_ != 0) {
        if (munmap(data_, mmap_size_) != 0) {
          std::stringstream ss;
          ss << "Failed to mummap file [ " << filename_ << " ], "
             << strerror(errno);
          LOG(ERROR) << ss.str();
          THROW_RUNTIME_ERROR(ss.str());
        }
      }
      size_t new_mmap_size = size * sizeof(T);
      int rt = ftruncate(fd_, new_mmap_size);
      if (rt == -1) {
        std::stringstream ss;
        ss << "Failed to ftruncate " << rt << ", " << strerror(errno);
        LOG(ERROR) << ss.str();
        THROW_RUNTIME_ERROR(ss.str());
      }
      if (new_mmap_size == 0) {
        data_ = NULL;
      } else {
        data_ = reinterpret_cast<T*>(mmap(
            NULL, new_mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        if (data_ == MAP_FAILED) {
          std::stringstream ss;
          ss << "Failed to mmap, " << strerror(errno);
          LOG(ERROR) << ss.str();
          THROW_RUNTIME_ERROR(ss.str());
        }
      }
      size_ = size;
      mmap_size_ = new_mmap_size;
    } else {
      size_t target_mmap_size = size * sizeof(T);
      if (target_mmap_size <= mmap_size_) {
        size_ = size;
      } else {
        T* new_data = NULL;
        size_t new_mmap_size = size * sizeof(T);
        if (hugepage_prefered_) {
          new_data = reinterpret_cast<T*>(allocate_hugepages(new_mmap_size));
          if (new_data == MAP_FAILED) {
            LOG(ERROR) << "mmap with hugepage failed, " << strerror(errno)
                       << ", try with normal pages";
            new_data = NULL;
          } else {
            new_mmap_size = hugepage_round_up(new_mmap_size);
          }
        }
        if (new_data == NULL) {
          new_data = reinterpret_cast<T*>(
              mmap(NULL, new_mmap_size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
          if (new_data == MAP_FAILED) {
            std::stringstream ss;
            ss << "mmap failed " << strerror(errno);
            LOG(ERROR) << ss.str();
            THROW_RUNTIME_ERROR(ss.str());
          }
        }

        size_t copy_size = std::min(size, size_);
        if (copy_size > 0 && data_ != NULL) {
          memcpy(reinterpret_cast<void*>(new_data),
                 reinterpret_cast<void*>(data_), copy_size * sizeof(T));
        }

        std::string old_filename = filename_;
        reset();

        filename_ = old_filename;
        data_ = new_data;
        size_ = size;
        mmap_size_ = new_mmap_size;
      }
    }
  }

  void touch(const std::string& filename) {
    dump(filename);
    open(filename, true);
  }

  T* data() { return data_; }
  const T* data() const { return data_; }

  void set(size_t idx, const T& val) { data_[idx] = val; }

  const T& get(size_t idx) const { return data_[idx]; }

  const T& operator[](size_t idx) const { return data_[idx]; }
  T& operator[](size_t idx) { return data_[idx]; }

  size_t size() const { return size_; }

  bool is_sync_to_file() const { return sync_to_file_; }

  void swap(mmap_array<T>& rhs) {
    std::swap(filename_, rhs.filename_);
    std::swap(fd_, rhs.fd_);
    std::swap(data_, rhs.data_);
    std::swap(size_, rhs.size_);
    std::swap(mmap_size_, rhs.mmap_size_);
    std::swap(hugepage_prefered_, rhs.hugepage_prefered_);
    std::swap(sync_to_file_, rhs.sync_to_file_);
  }

  const std::string& filename() const { return filename_; }

  void set_writable(bool is_writable) { is_writable_ = is_writable; }

  void ensure_writable(const std::string& work_dir) {
    if (is_writable_) {
      return;
    }
    std::string filename = std::filesystem::path(filename_).filename().string();
    std::string target_path = tmp_dir(work_dir) + "/" + filename;
    copy_file(filename_, target_path);
    reset();
    open(target_path, true, true);
  }

 private:
  std::string filename_;
  int fd_;
  T* data_;
  size_t size_;

  size_t mmap_size_;

  bool sync_to_file_;
  bool hugepage_prefered_;
  bool is_writable_ = true;
};

struct string_item {
  uint64_t offset : 48;
  uint32_t length : 16;
};

template <>
class mmap_array<std::string_view> {
 public:
  mmap_array() {}
  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
  ~mmap_array() {}

  void reset() {
    items_.reset();
    data_.reset();
  }

  void set_hugepage_prefered(bool val) {
    items_.set_hugepage_prefered(val);
    data_.set_hugepage_prefered(val);
  }

  void open(const std::string& filename, bool sync_to_file = false,
            bool is_writable = true) {
    is_writable_ = is_writable;
    items_.open(filename + ".items", sync_to_file, is_writable);
    data_.open(filename + ".data", sync_to_file, is_writable);
  }

  void open_with_hugepages(const std::string& filename) {
    items_.open_with_hugepages(filename + ".items");
    data_.open_with_hugepages(filename + ".data");
  }

  void touch(const std::string& filename) {
    items_.touch(filename + ".items");
    data_.touch(filename + ".data");
  }

  void dump(const std::string& filename) {
    // Compact before dumping to reclaim unused space
    auto plan = prepare_compaction_plan();
    bool should_stream =
        !data_.is_sync_to_file() && plan.total_size < data_.size();
    if (should_stream) {
      stream_compact_and_dump(plan, filename + ".data", filename + ".items");
      return;
    }

    compact();
    items_.dump(filename + ".items");
    data_.dump(filename + ".data");
  }

  void resize(size_t size, size_t data_size) {
    items_.resize(size);
    data_.resize(data_size);
  }

  size_t avg_size() const {
    if (items_.size() == 0) {
      return 0;
    }
    size_t total_length = 0;
    size_t non_zero_count = 0;
    for (size_t i = 0; i < items_.size(); ++i) {
      if (items_.get(i).length > 0) {
        ++non_zero_count;
        total_length += items_.get(i).length;
      }
    }
    return non_zero_count > 0
               ? (total_length + non_zero_count - 1) / non_zero_count
               : 0;
  }

  void set(size_t idx, size_t offset, const std::string_view& val) {
    items_.set(idx, {offset, static_cast<uint32_t>(val.size())});
    assert(data_.data() + offset + val.size() <= data_.data() + data_.size());
    memcpy(data_.data() + offset, val.data(), val.size());
  }

  std::string_view get(size_t idx) const {
    const string_item& item = items_.get(idx);
    return std::string_view(data_.data() + item.offset, item.length);
  }

  size_t size() const { return items_.size(); }

  size_t data_size() const { return data_.size(); }

  void swap(mmap_array& rhs) {
    items_.swap(rhs.items_);
    data_.swap(rhs.data_);
  }

  void set_writable(bool is_writable) {
    items_.set_writable(is_writable);
    data_.set_writable(is_writable);
    is_writable_ = is_writable;
  }

  void ensure_writable(const std::string& work_dir) {
    if (is_writable_) {
      return;
    }
    items_.ensure_writable(work_dir);
    data_.ensure_writable(work_dir);
    is_writable_ = true;
  }

  // Compact the data buffer by removing unused space and updating offsets
  // This is an in-place operation that shifts valid string data forward
  // Returns the compacted data size. Note that the reserved size of data buffer
  // is not changed, and new strings can still be appended after the compacted
  // data.
  size_t compact() {
    auto plan = prepare_compaction_plan();
    if (items_.size() == 0) {
      return 0;
    }
    size_t size_before_compact = data_.size();
    if (plan.total_size == size_before_compact) {
      return size_before_compact;
    }

    std::vector<char> temp_buf(plan.total_size);
    size_t write_offset = 0;
    size_t limit_offset = 0;
    for (const auto& entry : plan.entries) {
      const char* src = data_.data() + entry.offset;
      char* dst = temp_buf.data() + write_offset;
      limit_offset = std::max(limit_offset, entry.offset + entry.length);
      memcpy(dst, src, entry.length);
      items_.set(entry.index,
                 {static_cast<uint64_t>(write_offset), entry.length});
      write_offset += entry.length;
    }
    assert(write_offset == plan.total_size);
    memcpy(data_.data(), temp_buf.data(), plan.total_size);

    VLOG(1) << "Compaction completed. New data size: " << plan.total_size
            << ", old data size: " << limit_offset;
    return plan.total_size;
  }

 private:
  struct CompactionPlan {
    struct Entry {
      size_t index;
      uint64_t offset;
      uint32_t length;
    };
    std::vector<Entry> entries;
    size_t total_size = 0;
  };

  CompactionPlan prepare_compaction_plan() const {
    CompactionPlan plan;
    plan.entries.reserve(items_.size());
    for (size_t i = 0; i < items_.size(); ++i) {
      const string_item& item = items_.get(i);
      plan.total_size += item.length;
      plan.entries.push_back({i, item.offset, item.length});
    }
    return plan;
  }

  void stream_compact_and_dump(const CompactionPlan& plan,
                               const std::string& data_filename,
                               const std::string& items_filename) {
    size_t size_before_compact = data_.size();
    FILE* fout = fopen(data_filename.c_str(), "wb");
    if (fout == NULL) {
      std::stringstream ss;
      ss << "Failed to open file [ " << data_filename << " ], "
         << strerror(errno);
      LOG(ERROR) << ss.str();
      THROW_RUNTIME_ERROR(ss.str());
    }

    size_t write_offset = 0;
    for (const auto& entry : plan.entries) {
      if (entry.length > 0) {
        const char* src = data_.data() + entry.offset;
        if (fwrite(src, 1, entry.length, fout) != entry.length) {
          std::stringstream ss;
          ss << "Failed to fwrite file [ " << data_filename << " ], "
             << strerror(errno);
          LOG(ERROR) << ss.str();
          THROW_RUNTIME_ERROR(ss.str());
        }
      }
      items_.set(entry.index,
                 {static_cast<uint64_t>(write_offset), entry.length});
      write_offset += entry.length;
    }
    assert(write_offset == plan.total_size);

    if (fflush(fout) != 0) {
      std::stringstream ss;
      ss << "Failed to fflush file [ " << data_filename << " ], "
         << strerror(errno);
      LOG(ERROR) << ss.str();
      THROW_RUNTIME_ERROR(ss.str());
    }
    int fd = fileno(fout);
    if (fd == -1) {
      std::stringstream ss;
      ss << "Failed to get file descriptor for [ " << data_filename << " ], "
         << strerror(errno);
      LOG(ERROR) << ss.str();
      THROW_RUNTIME_ERROR(ss.str());
    }
    if (ftruncate(fd, size_before_compact) != 0) {
      std::stringstream ss;
      ss << "Failed to ftruncate file [ " << data_filename << " ], "
         << strerror(errno);
      LOG(ERROR) << ss.str();
      THROW_RUNTIME_ERROR(ss.str());
    }
    if (fclose(fout) != 0) {
      std::stringstream ss;
      ss << "Failed to fclose file [ " << data_filename << " ], "
         << strerror(errno);
      LOG(ERROR) << ss.str();
      THROW_RUNTIME_ERROR(ss.str());
    }

    data_.reset();
    std::filesystem::perms readPermission = std::filesystem::perms::owner_read;
    std::error_code errorCode;
    std::filesystem::permissions(data_filename, readPermission,
                                 std::filesystem::perm_options::add, errorCode);
    if (errorCode) {
      std::stringstream ss;
      ss << "Failed to set read permission for file: " << data_filename << " "
         << errorCode.message() << std::endl;
      LOG(ERROR) << ss.str();
      THROW_RUNTIME_ERROR(ss.str());
    }

    VLOG(1) << "Compaction completed. New data size: " << plan.total_size
            << ", old data size: " << size_before_compact;
    items_.dump(items_filename);
  }

  mmap_array<string_item> items_;
  mmap_array<char> data_;
  bool is_writable_ = true;
};

}  // namespace neug
