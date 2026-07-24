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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <optional>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "neug/storages/checkpoint.h"
#include "neug/storages/container/mmap_container.h"
#include "neug/utils/exception/exception.h"

namespace neug::csr_dump {

inline void validate_edge_count(uint64_t expected, size_t actual) {
  if (actual == expected) {
    return;
  }
  LOG(WARNING) << "Inconsistent edge count"
               << ": expected " << expected << ", actual " << actual;
  THROW_STORAGE_EXCEPTION("Inconsistent edge count: expected " +
                          std::to_string(expected) + ", actual " +
                          std::to_string(actual));
}

class ChecksumReuseFileDumper {
 public:
  ChecksumReuseFileDumper(Checkpoint& ckp, const IDataContainer* container)
      : ckp_(ckp) {
    const auto* mapped = dynamic_cast<const MMapContainer*>(container);
    const auto* mapped_header =
        mapped == nullptr ? nullptr : mapped->GetHeader();
    if (mapped != nullptr && !mapped->GetPath().empty() &&
        mapped_header != nullptr) {
      FileHeader zeroed{};
      if (memcmp(mapped_header, &zeroed, sizeof(FileHeader)) != 0) {
        reusable_source_ = mapped;
      }
    }

    MD5_Init(&md5_);
    if (reusable_source_ == nullptr) {
      open_output();
    }
  }

  void AddSegment(const char* data, size_t len) {
    if (len == 0) {
      return;
    }
    MD5_Update(&md5_, data, len);
    if (reusable_source_ == nullptr) {
      WriteSegment(data, len);
    }
  }

  void operator()(const char* data, size_t len) { AddSegment(data, len); }

  bool BeginRewriteIfChanged() {
    MD5_Final(header_.data_md5, &md5_);
    if (reusable_source_ != nullptr &&
        memcmp(reusable_source_->GetHeader()->data_md5, header_.data_md5,
               sizeof(header_.data_md5)) != 0) {
      open_output();
    }
    return reusable_source_ != nullptr && runtime_file_.has_value();
  }

  void WriteSegment(const char* data, size_t len) {
    if (len != 0) {
      out_.write(data, static_cast<std::streamsize>(len));
    }
  }

  std::string CommitOrReuse() {
    if (!runtime_file_.has_value()) {
      return ckp_.LinkToSnapshot(reusable_source_->GetPath());
    }
    out_.seekp(0);
    out_.write(reinterpret_cast<const char*>(&header_), sizeof(header_));
    out_.flush();
    if (!out_.good()) {
      THROW_IO_EXCEPTION("Failed to flush file: " + runtime_file_->path());
    }
    out_.close();
    return ckp_.CommitRuntimeFile(std::move(*runtime_file_));
  }

 private:
  void open_output() {
    runtime_file_.emplace(ckp_.CreateRuntimeFile());
    const auto& path = runtime_file_->path();
    out_.open(path, std::ios::binary);
    if (!out_.is_open()) {
      THROW_IO_EXCEPTION("Failed to open file for writing: " + path);
    }
    out_.seekp(sizeof(header_));
  }

  Checkpoint& ckp_;
  const MMapContainer* reusable_source_{nullptr};
  FileHeader header_{};
  MD5_CTX md5_;
  std::optional<CheckpointFileManager::RuntimeFileHandle> runtime_file_;
  std::ofstream out_;
};

}  // namespace neug::csr_dump
