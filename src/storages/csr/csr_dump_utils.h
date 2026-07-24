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
#include <string>
#include <utility>

#include <glog/logging.h>

#include "neug/storages/checkpoint.h"
#include "neug/storages/container/mmap_container.h"
#include "neug/utils/exception/exception.h"

namespace neug::csr_dump {

struct Snapshot {
  std::string path;
  FileHeader header{};
};

inline Snapshot get_snapshot(const IDataContainer* container) {
  Snapshot snapshot;
  const auto* mapped = dynamic_cast<const MMapContainer*>(container);
  const auto* header = mapped == nullptr ? nullptr : mapped->GetHeader();
  if (mapped == nullptr || mapped->GetPath().empty() || header == nullptr) {
    return snapshot;
  }
  FileHeader zeroed{};
  if (memcmp(header, &zeroed, sizeof(FileHeader)) != 0) {
    snapshot.path = mapped->GetPath();
    snapshot.header = *header;
  }
  return snapshot;
}

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

inline void write_segment(std::ofstream& out, const char* data, size_t len) {
  if (len != 0) {
    out.write(data, static_cast<std::streamsize>(len));
  }
}

template <typename WRITE_DATA>
std::string commit_snapshot(Checkpoint& ckp, FileHeader& header,
                            bool rewrite_header, WRITE_DATA&& write_data) {
  auto runtime_file = ckp.CreateRuntimeFile();
  const auto& path = runtime_file.path();
  std::ofstream out(path, std::ios::binary);
  if (!out.is_open()) {
    THROW_IO_EXCEPTION("Failed to open file for writing: " + path);
  }
  out.write(reinterpret_cast<const char*>(&header), sizeof(header));
  write_data(out);
  if (rewrite_header) {
    out.seekp(0);
    out.write(reinterpret_cast<const char*>(&header), sizeof(header));
  }
  out.flush();
  if (!out.good()) {
    THROW_IO_EXCEPTION("Failed to flush file: " + path);
  }
  out.close();
  return ckp.CommitRuntimeFile(std::move(runtime_file));
}

template <typename NORMALIZE, typename WRITE_DATA>
std::string dump_normalized_file(Checkpoint& ckp,
                                 const IDataContainer* container,
                                 NORMALIZE&& normalize,
                                 WRITE_DATA&& write_data) {
  Snapshot source = get_snapshot(container);
  FileHeader header{};
  MD5_CTX ctx;
  auto normalize_and_hash = [&](auto&& sink) {
    MD5_Init(&ctx);
    auto hash = [&](const char* data, size_t len) {
      if (len != 0) {
        MD5_Update(&ctx, data, len);
      }
      sink(data, len);
    };
    normalize(hash);
    MD5_Final(header.data_md5, &ctx);
  };

  if (source.path.empty()) {
    return commit_snapshot(ckp, header, true, [&](std::ofstream& out) {
      normalize_and_hash(
          [&](const char* data, size_t len) { write_segment(out, data, len); });
    });
  }

  normalize_and_hash([](const char*, size_t) {});
  if (memcmp(source.header.data_md5, header.data_md5,
             sizeof(header.data_md5)) == 0) {
    return ckp.LinkToSnapshot(source.path);
  }
  return commit_snapshot(ckp, header, false,
                         std::forward<WRITE_DATA>(write_data));
}

}  // namespace neug::csr_dump
