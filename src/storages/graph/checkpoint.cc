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

#include "neug/storages/checkpoint.h"

#include <filesystem>
#include <string>

#include "neug/storages/container/container_utils.h"

#include <glog/logging.h>

namespace neug {

Checkpoint::Checkpoint(std::string path, uint32_t id)
    : path_(std::move(path)), id_(id) {}

void Checkpoint::create_dirs() const {
  assert(!IsEmpty());
  std::error_code ec;
  std::filesystem::create_directories(snapshot_dir(), ec);
  if (ec) {
    LOG(ERROR) << "Checkpoint::create_dirs: failed to create " << snapshot_dir()
               << ": " << ec.message();
  }
  std::filesystem::create_directories(runtime_dir(), ec);
  if (ec) {
    LOG(ERROR) << "Checkpoint::create_dirs: failed to create " << runtime_dir()
               << ": " << ec.message();
  }
  std::filesystem::create_directories(wal_dir(), ec);
  if (ec) {
    LOG(ERROR) << "Checkpoint::create_dirs: failed to create " << wal_dir()
               << ": " << ec.message();
  }
}

std::unique_ptr<IDataContainer> Checkpoint::CreateRuntimeContainer(
    size_t size, MemoryLevel level) {
  assert(!IsEmpty());
  std::string path = runtime_dir() + "/" + create_runtime_object();
  auto ret = OpenContainer("", path, level);
  ret->Resize(size);
  return ret;
}

SnapshotMeta Checkpoint::LoadMeta() {
  assert(!IsEmpty());
  SnapshotMeta meta;
  meta.Open(meta_path());
  return meta;
}

void Checkpoint::SaveMeta(SnapshotMeta meta) {
  assert(!IsEmpty());
  meta.Dump(meta_path());
}

std::string Checkpoint::create_runtime_object() {
  std::lock_guard<std::mutex> lock(mutex_);
  while (true) {
    std::string uuid = UUIDGenerator().generate();
    if (runtime_objects_.count(uuid) == 0 &&
        uncommitted_runtime_objects_.count(uuid) == 0) {
      uncommitted_runtime_objects_.insert(uuid);
      return uuid;
    }
  }
}

}  // namespace neug
