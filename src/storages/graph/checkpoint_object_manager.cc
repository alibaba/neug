/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/storages/checkpoint_object_manager.h"

#include <limits>
#include <utility>

#include "neug/storages/checkpoint.h"
#include "neug/storages/container/file_header.h"
#include "neug/utils/exception/exception.h"

namespace neug {

const void* CheckpointObjectReader::Get(const std::string& address,
                                        const CheckpointObjectSlice& slice) {
  auto entry = objects_.find(address);
  if (entry == objects_.end()) {
    auto object = checkpoint_.OpenFile(address, MemoryLevel::kInMemory);
    if (!object->GetDataSize())
      THROW_CHECKPOINT_EXCEPTION("Checkpoint object has no payload: " +
                                 address);
    if (object->IsDirty())
      THROW_CHECKPOINT_EXCEPTION("Checkpoint object checksum mismatch: " +
                                 address);
    entry = objects_.emplace(address, std::move(object)).first;
  }
  const auto& object = entry->second;
  const auto size = object->GetDataSize();
  if (!slice.length || slice.offset > size ||
      slice.length > size - slice.offset)
    THROW_CHECKPOINT_EXCEPTION("Slice is outside checkpoint object: " +
                               address);
  return static_cast<const char*>(object->GetData()) + slice.offset;
}

CheckpointObjectWriter::CheckpointObjectWriter(Checkpoint& checkpoint)
    : checkpoint_(checkpoint),
      file_(checkpoint.CreateRuntimeFile()),
      output_(fopen(file_.path().c_str(), "wb"), &fclose) {
  if (!output_)
    THROW_IO_EXCEPTION("Cannot open checkpoint object: " + file_.path());
  // Reserve the header; finalize its checksum after writing the payload.
  FileHeader header{};
  Write(&header, sizeof(header));
  checksum_.MD5Init();
}

void CheckpointObjectWriter::Write(const void* data, size_t length) {
  if (fwrite(data, 1, length, output_.get()) != length) {
    output_.reset();
    THROW_IO_EXCEPTION("Cannot write checkpoint object: " + file_.path());
  }
}

void CheckpointObjectWriter::Append(const void* data, size_t length,
                                    CommitCallback on_commit) {
  if (!output_)
    THROW_INTERNAL_EXCEPTION("Checkpoint object writer is closed");
  if (!data || !length || length > std::numeric_limits<size_t>::max() - offset_)
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid checkpoint object fragment");
  if (!on_commit)
    THROW_INVALID_ARGUMENT_EXCEPTION("Missing checkpoint object callback");
  fragments_.push_back({{offset_, length}, std::move(on_commit)});
  Write(data, length);
  UpdateMD5(checksum_, data, length);
  offset_ += length;
}

std::string CheckpointObjectWriter::Commit() {
  if (!output_)
    THROW_INTERNAL_EXCEPTION("Checkpoint object writer is closed");
  if (!offset_) {
    output_.reset();
    return {};
  }
  // Finalize the header and close the file before publishing the object.
  FileHeader header{};
  checksum_.MD5Final(header.data_md5);
  if (fseek(output_.get(), 0, SEEK_SET) != 0) {
    output_.reset();
    THROW_IO_EXCEPTION("Cannot seek checkpoint object: " + file_.path());
  }
  Write(&header, sizeof(header));
  if (fclose(output_.release()) != 0)
    THROW_IO_EXCEPTION("Cannot close checkpoint object: " + file_.path());
  const auto address = checkpoint_.CommitRuntimeFile(std::move(file_));
  auto fragments = std::move(fragments_);
  for (const auto& fragment : fragments)
    fragment.on_commit(address, fragment.slice);
  return address;
}

}  // namespace neug
