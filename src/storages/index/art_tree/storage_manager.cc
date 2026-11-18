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

#include "neug/storages/index/art_tree/storage_manager.h"

#include "neug/storages/container/file_header.h"

namespace neug::art {

ARTStorageManager::ARTStorageManager(Checkpoint& checkpoint)
    : checkpoint_(checkpoint),
      file_(checkpoint.CreateRuntimeFile()),
      output_(fopen(file_.path().c_str(), "wb"), &fclose) {
  if (!output_)
    THROW_IO_EXCEPTION("Cannot open ART object: " + file_.path());
  // Reserve the header; finalize its checksum after writing the payload.
  FileHeader header{};
  Write(&header, sizeof(header));
  checksum_.MD5Init();
}

void ARTStorageManager::Write(const void* data, size_t length) {
  if (fwrite(data, 1, length, output_.get()) != length)
    THROW_IO_EXCEPTION("Cannot write ART object: " + file_.path());
}

ARTStorageSlice ARTStorageManager::Append(const void* data, idx_t length) {
  if (!output_)
    throw InternalException("Cannot append to a committed ART object");
  ARTStorageSlice slice{offset_, length, ComputeMD5(data, length)};
  // Write directly from the live buffer and track the whole payload checksum.
  Write(data, length);
  UpdateMD5(checksum_, data, length);
  offset_ += length;
  return slice;
}

string ARTStorageManager::Commit() {
  if (!output_)
    throw InternalException("ART object is already committed");
  // Finalize the header and close the file before publishing the object.
  FileHeader header{};
  checksum_.MD5Final(header.data_md5);
  if (fseek(output_.get(), 0, SEEK_SET) != 0)
    THROW_IO_EXCEPTION("Cannot seek ART object: " + file_.path());
  Write(&header, sizeof(header));
  if (fclose(output_.release()) != 0)
    THROW_IO_EXCEPTION("Cannot close ART object: " + file_.path());
  return checkpoint_.CommitRuntimeFile(std::move(file_));
}

}  // namespace neug::art
