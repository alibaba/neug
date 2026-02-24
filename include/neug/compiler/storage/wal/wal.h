/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <cstdint>
#include <unordered_set>

#include "neug/compiler/common/enums/rel_direction.h"
#include "neug/compiler/common/file_system/file_info.h"
#include "neug/compiler/common/serializer/buffered_file.h"
#include "neug/compiler/storage/wal/wal_record.h"

namespace neug {
namespace binder {
struct BoundAlterInfo;
struct BoundCreateTableInfo;
}  // namespace binder
namespace common {
class BufferedFileWriter;
class VirtualFileSystem;
class ValueVector;
}  // namespace common

namespace catalog {
class CatalogEntry;
struct SequenceRollbackData;
}  // namespace catalog

namespace storage {
class WALReplayer;
class WAL {
  friend class WALReplayer;

 public:
  WAL() {}
  WAL(const std::string& directory, bool readOnly,
      common::VirtualFileSystem* vfs, main::ClientContext* context) {}

  ~WAL() = default;

  void logCreateCatalogEntryRecord(catalog::CatalogEntry* catalogEntry,
                                   bool isInternal);
  void logCreateCatalogEntryRecord(
      catalog::CatalogEntry* catalogEntry,
      std::vector<catalog::CatalogEntry*> childrenEntries, bool isInternal);
  void logDropCatalogEntryRecord(uint64_t tableID,
                                 catalog::CatalogEntryType type);
  void logAlterCatalogEntryRecord(const binder::BoundAlterInfo* alterInfo);
  void logUpdateSequenceRecord(common::sequence_id_t sequenceID,
                               uint64_t kCount);

  void logTableInsertion(common::table_id_t tableID,
                         common::TableType tableType, common::row_idx_t numRows,
                         const std::vector<common::ValueVector*>& vectors);
  void logNodeDeletion(common::table_id_t tableID, common::offset_t nodeOffset,
                       common::ValueVector* pkVector);
  void logNodeUpdate(common::table_id_t tableID, common::column_id_t columnID,
                     common::offset_t nodeOffset,
                     common::ValueVector* propertyVector);
  void logRelDelete(common::table_id_t tableID,
                    common::ValueVector* srcNodeVector,
                    common::ValueVector* dstNodeVector,
                    common::ValueVector* relIDVector);
  void logRelDetachDelete(common::table_id_t tableID,
                          common::RelDataDirection direction,
                          common::ValueVector* srcNodeVector);
  void logRelUpdate(common::table_id_t tableID, common::column_id_t columnID,
                    common::ValueVector* srcNodeVector,
                    common::ValueVector* dstNodeVector,
                    common::ValueVector* relIDVector,
                    common::ValueVector* propertyVector);
  void logCopyTableRecord(common::table_id_t tableID);

  void logBeginTransaction();
  void logAndFlushCommit();
  void logRollback();
  void logAndFlushCheckpoint();

  void clearWAL();

  void flushAllPages();

  void addToUpdatedTables(const common::table_id_t nodeTableID) {
    updatedTables.insert(nodeTableID);
  }
  std::unordered_set<common::table_id_t>& getUpdatedTables() {
    return updatedTables;
  }

  uint64_t getFileSize() const { return bufferedWriter->getFileSize(); }

 private:
  void addNewWALRecordNoLock(const WALRecord& walRecord);

 private:
  std::unordered_set<common::table_id_t> updatedTables;
  std::unique_ptr<common::FileInfo> fileInfo;
  std::shared_ptr<common::BufferedFileWriter> bufferedWriter;
  std::string directory;
  std::mutex mtx;
  common::VirtualFileSystem* vfs;
};

}  // namespace storage
}  // namespace neug
