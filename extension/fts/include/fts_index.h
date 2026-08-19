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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "fts_sqlite.h"
#include "neug/storages/checkpoint_file_manager.h"
#include "neug/storages/index/storage_index.h"

namespace neug::fts_ext {

enum class FTSScoreOrder { kAscending, kDescending };

struct FTSQueryParams final : IndexQueryParams {
  std::vector<vid_t> scalar_filter;
  bool use_scalar_filter{false};
  std::string query_string;
  std::optional<uint64_t> limit;
  FTSScoreOrder order{FTSScoreOrder::kAscending};
};

class FTSDumpContainer final : public IDataContainer {
 public:
  FTSDumpContainer(std::shared_ptr<SQLiteConnection> read_connection,
                   std::shared_ptr<SQLiteConnection> write_connection,
                   std::string runtime_path);

  ContainerType GetContainerType() const override {
    return ContainerType::kFileSharedMMap;
  }
  void Resize(size_t) override {}
  std::string GetPath() const override { return runtime_path_; }
  void Open(const std::string&) override {}
  void Close() override;
  void Sync() override;
  void Dump(const std::string& new_path) override;
  bool IsDirty() override { return true; }
  std::shared_ptr<IDataContainer> Fork(Checkpoint&, MemoryLevel) override;

 private:
  std::shared_ptr<SQLiteConnection> read_connection_;
  std::shared_ptr<SQLiteConnection> write_connection_;
  std::string runtime_path_;
};

class FTSIndex final : public StorageIndex {
 public:
  ~FTSIndex() override;

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override;
  void Open(Checkpoint& ckp, const CheckpointManifest& manifest,
            const ModuleDescriptor& descriptor, MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& manifest,
            const std::string& key) override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;
  std::unique_ptr<Module> Clone() const override;

  Status Rebind(const IndexBindContext& context) override;
  Status BulkBuild(const VertexSet& vertices) override;

  static std::string type_name() { return "fts_index"; }

 protected:
  result<std::vector<SearchCandidate>> SearchImpl(
      const IndexQueryParams& params) override;
  Status AppendImpl(index_id_t index_id, const Value& value) override;

 private:
  void ParseOptions();
  void OpenInternal(Checkpoint& ckp, const CheckpointManifest* manifest,
                    const ModuleDescriptor& descriptor, MemoryLevel level);
  void CreateTable();
  void ValidateExistingTable();
  void PrepareStatements();
  void FinalizeStatements();

  static constexpr const char* kIndexFilePath = "fts_file";
  static constexpr const char* kAccessorRef = "index_id_accessor";

  std::shared_ptr<SQLiteConnection> read_connection_{
      std::make_shared<SQLiteConnection>()};
  std::shared_ptr<SQLiteConnection> write_connection_{
      std::make_shared<SQLiteConnection>()};
  std::shared_ptr<SQLiteStatement> search_asc_statement_{
      std::make_shared<SQLiteStatement>()};
  std::shared_ptr<SQLiteStatement> search_desc_statement_{
      std::make_shared<SQLiteStatement>()};
  std::shared_ptr<SQLiteStatement> append_statements_{
      std::make_shared<SQLiteStatement>()};
  std::string runtime_path_;
  std::shared_ptr<CheckpointFileManager::RuntimeFileHandle> runtime_file_;
  std::string table_name_;
  std::string tokenizer_{"unicode61"};
  std::string prefix_;
  std::string detail_{"full"};
  const ColumnBase* bound_column_{nullptr};
};

}  // namespace neug::fts_ext
