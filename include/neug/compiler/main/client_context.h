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

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>

#include "neug/compiler/common/timer.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/function/table/scan_replacement.h"
#include "neug/compiler/main/client_config.h"
#include "neug/compiler/parser/statement.h"
#include "neug/compiler/processor/warning_context.h"
#include "neug/compiler/transaction/transaction.h"
#include "prepared_statement.h"

namespace neug {
namespace parser {
class StandaloneCallRewriter;
}  // namespace parser

namespace binder {
class Binder;
class ExpressionBinder;
}  // namespace binder

namespace common {
class RandomEngine;
class TaskScheduler;
class ProgressBar;
}  // namespace common

namespace extension {
class ExtensionManager;
}  // namespace extension

namespace processor {
class ImportDB;
class TableFunctionCall;
}  // namespace processor

namespace graph {
class GraphEntrySet;
}

namespace main {
struct DBConfig;
class MetadataManager;
class DatabaseManager;
class AttachedKuzuDatabase;
struct SpillToDiskSetting;
struct ExtensionOption;
class EmbeddedShell;

struct ActiveQuery {
  explicit ActiveQuery();
  std::atomic<bool> interrupted;
  common::Timer timer;

  void reset();
};

/**
 * @brief Contain client side configuration. We make profiler associated per
 * query, so profiler is not maintained in client context.
 */
class NEUG_API ClientContext {
  friend class Connection;
  friend class binder::Binder;
  friend class binder::ExpressionBinder;
  friend class processor::ImportDB;
  friend class processor::TableFunctionCall;
  friend class parser::StandaloneCallRewriter;
  friend struct SpillToDiskSetting;
  friend class main::EmbeddedShell;
  friend class extension::ExtensionManager;

 public:
  explicit ClientContext(MetadataManager* database);
  ~ClientContext();

  // Client config
  const ClientConfig* getClientConfig() const { return &clientConfig; }
  ClientConfig* getClientConfigUnsafe() { return &clientConfig; }
  common::Value getCurrentSetting(const std::string& optionName) const;

  // TODO: This will be removed after decoupling other dependencies.
  // Currently returns a dummy `transaction` to maintain compatibility. It will
  // be removed once dependent interfaces are refactored.
  transaction::Transaction* getTransaction() const;

  std::unique_ptr<function::ScanReplacementData> tryReplace(
      const std::string& objectName) const;

  // Extension
  void setExtensionOption(std::string name, common::Value value);
  const main::ExtensionOption* getExtensionOption(std::string optionName) const;

  MetadataManager* getMetadataManager() const { return localDatabase; }
  std::shared_ptr<storage::StatsManager> getStatsManager() const;
  storage::MemoryManager* getMemoryManager() const;
  extension::ExtensionManager* getExtensionManager() const;
  catalog::Catalog* getCatalog() const;
  common::VirtualFileSystem* getVFSUnsafe() const;

  static std::string getEnvVariable(const std::string& name);

  bool hasDefaultDatabase() const;
  void setUseInternalCatalogEntry(bool useInternalCatalogEntry) {
    this->useInternalCatalogEntry_ = useInternalCatalogEntry;
  }
  bool useInternalCatalogEntry() const {
    return clientConfig.enableInternalCatalog ? true : useInternalCatalogEntry_;
  }

  graph::GraphEntrySet& getGraphEntrySetUnsafe();

  const graph::GraphEntrySet& getGraphEntrySet() const;

  void cleanUp();

  // Query.
  std::unique_ptr<PreparedStatement> prepare(std::string_view query);

 private:
  static std::unique_ptr<PreparedStatement> preparedStatementWithError(
      std::string_view errMsg);

  std::vector<std::shared_ptr<parser::Statement>> parseQuery(
      std::string_view query);

  std::unique_ptr<PreparedStatement> prepareNoLock(
      std::shared_ptr<parser::Statement> parsedStatement,
      bool shouldCommitNewTransaction,
      std::optional<
          std::unordered_map<std::string, std::shared_ptr<common::Value>>>
          inputParams = std::nullopt);

  // Client side configurable settings.
  ClientConfig clientConfig;
  // Current query.
  ActiveQuery activeQuery;
  // Replace external object as pointer Value;
  std::vector<function::ScanReplacement> scanReplacements;
  // Extension configurable settings.
  std::unordered_map<std::string, common::Value> extensionOptionValues;
  // Local database.
  MetadataManager* localDatabase;
  // Warning information
  processor::WarningContext warningContext;
  // Graph entries
  std::unique_ptr<graph::GraphEntrySet> graphEntrySet;
  std::mutex mtx;
  // Whether the query can access internal tables/sequences or not.
  bool useInternalCatalogEntry_ = false;
};

}  // namespace main
}  // namespace neug
