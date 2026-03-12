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

#include "neug/compiler/main/client_context.h"

#include <fstream>

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/common/random_engine.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/common/task_system/progress_bar.h"
#include "neug/compiler/common/task_system/task.h"
#include "neug/compiler/extension/extension.h"
#include "neug/compiler/extension/extension_manager.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/graph/graph_entry.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/main/option_config.h"
#include "neug/compiler/main/plan_printer.h"
#include "neug/compiler/optimizer/optimizer.h"
#include "neug/compiler/parser/parser.h"
#include "neug/compiler/parser/visitor/standalone_call_rewriter.h"
#include "neug/compiler/parser/visitor/statement_read_write_analyzer.h"
#include "neug/compiler/planner/planner.h"
#include "neug/compiler/storage/stats_manager.h"
#include "neug/utils/exception/exception.h"

#if defined(_WIN32)
#include "neug/compiler/common/windows_utils.h"
#endif

using namespace neug::parser;
using namespace neug::binder;
using namespace neug::common;
using namespace neug::catalog;
using namespace neug::planner;
using namespace neug::transaction;

namespace neug {
namespace main {

ActiveQuery::ActiveQuery() : interrupted{false} {}

void ActiveQuery::reset() {
  interrupted = false;
  timer = Timer();
}

ClientContext::ClientContext(MetadataManager* database)
    : localDatabase{database}, warningContext(&clientConfig) {
  graphEntrySet = std::make_unique<graph::GraphEntrySet>();
#if defined(_WIN32)
  clientConfig.homeDirectory = getEnvVariable("USERPROFILE");
#else
  clientConfig.homeDirectory = getEnvVariable("HOME");
#endif
  clientConfig.fileSearchPath = "";
  clientConfig.enableSemiMask = ClientConfigDefault::ENABLE_SEMI_MASK;
  clientConfig.enableZoneMap = ClientConfigDefault::ENABLE_ZONE_MAP;
  // clientConfig.numThreads = database->dbConfig.maxNumThreads;
  clientConfig.timeoutInMS = ClientConfigDefault::TIMEOUT_IN_MS;
  clientConfig.varLengthMaxDepth = ClientConfigDefault::VAR_LENGTH_MAX_DEPTH;
  clientConfig.enableProgressBar = ClientConfigDefault::ENABLE_PROGRESS_BAR;
  clientConfig.showProgressAfter = ClientConfigDefault::SHOW_PROGRESS_AFTER;
  clientConfig.recursivePatternSemantic =
      ClientConfigDefault::RECURSIVE_PATTERN_SEMANTIC;
  clientConfig.recursivePatternCardinalityScaleFactor =
      ClientConfigDefault::RECURSIVE_PATTERN_FACTOR;
  clientConfig.disableMapKeyCheck = ClientConfigDefault::DISABLE_MAP_KEY_CHECK;
  clientConfig.warningLimit = ClientConfigDefault::WARNING_LIMIT;
}

ClientContext::~ClientContext() = default;

Value ClientContext::getCurrentSetting(const std::string& optionName) const {
  auto lowerCaseOptionName = optionName;
  StringUtils::toLower(lowerCaseOptionName);
  const ConfigurationOption* option = nullptr;
  if (option != nullptr) {
    return option->getSetting(this);
  }
  if (extensionOptionValues.contains(lowerCaseOptionName)) {
    return extensionOptionValues.at(lowerCaseOptionName);
  }
  const auto defaultOption = getExtensionOption(lowerCaseOptionName);
  if (defaultOption != nullptr) {
    return defaultOption->defaultValue;
  }
  THROW_RUNTIME_ERROR("Invalid option name: " + lowerCaseOptionName + ".");
}

Transaction* ClientContext::getTransaction() const {
  return &neug::Constants::DEFAULT_TRANSACTION;
}

std::unique_ptr<function::ScanReplacementData> ClientContext::tryReplace(
    const std::string& objectName) const {
  for (auto& scanReplacement : scanReplacements) {
    auto replaceData = scanReplacement.replaceFunc(objectName);
    if (replaceData == nullptr) {
      continue;
    }
    return replaceData;
  }
  return nullptr;
}

void ClientContext::setExtensionOption(std::string name, Value value) {
  StringUtils::toLower(name);
  extensionOptionValues.insert_or_assign(name, std::move(value));
}

const main::ExtensionOption* ClientContext::getExtensionOption(
    std::string optionName) const {
  return localDatabase->extensionManager->getExtensionOption(optionName);
}

std::shared_ptr<storage::StatsManager> ClientContext::getStatsManager() const {
  return localDatabase->getStatsManager();
}

storage::MemoryManager* ClientContext::getMemoryManager() const {
  return localDatabase->memoryManager.get();
}

extension::ExtensionManager* ClientContext::getExtensionManager() const {
  return localDatabase->extensionManager.get();
}

Catalog* ClientContext::getCatalog() const {
  return localDatabase->catalog.get();
}

VirtualFileSystem* ClientContext::getVFSUnsafe() const {
  return localDatabase->vfs.get();
}

std::string ClientContext::getEnvVariable(const std::string& name) {
#if defined(_WIN32)
  auto envValue = WindowsUtils::utf8ToUnicode(name.c_str());
  auto result = _wgetenv(envValue.c_str());
  if (!result) {
    return std::string();
  }
  return WindowsUtils::unicodeToUTF8(result);
#else
  const char* env = getenv(name.c_str());
  if (!env) {
    return std::string();
  }
  return env;
#endif
}

bool ClientContext::hasDefaultDatabase() const { return false; }

graph::GraphEntrySet& ClientContext::getGraphEntrySetUnsafe() {
  return *graphEntrySet;
}

const graph::GraphEntrySet& ClientContext::getGraphEntrySet() const {
  return *graphEntrySet;
}

void ClientContext::cleanUp() {}

std::unique_ptr<PreparedStatement> ClientContext::prepare(
    std::string_view query) {
  std::unique_lock lck{mtx};
  auto parsedStatements = std::vector<std::shared_ptr<Statement>>();
  try {
    parsedStatements = parseQuery(query);
  } catch (std::exception& exception) {
    return preparedStatementWithError(exception.what());
  }
  if (parsedStatements.size() > 1) {
    return preparedStatementWithError(
        "Connection Exception: We do not support prepare multiple statements.");
  }
  auto result =
      prepareNoLock(parsedStatements[0], true /*shouldCommitNewTransaction*/);
  useInternalCatalogEntry_ = false;
  return result;
}

std::unique_ptr<PreparedStatement> ClientContext::preparedStatementWithError(
    std::string_view errMsg) {
  auto preparedStatement = std::make_unique<PreparedStatement>();
  preparedStatement->success = false;
  preparedStatement->errMsg = errMsg;
  return preparedStatement;
}

std::vector<std::shared_ptr<Statement>> ClientContext::parseQuery(
    std::string_view query) {
  if (query.empty()) {
    THROW_CONNECTION_EXCEPTION("Query is empty.");
  }
  std::vector<std::shared_ptr<Statement>> statements;
  auto parserTimer = TimeMetric(true /*enable*/);
  parserTimer.start();
  auto parsedStatements = Parser::parseQuery(query);
  parserTimer.stop();
  const auto avgParsingTime =
      parserTimer.getElapsedTimeMS() / parsedStatements.size() / 1.0;
  StandaloneCallRewriter standaloneCallAnalyzer{this,
                                                parsedStatements.size() == 1};
  for (auto i = 0u; i < parsedStatements.size(); i++) {
    auto rewriteQuery =
        standaloneCallAnalyzer.getRewriteQuery(*parsedStatements[i]);
    if (rewriteQuery.empty()) {
      parsedStatements[i]->setParsingTime(avgParsingTime);
      statements.push_back(std::move(parsedStatements[i]));
    } else {
      parserTimer.start();
      auto rewrittenStatements = Parser::parseQuery(rewriteQuery);
      parserTimer.stop();
      const auto avgRewriteParsingTime =
          parserTimer.getElapsedTimeMS() / rewrittenStatements.size() / 1.0;
      NEUG_ASSERT(rewrittenStatements.size() >= 1);
      for (auto j = 0u; j < rewrittenStatements.size() - 1; j++) {
        rewrittenStatements[j]->setParsingTime(avgParsingTime +
                                               avgRewriteParsingTime);
        rewrittenStatements[j]->setToInternal();
        statements.push_back(std::move(rewrittenStatements[j]));
      }
      auto lastRewrittenStatement = rewrittenStatements.back();
      lastRewrittenStatement->setParsingTime(avgParsingTime +
                                             avgRewriteParsingTime);
      statements.push_back(std::move(lastRewrittenStatement));
    }
  }
  return statements;
}

std::unique_ptr<PreparedStatement> ClientContext::prepareNoLock(
    std::shared_ptr<Statement> parsedStatement, bool shouldCommitNewTransaction,
    std::optional<std::unordered_map<std::string, std::shared_ptr<Value>>>
        inputParams) {
  auto preparedStatement = std::make_unique<PreparedStatement>();
  auto prepareTimer = TimeMetric(true /* enable */);
  prepareTimer.start();
  try {
    preparedStatement->preparedSummary.statementType =
        parsedStatement->getStatementType();
    auto readWriteAnalyzer = StatementReadWriteAnalyzer(this);

    readWriteAnalyzer.visit(*parsedStatement);

    preparedStatement->readOnly = readWriteAnalyzer.isReadOnly();
    preparedStatement->parsedStatement = std::move(parsedStatement);

    auto binder = Binder(this);
    if (inputParams) {
      binder.setInputParameters(*inputParams);
    }
    const auto boundStatement =
        binder.bind(*preparedStatement->parsedStatement);
    preparedStatement->parameterMap = binder.getParameterMap();
    preparedStatement->statementResult = std::make_unique<BoundStatementResult>(
        boundStatement->getStatementResult()->copy());
    auto planner = Planner(this);
    auto bestPlan = planner.getBestPlan(*boundStatement);
    optimizer::Optimizer::optimize(bestPlan.get(), this,
                                   planner.getCardinalityEstimator());
    preparedStatement->logicalPlan = std::move(bestPlan);

  } catch (std::exception& exception) {
    preparedStatement->success = false;
    preparedStatement->errMsg = exception.what();
  }
  preparedStatement->useInternalCatalogEntry = useInternalCatalogEntry_;
  prepareTimer.stop();
  preparedStatement->preparedSummary.compilingTime =
      preparedStatement->parsedStatement->getParsingTime() +
      prepareTimer.getElapsedTimeMS();
  return preparedStatement;
}

}  // namespace main
}  // namespace neug
