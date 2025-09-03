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

#include "neug/compiler/main/database_manager.h"

#include "neug/compiler/common/string_utils.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace main {

DatabaseManager::DatabaseManager() : defaultDatabase{""} {}

void DatabaseManager::registerAttachedDatabase(
    std::unique_ptr<AttachedDatabase> attachedDatabase) {
  if (defaultDatabase == "") {
    defaultDatabase = attachedDatabase->getDBName();
  }
  if (hasAttachedDatabase(attachedDatabase->getDBName())) {
    THROW_RUNTIME_ERROR(
        stringFormat("Duplicate attached database name: {}. Attached database "
                     "name must be unique.",
                     attachedDatabase->getDBName()));
  }
  attachedDatabases.push_back(std::move(attachedDatabase));
}

bool DatabaseManager::hasAttachedDatabase(const std::string& name) {
  auto upperCaseName = StringUtils::getUpper(name);
  for (auto& attachedDatabase : attachedDatabases) {
    auto attachedDBName = StringUtils::getUpper(attachedDatabase->getDBName());
    if (attachedDBName == upperCaseName) {
      return true;
    }
  }
  return false;
}

AttachedDatabase* DatabaseManager::getAttachedDatabase(
    const std::string& name) {
  auto upperCaseName = StringUtils::getUpper(name);
  for (auto& attachedDatabase : attachedDatabases) {
    auto attachedDBName = StringUtils::getUpper(attachedDatabase->getDBName());
    if (attachedDBName == upperCaseName) {
      return attachedDatabase.get();
    }
  }
  THROW_RUNTIME_ERROR(stringFormat("No database named {}.", name));
}

void DatabaseManager::detachDatabase(const std::string& databaseName) {
  auto upperCaseName = StringUtils::getUpper(databaseName);
  for (auto it = attachedDatabases.begin(); it != attachedDatabases.end();
       ++it) {
    auto attachedDBName = (*it)->getDBName();
    StringUtils::toUpper(attachedDBName);
    if (attachedDBName == upperCaseName) {
      attachedDatabases.erase(it);
      return;
    }
  }
  THROW_RUNTIME_ERROR(
      stringFormat("Database: {} doesn't exist.", databaseName));
}

void DatabaseManager::setDefaultDatabase(const std::string& databaseName) {
  if (getAttachedDatabase(databaseName) == nullptr) {
    THROW_RUNTIME_ERROR(stringFormat("No database named {}.", databaseName));
  }
  defaultDatabase = databaseName;
}

std::vector<AttachedDatabase*> DatabaseManager::getAttachedDatabases() const {
  std::vector<AttachedDatabase*> attachedDatabasesPtr;
  for (auto& attachedDatabase : attachedDatabases) {
    attachedDatabasesPtr.push_back(attachedDatabase.get());
  }
  return attachedDatabasesPtr;
}

void DatabaseManager::invalidateCache() {
  for (auto& attachedDatabase : attachedDatabases) {
    attachedDatabase->invalidateCache();
  }
}

}  // namespace main
}  // namespace gs
