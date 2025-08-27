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

#include "neug/compiler/main/attached_database.h"

namespace gs {
namespace binder {
struct AttachOption;
}

namespace storage {

using attach_function_t = std::unique_ptr<main::AttachedDatabase> (*)(
    std::string dbPath, std::string dbName, main::ClientContext* clientContext,
    const binder::AttachOption& attachOption);

class StorageExtension {
 public:
  explicit StorageExtension(attach_function_t attachFunction)
      : attachFunction{attachFunction} {}
  virtual bool canHandleDB(std::string /*dbType*/) const { return false; }

  std::unique_ptr<main::AttachedDatabase> attach(
      std::string dbName, std::string dbPath,
      main::ClientContext* clientContext,
      const binder::AttachOption& attachOption) const {
    return attachFunction(std::move(dbName), std::move(dbPath), clientContext,
                          attachOption);
  }

  virtual ~StorageExtension() = default;

 private:
  attach_function_t attachFunction;
};

}  // namespace storage
}  // namespace gs
