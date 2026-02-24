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
#include <memory>

namespace neug {
namespace extension {

enum class ExtensionAction : uint8_t {
  INSTALL = 0,
  LOAD = 1,
  UNINSTALL = 2,
};

struct ExtensionAuxInfo {
  ExtensionAction action;
  std::string path;

  ExtensionAuxInfo(ExtensionAction action, std::string path)
      : action{action}, path{std::move(path)} {}

  virtual ~ExtensionAuxInfo() = default;

  template <typename TARGET>
  const TARGET& contCast() const {
    return dynamic_cast<const TARGET&>(*this);
  }

  virtual std::unique_ptr<ExtensionAuxInfo> copy() {
    return std::make_unique<ExtensionAuxInfo>(*this);
  }
};

struct InstallExtensionAuxInfo : public ExtensionAuxInfo {
  std::string extensionRepo;

  explicit InstallExtensionAuxInfo(std::string extensionRepo, std::string path)
      : ExtensionAuxInfo{ExtensionAction::INSTALL, std::move(path)},
        extensionRepo{std::move(extensionRepo)} {}

  std::unique_ptr<ExtensionAuxInfo> copy() override {
    return std::make_unique<InstallExtensionAuxInfo>(*this);
  }
};

}  // namespace extension
}  // namespace neug
