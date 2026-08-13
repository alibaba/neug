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

#include "neug/compiler/extension/extension_manager.h"

#include <utility>
#include <vector>

#include "generated_extension_loader.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/extension/extension.h"

namespace neug {
namespace extension {

std::atomic<const ExtensionManager::LoadedExtensionMap*>
    ExtensionManager::loaded_extensions_{new LoadedExtensionMap()};

std::string ExtensionManager::NormalizeExtensionName(std::string name) {
  common::StringUtils::toLower(name);
  return name;
}

const main::ExtensionOption* ExtensionManager::getExtensionOption(
    std::string name) const {
  common::StringUtils::toLower(name);
  return extensionOptions.contains(name) ? &extensionOptions.at(name) : nullptr;
}

ExtensionManager::LoadTicket ExtensionManager::AcquireLoad(
    const std::string& name) {
  const auto normalized_name = NormalizeExtensionName(name);
  while (true) {
    auto snapshot = loaded_extensions_.load(std::memory_order_acquire);
    auto iter = snapshot->find(normalized_name);
    if (iter == snapshot->end()) {
      auto extension = std::make_shared<LoadedExtension>();
      // Published snapshots live for the process lifetime so lock-free readers
      // can safely retain raw snapshot pointers. Extensions and their dynamic
      // library handles have the same lifetime.
      auto updated = new LoadedExtensionMap(*snapshot);
      updated->emplace(normalized_name, extension);
      if (loaded_extensions_.compare_exchange_weak(snapshot, updated,
                                                   std::memory_order_release,
                                                   std::memory_order_acquire)) {
        return LoadTicket(true, std::move(extension));
      }
      delete updated;
      continue;
    }

    auto extension = iter->second;
    auto state = extension->state.load(std::memory_order_acquire);
    while (state == LoadState::LOADING) {
      extension->state.wait(state, std::memory_order_acquire);
      state = extension->state.load(std::memory_order_acquire);
    }
    if (state == LoadState::LOADED) {
      return LoadTicket(false, std::move(extension));
    }
    if (extension->state.compare_exchange_weak(state, LoadState::LOADING,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
      return LoadTicket(true, std::move(extension));
    }
  }
}

void ExtensionManager::CompleteLoad(const LoadTicket& ticket, void* handle,
                                    InitFunc init) {
  if (!ticket.owns_load || !ticket.extension || !handle || !init) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid extension load completion");
  }
  ticket.extension->handle = handle;
  ticket.extension->init = init;
  ticket.extension->state.store(LoadState::LOADED, std::memory_order_release);
  ticket.extension->state.notify_all();
}

void ExtensionManager::FailLoad(const LoadTicket& ticket) {
  if (!ticket.owns_load || !ticket.extension) {
    return;
  }
  ticket.extension->state.store(LoadState::FAILED, std::memory_order_release);
  ticket.extension->state.notify_all();
}

void ExtensionManager::ReplayLoadedExtensions() {
  std::vector<std::pair<std::string, InitFunc>> init_functions;
  auto snapshot = loaded_extensions_.load(std::memory_order_acquire);
  init_functions.reserve(snapshot->size());
  for (const auto& [name, extension] : *snapshot) {
    auto state = extension->state.load(std::memory_order_acquire);
    while (state == LoadState::LOADING) {
      extension->state.wait(state, std::memory_order_acquire);
      state = extension->state.load(std::memory_order_acquire);
    }
    if (state == LoadState::LOADED) {
      init_functions.emplace_back(name, extension->init);
    }
  }
  for (const auto& [name, init] : init_functions) {
    try {
      init();
    } catch (const std::exception& e) {
      THROW_RUNTIME_ERROR("Extension initialization failed: " + name +
                          ". Error: " + std::string(e.what()));
    } catch (...) {
      THROW_RUNTIME_ERROR(
          "Extension initialization failed with unknown error: " + name);
    }
  }
}

}  // namespace extension
}  // namespace neug
