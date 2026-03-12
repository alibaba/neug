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

#include <string>

#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/catalog_entry_type.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/transaction/transaction.h"
#include "neug/utils/api.h"

#define ADD_EXTENSION_OPTION(OPTION) \
  db->addExtensionOption(OPTION::NAME, OPTION::TYPE, OPTION::getDefaultValue())

#define ADD_CONFIDENTIAL_EXTENSION_OPTION(OPTION)    \
  db->addExtensionOption(OPTION::NAME, OPTION::TYPE, \
                         OPTION::getDefaultValue(), true)

namespace neug {
namespace function {
struct TableFunction;
}  // namespace function
namespace main {
class MetadataManager;
}  // namespace main

namespace extension {

typedef void (*ext_init_func_t)(main::ClientContext*);
typedef const char* (*ext_name_func_t)();
using ext_load_func_t = ext_init_func_t;
typedef void (*ext_install_func_t)(const std::string&, main::ClientContext&);

std::string getPlatform();

std::string getVersion();

class NEUG_API Extension {
 public:
  virtual ~Extension() = default;
};

struct ExtensionRepoInfo {
  std::string hostPath;
  std::string hostURL;
  std::string repoURL;
};

enum class ExtensionSource : uint8_t { OFFICIAL, USER };

struct ExtensionSourceUtils {
  static std::string toString(ExtensionSource source);
};

template <typename T>
void addFunc(main::MetadataManager& database, std::string name,
             catalog::CatalogEntryType functionType, bool isInternal = false) {
  auto catalog = database.getCatalog();
  if (catalog->containsFunction(&transaction::DUMMY_TRANSACTION, name,
                                isInternal)) {
    return;
  }
  catalog->addFunction(&transaction::DUMMY_TRANSACTION, functionType,
                       std::move(name), T::getFunctionSet(), isInternal);
}

struct NEUG_API ExtensionUtils {
  static constexpr const char* OFFICIAL_EXTENSION_REPO =
      "https://graphscope.oss-cn-beijing.aliyuncs.com/neug/extensions/";

  static constexpr const char* EXTENSION_FILE_REPO_PATH = "v{}/{}/{}/{}";

  static constexpr const char* SHARED_LIB_REPO = "v{}/{}/common/{}";

  static constexpr const char* EXTENSION_FILE_NAME = "lib{}.neug_extension";

  static constexpr const char* EXTENSION_LOADER_SUFFIX = "_loader";

  static constexpr const char* EXTENSION_INSTALLER_SUFFIX = "_installer";

  static bool isFullPath(const std::string& extension);

  static ExtensionRepoInfo getExtensionLibRepoInfo(
      const std::string& extensionName, const std::string& extensionRepo);

  static std::string getExtensionFileName(const std::string& name);

  template <typename T>
  static void addTableFunc(main::MetadataManager& database) {
    addFunc<T>(database, T::name,
               catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
  }

  template <typename T>
  static void addStandaloneTableFunc(main::MetadataManager& database) {
    addFunc<T>(database, T::name,
               catalog::CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY,
               false /* isInternal */);
  }
  template <typename T>
  static void addInternalStandaloneTableFunc(main::MetadataManager& database) {
    addFunc<T>(database, T::name,
               catalog::CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY,
               true /* isInternal */);
  }

  template <typename T>
  static void addScalarFunc(main::MetadataManager& database) {
    addFunc<T>(database, T::name,
               catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY);
  }

  template <typename T>
  static void addScalarFuncAlias(main::MetadataManager& database) {
    addFunc<typename T::alias>(
        database, T::name, catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY);
  }
};

class NEUG_API ExtensionLibLoader {
 public:
  static constexpr const char* EXTENSION_LOAD_FUNC_NAME = "load";

  static constexpr const char* EXTENSION_INIT_FUNC_NAME = "init";

  static constexpr const char* EXTENSION_NAME_FUNC_NAME = "name";

  static constexpr const char* EXTENSION_INSTALL_FUNC_NAME = "install";

 public:
  ExtensionLibLoader(const std::string& extensionName, const std::string& path);

  ext_load_func_t getLoadFunc();

  ext_init_func_t getInitFunc();

  ext_name_func_t getNameFunc();

  ext_install_func_t getInstallFunc();

 private:
  void* getDynamicLibFunc(const std::string& funcName);

 private:
  std::string extensionName;
  void* libHdl;
};

#ifdef _WIN32
std::wstring utf8ToUnicode(const char* input);

void* dlopen(const char* file, int /*mode*/);

void* dlsym(void* handle, const char* name);
#endif

}  // namespace extension
}  // namespace neug
