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

#include "neug/compiler/extension/extension.h"

#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/common/system_message.h"
#include "neug/compiler/main/client_context.h"
#include "neug/utils/exception/exception.h"
#ifdef _WIN32

#include "windows.h"
#define RTLD_NOW 0
#define RTLD_LOCAL 0

#else
#include <dlfcn.h>
#endif

namespace neug {
namespace extension {

std::string getOS() {
#if defined(_WIN32)
  return "win";
#elif defined(__APPLE__)
  return "osx";
#elif defined(__linux__)
  return "linux";
#else
  THROW_EXCEPTION_WITH_FILE_LINE("Unsupported operating system");
#endif
}

std::string getArch() {
  std::string arch = "x86_64";
#if defined(__x86_64__) || defined(_M_X64)
  arch = "x86_64";
#elif defined(__i386__) || defined(_M_IX86)
  arch = "x86";
#elif defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64) || defined(_M_ARM64)
  arch = "arm64";
#elif defined(__arm__) || defined(_M_ARM)
  arch = "arm";
#endif
  return arch;
}

std::string getPlatform() { return getOS() + "_" + getArch(); }

// Return NEUG_VERSION macro as the extension version to download
std::string getVersion() { return NEUG_VERSION; }

static bool startsWith(const std::string& str, const std::string& prefix) {
  return str.size() >= prefix.size() &&
         str.compare(0, prefix.size(), prefix) == 0;
}

static ExtensionRepoInfo getExtensionRepoInfo(std::string& extensionURL) {
  std::string scheme;
  if (startsWith(extensionURL, "https://")) {
    scheme = "https";
    extensionURL = extensionURL.substr(8);
  } else if (startsWith(extensionURL, "http://")) {
    scheme = "http";
    extensionURL = extensionURL.substr(7);
  } else {
    scheme = "https";
  }
  auto pos = extensionURL.find('/');
  std::string host =
      (pos == std::string::npos) ? extensionURL : extensionURL.substr(0, pos);
  std::string path =
      (pos == std::string::npos) ? "/" : extensionURL.substr(pos);
  if (path.empty() || path[0] != '/') {
    path = "/" + path;
  }
  return {path, host, scheme + "://" + host + path};
}

std::string ExtensionSourceUtils::toString(ExtensionSource source) {
  switch (source) {
  case ExtensionSource::OFFICIAL:
    return "OFFICIAL";
  case ExtensionSource::USER:
    return "USER";
  default:
    NEUG_UNREACHABLE;
  }
}

static ExtensionRepoInfo getExtensionFilePath(const std::string& extensionName,
                                              const std::string& extensionRepo,
                                              const std::string& fileName) {
  auto extensionURL = common::stringFormat(
      extensionRepo + ExtensionUtils::EXTENSION_FILE_REPO_PATH, getVersion(),
      getPlatform(), extensionName, fileName);
  return getExtensionRepoInfo(extensionURL);
}

ExtensionRepoInfo ExtensionUtils::getExtensionLibRepoInfo(
    const std::string& extensionName, const std::string& extensionRepo) {
  return getExtensionFilePath(extensionName, extensionRepo,
                              getExtensionFileName(extensionName));
}

std::string ExtensionUtils::getExtensionFileName(const std::string& name) {
  return common::stringFormat(EXTENSION_FILE_NAME,
                              common::StringUtils::getLower(name));
}

ExtensionLibLoader::ExtensionLibLoader(const std::string& extensionName,
                                       const std::string& path)
    : extensionName{extensionName} {
  libHdl = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (libHdl == nullptr) {
    THROW_IO_EXCEPTION(
        common::stringFormat("Failed to load library: {} which is needed by "
                             "extension: {}.\nError: {}.",
                             path, extensionName, common::dlErrMessage()));
  }
}

ext_load_func_t ExtensionLibLoader::getLoadFunc() {
  return (ext_load_func_t) getDynamicLibFunc(EXTENSION_LOAD_FUNC_NAME);
}

ext_init_func_t ExtensionLibLoader::getInitFunc() {
  return (ext_init_func_t) getDynamicLibFunc(EXTENSION_INIT_FUNC_NAME);
}

ext_name_func_t ExtensionLibLoader::getNameFunc() {
  return (ext_name_func_t) getDynamicLibFunc(EXTENSION_NAME_FUNC_NAME);
}

ext_install_func_t ExtensionLibLoader::getInstallFunc() {
  return (ext_install_func_t) getDynamicLibFunc(EXTENSION_INSTALL_FUNC_NAME);
}

void* ExtensionLibLoader::getDynamicLibFunc(const std::string& funcName) {
  auto sym = dlsym(libHdl, funcName.c_str());
  if (sym == nullptr) {
    THROW_IO_EXCEPTION(common::stringFormat(
        "Failed to load {} function in extension {}.\nError: {}", funcName,
        extensionName, common::dlErrMessage()));
  }
  return sym;
}

#ifdef _WIN32
std::wstring utf8ToUnicode(const char* input) {
  uint32_t result;

  result = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
  if (result == 0) {
    THROW_IO_EXCEPTION("Failure in MultiByteToWideChar");
  }
  auto buffer = std::make_unique<wchar_t[]>(result);
  result = MultiByteToWideChar(CP_UTF8, 0, input, -1, buffer.get(), result);
  if (result == 0) {
    THROW_IO_EXCEPTION("Failure in MultiByteToWideChar");
  }
  return std::wstring(buffer.get(), result);
}

void* dlopen(const char* file, int /*mode*/) {
  NEUG_ASSERT(file);
  auto fpath = utf8ToUnicode(file);
  return (void*) LoadLibraryW(fpath.c_str());
}

void* dlsym(void* handle, const char* name) {
  NEUG_ASSERT(handle);
  return (void*) GetProcAddress((HINSTANCE) handle, name);
}
#endif

}  // namespace extension
}  // namespace neug
