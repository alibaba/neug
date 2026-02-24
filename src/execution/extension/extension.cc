/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <dlfcn.h>
#include <glog/logging.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include "neug/compiler/common/types/types.h"
#include "neug/utils/function_type.h"

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
#include "httplib.h"
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#include "neug/execution/extension/extension.h"
#include "neug/storages/graph/schema.h"

namespace neug {
namespace extension {

std::string getUserExtensionDir(const std::string& extension_name) {
  const char* home = std::getenv("EXTENSION_HOME");
  std::string base = home ? home : "/tmp";
  return base + "/extension/" + extension_name;
}

Status install_extension(const std::string& extension_name) {
  LOG(INFO) << "[Admin] INSTALL extension: " << extension_name;
  std::string extDir = getUserExtensionDir(extension_name);

  std::error_code ec;
  std::filesystem::create_directories(extDir, ec);
  if (ec) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to create extension directory: " + extDir +
                      " ec=" + ec.message());
  }

  auto fileName =
      neug::extension::ExtensionUtils::getExtensionFileName(extension_name);
  auto localLibPath = extDir + "/" + fileName;

  const std::string& repo =
      neug::extension::ExtensionUtils::OFFICIAL_EXTENSION_REPO;
  auto repoInfo = neug::extension::ExtensionUtils::getExtensionLibRepoInfo(
      extension_name, repo);

  LOG(INFO) << "[Admin] Download URL host=" << repoInfo.hostURL
            << " path=" << repoInfo.hostPath << " full=" << repoInfo.repoURL;

  if (!std::filesystem::exists(localLibPath)) {
    auto st = downloadExtensionFile(repoInfo, localLibPath);
    if (!st.ok()) {
      return Status(StatusCode::ERR_IO_ERROR, "Failed to download extension " +
                                                  extension_name + " : " +
                                                  st.error_message());
    }
    LOG(INFO) << "[Admin] Extension " << extension_name << " downloaded to "
              << localLibPath;
  } else {
    LOG(INFO) << "[Admin] Extension file already exists: " << localLibPath;
  }

  bool checksumChecked = false;
  auto verifySt =
      verifyExtensionChecksum(repoInfo, localLibPath, checksumChecked);
  if (!verifySt.ok()) {
    std::filesystem::remove(localLibPath);
    return Status(
        StatusCode::ERR_IO_ERROR,
        "Extension integrity check failed: " + verifySt.error_message());
  }
  if (checksumChecked) {
    LOG(INFO) << "[Admin] Extension integrity verified for " << extension_name;
  }

  return Status::OK();
}

Status downloadExtensionFile(const ExtensionRepoInfo& repoInfo,
                             const std::string& localFilePath) {
#ifndef CPPHTTPLIB_OPENSSL_SUPPORT
  return Status(
      StatusCode::ERR_IO_ERROR,
      "HTTPS not supported (rebuild with CPPHTTPLIB_OPENSSL_SUPPORT)");
#else
  httplib::SSLClient cli(repoInfo.hostURL.c_str());
  cli.set_connection_timeout(10, 0);
  cli.set_read_timeout(60, 0);

  httplib::Headers headers = {
      {"User-Agent", common::stringFormat("gs/v{}", NEUG_EXTENSION_VERSION)}};

  auto res = cli.Get(repoInfo.hostPath.c_str(), headers);
  if (!res || res->status != 200) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to download: " + repoInfo.repoURL +
                      (res ? (" (HTTP " + std::to_string(res->status) + ")")
                           : " (network error)"));
  }

  std::ofstream ofs(localFilePath, std::ios::binary);
  if (!ofs) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Cannot open local file: " + localFilePath);
  }
  ofs.write(res->body.data(), static_cast<std::streamsize>(res->body.size()));
  if (!ofs) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed writing file: " + localFilePath);
  }
  return Status::OK();
#endif
}

result<std::string> computeFileSHA256(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  if (!file.is_open()) {
    RETURN_ERROR(Status(StatusCode::ERR_IO_ERROR, "Cannot open file: " + path));
  }

  EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
  if (mdctx == nullptr) {
    RETURN_ERROR(
        Status(StatusCode::ERR_INTERNAL_ERROR, "Failed to create EVP_MD_CTX"));
  }

  if (EVP_DigestInit_ex(mdctx, EVP_sha256(), nullptr) != 1) {
    EVP_MD_CTX_free(mdctx);
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR,
                        "Failed to initialize SHA256 digest"));
  }

  constexpr size_t kBufferSize = 8192;
  char buffer[kBufferSize];
  while (file.read(buffer, kBufferSize) || file.gcount() > 0) {
    if (EVP_DigestUpdate(mdctx, buffer, file.gcount()) != 1) {
      EVP_MD_CTX_free(mdctx);
      RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR,
                          "Failed to update SHA256 digest"));
    }
  }

  unsigned char hash[EVP_MAX_MD_SIZE];
  unsigned int hash_len = 0;
  if (EVP_DigestFinal_ex(mdctx, hash, &hash_len) != 1) {
    EVP_MD_CTX_free(mdctx);
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR,
                        "Failed to finalize SHA256 digest"));
  }

  EVP_MD_CTX_free(mdctx);

  std::ostringstream ss;
  for (unsigned int i = 0; i < hash_len; i++) {
    ss << std::hex << std::setw(2) << std::setfill('0')
       << static_cast<int>(hash[i]);
  }

  return ss.str();
}

Status verifyExtensionChecksum(const ExtensionRepoInfo& libRepoInfo,
                               const std::string& localLibPath,
                               bool& checksumChecked) {
  checksumChecked = false;

  std::string checksumURL = libRepoInfo.repoURL + ".sha256";
  std::string checksumPath = libRepoInfo.hostPath + ".sha256";
  std::string checksumHost = libRepoInfo.hostURL;

#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
  httplib::SSLClient cli(checksumHost.c_str());
  cli.set_connection_timeout(5, 0);
  cli.set_read_timeout(10, 0);

  httplib::Headers headers = {
      {"User-Agent", common::stringFormat("gs/v{}", NEUG_EXTENSION_VERSION)}};

  auto res = cli.Get(checksumPath.c_str(), headers);
  if (!res || res->status != 200) {
    LOG(WARNING) << "[Admin] No checksum file found at " << checksumURL
                 << ", skipping integrity check";
    return Status::OK();
  }

  std::string expectedChecksum = res->body;

  auto spacePos = expectedChecksum.find(' ');
  if (spacePos != std::string::npos) {
    expectedChecksum = expectedChecksum.substr(0, spacePos);
  }

  expectedChecksum.erase(
      std::remove(expectedChecksum.begin(), expectedChecksum.end(), '\n'),
      expectedChecksum.end());
  expectedChecksum.erase(
      std::remove(expectedChecksum.begin(), expectedChecksum.end(), '\r'),
      expectedChecksum.end());

  auto computedResult = computeFileSHA256(localLibPath);
  if (!computedResult) {
    return computedResult.error();
  }

  std::string computedChecksum = computedResult.value();

  std::transform(expectedChecksum.begin(), expectedChecksum.end(),
                 expectedChecksum.begin(), ::tolower);
  std::transform(computedChecksum.begin(), computedChecksum.end(),
                 computedChecksum.begin(), ::tolower);

  if (expectedChecksum != computedChecksum) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Checksum verification failed for " + localLibPath +
                      ". Expected: " + expectedChecksum +
                      ", Got: " + computedChecksum);
  }

  checksumChecked = true;
  LOG(INFO) << "[Admin] Checksum verification passed for " << localLibPath;
  return Status::OK();
#else
  return Status(StatusCode::ERR_IO_ERROR,
                "Checksum verification requires OpenSSL support");
#endif
}

Status load_extension(const std::string& extension_name) {
  LOG(INFO) << "[Admin] LOAD extension: " << extension_name;
  auto fileName =
      neug::extension::ExtensionUtils::getExtensionFileName(extension_name);

  std::string userExtDir = getUserExtensionDir(extension_name);
  std::string userLibPath = userExtDir + "/" + fileName;
  if (std::filesystem::exists(userLibPath)) {
    LOG(INFO) << "[Admin] Loading extension from user install: " << userLibPath;
    void* handle = dlopen(userLibPath.c_str(), RTLD_NOW | RTLD_GLOBAL);
    if (!handle) {
      return Status(StatusCode::ERR_IO_ERROR,
                    "Failed to load extension library: " + userLibPath +
                        ". Error: " + std::string(dlerror()));
    }
    dlerror();
    typedef void (*init_func_t)();
    init_func_t init_func = (init_func_t) dlsym(handle, "Init");
    const char* dlsym_error = dlerror();
    if (dlsym_error) {
      dlclose(handle);
      return Status(
          StatusCode::ERR_IO_ERROR,
          "Failed to find 'Init' function in extension: " + extension_name +
              ". Error: " + std::string(dlsym_error));
    }
    try {
      (*init_func)();
      LOG(INFO) << "[Admin] Extension " << extension_name
                << " loaded and initialized successfully";
    } catch (const std::exception& e) {
      dlclose(handle);
      return Status(StatusCode::ERR_IO_ERROR,
                    "Extension initialization failed: " + extension_name +
                        ". Error: " + std::string(e.what()));
    } catch (...) {
      dlclose(handle);
      return Status(StatusCode::ERR_IO_ERROR,
                    "Extension initialization failed with unknown error: " +
                        extension_name);
    }
    LOG(INFO) << "[Admin] Extension " << extension_name << " is now available";
    return Status::OK();
  }

  // Not found
  LOG(ERROR) << "[Admin] Extension " << extension_name
             << " not found in user install or wheel package";
  return Status(StatusCode::ERR_IO_ERROR,
                "Extension " + extension_name +
                    " not found in user install or wheel package");
}

Status uninstall_extension(const std::string& extension_name) {
  LOG(INFO) << "[Admin] UNINSTALL extension: " << extension_name;
  std::string extDir = getUserExtensionDir(extension_name);
  std::error_code ec;
  if (!std::filesystem::exists(extDir)) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Cannot uninstall extension: " + extension_name +
                      " since it has not been installed.");
  }
  if (!std::filesystem::remove_all(extDir, ec)) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "An error occurred while uninstalling extension: " +
                      extension_name + ". Error: " + ec.message());
  }
  LOG(INFO) << "[Admin] Extension: " << extension_name
            << " has been uninstalled";
  return Status::OK();
}

}  // namespace extension
}  // namespace neug