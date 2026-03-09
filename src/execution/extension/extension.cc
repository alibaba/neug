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

#include <cstdlib>
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

#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
#include <openssl/x509_vfy.h>
#endif

#include "neug/execution/extension/extension.h"
#include "neug/storages/graph/schema.h"

namespace neug {
namespace extension {

std::string getUserExtensionDir(const std::string& extension_name) {
  std::string base;
  const char* pyenv = std::getenv("NEUG_EXTENSION_HOME_PYENV");
  if (pyenv && *pyenv) {
    base = pyenv;
  }
#if defined(NEUG_EXTENSION_HOME_MACRO)
  else {
    base = NEUG_EXTENSION_HOME_MACRO;
  }
#endif
  if (base.empty()) {
    base = "/tmp";
  }
  return base + "/extension/" + extension_name;
}

#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
// Try to find and set CA certificate paths automatically
static void trySetCaCertPaths(httplib::SSLClient& cli) {
  // First check environment variables
  const char* ssl_cert_file = std::getenv("SSL_CERT_FILE");
  const char* ssl_cert_dir = std::getenv("SSL_CERT_DIR");

  if (ssl_cert_file || ssl_cert_dir) {
    cli.set_ca_cert_path(ssl_cert_file ? ssl_cert_file : "",
                         ssl_cert_dir ? ssl_cert_dir : "");
    LOG(INFO) << "[Admin] Using CA certificate paths from environment: "
              << "SSL_CERT_FILE=" << (ssl_cert_file ? ssl_cert_file : "not set")
              << ", SSL_CERT_DIR=" << (ssl_cert_dir ? ssl_cert_dir : "not set");
    return;
  }

  // Combined CA certificate file paths for both macOS and Linux
  // Try all possible paths regardless of platform
  // Linux paths are tried first, then macOS paths
  const char* common_ca_files[] = {
      // Linux paths (tried first)
      "/etc/ssl/certs/ca-certificates.crt",        // Debian/Ubuntu
      "/etc/ssl/certs/ca-bundle.crt",              // Some distributions
      "/etc/pki/tls/certs/ca-bundle.crt",          // RedHat/CentOS
      "/usr/local/ssl/certs/ca-certificates.crt",  // Some custom installations
      "/etc/ssl/ca-bundle.pem",                    // Alternative location
      "/etc/ssl/certs/ca-bundle.pem",              // Alternative bundle format
      "/usr/share/ssl/certs/ca-bundle.crt",        // Some installations
      "/usr/share/ca-certificates/ca-certificates.crt",  // Alternative location
      "/etc/ca-certificates/extracted/ca-bundle.trust.crt",  // Debian extracted
      // macOS paths
      "/etc/ssl/cert.pem",                   // macOS system
      "/private/etc/ssl/cert.pem",           // macOS alternative
      "/usr/local/etc/openssl/cert.pem",     // Homebrew OpenSSL (Intel Mac)
      "/opt/homebrew/etc/openssl/cert.pem",  // Homebrew OpenSSL (Apple Silicon)
      "/System/Library/OpenSSL/certs/cert.pem",  // System OpenSSL (older macOS)
      "/usr/local/etc/ca-certificates/cert.pem",  // Alternative location
      nullptr};

  // Combined CA certificate directory paths for both macOS and Linux
  // Linux paths are tried first, then macOS paths
  const char* common_ca_dirs[] = {
      // Linux paths (tried first)
      "/etc/ssl/certs",              // Most Linux distributions
      "/etc/pki/tls/certs",          // RedHat/CentOS
      "/usr/local/ssl/certs",        // Some custom installations
      "/usr/share/ssl/certs",        // Alternative location
      "/usr/share/ca-certificates",  // Debian/Ubuntu
      "/etc/ca-certificates/extracted/tls-ca-bundle.pem",  // Debian extracted
      "/etc/ssl",  // Standard SSL directory
      // macOS paths
      "/usr/local/etc/openssl/certs",     // Homebrew OpenSSL (Intel Mac)
      "/opt/homebrew/etc/openssl/certs",  // Homebrew OpenSSL (Apple Silicon)
      "/System/Library/OpenSSL/certs",    // System OpenSSL (older macOS)
      "/private/etc/ssl/certs",           // macOS alternative
      "/private/etc/ssl",                 // macOS alternative
      nullptr};

  // Try to find a valid CA certificate file
  for (int i = 0; common_ca_files[i] != nullptr; i++) {
    if (std::filesystem::exists(common_ca_files[i])) {
      cli.set_ca_cert_path(common_ca_files[i], "");
      LOG(INFO) << "[Admin] Auto-detected CA certificate file: "
                << common_ca_files[i];
      return;
    }
  }

  // Try to find a valid CA certificate directory
  for (int i = 0; common_ca_dirs[i] != nullptr; i++) {
    if (std::filesystem::exists(common_ca_dirs[i]) &&
        std::filesystem::is_directory(common_ca_dirs[i])) {
      cli.set_ca_cert_path("", common_ca_dirs[i]);
      LOG(INFO) << "[Admin] Auto-detected CA certificate directory: "
                << common_ca_dirs[i];
      return;
    }
  }

  // If nothing found, rely on OpenSSL defaults
  // On macOS, httplib may use Keychain if
  // CPPHTTPLIB_USE_CERTS_FROM_MACOSX_KEYCHAIN is defined
  LOG(WARNING)
      << "[Admin] No CA certificate paths found, using OpenSSL defaults";
#ifdef __APPLE__
  LOG(WARNING) << "[Admin] On macOS, consider defining "
                  "CPPHTTPLIB_USE_CERTS_FROM_MACOSX_KEYCHAIN "
               << "to use Keychain certificates, or set "
                  "SSL_CERT_FILE/SSL_CERT_DIR environment variables";
#endif
}
#endif

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

  if (std::filesystem::exists(localLibPath)) {
    auto verifySt = verifyExtensionChecksum(repoInfo, localLibPath);
    // existing extension lib in local path is not consistent with the remote
    // repo, remove it
    if (!verifySt.ok()) {
      std::error_code ec;
      if (!std::filesystem::remove(localLibPath, ec)) {
        LOG(ERROR) << "[Admin] Cannot delete existing extension file (no "
                      "permission or error): " +
                          localLibPath + " ec=" + ec.message() +
                          ". Please delete it manually.";
        return Status(
            StatusCode::ERR_IO_ERROR,
            "Cannot delete existing extension file (no permission or error): " +
                localLibPath + " ec=" + ec.message() +
                ". Please delete it manually.");
      }
      LOG(INFO) << "[Admin] Removed existing extension file: " << localLibPath;
    }
  }

  if (!std::filesystem::exists(localLibPath)) {
    auto st = downloadExtensionFile(repoInfo, localLibPath);
    if (!st.ok()) {
      return Status(StatusCode::ERR_IO_ERROR,
                    "Failed to download extension " + extension_name +
                        " to local lib path " + localLibPath + " : " +
                        st.error_message());
    }
    LOG(INFO) << "[Admin] Extension " << extension_name << " downloaded to "
              << localLibPath;
    auto verifySt = verifyExtensionChecksum(repoInfo, localLibPath);
    if (!verifySt.ok()) {
      std::filesystem::remove(localLibPath);
      return Status(
          StatusCode::ERR_IO_ERROR,
          "Extension integrity check failed: " + verifySt.error_message());
    }
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

  // Try to automatically detect and set CA certificate paths
  trySetCaCertPaths(cli);

  httplib::Headers headers = {
      {"User-Agent", common::stringFormat("gs/v{}", getVersion())}};

  auto res = cli.Get(repoInfo.hostPath.c_str(), headers);
  if (!res) {
    std::string error_detail = httplib::to_string(res.error());
    LOG(ERROR) << "[Admin] Download failed, res.error()=" << error_detail;

    // If SSL verification failed, get detailed error information
    if (res.error() == httplib::Error::SSLServerVerification) {
      long verify_result = cli.get_openssl_verify_result();
      if (verify_result != 0) {
        const char* verify_error_str =
            X509_verify_cert_error_string(verify_result);
        LOG(ERROR) << "[Admin] SSL verification failed: "
                   << "error_code=" << verify_result << ", error_description=\""
                   << (verify_error_str ? verify_error_str : "unknown")
                   << "\", host=" << repoInfo.hostURL;

        // Log CA certificate paths for debugging
        const char* ssl_cert_file = std::getenv("SSL_CERT_FILE");
        const char* ssl_cert_dir = std::getenv("SSL_CERT_DIR");
        LOG(ERROR) << "[Admin] CA certificate paths: "
                   << "SSL_CERT_FILE="
                   << (ssl_cert_file ? ssl_cert_file
                                     : "not set (using default)")
                   << ", SSL_CERT_DIR="
                   << (ssl_cert_dir ? ssl_cert_dir : "not set (using default)");

        error_detail += " (SSL verify error: " + std::to_string(verify_result);
        if (verify_error_str) {
          error_detail += " - " + std::string(verify_error_str);
        }
        error_detail += ")";
      }
    }

    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to download: " + repoInfo.repoURL +
                      " (network error: " + error_detail + ")");
  } else if (res->status != 200) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to download: " + repoInfo.repoURL + " (HTTP " +
                      std::to_string(res->status) + ")");
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
                               const std::string& localLibPath) {
  std::string checksumURL = libRepoInfo.repoURL + ".sha256";
  std::string checksumPath = libRepoInfo.hostPath + ".sha256";
  std::string checksumHost = libRepoInfo.hostURL;

#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
  httplib::SSLClient cli(checksumHost.c_str());
  cli.set_connection_timeout(5, 0);
  cli.set_read_timeout(10, 0);

  // Try to automatically detect and set CA certificate paths
  trySetCaCertPaths(cli);

  httplib::Headers headers = {
      {"User-Agent", common::stringFormat("gs/v{}", getVersion())}};

  auto res = cli.Get(checksumPath.c_str(), headers);
  if (!res) {
    std::string error_detail = httplib::to_string(res.error());
    LOG(ERROR) << "[Admin] Download failed, res.error()=" << error_detail;

    // If SSL verification failed, get detailed error information
    if (res.error() == httplib::Error::SSLServerVerification) {
      long verify_result = cli.get_openssl_verify_result();
      if (verify_result != 0) {
        const char* verify_error_str =
            X509_verify_cert_error_string(verify_result);
        LOG(ERROR) << "[Admin] SSL verification failed: "
                   << "error_code=" << verify_result << ", error_description=\""
                   << (verify_error_str ? verify_error_str : "unknown")
                   << "\", host=" << checksumHost;

        // Log CA certificate paths for debugging
        const char* ssl_cert_file = std::getenv("SSL_CERT_FILE");
        const char* ssl_cert_dir = std::getenv("SSL_CERT_DIR");
        LOG(ERROR) << "[Admin] CA certificate paths: "
                   << "SSL_CERT_FILE="
                   << (ssl_cert_file ? ssl_cert_file
                                     : "not set (using default)")
                   << ", SSL_CERT_DIR="
                   << (ssl_cert_dir ? ssl_cert_dir : "not set (using default)");

        error_detail += " (SSL verify error: " + std::to_string(verify_result);
        if (verify_error_str) {
          error_detail += " - " + std::string(verify_error_str);
        }
        error_detail += ")";
      }
    }

    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to download checksum: " + checksumURL +
                      " (network error: " + error_detail + ")");
  } else if (res->status != 200) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to download checksum: " + checksumURL + " (HTTP " +
                      std::to_string(res->status) + ")");
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

  LOG(INFO) << "[Admin] Checksum verification passed for " << localLibPath;
  return Status::OK();
#else
  return Status(StatusCode::ERR_IO_ERROR,
                "Checksum verification requires OpenSSL support");
#endif
}

// Promote libneug.so to RTLD_GLOBAL so that extensions loaded via dlopen
// can resolve neug symbols.  When the host process (e.g. Python) loads
// libneug.so with RTLD_LOCAL, the symbols stay in a local scope and are
// invisible to subsequently dlopen'd extensions even though they list
// libneug.so in DT_NEEDED.  Re-opening with RTLD_NOLOAD | RTLD_GLOBAL
// promotes the already-loaded instance without reloading it.
static void ensureNeugSymbolsGlobal() {
  static bool promoted = false;
  if (promoted)
    return;
  Dl_info info;
  if (dladdr(reinterpret_cast<void*>(&ensureNeugSymbolsGlobal), &info) &&
      info.dli_fname) {
    dlopen(info.dli_fname, RTLD_NOW | RTLD_NOLOAD | RTLD_GLOBAL);
  }
  promoted = true;
}

Status load_extension(const std::string& extension_name) {
  LOG(INFO) << "[Admin] LOAD extension: " << extension_name;
  ensureNeugSymbolsGlobal();
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
  LOG(ERROR) << "[Admin] Extension " << userLibPath
             << " not found in user install or wheel package";
  return Status(StatusCode::ERR_IO_ERROR,
                "Extension " + userLibPath +
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