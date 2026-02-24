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

#include "neug/utils/file_utils.h"

#include <glog/logging.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>

#include "neug/utils/exception/exception.h"

namespace neug {

void ensure_directory_exists(const std::string& dir_path) {
  if (dir_path.empty()) {
    LOG(ERROR) << "Error: Directory path is empty.";
    return;
  }
  std::filesystem::path dir(dir_path);
  if (!std::filesystem::exists(dir)) {
    std::filesystem::create_directories(dir);
    LOG(INFO) << "Directory created: " << dir_path;
  } else {
    LOG(INFO) << "Directory already exists: " << dir_path;
  }
}

bool read_string_from_file(const std::string& file_path, std::string& content) {
  std::ifstream inputFile(file_path);

  if (!inputFile.is_open()) {
    LOG(ERROR) << "Error: Could not open the file " << file_path;
    return false;
  }
  std::ostringstream buffer;
  buffer << inputFile.rdbuf();
  content = buffer.str();
  return true;
}

bool write_string_to_file(const std::string& content,
                          const std::string& file_path) {
  std::ofstream outputFile(file_path, std::ios::out | std::ios::trunc);

  if (!outputFile.is_open()) {
    LOG(ERROR) << "Error: Could not open the file " << file_path;
    return false;
  }
  outputFile << content;
  return true;
}

void copy_directory(const std::string& src, const std::string& dst,
                    bool overwrite, bool recursive) {
  if (!std::filesystem::exists(src)) {
    LOG(ERROR) << "Source file does not exist: " << src << std::endl;
    return;
  }
  if (overwrite && std::filesystem::exists(dst)) {
    std::filesystem::remove_all(dst);
  }
  std::filesystem::create_directory(dst);

  for (const auto& entry : std::filesystem::directory_iterator(src)) {
    const auto& path = entry.path();
    auto dest = std::filesystem::path(dst) / path.filename();
    if (std::filesystem::is_directory(path)) {
      if (recursive) {
        copy_directory(path.string(), dest.string(), overwrite, recursive);
      }
    } else if (std::filesystem::is_regular_file(path)) {
      std::error_code errorCode;
      std::filesystem::create_hard_link(path, dest, errorCode);
      if (errorCode) {
        LOG(ERROR) << "Failed to create hard link from " << path << " to "
                   << dest << " " << errorCode.message() << std::endl;
        THROW_IO_EXCEPTION("Failed to create hard link from " + path.string() +
                           " to " + dest.string() + " " + errorCode.message());
      }
    }
  }
}

void remove_directory(const std::string& dir_path) {
  if (std::filesystem::exists(dir_path)) {
    std::error_code errorCode;
    std::filesystem::remove_all(dir_path, errorCode);
    if (errorCode == std::errc::no_such_file_or_directory) {
      return;
    }
    if (errorCode) {
      LOG(ERROR) << "Failed to remove directory: " << dir_path << ", "
                 << errorCode.message();
      THROW_IO_EXCEPTION("Failed to remove directory: " + dir_path + ", " +
                         errorCode.message());
    }
  }
}

}  // namespace neug
