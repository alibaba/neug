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

void read_file(const std::string& filename, void* buffer, size_t size,
               size_t num) {
  FILE* fin = fopen(filename.c_str(), "rb");
  if (fin == nullptr) {
    std::stringstream ss;
    ss << "Failed to open file " << filename << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  size_t ret_len = 0;
  if ((ret_len = fread(buffer, size, num, fin)) != num) {
    std::stringstream ss;
    ss << "Failed to read file " << filename << ", expected " << num << ", got "
       << ret_len << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  int ret = 0;
  if ((ret = fclose(fin)) != 0) {
    std::stringstream ss;
    ss << "Failed to close file " << filename << ", error code: " << ret << " "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
}

void write_file(const std::string& filename, const void* buffer, size_t size,
                size_t num) {
  FILE* fout = fopen(filename.c_str(), "wb");
  if (fout == nullptr) {
    std::stringstream ss;
    ss << "Failed to open file " << filename << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  size_t ret_len = 0;
  if ((ret_len = fwrite(buffer, size, num, fout)) != num) {
    std::stringstream ss;
    ss << "Failed to write file " << filename << ", expected " << num
       << ", got " << ret_len << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  int ret = 0;
  if ((ret = fclose(fout)) != 0) {
    std::stringstream ss;
    ss << "Failed to close file " << filename << ", error code: " << ret << " "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
}

void write_statistic_file(const std::string& filename, size_t capacity,
                          size_t size) {
  size_t buffer[2] = {capacity, size};
  write_file(filename, buffer, sizeof(size_t), 2);
}

void read_statistic_file(const std::string& filename, size_t& capacity,
                         size_t& size) {
  size_t buffer[2];
  read_file(filename, buffer, sizeof(size_t), 2);
  capacity = buffer[0];
  size = buffer[1];
}

}  // namespace neug
