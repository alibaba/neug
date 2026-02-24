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
#pragma once

#include <cstdio>
#include <filesystem>
#include <set>
#include <string>
#include "neug/config.h"

namespace neug {

/*
A simple file lock mechanism to ensure that only one instance of the
NeugDB can run at a time. This is useful for preventing multiple processes
from modifying the database at the same time, which could lead to data
corruption or inconsistencies.
*/
class FileLock {
 public:
  static constexpr const char* LOCK_FILE_NAME = "neugdb.lock";
  explicit FileLock(const std::string& data_dir)
      : data_dir_(data_dir), lock_file_path_(data_dir + "/" + LOCK_FILE_NAME) {}

  static void CleanupAllLocks() {
    for (const auto& lock_file : allLockFiles) {
      if (std::filesystem::exists(lock_file)) {
        std::remove(lock_file.c_str());
      }
    }
    allLockFiles.clear();
  }

  ~FileLock() { unlock(); }

  bool lock(std::string& error_msg, DBMode mode);

  void unlock();

 private:
  std::string data_dir_;
  std::string lock_file_path_;
  bool locked_ = false;

  static std::set<std::string> allLockFiles;
};

}  // namespace neug
