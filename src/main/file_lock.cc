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

#include "src/main/file_lock.h"

namespace gs {

bool FileLock::lock(std::string& error_msg) {
  // If the lock file already exists, it means another process is using the
  // database.
  if (std::filesystem::exists(lock_file_path_)) {
    LOG(ERROR) << "Lock file already exists: " << lock_file_path_;
    // Read the lock file's content
    std::ifstream lock_file(lock_file_path_);
    if (!lock_file.is_open()) {
      LOG(ERROR) << "Failed to open lock file: " << lock_file_path_;
      return false;
    }
    std::getline(lock_file, error_msg);
    return false;
  }

  // Create the lock file
  std::ofstream lock_file(lock_file_path_);
  if (!lock_file.is_open()) {
    LOG(ERROR) << "Failed to create lock file: " << lock_file_path_;
    return false;
  }
  // Write the current process ID to the lock file
  lock_file << "Locked by process ID: " << getpid() << "\n";
  lock_file.close();
  VLOG(10) << "Successfully locked directory: " << data_dir_
           << ", lock file: " << lock_file_path_;
  allLockFiles.insert(lock_file_path_);
  return true;
}

void FileLock::unlock() {
  if (lock_file_.is_open()) {
    lock_file_.close();
  }
  remove(lock_file_path_.c_str());
  if (allLockFiles.find(lock_file_path_) != allLockFiles.end()) {
    allLockFiles.erase(lock_file_path_);
    VLOG(10) << "Unlocked directory: " << data_dir_
             << ", lock file: " << lock_file_path_;
  } else {
    LOG(WARNING) << "Attempted to unlock a non-locked file: "
                 << lock_file_path_;
  }
}

std::set<std::string> FileLock::allLockFiles;
}  // namespace gs