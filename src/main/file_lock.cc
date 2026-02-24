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

#include "neug/main/file_lock.h"
#include <errno.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fstream>

#include "neug/utils/exception/exception.h"

namespace neug {

bool FileLock::lock(std::string& error_msg, DBMode mode) {
  // If the lock file already exists, it means another process is using the
  // database.
  struct stat buffer;
  if (stat(lock_file_path_.c_str(), &buffer) == 0) {
    LOG(ERROR) << "Lock file already exists: " << lock_file_path_;
    std::ifstream lock_file(lock_file_path_);
    if (!lock_file.is_open()) {
      // Get the error message if the file cannot be opened
      error_msg = "Failed to open lock file: " + lock_file_path_ +
                  ", error: " + std::string(strerror(errno));
      LOG(ERROR) << error_msg;
      THROW_IO_EXCEPTION(error_msg);
    }
    std::string line;
    std::getline(lock_file, line);
    // Expect content like "READ", "WRITE".
    if (line.find("READ") != std::string::npos) {
      if (mode == DBMode::READ_WRITE) {
        error_msg =
            "The database has already been locked for read-only access: " +
            line +
            ", if you are sure you want to proceed(in case the process "
            "has already died), please remove the lock file manually.";
        LOG(ERROR) << error_msg;
        THROW_DATABASE_LOCKED_EXCEPTION(error_msg);
      }
      LOG(INFO) << "Database is locked for read access, proceeding in "
                   "read-only mode.";
      return true;
    } else if (line.find("WRITE") != std::string::npos) {
      error_msg = "The database is already locked for write access: " + line +
                  ", if you are sure you want to proceed(in case the process "
                  "has already died), please remove the "
                  "lock file manually.";
      LOG(ERROR) << error_msg;
      THROW_DATABASE_LOCKED_EXCEPTION(error_msg);
    } else {
      error_msg = "Unknown lock type in lock file: " + line;
      LOG(ERROR) << error_msg;
      return false;
    }
  }

  int32_t fd = ::open(lock_file_path_.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
  if (fd < 0) {
    error_msg = "Failed to create lock file: " + std::string(strerror(errno));
    if (errno == EACCES) {
      THROW_PERMISSION_DENIED(
          "Permission denied when trying to create lock file: " +
          lock_file_path_);
    } else if (errno == EEXIST) {
      error_msg = "Lock file already exists: " + lock_file_path_ +
                  ", if you are sure you want to proceed(in case the process "
                  "has already died), please remove the lock file manually.";
    } else {
      error_msg = "Failed to create lock file: " + std::string(strerror(errno));
    }
    LOG(ERROR) << error_msg;
    return false;
  }

  // Write to the lock file to indicate the lock type
  std::string content = (mode == DBMode::READ_WRITE ? "WRITE" : "READ");
  content += ":" + std::to_string(getpid()) + "\n";
  if (write(fd, content.c_str(), content.size()) < 0) {
    error_msg = "Failed to write to lock file: " + std::string(strerror(errno));
    LOG(ERROR) << error_msg;
    ::close(fd);
    return false;
  }
  VLOG(10) << "Successfully locked directory: " << data_dir_
           << ", lock file: " << lock_file_path_;
  allLockFiles.insert(lock_file_path_);

  ::close(fd);  // Close the file descriptor after writing
  locked_ = true;
  return true;
}

void FileLock::unlock() {
  if (!locked_) {
    return;
  }
  if (std::remove(lock_file_path_.c_str()) != 0) {
    LOG(ERROR) << "Failed to remove lock file: " << lock_file_path_;
  } else {
    VLOG(10) << "Successfully removed lock file: " << lock_file_path_;
  }
  if (allLockFiles.find(lock_file_path_) != allLockFiles.end()) {
    allLockFiles.erase(lock_file_path_);
  }
  locked_ = false;
}

std::set<std::string> FileLock::allLockFiles;
}  // namespace neug
