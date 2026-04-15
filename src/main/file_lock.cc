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
#include <map>
#include <mutex>
#include <string>

#include "neug/utils/exception/exception.h"

namespace neug {

class CurrentHoldDbs {
 public:
  static CurrentHoldDbs& get() {
    static CurrentHoldDbs instance;
    return instance;
  }

  bool lock(const std::string& db_path, DBMode mode) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (opened_dbs_.find(db_path) != opened_dbs_.end()) {
      if (opened_dbs_[db_path] > 0 && mode == DBMode::READ_ONLY) {
        opened_dbs_[db_path] += 1;
        return true;  // Database is already opened in the same mode
      } else {
        // Database is already opened in a different mode, which is not allowed
        return false;
      }
      return false;  // Database is already opened
    }
    opened_dbs_[db_path] = (mode == DBMode::READ_ONLY);
    return true;
  }

  void unlock(const std::string& db_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (opened_dbs_.find(db_path) != opened_dbs_.end()) {
      opened_dbs_[db_path] -= 1;
      if (opened_dbs_[db_path] <= 0) {
        opened_dbs_.erase(db_path);
      }
    }
  }

 private:
  std::mutex mutex_;
  std::map<std::string, int> opened_dbs_;
};

FileLock::FileLock(const std::string& data_dir)
    : lock_file_path_(data_dir + "/" + LOCK_FILE_NAME),
      fd_(-1),
      locked_(false) {
  fd_ = ::open(lock_file_path_.c_str(), O_RDWR | O_CREAT, 0600);
  if (fd_ == -1) {
    if (errno == EACCES) {
      THROW_PERMISSION_DENIED(
          "Permission denied when creating lock file: " + lock_file_path_ +
          ", please check the permissions of the data directory.");
    }
    THROW_DATABASE_LOCKED_EXCEPTION(
        "Failed to create lock file: " + lock_file_path_ +
        ", error: " + std::string(strerror(errno)));
  }
}

FileLock::~FileLock() {
  if (fd_ != -1) {
    unlock();
    ::close(fd_);
  }
}

bool FileLock::lock(std::string& error_msg, DBMode mode) {
  // If the lock file already exists, it means the current process has already
  // locked the database, return an error message.
  if (CurrentHoldDbs::get().lock(lock_file_path_, mode)) {
    // This process is the first one to lock the database, proceed with locking
    // the file.
    if (lock(mode == DBMode::READ_ONLY ? F_RDLCK : F_WRLCK, false, error_msg)) {
      locked_ = true;
    } else {
      locked_ = false;
      CurrentHoldDbs::get().unlock(lock_file_path_);
    }
    return locked_;
  } else {
    // The database is already locked by the current process, return an error
    // message.
    locked_ = false;
    error_msg = "Lock file is already locked by the current process: " +
                lock_file_path_ +
                ", you may have already opened the database in this process, "
                "please check.";
    return false;
  }
}

void FileLock::unlock() {
  if (!locked_) {
    return;  // Not locked, nothing to do
  }
  std::string error_msg;
  lock(F_UNLCK, true, error_msg);
  CurrentHoldDbs::get().unlock(lock_file_path_);
  locked_ = false;
}

bool FileLock::lock(short type, bool wait, std::string& error_msg) {
  struct flock fl;
  std::memset(&fl, 0, sizeof(fl));
  fl.l_type = type;
  fl.l_whence = SEEK_SET;
  fl.l_start = 0;
  fl.l_len = 0;  // Lock the whole file

  int cmd = wait ? F_SETLKW : F_SETLK;
  while (true) {
    if (::fcntl(fd_, cmd, &fl) == 0) {
      return true;  // Lock acquired successfully
    } else if (errno == EACCES || errno == EAGAIN) {
      // The file is already locked by another process
      error_msg =
          "Lock file is already locked by another process: " + lock_file_path_ +
          ", please check if another instance of the database is running.";
      return false;
    } else if (errno == EINTR) {
      // Interrupted by a signal, retry
      continue;
    } else {
      // An unexpected error occurred
      error_msg = "Failed to acquire lock: " + std::string(strerror(errno));
      return false;
    }
  }
}

}  // namespace neug
