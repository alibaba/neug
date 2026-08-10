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

// A helper class to track the databases currently locked by the current
// process. This is necessary because the file lock is shared across the whole
// process, and we need to ensure that if the same process tries to open the
// same database multiple times, it should be allowed if the lock mode is
// compatible, and should be rejected if the lock mode is incompatible.
class CurrentHoldDbs {
 public:
  static CurrentHoldDbs& get() {
    static CurrentHoldDbs instance;
    return instance;
  }

  bool lock(const std::string& db_path, DBMode mode, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto existing = opened_dbs_.find(db_path);
    if (existing != opened_dbs_.end()) {
      if (existing->second.mode == DBMode::READ_ONLY &&
          mode == DBMode::READ_ONLY) {
        ++existing->second.references;
        return true;
      }
      error_msg = mode == DBMode::READ_ONLY
                      ? "Lock file is already locked in write mode by the "
                        "current process: " +
                            db_path
                      : "Lock file is already locked in read or write mode "
                        "by the current process: " +
                            db_path;
      return false;
    }

    // The lock file is runtime coordination metadata rather than database
    // state. Create it on demand for legacy databases that predate the file;
    // read-only opens still use a read-only descriptor and a shared lock.
    const int flags = (mode == DBMode::READ_ONLY ? O_RDONLY : O_RDWR) | O_CREAT;
    const int fd = ::open(db_path.c_str(), flags, 0600);
    if (fd == -1) {
      if (errno == EACCES) {
        THROW_PERMISSION_DENIED(
            "Permission denied when opening lock file: " + db_path +
            ", please check the permissions of the data directory.");
      }
      THROW_RUNTIME_ERROR("Failed to open lock file: " + db_path +
                          ", error: " + std::string(strerror(errno)));
    }
    if (!setLock(fd, mode == DBMode::READ_ONLY ? F_RDLCK : F_WRLCK, false,
                 error_msg)) {
      ::close(fd);
      return false;
    }
    opened_dbs_.emplace(db_path, Entry{fd, mode, 1});
    return true;
  }

  void unlock(const std::string& db_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto existing = opened_dbs_.find(db_path);
    if (existing == opened_dbs_.end()) {
      return;
    }
    if (--existing->second.references != 0) {
      return;
    }
    std::string error_msg;
    if (!setLock(existing->second.fd, F_UNLCK, true, error_msg)) {
      LOG(ERROR) << "Failed to unlock file lock: " << error_msg;
    }
    ::close(existing->second.fd);
    opened_dbs_.erase(existing);
  }

 private:
  struct Entry {
    int fd;
    DBMode mode;
    size_t references;
  };

  static bool setLock(int fd, short type, bool wait, std::string& error_msg) {
    struct flock fl;
    std::memset(&fl, 0, sizeof(fl));
    fl.l_type = type;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 0;

    const int cmd = wait ? F_SETLKW : F_SETLK;
    while (true) {
      if (::fcntl(fd, cmd, &fl) == 0) {
        return true;
      }
      if (errno == EACCES || errno == EAGAIN) {
        error_msg = "Lock file is already locked by another process.";
        return false;
      }
      if (errno != EINTR) {
        error_msg = "Failed to acquire lock: " + std::string(strerror(errno));
        return false;
      }
    }
  }

  std::mutex mutex_;
  std::map<std::string, Entry> opened_dbs_;
};

FileLock::FileLock(const std::string& data_dir)
    : lock_file_path_(data_dir + "/" + LOCK_FILE_NAME), locked_(false) {}

FileLock::~FileLock() { unlock(); }

bool FileLock::lock(std::string& error_msg, DBMode mode) {
  if (locked_) {
    error_msg = "File lock is already held by this object: " + lock_file_path_;
    return false;
  }
  locked_ = CurrentHoldDbs::get().lock(lock_file_path_, mode, error_msg);
  return locked_;
}

void FileLock::unlock() {
  if (!locked_) {
    return;  // Not locked, nothing to do
  }
  CurrentHoldDbs::get().unlock(lock_file_path_);
  locked_ = false;
}

}  // namespace neug
