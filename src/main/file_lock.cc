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
#include <cstring>
#include <fstream>

#include "neug/utils/exception/exception.h"

namespace neug {

FileLock::FileLock(const std::string& data_dir)
    : lock_file_path_(data_dir + "/" + LOCK_FILE_NAME), fd_(-1) {
  fd_ = ::open(lock_file_path_.c_str(), O_RDWR | O_CREAT, 0666);
  if (fd_ == -1) {
    throw std::runtime_error("Failed to create lock file: " + lock_file_path_ +
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
  // If the lock file already exists, it means another process is using the
  // database.
  if (mode == DBMode::READ_ONLY) {
    return lock(F_RDLCK, false, error_msg);
  } else {
    return lock(F_WRLCK, false, error_msg);
  }
}

void FileLock::unlock() {
  std::string error_msg;
  lock(F_UNLCK, true, error_msg);
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
          ", if you are sure you want to proceed(in case the process "
          "has already died), please remove the lock file manually.";
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
