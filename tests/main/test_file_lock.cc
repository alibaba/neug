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

#include <unistd.h>

#include <errno.h>
#include <fcntl.h>
#include <sys/wait.h>

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "neug/main/file_lock.h"

namespace neug {
namespace test {

TEST(FileLockTest, ClosingOneReaderKeepsProcessLock) {
  const auto db_dir = std::filesystem::temp_directory_path() /
                      ("neug_file_lock_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);
  std::filesystem::create_directories(db_dir);

  std::string error;
  FileLock writer(db_dir.string());
  ASSERT_TRUE(writer.lock(error, DBMode::READ_WRITE)) << error;
  writer.unlock();

  FileLock first_reader(db_dir.string());
  FileLock second_reader(db_dir.string());
  ASSERT_TRUE(first_reader.lock(error, DBMode::READ_ONLY)) << error;
  ASSERT_TRUE(second_reader.lock(error, DBMode::READ_ONLY)) << error;
  first_reader.unlock();

  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    const auto lock_path = db_dir / FileLock::LOCK_FILE_NAME;
    const int fd = ::open(lock_path.c_str(), O_RDWR);
    if (fd == -1) {
      ::_exit(1);
    }
    struct flock lock {};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_len = 0;
    const int result = ::fcntl(fd, F_SETLK, &lock);
    ::close(fd);
    ::_exit(result == -1 && (errno == EACCES || errno == EAGAIN) ? 0 : 1);
  }

  int child_status = 0;
  ASSERT_EQ(::waitpid(child, &child_status, 0), child);
  EXPECT_TRUE(WIFEXITED(child_status));
  EXPECT_EQ(WEXITSTATUS(child_status), 0);

  second_reader.unlock();
  std::filesystem::remove_all(db_dir);
}

}  // namespace test
}  // namespace neug
