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
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <filesystem>
#include <string>
#include <type_traits>

#include <poll.h>

#include <gtest/gtest.h>

#include "neug/main/file_lock.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace test {

static_assert(!std::is_copy_constructible_v<FileLock>);
static_assert(!std::is_copy_assignable_v<FileLock>);

TEST(FileLockTest, ReadOnlyCreatesMissingCoordinationFile) {
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_missing_file_lock_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);
  std::filesystem::create_directories(db_dir);

  const auto lock_path = db_dir / FileLock::LOCK_FILE_NAME;
  ASSERT_FALSE(std::filesystem::exists(lock_path));

  std::string error;
  FileLock reader(db_dir.string());
  ASSERT_TRUE(reader.lock(error, DBMode::READ_ONLY)) << error;
  EXPECT_TRUE(std::filesystem::exists(lock_path));
  reader.unlock();

  std::filesystem::remove_all(db_dir);
}

TEST(FileLockTest, ReadOnlyLockDoesNotRequireWritePermission) {
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_read_only_file_lock_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);
  std::filesystem::create_directories(db_dir);

  std::string error;
  FileLock initializer(db_dir.string());
  ASSERT_TRUE(initializer.lock(error, DBMode::READ_WRITE)) << error;
  initializer.unlock();

  const auto lock_path = db_dir / FileLock::LOCK_FILE_NAME;
  ASSERT_EQ(::chmod(lock_path.c_str(), S_IRUSR | S_IRGRP | S_IROTH), 0);

  FileLock reader(db_dir.string());
  ASSERT_TRUE(reader.lock(error, DBMode::READ_ONLY)) << error;
  reader.unlock();

  ASSERT_EQ(::chmod(lock_path.c_str(), S_IRUSR | S_IWUSR), 0);
  std::filesystem::remove_all(db_dir);
}

TEST(FileLockTest, ReadOnlyOpenOnReadOnlyDirectoryReportsWritableHint) {
  if (::geteuid() == 0) {
    GTEST_SKIP() << "chmod-based permission checks do not apply to root";
  }
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_read_only_dir_lock_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);
  std::filesystem::create_directories(db_dir);

  // The lock file is missing and the directory is not writable, so the
  // O_RDONLY | O_CREAT open must fail and explain that read-only mode still
  // requires a writable data directory.
  ASSERT_EQ(::chmod(db_dir.c_str(),
                    S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH),
            0);

  std::string error;
  FileLock reader(db_dir.string());
  bool permission_denied = false;
  try {
    (void) reader.lock(error, DBMode::READ_ONLY);
  } catch (const exception::PermissionDeniedException& err) {
    permission_denied = true;
    EXPECT_NE(std::string(err.what())
                  .find("Read-only mode still requires a writable data "
                        "directory"),
              std::string::npos)
        << err.what();
  }
  EXPECT_TRUE(permission_denied)
      << "read-only lock on a read-only directory should report a "
         "permission error, got: "
      << error;

  ASSERT_EQ(::chmod(db_dir.c_str(), S_IRWXU), 0);
  std::filesystem::remove_all(db_dir);
}

TEST(FileLockTest, RejectsRepeatedLockOnSameObject) {
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_repeated_file_lock_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);
  std::filesystem::create_directories(db_dir);

  std::string error;
  FileLock initializer(db_dir.string());
  ASSERT_TRUE(initializer.lock(error, DBMode::READ_WRITE)) << error;
  initializer.unlock();

  FileLock reader(db_dir.string());
  ASSERT_TRUE(reader.lock(error, DBMode::READ_ONLY)) << error;
  EXPECT_FALSE(reader.lock(error, DBMode::READ_ONLY));
  EXPECT_NE(error.find("already held by this object"), std::string::npos);
  reader.unlock();

  FileLock writer(db_dir.string());
  ASSERT_TRUE(writer.lock(error, DBMode::READ_WRITE)) << error;
  writer.unlock();

  std::filesystem::remove_all(db_dir);
}

TEST(FileLockTest, RejectsIncompatibleModeInSameProcess) {
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_mixed_mode_file_lock_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);
  std::filesystem::create_directories(db_dir);

  std::string error;
  // A held read lock rejects a same-process writer. The message must point
  // at the data directory (not the internal lock file) and tell the user
  // what to do next.
  {
    FileLock reader(db_dir.string());
    ASSERT_TRUE(reader.lock(error, DBMode::READ_ONLY)) << error;

    FileLock writer(db_dir.string());
    EXPECT_FALSE(writer.lock(error, DBMode::READ_WRITE));
    EXPECT_NE(error.find("you can't open it in write mode in the same process"),
              std::string::npos)
        << error;
    EXPECT_NE(error.find(db_dir.string()), std::string::npos) << error;
    EXPECT_EQ(error.find(FileLock::LOCK_FILE_NAME), std::string::npos) << error;

    // The rejected writer must not disturb the held read lock.
    FileLock second_reader(db_dir.string());
    EXPECT_TRUE(second_reader.lock(error, DBMode::READ_ONLY)) << error;
  }

  // A held write lock rejects a same-process reader.
  {
    FileLock writer(db_dir.string());
    ASSERT_TRUE(writer.lock(error, DBMode::READ_WRITE)) << error;

    FileLock reader(db_dir.string());
    EXPECT_FALSE(reader.lock(error, DBMode::READ_ONLY));
    EXPECT_NE(
        error.find("you can't open it in read-only mode in the same process"),
        std::string::npos)
        << error;
  }

  // Once the conflicting handle is gone, the lock can be taken again.
  FileLock reader(db_dir.string());
  EXPECT_TRUE(reader.lock(error, DBMode::READ_ONLY)) << error;
  reader.unlock();

  std::filesystem::remove_all(db_dir);
}

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

  FileLock final_writer(db_dir.string());
  ASSERT_TRUE(final_writer.lock(error, DBMode::READ_WRITE)) << error;
  final_writer.unlock();

  std::filesystem::remove_all(db_dir);
}

TEST(FileLockTest, ForkedChildReacquiresProcessLock) {
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_forked_file_lock_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);
  std::filesystem::create_directories(db_dir);

  std::string error;
  FileLock parent_reader(db_dir.string());
  FileLock second_parent_reader(db_dir.string());
  ASSERT_TRUE(parent_reader.lock(error, DBMode::READ_ONLY)) << error;
  ASSERT_TRUE(second_parent_reader.lock(error, DBMode::READ_ONLY)) << error;

  int ready_pipe[2];
  int release_pipe[2];
  ASSERT_EQ(::pipe(ready_pipe), 0);
  ASSERT_EQ(::pipe(release_pipe), 0);

  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::close(ready_pipe[0]);
    ::close(release_pipe[1]);

    std::string child_error;
    // Discard descriptors and accounting copied from the parent before the
    // child acquires any locks of its own.
    parent_reader.unlock();
    FileLock child_reader(db_dir.string());
    const bool acquired = child_reader.lock(child_error, DBMode::READ_ONLY);
    if (acquired) {
      // This inherited handle belongs to the parent process and must not
      // release the child process's newly acquired lock.
      second_parent_reader.unlock();
    }
    const char status = acquired ? '1' : '0';
    (void) ::write(ready_pipe[1], &status, 1);

    char release = 0;
    (void) ::read(release_pipe[0], &release, 1);
    child_reader.unlock();
    ::_exit(acquired ? 0 : 1);
  }

  ::close(ready_pipe[1]);
  ::close(release_pipe[0]);
  pollfd ready{ready_pipe[0], POLLIN, 0};
  if (::poll(&ready, 1, 5000) != 1) {
    (void) ::kill(child, SIGKILL);
    int child_wait_status = 0;
    (void) ::waitpid(child, &child_wait_status, 0);
    ::close(ready_pipe[0]);
    ::close(release_pipe[1]);
    parent_reader.unlock();
    second_parent_reader.unlock();
    std::filesystem::remove_all(db_dir);
    FAIL() << "Timed out waiting for forked child to acquire the lock";
    return;
  }
  char child_status = '0';
  EXPECT_EQ(::read(ready_pipe[0], &child_status, 1), 1);
  EXPECT_EQ(child_status, '1');

  parent_reader.unlock();
  second_parent_reader.unlock();
  FileLock writer(db_dir.string());
  EXPECT_FALSE(writer.lock(error, DBMode::READ_WRITE));

  EXPECT_EQ(::write(release_pipe[1], "1", 1), 1);
  ::close(ready_pipe[0]);
  ::close(release_pipe[1]);

  int wait_status = 0;
  ASSERT_EQ(::waitpid(child, &wait_status, 0), child);
  EXPECT_TRUE(WIFEXITED(wait_status));
  EXPECT_EQ(WEXITSTATUS(wait_status), 0);

  ASSERT_TRUE(writer.lock(error, DBMode::READ_WRITE)) << error;
  writer.unlock();
  std::filesystem::remove_all(db_dir);
}

}  // namespace test
}  // namespace neug
