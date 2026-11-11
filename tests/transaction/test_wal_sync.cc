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

#include <cerrno>
#include <stdexcept>

#include "gtest/gtest.h"
#include "neug/config.h"
#include "neug/main/neug_db.h"
#include "neug/transaction/wal/wal_sync.h"

namespace neug {
namespace {

TEST(WalSyncTest, DefaultsAndParsing) {
  EXPECT_EQ(NeugDBConfig().wal_sync_mode, WalSyncMode::STANDARD);
  EXPECT_EQ(NeugDBConfig("graph").wal_sync_mode, WalSyncMode::STANDARD);
  for (const auto* value : {"STANDARD", "standard", "Standard", "sTaNdArD"}) {
    EXPECT_EQ(ParseWalSyncMode(value), WalSyncMode::STANDARD);
  }
  for (const auto* value : {"FULL", "full", "Full", "fUlL"}) {
    EXPECT_EQ(ParseWalSyncMode(value), WalSyncMode::FULL);
  }
  for (const auto* value : {"", "NONE", "INVALID", " FULL", "full "}) {
    EXPECT_THROW(ParseWalSyncMode(value), std::invalid_argument);
  }
}

TEST(WalSyncTest, RejectsInvalidDatabasePolicy) {
  NeugDBConfig config(":memory:");
  config.wal_sync_mode = static_cast<WalSyncMode>(99);
  NeugDB db;
  EXPECT_THROW(db.Open(config), std::exception);
}

TEST(WalSyncTest, SelectsRequestedSyncOperation) {
  int standard_calls = 0;
  int full_calls = 0;
  auto standard = [&] {
    ++standard_calls;
    return 0;
  };
  auto full = [&] {
    ++full_calls;
    return 0;
  };
  EXPECT_EQ(detail::SyncWal(WalSyncMode::STANDARD, standard, full), 0);
  EXPECT_EQ(standard_calls, 1);
  EXPECT_EQ(full_calls, 0);
  EXPECT_EQ(detail::SyncWal(WalSyncMode::FULL, standard, full), 0);
  EXPECT_EQ(standard_calls, 1);
  EXPECT_EQ(full_calls, 1);
}

TEST(WalSyncTest, UnsupportedFullFallsBack) {
  for (int error : {ENOTSUP, EINVAL, ENOSYS}) {
    int standard_calls = 0;
    EXPECT_EQ(detail::SyncWal(
                  WalSyncMode::FULL,
                  [&] {
                    ++standard_calls;
                    return 0;
                  },
                  [error] {
                    errno = error;
                    return -1;
                  }),
              0);
    EXPECT_EQ(standard_calls, 1);
  }
}

TEST(WalSyncTest, SyncFailuresAreNotHidden) {
  for (auto mode : {WalSyncMode::STANDARD, WalSyncMode::FULL}) {
    EXPECT_EQ(detail::SyncWal(
                  mode,
                  [] {
                    errno = EIO;
                    return -1;
                  },
                  [] {
                    errno = ENOTSUP;
                    return -1;
                  }),
              -1);
    EXPECT_EQ(errno, EIO);
  }
  for (int error : {EIO, EBADF, ENOSPC, EINTR}) {
    bool fell_back = false;
    EXPECT_EQ(detail::SyncWal(
                  WalSyncMode::FULL,
                  [&] {
                    fell_back = true;
                    return 0;
                  },
                  [error] {
                    errno = error;
                    return -1;
                  }),
              -1);
    EXPECT_EQ(errno, error);
    EXPECT_FALSE(fell_back);
  }
}

}  // namespace
}  // namespace neug
