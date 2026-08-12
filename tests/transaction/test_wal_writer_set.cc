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

#include <gtest/gtest.h>

#include "neug/main/wal_writer_set.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace {

TEST(WalWriterSetTest, RetainsDirectWriterAcrossTransactionalActivation) {
  WalWriterSet writers(3, DBMode::READ_ONLY, "");
  auto* const direct_writer = &writers.DirectWriter();

  EXPECT_EQ(&writers.WriterFor(0), direct_writer);

  writers.ActivateTransactional("");
  EXPECT_EQ(&writers.WriterFor(0), direct_writer);
  EXPECT_NE(&writers.WriterFor(1), direct_writer);
  EXPECT_NE(&writers.WriterFor(2), direct_writer);
  EXPECT_NE(&writers.WriterFor(1), &writers.WriterFor(2));

  writers.DeactivateTransactional();
  EXPECT_EQ(&writers.DirectWriter(), direct_writer);
  EXPECT_THROW(writers.WriterFor(1), exception::InvalidArgumentException);
}

TEST(WalWriterSetTest, RotationRetainsActiveWriterIdentity) {
  WalWriterSet writers(2, DBMode::READ_ONLY, "");
  writers.ActivateTransactional("");
  auto* const direct_writer = &writers.WriterFor(0);
  auto* const transactional_writer = &writers.WriterFor(1);

  writers.RotateActive("");
  EXPECT_EQ(&writers.WriterFor(0), direct_writer);
  EXPECT_EQ(&writers.WriterFor(1), transactional_writer);
}

}  // namespace
}  // namespace neug
