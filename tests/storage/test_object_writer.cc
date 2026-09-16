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

#include "neug/storages/chunk/object_writer.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "neug/storages/chunk/chunk_types.h"

namespace neug {
namespace {

TEST(Crc32cTest, EmptyIsZero) { EXPECT_EQ(Crc32c("", 0), 0u); }

TEST(Crc32cTest, DeterministicAndSensitive) {
  const std::string a = "hello world";
  const std::string b = "hello worle";  // one byte differs
  EXPECT_EQ(Crc32c(a.data(), a.size()), Crc32c(a.data(), a.size()));
  EXPECT_NE(Crc32c(a.data(), a.size()), Crc32c(b.data(), b.size()));
}

TEST(ObjectWriterTest, PacksBlocksAndAssignsSlicesOnSeal) {
  std::vector<std::string> committed;
  uint64_t next_id = 100;
  ObjectWriter writer(
      [&](const void* data, size_t len) {
        committed.emplace_back(static_cast<const char*>(data), len);
        return next_id++;
      },
      /*target_bytes=*/1024);

  const std::string b1(100, 'a');
  const std::string b2(200, 'b');
  EXPECT_EQ(writer.AppendBlock(b1.data(), static_cast<uint32_t>(b1.size())),
            0u);
  EXPECT_EQ(writer.AppendBlock(b2.data(), static_cast<uint32_t>(b2.size())),
            1u);

  // 312 < 1024 including alignment: object_id is still pending.
  EXPECT_TRUE(committed.empty());
  EXPECT_EQ(writer.slices()[0].object_id, 0u);

  writer.Seal();
  ASSERT_EQ(committed.size(), 1u);
  EXPECT_EQ(committed[0], b1 + std::string(12, '\0') +
                              b2);  // aligned payloads, no per-object header

  const auto& slices = writer.slices();
  EXPECT_EQ(slices[0].object_id, 100u);
  EXPECT_EQ(slices[1].object_id, 100u);
  EXPECT_EQ(slices[0].offset, 0u);
  EXPECT_EQ(slices[0].length, 100u);
  EXPECT_EQ(slices[1].offset, 112u);
  EXPECT_EQ(slices[1].length, 200u);
  EXPECT_EQ(slices[0].crc32c, Crc32c(b1.data(), b1.size()));
  EXPECT_EQ(slices[1].crc32c, Crc32c(b2.data(), b2.size()));
}

TEST(ObjectWriterTest, RollsToNewObjectAtTargetBytes) {
  std::vector<size_t> committed_sizes;
  uint64_t next_id = 1;
  ObjectWriter writer(
      [&](const void*, size_t len) {
        committed_sizes.push_back(len);
        return next_id++;
      },
      /*target_bytes=*/256);

  const std::string block(200, 'x');
  writer.AppendBlock(block.data(), static_cast<uint32_t>(block.size()));
  EXPECT_TRUE(committed_sizes.empty());  // 200 < 256

  writer.AppendBlock(block.data(), static_cast<uint32_t>(block.size()));
  ASSERT_EQ(committed_sizes.size(), 1u);  // 408 >= 256 -> rolled
  EXPECT_EQ(committed_sizes[0], 408u);
  EXPECT_EQ(writer.slices()[0].object_id, 1u);
  EXPECT_EQ(writer.slices()[1].object_id, 1u);

  writer.AppendBlock(block.data(), static_cast<uint32_t>(block.size()));
  writer.Seal();
  ASSERT_EQ(committed_sizes.size(), 2u);
  EXPECT_EQ(committed_sizes[1], 200u);
  EXPECT_EQ(writer.slices()[2].object_id, 2u);
}

TEST(ObjectWriterTest, SealOnEmptyIsNoOp) {
  bool called = false;
  ObjectWriter writer([&](const void*, size_t) {
    called = true;
    return 1u;
  });
  writer.Seal();
  EXPECT_FALSE(called);
  EXPECT_TRUE(writer.slices().empty());
}

TEST(ColumnUidRegistryTest, MonotonicNonReused) {
  ColumnUidRegistry reg;
  EXPECT_EQ(reg.Allocate(), 1u);
  EXPECT_EQ(reg.Allocate(), 2u);
  EXPECT_EQ(reg.next(), 3u);
  reg.SetNext(100);
  EXPECT_EQ(reg.Allocate(), 100u);
  EXPECT_EQ(reg.next(), 101u);
}

}  // namespace
}  // namespace neug
