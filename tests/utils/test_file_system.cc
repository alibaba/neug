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

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/vfs/file_system.h"

namespace neug::test {
namespace {

class TaggedFileSystem final : public fsys::FileSystem {
 public:
  explicit TaggedFileSystem(std::string tag) : tag_(std::move(tag)) {}

  std::vector<std::string> glob(const std::string&) override { return {tag_}; }

 private:
  std::string tag_;
};

TEST(FileSystemRegistry, DuplicateRegistrationKeepsExistingFactory) {
  fsys::FileSystemRegistry registry;
  ASSERT_TRUE(registry.Register("replay-safe", [](const reader::FileSchema&) {
    return std::make_unique<TaggedFileSystem>("first");
  }));
  EXPECT_FALSE(registry.Register("replay-safe", [](const reader::FileSchema&) {
    return std::make_unique<TaggedFileSystem>("second");
  }));

  reader::FileSchema schema;
  schema.protocol = "replay-safe";
  auto file_system = registry.Provide(schema);
  EXPECT_EQ(file_system->glob("unused"), std::vector<std::string>({"first"}));
}

}  // namespace
}  // namespace neug::test
