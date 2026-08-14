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

#include <dlfcn.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "neug/main/neug_db.h"

namespace {

using GetInitCountFunc = uint64_t (*)();

TEST(ExtensionLoad, DuplicateQueryInitializesExtensionOnce) {
  ASSERT_EQ(
      setenv("NEUG_EXTENSION_HOME_PYENV", TEST_OUT_OF_TREE_EXTENSION_HOME, 1),
      0);

  const auto db_path = std::filesystem::temp_directory_path() /
                       "neug_duplicate_extension_load_test";
  std::filesystem::remove_all(db_path);

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(db_path.string()));
  auto conn = db.Connect();
  ASSERT_NE(conn, nullptr);

  auto first_load = conn->Query("LOAD test_out_of_tree;");
  ASSERT_TRUE(first_load.has_value()) << first_load.error().ToString();

  auto duplicate_load = conn->Query("LOAD test_out_of_tree;");
  ASSERT_TRUE(duplicate_load.has_value()) << duplicate_load.error().ToString();

  void* handle = dlopen(TEST_OUT_OF_TREE_EXTENSION_PATH, RTLD_NOW | RTLD_LOCAL);
  ASSERT_NE(handle, nullptr) << dlerror();
  dlerror();
  auto get_init_count =
      reinterpret_cast<GetInitCountFunc>(dlsym(handle, "GetInitCount"));
  const char* symbol_error = dlerror();
  ASSERT_EQ(symbol_error, nullptr)
      << "Failed to resolve GetInitCount: " << symbol_error;
  ASSERT_NE(get_init_count, nullptr);
  EXPECT_EQ(get_init_count(), 1);
  dlclose(handle);

  conn.reset();
  db.Close();
  std::filesystem::remove_all(db_path);
}

}  // namespace
