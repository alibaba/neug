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
#include <filesystem>

#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"

namespace neug {
namespace test {

class ArrayServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() / "neug_array_tp_test";
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directories(test_dir_);

    db_ = std::make_unique<NeugDB>();
    db_->Open((test_dir_ / "graph").string(), 4);

    auto conn = db_->Connect();
    ASSERT_TRUE(
        conn->Query("CREATE NODE TABLE Sensor(id INT64, readings INT32[3], "
                    "PRIMARY KEY(id));"));
    conn->Close();
  }

  void TearDown() override {
    if (db_ && !db_->IsClosed()) {
      db_->Close();
    }
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  std::filesystem::path test_dir_;
  std::unique_ptr<NeugDB> db_;
};

TEST_F(ArrayServiceTest, SessionCreateAndReturnArrayProperty) {
  ServiceConfig config;
  config.query_port = 0;
  config.host_str = "127.0.0.1";
  NeugDBService service(*db_, config);

  auto guard = service.AcquireSession();
  ASSERT_TRUE(guard);

  auto create1 = guard->Eval(
      R"({"query":"CREATE (s:Sensor {id: 1, readings: [10, 20, 30]});","access_mode":"","parameters":null})");
  ASSERT_TRUE(create1) << create1.error().ToString();

  auto create2 = guard->Eval(
      R"({"query":"CREATE (s:Sensor {id: 2, readings: [40, 50, 60]});","access_mode":"","parameters":null})");
  ASSERT_TRUE(create2) << create2.error().ToString();

  auto result = guard->Eval(
      R"({"query":"MATCH (s:Sensor) WHERE s.id = 1 RETURN s.readings;","access_mode":"","parameters":null})");
  ASSERT_TRUE(result) << result.error().ToString();

  neug::QueryResponse response;
  ASSERT_TRUE(response.ParseFromString(result.value()));
  ASSERT_EQ(response.row_count(), 1u);
  ASSERT_EQ(response.arrays_size(), 1);
  ASSERT_TRUE(response.arrays(0).has_list_array());
  const auto& list = response.arrays(0).list_array();
  ASSERT_EQ(list.offsets_size(), 2);
  ASSERT_EQ(list.offsets(1) - list.offsets(0), 3);
}

}  // namespace test
}  // namespace neug
