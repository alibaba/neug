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

namespace neug {
namespace test {

// End-to-end coverage for C++ QueryResult LIST/ARRAY decoding (#607):
// Connection::Query → sink list_array → GetString / IsNull /
// GetCurrentRowAsString.
TEST(QueryResultListTest, ListAndArrayColumnsViaConnection) {
  const auto test_dir =
      std::filesystem::temp_directory_path() / "neug_query_result_list_e2e";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  NeugDB db;
  NeugDBConfig config;
  config.data_dir = (test_dir / "graph").string();
  config.mode = DBMode::READ_WRITE;
  config.checkpoint_on_close = false;
  ASSERT_TRUE(db.Open(config));
  auto conn = db.Connect();
  ASSERT_NE(conn, nullptr);

  // LIST literal
  {
    auto res = conn->Query("RETURN [1, 2] AS nums;");
    ASSERT_TRUE(res) << res.error().ToString();
    auto& qr = res.value();
    ASSERT_TRUE(qr.hasNext());
    EXPECT_FALSE(qr.IsNull(0));
    EXPECT_EQ(qr.GetString(0), "[1, 2]");
    EXPECT_EQ(qr.GetCurrentRowAsString(), "[1, 2]");
  }

  // Fixed-size ARRAY property (serialized as list_array on the wire)
  {
    auto ddl = conn->Query(
        "CREATE NODE TABLE Sensor(id INT64, readings INT32[3], "
        "PRIMARY KEY(id));");
    ASSERT_TRUE(ddl) << ddl.error().ToString();

    auto insert =
        conn->Query("CREATE (s:Sensor {id: 1, readings: [10, 20, 30]});");
    ASSERT_TRUE(insert) << insert.error().ToString();

    auto res =
        conn->Query("MATCH (s:Sensor) WHERE s.id = 1 RETURN s.readings;");
    ASSERT_TRUE(res) << res.error().ToString();
    auto& qr = res.value();
    ASSERT_TRUE(qr.hasNext());
    EXPECT_FALSE(qr.IsNull(0));
    EXPECT_EQ(qr.GetString(0), "[10, 20, 30]");
    EXPECT_EQ(qr.GetCurrentRowAsString(), "[10, 20, 30]");
  }

  conn->Close();
  db.Close();
  std::filesystem::remove_all(test_dir);
}

}  // namespace test
}  // namespace neug
