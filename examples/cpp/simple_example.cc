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

#include "glog/logging.h"
#include "neug/neug.h"

int main(int argc, char** argv) {
  neug::NeugDB db;  // In-memory database
  db.Open("");
  auto conn = db.Connect();
  CHECK(
      conn->Query("CREATE NODE TABLE person(id INT64, name STRING, age "
                  "INT64, PRIMARY KEY(id));"));
  CHECK(conn->Query("CREATE (a: person {id: 1, name: 'Alice', age: 30});"));
  CHECK(conn->Query("CREATE (b: person {id: 2, name: 'Bob', age: 25});"));
  auto result =
      conn->Query("MATCH (n: person) WHERE n.age > 26 RETURN n.name;");
  CHECK(result);
  LOG(INFO) << result.value().ToString();
  CHECK(result.value().length() == 1);
  CHECK(result.value().response().arrays(0).string_array().values(0) ==
        "Alice");
  db.Close();
  return 0;
}