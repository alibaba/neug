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
  gs::NeugDB db("");  // In-memory database
  auto conn = db.connect();
  CHECK(conn->query("CREATE NODE TABLE person(id INT64, name STRING, age "
                    "INT64, PRIMARY KEY(id));")
            .ok());
  CHECK(
      conn->query("CREATE (a: person {id: 1, name: 'Alice', age: 30});").ok());
  CHECK(conn->query("CREATE (b: person {id: 2, name: 'Bob', age: 25});").ok());
  auto result = conn->query("MATCH (n: person) WHERE n.age > 26 RETURN n.name");
  CHECK(result.ok());
  CHECK(result.value().hasNext());
  auto record = result.value().next();
  CHECK(record.ToString() == "<element { object { str: \"Alice\" } }>")
      << record.ToString();
  CHECK(!result.value().hasNext());
  return 0;
}