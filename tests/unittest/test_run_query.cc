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

#include <iostream>
#include <string>
#include "neug/main/neug_db.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"

int main(int argc, char** argv) {
  // Expect 1 args, data path
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <data_path> <query_string>"
              << std::endl;
    std::cerr << " Got " << argc - 1 << " arguments." << std::endl;
    return 1;
  }

  std::string data_path = argv[1];
  std::string query_string = argv[2];

  gs::NeugDB db;
  db.Open(data_path);
  auto conn = db.Connect();
  LOG(INFO) << "Running query: " << query_string;

  auto res = conn->Query(query_string);
  if (!res.ok()) {
    LOG(ERROR) << "Query failed: " << res.status().ToString();
    return 1;
  }
  LOG(INFO) << "Query result: " << res.value().length() << " records found.";
  while (res.value().hasNext()) {
    auto record = res.value().next();
    LOG(INFO) << "Record: " << record.ToString();
  }
  return 0;
}