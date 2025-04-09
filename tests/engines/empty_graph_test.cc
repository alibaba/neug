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

#include "src/engines/graph_db/database/graph_db.h"

namespace gs {
void testOpenEmptyGraph(const std::string &graph_dir) {
  GraphDB db;
  GraphDBConfig config;
}
} // namespace gs

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "Usage: empty_graph_test <graph_dir>" << std::endl;
    return -1;
  }
  gs::testOpenEmptyGraph(argv[1]);
  return 0;
}