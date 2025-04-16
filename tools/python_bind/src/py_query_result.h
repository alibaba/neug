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

#ifndef TOOLS_PYTHON_BIND_SRC_PY_QUERY_RESULT_H_
#define TOOLS_PYTHON_BIND_SRC_PY_QUERY_RESULT_H_

#include "third_party/pybind11/include/pybind11/pybind11.h"

#include "src/main/query_result.h"

namespace gs {

class PyQueryResult {
 public:
  static void initialize(pybind11::handle& m);

  PyQueryResult() = default;

  ~PyQueryResult() { close(); }

  bool hasNext();

  pybind11::list getNext();

  bool hasNextQueryResult();

  std::unique_ptr<PyQueryResult> getNextQueryResult();

  void close();

 private:
  std::shared_ptr<QueryResult> queryResult = nullptr;
  //   bool isOwned = false;
};

}  // namespace gs

#endif  // TOOLS_PYTHON_BIND_SRC_PY_QUERY_RESULT_H_