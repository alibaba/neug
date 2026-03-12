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

#include "pybind11/include/pybind11/pybind11.h"

#include "neug/main/query_result.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/result.h"

namespace neug {

class PyQueryResult {
 public:
  static void initialize(pybind11::handle& m);

  PyQueryResult(QueryResult&& result)
      : status_(Status::OK()), query_result_(std::move(std::move(result))) {}

  PyQueryResult(std::string&& result_str) : status_(Status::OK()) {
    LOG(INFO) << "Deserializing QueryResult from string: " << result_str.size();
    query_result_.Swap(QueryResult::From(std::move(result_str)));
  }

  PyQueryResult(const Status& status) : status_(status) {
    LOG(INFO) << "PyQueryResult created with status: " << status.ToString();
  }

  ~PyQueryResult() { close(); }

  bool hasNext();

  pybind11::list getNext();

  pybind11::list operator[](int index);

  void close();

  int32_t length() const;

  std::vector<std::string> column_names() const;

  int32_t status_code() const;

  const std::string& status_message() const;

  std::string get_bolt_response() const;

 private:
  size_t index_{0};
  Status status_;
  QueryResult query_result_;
};

}  // namespace neug

#endif  // TOOLS_PYTHON_BIND_SRC_PY_QUERY_RESULT_H_