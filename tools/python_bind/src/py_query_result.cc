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

#include "py_query_result.h"
#include <datetime.h>
namespace gs {

void PyQueryResult::initialize(pybind11::handle& m) {
  pybind11::class_<PyQueryResult>(m, "PyQueryResult")
      .def("hasNext", &PyQueryResult::hasNext)
      .def("getNext", &PyQueryResult::getNext)
      .def("hasNextQueryResult", &PyQueryResult::hasNextQueryResult)
      .def("getNextQueryResult", &PyQueryResult::getNextQueryResult)
      .def("get_status_code", &PyQueryResult::getStatusCode)
      .def("get_status_message", &PyQueryResult::getStatusMessage)
      .def("close", &PyQueryResult::close);
  // PyDateTime_IMPORT is a macro that must be invoked before calling any
  // other cpython datetime macros. One could also invoke this in a separate
  // function like constructor. See
  // https://docs.python.org/3/c-api/datetime.html for details.
  PyDateTime_IMPORT;
}

bool PyQueryResult::hasNext() { return false; }

pybind11::list PyQueryResult::getNext() { return pybind11::list(); }

bool PyQueryResult::hasNextQueryResult() { return false; }

std::unique_ptr<PyQueryResult> PyQueryResult::getNextQueryResult() {
  return nullptr;
}

void PyQueryResult::close() {}
}  // namespace gs
