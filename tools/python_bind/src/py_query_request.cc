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

#include "py_query_request.h"

#include <sstream>
#include <string>
#include "neug/execution/common/types/value.h"
#include "neug/main/query_request.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"

namespace neug {

runtime::Value PyParameterSerializer::SerializeParameter(
    const pybind11::object& parameter) {
  // Different serialization logic based on the type of parameter
  if (pybind11::isinstance<pybind11::int_>(parameter)) {
    return runtime::Value::INT64(parameter.cast<int64_t>());
  } else if (pybind11::isinstance<pybind11::float_>(parameter)) {
    return runtime::Value::DOUBLE(parameter.cast<double>());
  } else if (pybind11::isinstance<pybind11::str>(parameter)) {
    return runtime::Value::STRING(parameter.cast<std::string>());
  } else if (pybind11::isinstance<pybind11::bool_>(parameter)) {
    return runtime::Value::BOOLEAN(parameter.cast<bool>());
  } else if (pybind11::isinstance<pybind11::list>(parameter)) {
    auto list = parameter.cast<pybind11::list>();
    std::vector<runtime::Value> serialized_list;
    for (auto item : list) {
      pybind11::object obj =
          pybind11::reinterpret_borrow<pybind11::object>(item);
      serialized_list.push_back(SerializeParameter(obj));
    }
    if (serialized_list.empty()) {
      return runtime::Value::LIST(DataType::UNKNOWN,
                                  std::move(serialized_list));
    }
    DataType child_type = serialized_list[0].type();
    for (const auto& val : serialized_list) {
      if (val.type() != child_type) {
        throw std::invalid_argument(
            "All elements in the list parameter must have the same type.");
      }
    }
    return runtime::Value::LIST(child_type, std::move(serialized_list));
  }
  // handle case for date and datetime
  else {
    // TODO(zhanglei): Maybe not correct.
    pybind11::module datetime = pybind11::module::import("datetime");
    if (pybind11::isinstance(parameter, datetime.attr("date"))) {
      std::string date_str = parameter.attr("isoformat")().cast<std::string>();
      return runtime::Value::DATE(Date(date_str));
    }
    if (pybind11::isinstance(parameter, datetime.attr("datetime"))) {
      std::string datetime_str =
          parameter.attr("isoformat")().cast<std::string>();
      return runtime::Value::TIMESTAMPMS(DateTime(datetime_str));
    }
  }
  // TODO(zhanglei): Support interval, struct, etc.
  throw std::invalid_argument("Unsupported parameter type for serialization.");
}

void PyQueryRequest::initialize(pybind11::handle& m) {
  pybind11::class_<PyQueryRequest>(m, "PyQueryRequest")
      .def_static(
          "serialize_request", &PyQueryRequest::serialize_request,
          pybind11::arg("query"), pybind11::arg("access_mode") = "update",
          pybind11::arg("parameters") = pybind11::dict(),
          "Serialize a query request with parameters into a string.\n\n"
          "Args:\n"
          "    query (str): The query string to execute.\n"
          "    access_mode (str): The access mode of the query. It could be "
          "`read(r)`, `insert(i)`, `update(u)` (include deletion). User "
          "should specify the correct access mode for the query to ensure "
          "the correctness of the database. If the access mode is not "
          "specified, it will be set to `update` by default.\n"
          "    parameters (dict[str, Any], optional): The parameters to be "
          "used in the query. The parameters should be a dictionary, where "
          "the keys are the parameter names, and the values are the "
          "parameter values. If no parameters are needed, it can be set to "
          "None.\n"
          "\n"
          "Returns:\n"
          "    str: The serialized query request string.\n");
}

std::string PyQueryRequest::serialize_request(
    const std::string& query, const std::string& access_mode,
    const pybind11::dict& parameters) {
  neug::runtime::ParamsMap params;
  for (auto item : parameters) {
    pybind11::object key_obj =
        pybind11::reinterpret_borrow<pybind11::object>(item.first);
    pybind11::object value_obj =
        pybind11::reinterpret_borrow<pybind11::object>(item.second);
    std::string key = key_obj.cast<std::string>();
    params.emplace(key, PyParameterSerializer::SerializeParameter(value_obj));
  }
  return neug::RequestSerializer::SerializeRequest(query, access_mode, params);
}

}  // namespace neug
