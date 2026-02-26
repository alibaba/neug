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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace neug {

rapidjson::Document pyobject_to_rapidjson_document(
    const pybind11::object& obj,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Document doc(&allocator);
  if (pybind11::isinstance<pybind11::none>(obj)) {
    doc.SetNull();
  } else if (pybind11::isinstance<pybind11::bool_>(obj)) {
    bool val = obj.cast<bool>();
    doc.SetBool(val);
  } else if (pybind11::isinstance<pybind11::int_>(obj)) {
    int64_t val = obj.cast<int64_t>();
    doc.SetInt64(val);
  } else if (pybind11::isinstance<pybind11::float_>(obj)) {
    double val = obj.cast<double>();
    doc.SetDouble(val);
  } else if (pybind11::isinstance<pybind11::str>(obj)) {
    std::string val = obj.cast<std::string>();
    doc.SetString(val.c_str(), val.length(), allocator);
  } else if (pybind11::isinstance<pybind11::bool_>(obj)) {
    bool val = obj.cast<bool>();
    doc.SetBool(val);
  } else if (pybind11::isinstance<pybind11::list>(obj)) {
    auto list = obj.cast<pybind11::list>();
    doc.SetArray();
    for (auto item : list) {
      pybind11::object element =
          pybind11::reinterpret_borrow<pybind11::object>(item);
      rapidjson::Document element_doc =
          pyobject_to_rapidjson_document(element, allocator);
      doc.PushBack(element_doc, allocator);
    }
  } else {
    // TODO(zhanglei): Maybe not correct.
    pybind11::module datetime = pybind11::module::import("datetime");
    if (pybind11::isinstance(obj, datetime.attr("date"))) {
      std::string date_str = obj.attr("isoformat")().cast<std::string>();
      doc.SetString(date_str.c_str(), date_str.length(), allocator);
      return doc;
    } else if (pybind11::isinstance(obj, datetime.attr("datetime"))) {
      std::string datetime_str = obj.attr("isoformat").cast<std::string>();
      doc.SetString(datetime_str.c_str(), datetime_str.length(), allocator);
      return doc;
    } else {
      throw std::invalid_argument(
          "Unsupported parameter type for serialization.");
    }
  }
  return doc;
}

// TODO(zhanglei): Complete the implementation.
void PyParameterSerializer::SerializeParameter(
    rapidjson::Document& doc, const std::string key,
    const pybind11::object& parameter) {
  if (doc.IsNull()) {
    doc.SetObject();
  }
  rapidjson::Value json_key;
  json_key.SetString(key.c_str(), key.length(), doc.GetAllocator());
  auto& allocator = doc.GetAllocator();
  rapidjson::Document json_value =
      pyobject_to_rapidjson_document(parameter, allocator);
  doc.AddMember(json_key, json_value, allocator);
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
  rapidjson::Document req_doc(rapidjson::kObjectType);
  auto& allocator = req_doc.GetAllocator();
  req_doc.AddMember("query", rapidjson::Value(query.c_str(), allocator),
                    allocator);
  req_doc.AddMember("access_mode",
                    rapidjson::Value(access_mode.c_str(), allocator),
                    allocator);
  rapidjson::Document params_doc;
  for (auto item : parameters) {
    std::string key = item.first.cast<std::string>();
    pybind11::object parameter =
        pybind11::reinterpret_borrow<pybind11::object>(item.second);
    PyParameterSerializer::SerializeParameter(params_doc, key, parameter);
  }
  req_doc.AddMember("parameters", params_doc, allocator);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  req_doc.Accept(writer);
  return buffer.GetString();
}

}  // namespace neug
