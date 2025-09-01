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
#include "neug/storages/graph/schema.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"

#include <datetime.h>
namespace gs {

pybind11::object value_to_pyobject(const ::common::Value& value) {
  switch (value.item_case()) {
  case ::common::Value::kBoolean: {
    return pybind11::bool_(value.boolean());
  }
  case ::common::Value::kI32: {
    return pybind11::int_(value.i32());
  }
  case ::common::Value::kI64: {
    return pybind11::int_(value.i64());
  }
  case ::common::Value::kStr: {
    return pybind11::str(value.str());
  }
  case ::common::Value::kBlob: {
    return pybind11::bytes(value.blob());
  }
  case ::common::Value::kI32Array: {
    pybind11::list list;
    for (const auto& item : value.i32_array().item()) {
      list.append(pybind11::int_(item));
    }
    return list;
  }
  case ::common::Value::kI64Array: {
    pybind11::list list;
    for (const auto& item : value.i64_array().item()) {
      list.append(pybind11::int_(item));
    }
    return list;
  }
  case ::common::Value::kF64Array: {
    pybind11::list list;
    for (const auto& item : value.f64_array().item()) {
      list.append(pybind11::float_(item));
    }
    return list;
  }
  case ::common::Value::kStrArray: {
    pybind11::list list;
    for (const auto& item : value.str_array().item()) {
      list.append(pybind11::str(item));
    }
    return list;
  }
  case ::common::Value::kPairArray: {
    pybind11::list list;
    for (const auto& item : value.pair_array().item()) {
      pybind11::tuple tuple;
      tuple = pybind11::make_tuple(value_to_pyobject(item.key()),
                                   value_to_pyobject(item.val()));
    }
    return list;
  }
  case ::common::Value::kNone: {
    return pybind11::none();
  }
  case ::common::Value::kDate: {      // date32
    auto days = value.date().item();  // days since epoch
    Date day;
    day.from_num_days(days);
    return pybind11::cast<pybind11::object>(
        PyDate_FromDate(day.year(), day.month(), day.day()));
  }
  case ::common::Value::kTime: {
    LOG(FATAL) << "Time type is not supported:";
  }
  case ::common::Value::kTimestamp: {
    auto millseconds = value.timestamp().item();  // millseconds since epoch
    pybind11::object datetime =
        pybind11::module_::import("datetime").attr("datetime");
    pybind11::object fromtimestamp = datetime.attr("fromtimestamp");
    // Time_point to seconds and milliseconds for Python
    auto seconds_since_epoch = millseconds / 1000;
    auto remaining_ms = millseconds % 1000;
    return pybind11::cast<pybind11::object>(
        fromtimestamp(seconds_since_epoch)
            .attr("replace")(pybind11::arg("microsecond") =
                                 remaining_ms * 1000));
  }
  case ::common::Value::kU32: {
    return pybind11::int_(value.u32());
  }
  case ::common::Value::kU64: {
    return pybind11::int_(value.u64());
  }
  case ::common::Value::kF32: {
    return pybind11::float_(value.f32());
  }
  case ::common::Value::kF64: {
    return pybind11::float_(value.f64());
  }
  default: {
    THROW_NOT_SUPPORTED_EXCEPTION("Unknown value type");
  }
  }
}

pybind11::object name_or_id_to_pyobject(const common::NameOrId& name_or_id) {
  if (name_or_id.item_case() == common::NameOrId::kId) {
    return pybind11::int_(name_or_id.id());
  } else if (name_or_id.item_case() == common::NameOrId::kName) {
    return pybind11::str(name_or_id.name());
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unknown NameOrId type");
  }
}

pybind11::object vertex_to_pyobject(const results::Vertex& vertex) {
  pybind11::dict dict;
  dict["_ID"] = pybind11::int_(vertex.id());
  dict["_LABEL"] = vertex.label().name();
  for (const auto& property : vertex.properties()) {
    auto key = name_or_id_to_pyobject(property.key());
    auto value = value_to_pyobject(property.value());
    dict[key] = value;
  }
  return dict;
}

pybind11::object edge_to_pyobject(const results::Edge& edge) {
  pybind11::dict dict;
  dict["_ID"] = pybind11::int_(edge.id());
  dict["_LABEL"] = edge.label().name();
  dict["_SRC_LABEL"] = edge.src_label().name();
  dict["_DST_LABEL"] = edge.dst_label().name();
  dict["_SRC_ID"] = pybind11::int_(edge.src_id());
  dict["_DST_ID"] = pybind11::int_(edge.dst_id());
  for (const auto& property : edge.properties()) {
    auto key = name_or_id_to_pyobject(property.key());
    auto value = value_to_pyobject(property.value());
    dict[key] = value;
  }
  return dict;
}

pybind11::object graph_path_to_pyobject(const results::GraphPath& graph_path) {
  pybind11::list list;
  for (const auto& vertex_or_edge : graph_path.path()) {
    if (vertex_or_edge.inner_case() ==
        results::GraphPath::VertexOrEdge::kVertex) {
      list.append(vertex_to_pyobject(vertex_or_edge.vertex()));
    } else if (vertex_or_edge.inner_case() ==
               results::GraphPath::VertexOrEdge::kEdge) {
      list.append(edge_to_pyobject(vertex_or_edge.edge()));
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION("Unknown VertexOrEdge type");
    }
  }
  return list;
}

pybind11::object element_to_pyobject(const results::Element& element) {
  switch (element.inner_case()) {
  case results::Element::kVertex: {
    return vertex_to_pyobject(element.vertex());
  }
  case results::Element::kEdge: {
    return edge_to_pyobject(element.edge());
  }
  case results::Element::kGraphPath: {
    return graph_path_to_pyobject(element.graph_path());
  }
  case results::Element::kObject: {
    return value_to_pyobject(element.object());
  }
  default: {
    THROW_NOT_SUPPORTED_EXCEPTION("Unknown element type");
  }
  }
}

pybind11::object collection_to_pyobject(const results::Collection& collection) {
  pybind11::list list;
  for (const auto& element : collection.collection()) {
    list.append(element_to_pyobject(element));
  }
  return list;
}

pybind11::object entry_to_pyobject(const results::Entry* entry);

pybind11::object map_to_pyobject(const results::KeyValues& map) {
  pybind11::dict dict;
  for (const auto& pair : map.key_values()) {
    auto key = value_to_pyobject(pair.key());
    auto value = entry_to_pyobject(&pair.value());
    dict[key] = value;
  }
  return dict;
}

pybind11::object entry_to_pyobject(const results::Entry* entry) {
  if (!entry) {
    return pybind11::none();
  }
  switch (entry->inner_case()) {
  case results::Entry::kElement: {
    return element_to_pyobject(entry->element());
  }
  case results::Entry::kCollection: {
    return collection_to_pyobject(entry->collection());
  }
  case results::Entry::kMap: {
    return map_to_pyobject(entry->map());
  }
  default: {
    THROW_NOT_SUPPORTED_EXCEPTION("Unknown entry type");
  }
  }
}

void PyQueryResult::initialize(pybind11::handle& m) {
  pybind11::class_<PyQueryResult>(
      m, "PyQueryResult",
      "PyQueryResult is a wrapper for query results in GraphScope NeuG. It "
      "actually store the query results defined by the proto results.proto "
      "file. ")
      .def(
          pybind11::init([](const std::string& result_str) {
            return new PyQueryResult(result_str);
          }),
          pybind11::arg("result_str"),
          "Initialize a PyQueryResult with a serialized result string.\n\n"
          "Args:\n"
          "    result_str (str): The serialized query result string.\n\n"
          "Returns:\n"
          "    PyQueryResult: A new instance of PyQueryResult initialized with "
          "the provided result string.")
      .def("hasNext", &PyQueryResult::hasNext,
           "Check if there are more results "
           "available in the query result.\n\n"
           "Returns:\n\n"
           "    bool: True if there are more results, False otherwise.")
      .def("getNext", &PyQueryResult::getNext,
           "Get the next result from the "
           "query result.\n\n"
           "Returns:\n"
           "    list: A list of results, where each result is a dictionary "
           "representing a vertex, edge, or graph path.")
      .def("__getitem__", &PyQueryResult::operator[],
           "Get the result at the specified index.\n\n"
           "Args:\n"
           "    index (int): The index of the result to retrieve.\n\n"
           "Returns:\n"
           "    list: A list of results at the specified index, where each "
           "result is a dictionary representing a vertex, edge, or graph "
           "path.\n\n"
           "Raises:\n"
           "    IndexError: If the index is out of range of the query "
           "results.\n\n")
      .def("length", &PyQueryResult::length,
           "Get the number of results "
           "in the query result.\n\n"
           "Returns:\n"
           "    int: The number of results in the query result.")
      .def("close", &PyQueryResult::close,
           "Close the query result and "
           "release any resources associated with it.\n\n"
           "This method is a no-op in this implementation, but it is provided "
           "for compatibility with other query result implementations.")
      .def("get_result_schema", &PyQueryResult::get_result_schema,
           "Get the schema of the query result.\n\n"
           "Returns:\n"
           "    str: The schema of the query result, which is a string "
           "representing the structure of the results, including vertex and "
           "edge labels, properties, etc.")
      .def("status_code", &PyQueryResult::status_code,
           "Get the status code of the query result.\n\n"
           "Returns:\n"
           "    int: The status code of the query result, indicating success "
           "or failure."
           "A status code of 0 indicates success, while non-zero values "
           "indicate various error conditions. For details on error codes, "
           "refer to the 'StatusCode' enum in the `error.proto` file.")
      .def("status_message", &PyQueryResult::status_message,
           "Get the status message of the query result.\n\n"
           "Returns:\n"
           "    str: The status message of the query result, providing "
           "additional information about the status of the query execution. "
           "This message can include details about errors, warnings, or "
           "other relevant information related to the query execution. "
           "If the query executed successfully, this message may indicate "
           "success or provide context about the results returned.");

  // PyDateTime_IMPORT is a macro that must be invoked before calling any
  // other cpython datetime macros. One could also invoke this in a separate
  // function like constructor. See
  // https://docs.python.org/3/c-api/datetime.html for details.
  PyDateTime_IMPORT;
}

bool PyQueryResult::hasNext() { return query_result_.hasNext(); }

pybind11::list PyQueryResult::getNext() {
  if (!hasNext()) {
    THROW_RUNTIME_ERROR("No more results");
  }
  auto result = query_result_.next();

  pybind11::list list;
  for (const auto& entry : result.entries()) {
    list.append(entry_to_pyobject(entry));
  }
  return list;
}

pybind11::list PyQueryResult::operator[](int32_t index) {
  if (index < 0) {
    index += query_result_.length();
  }
  if (index < 0 || index >= (int32_t) query_result_.length()) {
    throw pybind11::index_error("Index out of range");
  }
  pybind11::list list;
  auto record_line = query_result_[index];
  for (const auto& entry : record_line.entries()) {
    list.append(entry_to_pyobject(entry));
  }
  return list;
}

void PyQueryResult::close() {}

int32_t PyQueryResult::length() const { return query_result_.length(); }

const std::string& PyQueryResult::get_result_schema() const {
  return query_result_.get_result_schema();
}

int32_t PyQueryResult::status_code() const { return status_.error_code(); }

const std::string& PyQueryResult::status_message() const {
  return status_.error_message();
}

}  // namespace gs
