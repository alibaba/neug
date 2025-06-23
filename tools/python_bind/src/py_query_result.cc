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
#include "src/storages/rt_mutable_graph/schema.h"
#include "src/utils/property/types.h"

#include <datetime.h>
namespace gs {

pybind11::object value_to_pyobject(const common::Value& value) {
  switch (value.item_case()) {
  case common::Value::kBoolean: {
    return pybind11::bool_(value.boolean());
  }
  case common::Value::kI32: {
    return pybind11::int_(value.i32());
  }
  case common::Value::kI64: {
    return pybind11::int_(value.i64());
  }
  case common::Value::kStr: {
    return pybind11::str(value.str());
  }
  case common::Value::kBlob: {
    return pybind11::bytes(value.blob());
  }
  case common::Value::kI32Array: {
    pybind11::list list;
    for (const auto& item : value.i32_array().item()) {
      list.append(pybind11::int_(item));
    }
    return list;
  }
  case common::Value::kI64Array: {
    pybind11::list list;
    for (const auto& item : value.i64_array().item()) {
      list.append(pybind11::int_(item));
    }
    return list;
  }
  case common::Value::kF64Array: {
    pybind11::list list;
    for (const auto& item : value.f64_array().item()) {
      list.append(pybind11::float_(item));
    }
    return list;
  }
  case common::Value::kStrArray: {
    pybind11::list list;
    for (const auto& item : value.str_array().item()) {
      list.append(pybind11::str(item));
    }
    return list;
  }
  case common::Value::kPairArray: {
    pybind11::list list;
    for (const auto& item : value.pair_array().item()) {
      pybind11::tuple tuple;
      tuple = pybind11::make_tuple(value_to_pyobject(item.key()),
                                   value_to_pyobject(item.val()));
    }
    return list;
  }
  case common::Value::kNone: {
    return pybind11::none();
  }
  case common::Value::kDate: {        // date32
    auto days = value.date().item();  // days since epoch
    Date day;
    day.from_num_days(days);
    return pybind11::cast<pybind11::object>(
        PyDate_FromDate(day.year(), day.month(), day.day()));
  }
  case common::Value::kTime: {
    LOG(FATAL) << "Time type is not supported:";
  }
  case common::Value::kTimestamp: {
    auto seconds = value.timestamp().item();  // millseconds since epoch
    auto microseconds = seconds * 1000;
    auto datetime = PyDateTime_FromTimestamp(PyLong_FromLongLong(microseconds));

    if (datetime == nullptr) {
      throw std::runtime_error("Failed to convert timestamp to datetime");
    }
    return pybind11::cast<pybind11::object>(datetime);
  }
  case common::Value::kU32: {
    return pybind11::int_(value.u32());
  }
  case common::Value::kU64: {
    return pybind11::int_(value.u64());
  }
  case common::Value::kF32: {
    return pybind11::float_(value.f32());
  }
  case common::Value::kF64: {
    return pybind11::float_(value.f64());
  }
  default: {
    throw std::runtime_error("Unknown value type");
  }
  }
}

pybind11::object name_or_id_to_pyobject(const common::NameOrId& name_or_id) {
  if (name_or_id.item_case() == common::NameOrId::kId) {
    return pybind11::int_(name_or_id.id());
  } else if (name_or_id.item_case() == common::NameOrId::kName) {
    return pybind11::str(name_or_id.name());
  } else {
    throw std::runtime_error("Unknown NameOrId type");
  }
}

pybind11::object property_to_pyobject(const results::Property& property) {
  pybind11::dict dict;
  dict["key"] = name_or_id_to_pyobject(property.key());
  dict["value"] = value_to_pyobject(property.value());
  return dict;
}

pybind11::object vertex_to_pyobject(const Schema& schema,
                                    const results::Vertex& vertex) {
  pybind11::dict dict;
  dict["id"] = pybind11::int_(vertex.id());
  dict["label"] = schema.get_vertex_label_name(vertex.label().id());
  pybind11::list properties;
  for (const auto& property : vertex.properties()) {
    properties.append(property_to_pyobject(property));
  }
  dict["properties"] = properties;
  return dict;
}

pybind11::object edge_to_pyobject(const Schema& schema,
                                  const results::Edge& edge) {
  pybind11::dict dict;
  dict["id"] = pybind11::int_(edge.id());
  dict["label"] = schema.get_edge_label_name(edge.label().id());
  dict["src_label"] = schema.get_vertex_label_name(edge.src_label().id());
  dict["dst_label"] = schema.get_vertex_label_name(edge.dst_label().id());
  dict["src_id"] = pybind11::int_(edge.src_id());
  dict["dst_id"] = pybind11::int_(edge.dst_id());
  pybind11::list properties;
  for (const auto& property : edge.properties()) {
    properties.append(property_to_pyobject(property));
  }
  dict["properties"] = properties;
  return dict;
}

pybind11::object graph_path_to_pyobject(const Schema& schema,
                                        const results::GraphPath& graph_path) {
  pybind11::list list;
  for (const auto& vertex_or_edge : graph_path.path()) {
    if (vertex_or_edge.inner_case() ==
        results::GraphPath::VertexOrEdge::kVertex) {
      list.append(vertex_to_pyobject(schema, vertex_or_edge.vertex()));
    } else if (vertex_or_edge.inner_case() ==
               results::GraphPath::VertexOrEdge::kEdge) {
      list.append(edge_to_pyobject(schema, vertex_or_edge.edge()));
    } else {
      throw std::runtime_error("Unknown VertexOrEdge type");
    }
  }
  return list;
}

pybind11::object element_to_pyobject(const Schema& schema,
                                     const results::Element& element) {
  switch (element.inner_case()) {
  case results::Element::kVertex: {
    return vertex_to_pyobject(schema, element.vertex());
  }
  case results::Element::kEdge: {
    return edge_to_pyobject(schema, element.edge());
  }
  case results::Element::kGraphPath: {
    return graph_path_to_pyobject(schema, element.graph_path());
  }
  case results::Element::kObject: {
    return value_to_pyobject(element.object());
  }
  default: {
    throw std::runtime_error("Unknown element type");
  }
  }
}

pybind11::object collection_to_pyobject(const Schema& schema,
                                        const results::Collection& collection) {
  pybind11::list list;
  for (const auto& element : collection.collection()) {
    list.append(element_to_pyobject(schema, element));
  }
  return list;
}

pybind11::object entry_to_pyobject(const Schema& schema,
                                   const results::Entry* entry);

pybind11::object map_to_pyobject(const Schema& schema,
                                 const results::KeyValues& map) {
  pybind11::dict dict;
  for (const auto& pair : map.key_values()) {
    auto key = value_to_pyobject(pair.key());
    auto value = entry_to_pyobject(schema, &pair.value());
    dict[key] = value;
  }
  return dict;
}

pybind11::object entry_to_pyobject(const Schema& schema,
                                   const results::Entry* entry) {
  if (!entry) {
    return pybind11::none();
  }
  switch (entry->inner_case()) {
  case results::Entry::kElement: {
    return element_to_pyobject(schema, entry->element());
  }
  case results::Entry::kCollection: {
    return collection_to_pyobject(schema, entry->collection());
  }
  case results::Entry::kMap: {
    return map_to_pyobject(schema, entry->map());
  }
  default: {
    throw std::runtime_error("Unknown entry type");
  }
  }
}

void PyQueryResult::initialize(pybind11::handle& m) {
  pybind11::class_<PyQueryResult>(
      m, "PyQueryResult",
      "PyQueryResult is a wrapper for query results in GraphScope NeuG. It "
      "actually store the query results defined by the proto results.proto "
      "file. ")
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
      .def("getResultSchema", &PyQueryResult::getResultSchema,
           "Get the schema of the query result.\n\n"
           "Returns:\n"
           "    str: The schema of the query result, which is a string "
           "representing the structure of the results, including vertex and "
           "edge labels, properties, etc.");

  // PyDateTime_IMPORT is a macro that must be invoked before calling any
  // other cpython datetime macros. One could also invoke this in a separate
  // function like constructor. See
  // https://docs.python.org/3/c-api/datetime.html for details.
  PyDateTime_IMPORT;
}

bool PyQueryResult::hasNext() { return query_result_.hasNext(); }

pybind11::list PyQueryResult::getNext() {
  if (!hasNext()) {
    throw std::runtime_error("No more results");
  }
  auto result = query_result_.next();

  pybind11::list list;
  for (const auto& entry : result.entries()) {
    list.append(entry_to_pyobject(schema_, entry));
  }
  return list;
}

void PyQueryResult::close() {}

int32_t PyQueryResult::length() const { return query_result_.length(); }

const std::string& PyQueryResult::getResultSchema() const {
  return query_result_.get_result_schema();
}

}  // namespace gs
