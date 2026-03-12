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
#include <pybind11/stl.h>
#include "neug/storages/graph/schema.h"
#include "neug/utils/bolt_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"

#include <datetime.h>
namespace neug {

inline bool is_valid(const std::string& map, size_t i) {
  return map.empty() || (static_cast<uint8_t>(map[i >> 3]) >> (i & 7)) & 1;
}

inline pybind11::object parse_json_to_py_object(const std::string& json_str) {
  pybind11::object loads = pybind11::module_::import("json").attr("loads");
  return loads(pybind11::str(json_str));
}

pybind11::object fetch_value_from_column(const neug::Array& column,
                                         size_t index) {
  if (column.has_bool_array()) {
    const auto& col = column.bool_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      return pybind11::bool_(col.values(index));
    } else {
      return pybind11::none();
    }
  } else if (column.has_int32_array()) {
    const auto& col = column.int32_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      return pybind11::int_(col.values(index));
    } else {
      return pybind11::none();
    }
  } else if (column.has_uint32_array()) {
    const auto& col = column.uint32_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      return pybind11::int_(col.values(index));
    } else {
      return pybind11::none();
    }
  } else if (column.has_int64_array()) {
    const auto& col = column.int64_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      return pybind11::int_(col.values(index));
    } else {
      return pybind11::none();
    }
  } else if (column.has_uint64_array()) {
    const auto& col = column.uint64_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      return pybind11::int_(col.values(index));
    } else {
      return pybind11::none();
    }
  } else if (column.has_float_array()) {
    const auto& col = column.float_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      return pybind11::float_(col.values(index));
    } else {
      return pybind11::none();
    }
  } else if (column.has_double_array()) {
    const auto& col = column.double_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      return pybind11::float_(col.values(index));
    } else {
      return pybind11::none();
    }
  } else if (column.has_string_array()) {
    const auto& col = column.string_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      return pybind11::str(col.values(index));
    } else {
      return pybind11::none();
    }
  } else if (column.has_date_array()) {
    const auto& col = column.date_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      Date day;
      day.from_timestamp(col.values(index));
      return pybind11::cast<pybind11::object>(
          PyDate_FromDate(day.year(), day.month(), day.day()));
    } else {
      return pybind11::none();
    }
  } else if (column.has_timestamp_array()) {
    const auto& col = column.timestamp_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      int64_t milliseconds_since_epoch = col.values(index);
      pybind11::object datetime =
          pybind11::module_::import("datetime").attr("datetime");
      pybind11::object utcfromtimestamp = datetime.attr("utcfromtimestamp");
      auto seconds_since_epoch = milliseconds_since_epoch / 1000;
      auto remaining_ms = milliseconds_since_epoch % 1000;
      return pybind11::cast<pybind11::object>(
          utcfromtimestamp(seconds_since_epoch)
              .attr("replace")(pybind11::arg("microsecond") =
                                   remaining_ms * 1000));
    } else {
      return pybind11::none();
    }
  } else if (column.has_interval_array()) {
    const auto& col = column.interval_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      return pybind11::str(col.values(index));
    } else {
      return pybind11::none();
    }
  } else if (column.has_list_array()) {
    const auto& col = column.list_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      pybind11::list list;
      uint32_t list_size = col.offsets(index + 1) - col.offsets(index);
      size_t offset = col.offsets(index);
      for (uint32_t i = 0; i < list_size; ++i) {
        list.append(fetch_value_from_column(col.elements(), offset + i));
      }
      return list;
    } else {
      return pybind11::none();
    }
  } else if (column.has_struct_array()) {
    const auto& col = column.struct_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      pybind11::list list;
      for (int i = 0; i < col.fields_size(); ++i) {
        const auto& field = col.fields(i);
        list.append(fetch_value_from_column(field, index));
      }
      return list;
    } else {
      return pybind11::none();
    }
  } else if (column.has_vertex_array()) {
    const auto& col = column.vertex_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      const auto& vertex = col.values(index);
      return parse_json_to_py_object(vertex);
    } else {
      return pybind11::none();
    }
  } else if (column.has_edge_array()) {
    const auto& col = column.edge_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      const auto& edge = col.values(index);
      return parse_json_to_py_object(edge);
    } else {
      return pybind11::none();
    }
  } else if (column.has_path_array()) {
    const auto& col = column.path_array();
    const auto& validity_map = col.validity();
    if (is_valid(validity_map, index)) {
      const auto& path = col.values(index);
      return parse_json_to_py_object(path);
    } else {
      return pybind11::none();
    }
  } else {
    LOG(ERROR) << "Failed to fetch value from column: unsupported column type."
               << column.DebugString();
    return pybind11::none();
  }
}

void PyQueryResult::initialize(pybind11::handle& m) {
  pybind11::class_<PyQueryResult>(
      m, "PyQueryResult",
      "PyQueryResult is a wrapper for query results returned by Neug. It "
      "provides an interface to access the results in a Pythonic way. ")
      .def(pybind11::init([](std::string&& result_str) {
             return new PyQueryResult(std::move(result_str));
           }),
           pybind11::arg("result_str"),
           "Initialize a PyQueryResult with a serialized result string.\n\n"
           "Args:\n"
           "    result_str (str): The serialized query result string.\n\n"
           "Returns:\n"
           "    PyQueryResult: A new instance of PyQueryResult initialized "
           "with "
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
           "This method is a no-op in this implementation, but it is "
           "provided "
           "for compatibility with other query result implementations.")
      .def("column_names", &PyQueryResult::column_names,
           "Get the projected column names of the query result.\n\n"
           "Returns:\n"
           "    List[str]: Column names in projection order.")
      .def("status_code", &PyQueryResult::status_code,
           "Get the status code of the query result.\n\n"
           "Returns:\n"
           "    int: The status code of the query result, indicating "
           "success "
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
           "success or provide context about the results returned.")
      .def("get_bolt_response", &PyQueryResult::get_bolt_response,
           "Get the query result in Bolt response format.\n\n"
           "Returns:\n"
           "    str: Query result in Bolt response format.");

  // PyDateTime_IMPORT is a macro that must be invoked before calling any
  // other cpython datetime macros. One could also invoke this in a separate
  // function like constructor. See
  // https://docs.python.org/3/c-api/datetime.html for details.
  PyDateTime_IMPORT;
}

bool PyQueryResult::hasNext() { return index_ < query_result_.length(); }

pybind11::list PyQueryResult::getNext() {
  if (!hasNext()) {
    THROW_RUNTIME_ERROR("No more results");
  }

  pybind11::list list;
  const auto& response = query_result_.response();
  int num_columns = response.arrays_size();
  for (int i = 0; i < num_columns; ++i) {
    const auto& column = response.arrays(i);
    list.append(fetch_value_from_column(column, index_));
  }
  ++index_;
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
  const auto& response = query_result_.response();
  int num_columns = response.arrays_size();
  for (int i = 0; i < num_columns; ++i) {
    const auto& column = response.arrays(i);
    list.append(fetch_value_from_column(column, index));
  }
  return list;
}

void PyQueryResult::close() {}

int32_t PyQueryResult::length() const { return query_result_.length(); }

std::vector<std::string> PyQueryResult::column_names() const {
  const auto& schema = query_result_.result_schema();
  std::vector<std::string> names(schema.name_size());
  for (int i = 0; i < schema.name_size(); ++i) {
    names[i] = schema.name(i);
  }
  return names;
}

int32_t PyQueryResult::status_code() const { return status_.error_code(); }

const std::string& PyQueryResult::status_message() const {
  return status_.error_message();
}

std::string PyQueryResult::get_bolt_response() const {
  return results_to_bolt_response(query_result_.response(), column_names());
}

}  // namespace neug
