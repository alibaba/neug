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
#include <arrow/c/bridge.h>
#include <datetime.h>
#include <pybind11/stl.h>
#include "neug/storages/graph/schema.h"
#include "neug/utils/bolt_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"

#include <datetime.h>
namespace neug {

inline int64_t get_million_seconds_from_timestamp(
    const std::shared_ptr<arrow::TimestampArray>& timestamp_array,
    size_t index) {
  auto type =
      std::static_pointer_cast<arrow::TimestampType>(timestamp_array->type());
  switch (type->unit()) {
  case arrow::TimeUnit::MILLI: {
    return timestamp_array->Value(index);
  }
  case arrow::TimeUnit::MICRO: {
    return timestamp_array->Value(index) / 1000;
  }
  case arrow::TimeUnit::NANO: {
    return timestamp_array->Value(index) / 1000000;
  }
  case arrow::TimeUnit::SECOND: {
    return timestamp_array->Value(index) * 1000;
  }
  default: {
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported TimeUnit type: " +
                                  std::to_string(type->unit()));
  }
  }
}

pybind11::object fetch_value_from_arrow_column(
    std::shared_ptr<arrow::Array> column, size_t index) {
  if (column->IsNull(index) || !column->IsValid(index)) {
    return pybind11::none();
  }
  switch (column->type()->id()) {
  case arrow::Type::BOOL: {
    auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(column);
    return pybind11::bool_(bool_array->Value(index));
  }
  case arrow::Type::INT32: {
    auto int32_array = std::static_pointer_cast<arrow::Int32Array>(column);
    return pybind11::int_(int32_array->Value(index));
  }
  case arrow::Type::UINT32: {
    auto uint32_array = std::static_pointer_cast<arrow::UInt32Array>(column);
    return pybind11::int_(uint32_array->Value(index));
  }
  case arrow::Type::INT64: {
    auto int64_array = std::static_pointer_cast<arrow::Int64Array>(column);
    return pybind11::int_(int64_array->Value(index));
  }
  case arrow::Type::UINT64: {
    auto uint64_array = std::static_pointer_cast<arrow::UInt64Array>(column);
    return pybind11::int_(uint64_array->Value(index));
  }
  case arrow::Type::FLOAT: {
    auto float_array = std::static_pointer_cast<arrow::FloatArray>(column);
    return pybind11::float_(float_array->Value(index));
  }
  case arrow::Type::DOUBLE: {
    auto double_array = std::static_pointer_cast<arrow::DoubleArray>(column);
    return pybind11::float_(double_array->Value(index));
  }
  case arrow::Type::STRING: {
    auto str_array = std::static_pointer_cast<arrow::StringArray>(column);
    return pybind11::str(str_array->GetView(index));
  }
  case arrow::Type::LARGE_STRING: {
    auto large_str_array =
        std::static_pointer_cast<arrow::LargeStringArray>(column);
    return pybind11::str(large_str_array->GetView(index));
  }
  case arrow::Type::DATE32: {
    auto date32_array = std::static_pointer_cast<arrow::Date32Array>(column);
    int32_t days_since_epoch = date32_array->Value(index);
    Date day;
    day.from_num_days(days_since_epoch);
    return pybind11::cast<pybind11::object>(
        PyDate_FromDate(day.year(), day.month(), day.day()));
  }
  case arrow::Type::DATE64: {
    auto date64_array = std::static_pointer_cast<arrow::Date64Array>(column);
    int64_t milliseconds_since_epoch = date64_array->Value(index);
    pybind11::object date = pybind11::module_::import("datetime").attr("date");
    pybind11::object fromtimestamp = date.attr("fromtimestamp");
    auto seconds_since_epoch = milliseconds_since_epoch / 1000;
    // construct a pydate object
    return pybind11::cast<pybind11::object>(fromtimestamp(seconds_since_epoch));
  }
  case arrow::Type::TIMESTAMP: {
    auto timestamp_array =
        std::static_pointer_cast<arrow::TimestampArray>(column);
    int64_t milliseconds_since_epoch =
        get_million_seconds_from_timestamp(timestamp_array, index);
    pybind11::object datetime =
        pybind11::module_::import("datetime").attr("datetime");
    pybind11::object utcfromtimestamp = datetime.attr("utcfromtimestamp");
    auto seconds_since_epoch = milliseconds_since_epoch / 1000;
    auto remaining_ms = milliseconds_since_epoch % 1000;
    return pybind11::cast<pybind11::object>(
        utcfromtimestamp(seconds_since_epoch)
            .attr("replace")(pybind11::arg("microsecond") =
                                 remaining_ms * 1000));
  }
  case arrow::Type::STRUCT: {
    auto struct_array = std::static_pointer_cast<arrow::StructArray>(column);
    pybind11::dict dict;
    for (int i = 0; i < struct_array->num_fields(); ++i) {
      auto field = struct_array->type()->field(i);
      auto child_array = struct_array->field(i);
      dict[pybind11::str(field->name())] =
          fetch_value_from_arrow_column(child_array, index);
    }
    // If the s
    return dict;
  }
  case arrow::Type::LIST: {
    auto list_array = std::static_pointer_cast<arrow::ListArray>(column);
    auto value_array = list_array->values();
    int32_t start = list_array->value_offset(index);
    int32_t end = list_array->value_offset(index + 1);
    pybind11::list list;
    for (int32_t i = start; i < end; ++i) {
      list.append(fetch_value_from_arrow_column(value_array, i));
    }
    return list;
  }
  case arrow::Type::SPARSE_UNION: {
    auto union_array = std::static_pointer_cast<arrow::UnionArray>(column);
    int8_t type_id = union_array->type_code(index);
    auto child_array = union_array->field(type_id);
    // If the child_field's name is _LABEL, we need to return the label
    auto obj = fetch_value_from_arrow_column(child_array, index);
    if (pybind11::isinstance<pybind11::dict>(obj)) {
      pybind11::dict dic = pybind11::cast<pybind11::dict>(obj);
    }
    return obj;
  }
  case arrow::Type::MAP: {
    auto map_array = std::static_pointer_cast<arrow::MapArray>(column);
    auto keys_array = map_array->keys();
    auto items_array = map_array->items();
    pybind11::dict dict;
    int32_t start = map_array->value_offset(index);
    int32_t end = map_array->value_offset(index + 1);
    for (int32_t i = start; i < end; ++i) {
      auto key = fetch_value_from_arrow_column(keys_array, i);
      auto value = fetch_value_from_arrow_column(items_array, i);
      dict[key] = value;
    }
    return dict;
  }
  case arrow::Type::NA: {
    return pybind11::none();
  }
  default: {
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported Arrow type: " +
                                  column->type()->ToString());
  }
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
  for (int i = 0; i < columns_.size(); ++i) {
    if (!columns_[i] || columns_[i]->IsNull(index_) ||
        !columns_[i]->IsValid(index_)) {
      list.append(pybind11::none());
    } else {
      list.append(fetch_value_from_arrow_column(columns_[i], index_));
    }
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
  for (int i = 0; i < columns_.size(); ++i) {
    if (!columns_[i] || columns_[i]->IsNull(index) ||
        !columns_[i]->IsValid(index)) {
      list.append(pybind11::none());
    } else {
      list.append(fetch_value_from_arrow_column(columns_[i], index));
    }
  }
  return list;
}

void PyQueryResult::close() {}

int32_t PyQueryResult::length() const { return query_result_.length(); }

std::vector<std::string> PyQueryResult::column_names() const {
  std::vector<std::string> names;
  auto table = query_result_.table();
  if (!table) {
    return names;
  }
  auto schema = table->schema();
  for (int i = 0; i < schema->num_fields(); ++i) {
    names.emplace_back(schema->field(i)->name());
  }
  return names;
}

int32_t PyQueryResult::status_code() const { return status_.error_code(); }

const std::string& PyQueryResult::status_message() const {
  return status_.error_message();
}

std::string PyQueryResult::get_bolt_response() const {
  return arrow_table_to_bolt_response(query_result_.table());
}

}  // namespace neug
