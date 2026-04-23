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
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>
#include "neug/compiler/common/arrow/arrow.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/bolt_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"

#include <datetime.h>

// ---------------------------------------------------------------------------
// Arrow C Data Interface helpers – build ArrowSchema / ArrowArray from the
// protobuf QueryResponse *without* touching any Arrow C++ symbol.
// ---------------------------------------------------------------------------
namespace neug {

// ---- Owned-resource wrappers for release callbacks ------------------------
struct OwnedSchema {
  std::vector<ArrowSchema> children;
  std::vector<ArrowSchema*> child_ptrs;
  std::vector<std::unique_ptr<char[]>> owned_strings;
};

struct OwnedArray {
  // keeps protobuf memory alive for zero-copy buffers (validity
  //  bitmaps and numeric RepeatedField data).
  std::shared_ptr<const neug::QueryResponse> response;
  std::vector<std::vector<uint8_t>> owned_buffers;
  std::vector<const void*> buffer_ptrs;
  std::vector<ArrowArray> children;
  std::vector<ArrowArray*> child_ptrs;
};

static void release_arrow_schema(ArrowSchema* s) {
  if (s && s->release) {
    s->release = nullptr;
    delete static_cast<OwnedSchema*>(s->private_data);
  }
}

static void release_arrow_array(ArrowArray* a) {
  if (a && a->release) {
    a->release = nullptr;
    delete static_cast<OwnedArray*>(a->private_data);
  }
}

// ---- Validity bitmap helpers ----------------------------------------------
static int64_t count_nulls(const std::string& validity, int64_t length) {
  if (validity.empty()) {
    return 0;
  }
  int64_t nulls = 0;
  for (int64_t i = 0; i < length; ++i) {
    if (!((static_cast<uint8_t>(validity[i >> 3]) >> (i & 7)) & 1)) {
      ++nulls;
    }
  }
  return nulls;
}

// ---- Common ArrowArray initializer ----------------------------------------
// Creates an OwnedArray, sets the validity buffer pointer (zero-copy: points
// directly into the protobuf bytes field), and fills arr fields.
// The caller must ensure `response` is non-null so that the validity memory
// stays alive until the ArrowArray is released.
// Returns the OwnedArray* so callers can add more buffers / children.
static OwnedArray* init_arrow_array(
    ArrowArray& arr, const std::string& validity, int64_t length, int n_buffers,
    std::shared_ptr<const neug::QueryResponse> response, int n_children = 0) {
  auto* holder = new OwnedArray();
  holder->response = std::move(response);
  holder->buffer_ptrs.resize(n_buffers, nullptr);
  // Zero-copy: point directly into the protobuf validity bytes.
  holder->buffer_ptrs[0] = validity.empty()
                               ? nullptr
                               : reinterpret_cast<const void*>(validity.data());

  std::memset(&arr, 0, sizeof(arr));
  arr.length = length;
  arr.null_count = count_nulls(validity, length);
  arr.n_buffers = n_buffers;
  arr.n_children = n_children;
  arr.private_data = holder;
  arr.release = release_arrow_array;
  return holder;
}

// ---- Schema helpers -------------------------------------------------------
static const char* dup_string(OwnedSchema& holder, const std::string& s) {
  auto p = std::make_unique<char[]>(s.size() + 1);
  std::memcpy(p.get(), s.c_str(), s.size() + 1);
  holder.owned_strings.push_back(std::move(p));
  return holder.owned_strings.back().get();
}

static void init_schema(ArrowSchema& s, const char* format, const char* name) {
  std::memset(&s, 0, sizeof(s));
  s.format = format;
  s.name = name;
  s.flags = ARROW_FLAG_NULLABLE;
  s.release = release_arrow_schema;
}

// ---- Forward declarations -------------------------------------------------
static void build_column_schema(OwnedSchema& root, ArrowSchema& col,
                                const neug::Array& pb_col,
                                const char* col_name);
static void build_column_array(
    ArrowArray& arr, const neug::Array& pb_col, int64_t length,
    std::shared_ptr<const neug::QueryResponse> response);

// ---- Schema builder -------------------------------------------------------
static void build_column_schema(OwnedSchema& root, ArrowSchema& col,
                                const neug::Array& pb_col,
                                const char* col_name) {
  // Leaf types – map protobuf oneof to Arrow format strings.
  // Types serialized as strings (interval, vertex, edge, path) use utf8 "u".
  if (pb_col.has_int32_array()) {
    init_schema(col, "i", col_name);
  } else if (pb_col.has_uint32_array()) {
    init_schema(col, "I", col_name);
  } else if (pb_col.has_int64_array()) {
    init_schema(col, "l", col_name);
  } else if (pb_col.has_uint64_array()) {
    init_schema(col, "L", col_name);
  } else if (pb_col.has_float_array()) {
    init_schema(col, "f", col_name);
  } else if (pb_col.has_double_array()) {
    init_schema(col, "g", col_name);
  } else if (pb_col.has_bool_array()) {
    init_schema(col, "b", col_name);
  } else if (pb_col.has_timestamp_array()) {
    init_schema(col, "tsm:", col_name);
  } else if (pb_col.has_date_array()) {
    init_schema(col, "tsm:", col_name);
  } else if (pb_col.has_string_array() || pb_col.has_interval_array() ||
             pb_col.has_vertex_array() || pb_col.has_edge_array() ||
             pb_col.has_path_array()) {
    init_schema(col, "u", col_name);
  } else if (pb_col.has_list_array()) {
    init_schema(col, "+l", col_name);
    col.n_children = 1;
    auto* ch = new OwnedSchema();
    ch->children.resize(1);
    ch->child_ptrs = {&ch->children[0]};
    col.children = ch->child_ptrs.data();
    build_column_schema(*ch, ch->children[0], pb_col.list_array().elements(),
                        "item");
    col.private_data = ch;
  } else if (pb_col.has_struct_array()) {
    const auto& sa = pb_col.struct_array();
    int nf = sa.fields_size();
    init_schema(col, "+s", col_name);
    col.n_children = nf;
    auto* ch = new OwnedSchema();
    ch->children.resize(nf);
    ch->child_ptrs.resize(nf);
    for (int i = 0; i < nf; ++i) {
      ch->child_ptrs[i] = &ch->children[i];
      build_column_schema(*ch, ch->children[i], sa.fields(i),
                          dup_string(*ch, "f" + std::to_string(i)));
    }
    col.children = ch->child_ptrs.data();
    col.private_data = ch;
  } else {
    init_schema(col, "u", col_name);  // fallback
  }
}

// ---- Array builders -------------------------------------------------------

// Zero-copy for numeric types: directly reference RepeatedField<T>::data().
template <typename CType, typename PBArray>
static void build_numeric_array(
    ArrowArray& arr, const PBArray& pb, const std::string& validity,
    int64_t length, std::shared_ptr<const neug::QueryResponse> response) {
  auto* h = init_arrow_array(arr, validity, length, /*n_buffers=*/2,
                             std::move(response));
  // Data buffer – zero-copy: point directly into protobuf RepeatedField.
  h->buffer_ptrs[1] = reinterpret_cast<const void*>(pb.values().data());
  arr.buffers = h->buffer_ptrs.data();
}

// Bool requires bit-packing (protobuf stores 1-byte-per-bool).
static void build_bool_array(
    ArrowArray& arr, const neug::BoolArray& pb, const std::string& validity,
    int64_t length, std::shared_ptr<const neug::QueryResponse> response) {
  auto* h = init_arrow_array(arr, validity, length, /*n_buffers=*/2,
                             std::move(response));
  std::vector<uint8_t> data((length + 7) / 8, 0);
  for (int64_t i = 0; i < length; ++i) {
    if (pb.values(i)) {
      data[i >> 3] |= (1 << (i & 7));
    }
  }
  h->owned_buffers.push_back(std::move(data));
  h->buffer_ptrs[1] = h->owned_buffers.back().data();
  arr.buffers = h->buffer_ptrs.data();
}

// String types require offsets+data concatenation (layout incompatible).
template <typename PBArray>
static void build_string_array(
    ArrowArray& arr, const PBArray& pb, const std::string& validity,
    int64_t length, std::shared_ptr<const neug::QueryResponse> response) {
  auto* h = init_arrow_array(arr, validity, length, /*n_buffers=*/3,
                             std::move(response));

  // Offsets
  std::vector<uint8_t> offsets_buf((length + 1) * sizeof(int32_t));
  auto* offsets = reinterpret_cast<int32_t*>(offsets_buf.data());
  int32_t running = 0;
  for (int64_t i = 0; i < length; ++i) {
    offsets[i] = running;
    running += static_cast<int32_t>(pb.values(i).size());
  }
  offsets[length] = running;
  h->owned_buffers.push_back(std::move(offsets_buf));
  h->buffer_ptrs[1] = h->owned_buffers.back().data();

  // Concatenated string data
  std::vector<uint8_t> data(running);
  int32_t pos = 0;
  for (int64_t i = 0; i < length; ++i) {
    const auto& s = pb.values(i);
    if (!s.empty()) {
      std::memcpy(data.data() + pos, s.data(), s.size());
    }
    pos += static_cast<int32_t>(s.size());
  }
  h->owned_buffers.push_back(std::move(data));
  h->buffer_ptrs[2] = h->owned_buffers.back().empty()
                          ? nullptr
                          : h->owned_buffers.back().data();
  arr.buffers = h->buffer_ptrs.data();
}

static void build_column_array(
    ArrowArray& arr, const neug::Array& col, int64_t length,
    std::shared_ptr<const neug::QueryResponse> response) {
  // Numeric types – zero-copy
  if (col.has_int32_array()) {
    return build_numeric_array<int32_t>(
        arr, col.int32_array(), col.int32_array().validity(), length, response);
  } else if (col.has_uint32_array()) {
    return build_numeric_array<uint32_t>(arr, col.uint32_array(),
                                         col.uint32_array().validity(), length,
                                         response);
  } else if (col.has_int64_array()) {
    return build_numeric_array<int64_t>(
        arr, col.int64_array(), col.int64_array().validity(), length, response);
  } else if (col.has_uint64_array()) {
    return build_numeric_array<uint64_t>(arr, col.uint64_array(),
                                         col.uint64_array().validity(), length,
                                         response);
  } else if (col.has_float_array()) {
    return build_numeric_array<float>(
        arr, col.float_array(), col.float_array().validity(), length, response);
  } else if (col.has_double_array()) {
    return build_numeric_array<double>(arr, col.double_array(),
                                       col.double_array().validity(), length,
                                       response);
  } else if (col.has_timestamp_array()) {
    return build_numeric_array<int64_t>(arr, col.timestamp_array(),
                                        col.timestamp_array().validity(),
                                        length, response);
  } else if (col.has_date_array()) {
    return build_numeric_array<int64_t>(
        arr, col.date_array(), col.date_array().validity(), length, response);
    // Bool – bit-pack conversion required
  } else if (col.has_bool_array()) {
    return build_bool_array(arr, col.bool_array(), col.bool_array().validity(),
                            length, response);
    // String-like types – offsets+data copy required
  } else if (col.has_string_array()) {
    return build_string_array(arr, col.string_array(),
                              col.string_array().validity(), length, response);
  } else if (col.has_interval_array()) {
    return build_string_array(arr, col.interval_array(),
                              col.interval_array().validity(), length,
                              response);
  } else if (col.has_vertex_array()) {
    return build_string_array(arr, col.vertex_array(),
                              col.vertex_array().validity(), length, response);
  } else if (col.has_edge_array()) {
    return build_string_array(arr, col.edge_array(),
                              col.edge_array().validity(), length, response);
  } else if (col.has_path_array()) {
    return build_string_array(arr, col.path_array(),
                              col.path_array().validity(), length, response);
  } else if (col.has_list_array()) {
    const auto& la = col.list_array();
    auto* h = init_arrow_array(arr, la.validity(), length, /*n_buffers=*/2,
                               response, /*n_children=*/1);
    // Offsets
    std::vector<uint8_t> offsets_buf((length + 1) * sizeof(int32_t));
    auto* offsets = reinterpret_cast<uint32_t*>(offsets_buf.data());
    for (int64_t i = 0; i <= length; ++i) {
      offsets[i] = static_cast<uint32_t>(la.offsets(i));
    }
    h->owned_buffers.push_back(std::move(offsets_buf));
    h->buffer_ptrs[1] = h->owned_buffers.back().data();
    arr.buffers = h->buffer_ptrs.data();
    // Child – pass response for potential zero-copy in nested numeric columns
    h->children.resize(1);
    h->child_ptrs = {&h->children[0]};
    build_column_array(h->children[0], la.elements(), offsets[length],
                       response);
    arr.children = h->child_ptrs.data();
  } else if (col.has_struct_array()) {
    const auto& sa = col.struct_array();
    int nf = sa.fields_size();
    auto* h = init_arrow_array(arr, sa.validity(), length, /*n_buffers=*/1,
                               response, /*n_children=*/nf);
    arr.buffers = h->buffer_ptrs.data();
    h->children.resize(nf);
    h->child_ptrs.resize(nf);
    for (int i = 0; i < nf; ++i) {
      build_column_array(h->children[i], sa.fields(i), length, response);
      h->child_ptrs[i] = &h->children[i];
    }
    arr.children = h->child_ptrs.data();
  } else {
    // Fallback: all-null utf8 column
    std::string null_validity((length + 7) / 8, '\0');
    // For the fallback we cannot zero-copy the validity because it is a local
    // string, so we allocate a copy.
    auto* h = new OwnedArray();
    h->response = response;
    std::vector<uint8_t> val_buf(null_validity.begin(), null_validity.end());
    h->owned_buffers.push_back(std::move(val_buf));
    h->buffer_ptrs.resize(3, nullptr);
    h->buffer_ptrs[0] = h->owned_buffers[0].data();
    std::vector<uint8_t> offsets_buf((length + 1) * sizeof(int32_t), 0);
    h->owned_buffers.push_back(std::move(offsets_buf));
    h->buffer_ptrs[1] = h->owned_buffers.back().data();
    std::vector<uint8_t> data_buf;
    h->owned_buffers.push_back(std::move(data_buf));
    h->buffer_ptrs[2] = nullptr;

    std::memset(&arr, 0, sizeof(arr));
    arr.length = length;
    arr.null_count = length;
    arr.n_buffers = 3;
    arr.buffers = h->buffer_ptrs.data();
    arr.private_data = h;
    arr.release = release_arrow_array;
  }
}

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
           "    str: Query result in Bolt response format.")
      .def("to_arrow", &PyQueryResult::to_arrow,
           "Convert the query result to a pyarrow.Table.\n\n"
           "Uses the Arrow C Data Interface to transfer data from Neug's\n"
           "internal representation to pyarrow without any symbol conflicts.\n"
           "Requires pyarrow to be installed.\n\n"
           "Returns:\n"
           "    pyarrow.Table: The query result as a pyarrow Table.\n\n"
           "Raises:\n"
           "    RuntimeError: If pyarrow is not installed or the conversion "
           "fails.");

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

pybind11::object PyQueryResult::to_arrow() const {
  // Zero-copy: share ownership of the QueryResponse protobuf.
  auto response = query_result_.shared_response();

  int64_t n_rows = static_cast<int64_t>(response->row_count());
  int n_cols = response->arrays_size();

  if (n_cols == 0) {
    // Return an empty pyarrow.Table
    auto pa = pybind11::module_::import("pyarrow");
    return pa.attr("table")(pybind11::dict());
  }

  // --- Build the top-level ArrowSchema (struct of columns) -----------------
  auto schema_holder = new OwnedSchema();
  schema_holder->children.resize(n_cols);
  schema_holder->child_ptrs.resize(n_cols);

  auto names = column_names();
  // Ensure we have enough names; pad with "col_N" if needed.
  while (static_cast<int>(names.size()) < n_cols) {
    names.push_back("col_" + std::to_string(names.size()));
  }

  for (int i = 0; i < n_cols; ++i) {
    schema_holder->child_ptrs[i] = &schema_holder->children[i];
    const char* cname = dup_string(*schema_holder, names[i]);
    build_column_schema(*schema_holder, schema_holder->children[i],
                        response->arrays(i), cname);
  }

  ArrowSchema root_schema;
  std::memset(&root_schema, 0, sizeof(root_schema));
  root_schema.format = "+s";
  root_schema.name = "neug_result";
  root_schema.flags = 0;
  root_schema.n_children = n_cols;
  root_schema.children = schema_holder->child_ptrs.data();
  root_schema.metadata = nullptr;
  root_schema.dictionary = nullptr;
  root_schema.private_data = schema_holder;
  root_schema.release = release_arrow_schema;

  // --- Build the top-level ArrowArray (struct of column arrays) -------------
  auto array_holder = new OwnedArray();
  array_holder->response = response;  // root also holds a reference
  // Struct root has 1 buffer (validity – nullptr means all-valid).
  array_holder->buffer_ptrs = {nullptr};
  array_holder->children.resize(n_cols);
  array_holder->child_ptrs.resize(n_cols);

  for (int i = 0; i < n_cols; ++i) {
    build_column_array(array_holder->children[i], response->arrays(i), n_rows,
                       response);
    array_holder->child_ptrs[i] = &array_holder->children[i];
  }

  ArrowArray root_array;
  std::memset(&root_array, 0, sizeof(root_array));
  root_array.length = n_rows;
  root_array.null_count = 0;
  root_array.offset = 0;
  root_array.n_buffers = 1;
  root_array.buffers = array_holder->buffer_ptrs.data();
  root_array.n_children = n_cols;
  root_array.children = array_holder->child_ptrs.data();
  root_array.dictionary = nullptr;
  root_array.private_data = array_holder;
  root_array.release = release_arrow_array;

  // --- Import into pyarrow via C Data Interface ----------------------------
  auto pa = pybind11::module_::import("pyarrow");

  // pyarrow.RecordBatch._import_from_c(ptr_array, ptr_schema)
  // Both pointers are consumed (release callbacks will be called by pyarrow).
  auto record_batch =
      pa.attr("RecordBatch")
          .attr("_import_from_c")(reinterpret_cast<uintptr_t>(&root_array),
                                  reinterpret_cast<uintptr_t>(&root_schema));

  // Convert to pyarrow.Table for a more user-friendly API.
  return pa.attr("Table").attr("from_batches")(
      pybind11::make_tuple(record_batch));
}

}  // namespace neug
