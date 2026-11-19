/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "result_reader.h"

#include <climits>
#include <unordered_set>

// The protobuf representation is private to this translation unit. Consumers
// only see the C view and never need generated headers or a language runtime.
#define NEUG_C_ARRAYS(X)             \
  X(Int32, int32, INT32)             \
  X(Uint32, uint32, UINT32)          \
  X(Int64, int64, INT64)             \
  X(Uint64, uint64, UINT64)          \
  X(Float, float, FLOAT)             \
  X(Double, double, DOUBLE)          \
  X(String, string, STRING)          \
  X(Bool, bool, BOOL)                \
  X(Date, date, DATE)                \
  X(Timestamp, timestamp, TIMESTAMP) \
  X(Interval, interval, INTERVAL)    \
  X(Vertex, vertex, NODE)            \
  X(Edge, edge, RELATIONSHIP)        \
  X(Path, path, PATH)

namespace neug {
namespace capi {
namespace {

void CheckView(const neug_value_view& value) {
  Require(
      value._owner && value._data && (value._source == 1 || value._source == 2),
      "Uninitialized value view");
}
const Array& ArrayOf(const neug_value_view& value) {
  CheckView(value);
  Require(value._source == 1, "Value is not a protobuf array", NEUG_ERROR_TYPE);
  return *static_cast<const Array*>(value._data);
}
bool Named(const StructArray& array) {
  Require(array.field_names_size() == 0 ||
              array.field_names_size() == array.fields_size(),
          "Struct field count mismatch", NEUG_ERROR_BAD_ENCODING);
  bool named = array.field_names_size() && !array.field_names(0).empty();
  std::unordered_set<std::string> names;
  for (const auto& name : array.field_names()) {
    Require(named ? (!name.empty() && names.insert(name).second) : name.empty(),
            "Struct fields must be all positional or unique nonempty names",
            NEUG_ERROR_BAD_ENCODING);
  }
  return named;
}
void Bitmap(const std::string& validity, uint64_t length) {
  Require(validity.empty() || validity.size() >= (length + 7) / 8,
          "Truncated validity bitmap", NEUG_ERROR_BAD_ENCODING);
}
const std::string& GraphText(const neug_value_view& value) {
  const auto& array = ArrayOf(value);
  const auto row = static_cast<int>(value._index);
  if (array.has_vertex_array()) {
    return array.vertex_array().values(row);
  }
  if (array.has_edge_array()) {
    return array.edge_array().values(row);
  }
  Require(array.has_path_array(), "Value is not a graph object",
          NEUG_ERROR_TYPE);
  return array.path_array().values(row);
}

void GraphShape(const rapidjson::Value& json, neug_type type) {
  Require(json.IsObject(), "Graph result must be an object",
          NEUG_ERROR_BAD_ENCODING);
  if (type == NEUG_TYPE_PATH) {
    Require(json.HasMember("nodes") && json["nodes"].IsArray() &&
                json.HasMember("rels") && json["rels"].IsArray(),
            "Path must contain nodes and rels arrays", NEUG_ERROR_BAD_ENCODING);
    for (const auto& node : json["nodes"].GetArray())
      GraphShape(node, NEUG_TYPE_NODE);
    for (const auto& edge : json["rels"].GetArray())
      GraphShape(edge, NEUG_TYPE_RELATIONSHIP);
    return;
  }
  Require(json.HasMember("_ID") && json["_ID"].IsInt64() &&
              json.HasMember("_LABEL") && json["_LABEL"].IsString(),
          "Graph object must contain integer ID and string label",
          NEUG_ERROR_BAD_ENCODING);
  if (type == NEUG_TYPE_RELATIONSHIP) {
    Require(json.HasMember("_SRC_ID") && json["_SRC_ID"].IsInt64() &&
                json.HasMember("_DST_ID") && json["_DST_ID"].IsInt64(),
            "Relationship must contain integer endpoint IDs",
            NEUG_ERROR_BAD_ENCODING);
  }
}

neug_value_view JsonView(const neug_value_view& parent,
                         const rapidjson::Value& child) {
  return {parent._owner, &child, 0, 2};
}
void NotNull(const neug_value_view& value) {
  Require(!IsNull(value), "Value is NULL", NEUG_ERROR_TYPE);
}

const rapidjson::Value& JsonData(const neug_value_view& value) {
  CheckView(value);
  if (value._source == 2) {
    return *static_cast<const rapidjson::Value*>(value._data);
  }
  Require(!IsNull(value), "Value is NULL", NEUG_ERROR_TYPE);
  auto* owner = static_cast<const neug_result*>(value._owner);
  auto key =
      std::make_pair(static_cast<const Array*>(value._data), value._index);
  std::lock_guard<std::mutex> lock(owner->cache_mutex);
  auto found = owner->graph_cache.find(key);
  if (found != owner->graph_cache.end()) {
    return *found->second;
  }
  auto document = std::make_unique<rapidjson::Document>();
  const auto& text = GraphText(value);
  ParseJson(*document, View(text));
  const auto& column = ArrayOf(value);
  GraphShape(*document, column.has_vertex_array()
                            ? NEUG_TYPE_NODE
                            : column.has_edge_array() ? NEUG_TYPE_RELATIONSHIP
                                                      : NEUG_TYPE_PATH);
  const auto& out = *document;
  owner->graph_cache.emplace(key, std::move(document));
  return out;
}

}  // namespace

neug_value_view Cell(const neug_result* result, uint64_t row, uint64_t column) {
  const auto& response = Response(result);
  Require(row < static_cast<uint64_t>(response.row_count()),
          "Row index out of range", NEUG_ERROR_INDEX);
  Require(column < static_cast<uint64_t>(response.arrays_size()),
          "Column index out of range", NEUG_ERROR_INDEX);
  return {result, &response.arrays(static_cast<int>(column)), row, 1};
}

neug_type Type(const neug_value_view& value) {
  CheckView(value);
  if (value._source == 2) {
    const auto& json = JsonData(value);
    if (json.IsNull())
      return NEUG_TYPE_NULL;
    if (json.IsBool())
      return NEUG_TYPE_BOOL;
    if (json.IsInt64())
      return NEUG_TYPE_INT64;
    if (json.IsUint64())
      return NEUG_TYPE_UINT64;
    if (json.IsNumber())
      return NEUG_TYPE_DOUBLE;
    if (json.IsString())
      return NEUG_TYPE_STRING;
    if (json.IsArray())
      return NEUG_TYPE_LIST;
    if (json.IsObject())
      return NEUG_TYPE_OBJECT;
    return NEUG_TYPE_UNKNOWN;
  }
  const auto& array = ArrayOf(value);
  switch (array.typed_array_case()) {
#define TYPE_CASE(Case, field, tag) \
  case Array::k##Case##Array:       \
    return NEUG_TYPE_##tag;
    NEUG_C_ARRAYS(TYPE_CASE)
#undef TYPE_CASE
  case Array::kListArray:
    return NEUG_TYPE_LIST;
  case Array::kStructArray:
    return Named(array.struct_array()) ? NEUG_TYPE_STRUCT : NEUG_TYPE_TUPLE;
  case Array::TYPED_ARRAY_NOT_SET:
    return NEUG_TYPE_NULL;
  }
  return NEUG_TYPE_UNKNOWN;
}

bool IsNull(const neug_value_view& value) {
  CheckView(value);
  if (value._source == 2) {
    return static_cast<const rapidjson::Value*>(value._data)->IsNull();
  }
  const auto& array = ArrayOf(value);
  const std::string* validity = nullptr;
  uint64_t length = 0;
  switch (array.typed_array_case()) {
#define NULL_CASE(Case, field, tag)               \
  case Array::k##Case##Array:                     \
    length = array.field##_array().values_size(); \
    validity = &array.field##_array().validity(); \
    break;
    NEUG_C_ARRAYS(NULL_CASE)
#undef NULL_CASE
  case Array::kListArray:
    Require(array.list_array().offsets_size() > 0, "Missing list offsets",
            NEUG_ERROR_BAD_ENCODING);
    length = array.list_array().offsets_size() - 1;
    validity = &array.list_array().validity();
    break;
  case Array::kStructArray:
    // Struct row length is inherited from its root/parent. The public locators
    // are only constructed from checked root rows or checked list offsets.
    validity = &array.struct_array().validity();
    length = value._index + 1;
    break;
  case Array::TYPED_ARRAY_NOT_SET:
    return true;
  }
  Require(value._index < length, "Invalid cell index", NEUG_ERROR_BAD_ENCODING);
  Bitmap(*validity, length);
  return !validity->empty() &&
         !(static_cast<uint8_t>((*validity)[value._index / 8]) &
           (1 << (value._index % 8)));
}

bool Boolean(const neug_value_view& value) {
  NotNull(value);
  Require(Type(value) == NEUG_TYPE_BOOL, "Expected boolean", NEUG_ERROR_TYPE);
  return value._source == 2 ? JsonData(value).GetBool()
                            : ArrayOf(value).bool_array().values(value._index);
}
int64_t Signed(const neug_value_view& value) {
  NotNull(value);
  if (value._source == 2) {
    const auto& json = JsonData(value);
    Require(json.IsInt64() || json.IsUint64(), "Expected integer",
            NEUG_ERROR_TYPE);
    Require(json.IsInt64(), "Integer exceeds INT64_MAX", NEUG_ERROR_OVERFLOW);
    return json.GetInt64();
  }
  const auto& array = ArrayOf(value);
  const auto row = static_cast<int>(value._index);
  switch (array.typed_array_case()) {
  case Array::kInt32Array:
    return array.int32_array().values(row);
  case Array::kInt64Array:
    return array.int64_array().values(row);
  case Array::kUint32Array:
    return array.uint32_array().values(row);
  case Array::kUint64Array: {
    const auto number = array.uint64_array().values(row);
    Require(number <= INT64_MAX, "Integer exceeds INT64_MAX",
            NEUG_ERROR_OVERFLOW);
    return static_cast<int64_t>(number);
  }
  case Array::kDateArray:
    return array.date_array().values(row);
  case Array::kTimestampArray:
    return array.timestamp_array().values(row);
  default:
    throw Error(NEUG_ERROR_TYPE, "Expected integer/date/timestamp");
  }
}
uint64_t Unsigned(const neug_value_view& value) {
  NotNull(value);
  if (value._source == 2) {
    const auto& json = JsonData(value);
    Require(json.IsInt64() || json.IsUint64(), "Expected integer",
            NEUG_ERROR_TYPE);
    Require(json.IsUint64(), "Negative integer cannot be unsigned",
            NEUG_ERROR_OVERFLOW);
    return json.GetUint64();
  }
  const auto& array = ArrayOf(value);
  if (array.has_uint64_array())
    return array.uint64_array().values(value._index);
  const auto number = Signed(value);
  Require(number >= 0, "Negative integer cannot be unsigned",
          NEUG_ERROR_OVERFLOW);
  return static_cast<uint64_t>(number);
}
double Number(const neug_value_view& value) {
  NotNull(value);
  if (value._source == 2) {
    const auto& json = JsonData(value);
    Require(json.IsNumber(), "Expected number", NEUG_ERROR_TYPE);
    return json.GetDouble();
  }
  const auto& array = ArrayOf(value);
  const auto row = static_cast<int>(value._index);
  if (array.has_float_array())
    return array.float_array().values(row);
  if (array.has_double_array())
    return array.double_array().values(row);
  if (array.has_uint64_array())
    return static_cast<double>(array.uint64_array().values(row));
  Require(Type(value) != NEUG_TYPE_DATE && Type(value) != NEUG_TYPE_TIMESTAMP,
          "Temporal values are not floating-point numbers", NEUG_ERROR_TYPE);
  return static_cast<double>(Signed(value));
}
neug_string_view String(const neug_value_view& value) {
  NotNull(value);
  if (value._source == 2) {
    const auto& json = JsonData(value);
    Require(json.IsString(), "Expected string", NEUG_ERROR_TYPE);
    return {json.GetString(), json.GetStringLength()};
  }
  const auto& array = ArrayOf(value);
  if (array.has_string_array())
    return View(array.string_array().values(value._index));
  if (array.has_interval_array())
    return View(array.interval_array().values(value._index));
  throw Error(NEUG_ERROR_TYPE, "Expected string/interval");
}

uint64_t Size(const neug_value_view& value) {
  NotNull(value);
  if (value._source == 1) {
    const auto& array = ArrayOf(value);
    if (array.has_list_array()) {
      const auto& list = array.list_array();
      const auto begin = list.offsets(value._index);
      const auto end = list.offsets(value._index + 1);
      Require(end >= begin, "List offsets are not ordered",
              NEUG_ERROR_BAD_ENCODING);
      return end - begin;
    }
    if (array.has_struct_array())
      return array.struct_array().fields_size();
  }
  const auto& json = JsonData(value);
  Require(json.IsArray() || json.IsObject(), "Expected container",
          NEUG_ERROR_TYPE);
  return json.IsArray() ? json.Size() : json.MemberCount();
}
neug_value_view Child(const neug_value_view& value, uint64_t index) {
  Require(index < Size(value), "Child index out of range", NEUG_ERROR_INDEX);
  if (value._source == 1) {
    const auto& array = ArrayOf(value);
    if (array.has_list_array()) {
      return {value._owner, &array.list_array().elements(),
              array.list_array().offsets(value._index) + index, 1};
    }
    if (array.has_struct_array()) {
      return {value._owner, &array.struct_array().fields(index), value._index,
              1};
    }
  }
  const auto& json = JsonData(value);
  return JsonView(value, json.IsArray()
                             ? json[static_cast<rapidjson::SizeType>(index)]
                             : (json.MemberBegin() + index)->value);
}
neug_string_view FieldName(const neug_value_view& value, uint64_t index) {
  Require(index < Size(value), "Field index out of range", NEUG_ERROR_INDEX);
  if (value._source == 1 && ArrayOf(value).has_struct_array()) {
    const auto& structure = ArrayOf(value).struct_array();
    return Named(structure) ? View(structure.field_names(index))
                            : neug_string_view{};
  }
  const auto& json = JsonData(value);
  Require(json.IsObject(), "Expected named object", NEUG_ERROR_TYPE);
  const auto& name = (json.MemberBegin() + index)->name;
  return {name.GetString(), name.GetStringLength()};
}
}  // namespace capi
}  // namespace neug
