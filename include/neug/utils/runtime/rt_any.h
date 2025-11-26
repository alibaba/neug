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

#ifndef INCLUDE_NEUG_UTILS_RUNTIME_RT_ANY_H_
#define INCLUDE_NEUG_UTILS_RUNTIME_RT_ANY_H_

#include <glog/logging.h>
#include <compare>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "neug/execution/common/types.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"

#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/generated/proto/plan/results.pb.h"
#include "neug/generated/proto/plan/type.pb.h"

namespace arrow {
class DataType;
}  // namespace arrow
namespace common {
class Value;
class IrDataType;
}  // namespace common

namespace gs {

class Encoder;

namespace runtime {

class CObject {
 public:
  virtual ~CObject() = default;
};

using Arena = std::vector<std::unique_ptr<CObject>>;

struct ArenaRef : public CObject {
  explicit ArenaRef(const std::shared_ptr<Arena>& arena) : arena_(arena) {}
  std::shared_ptr<Arena> arena_;
};

class VertexRecord {
 public:
  VertexRecord() = default;
  VertexRecord(label_t label, vid_t vid) : label_(label), vid_(vid) {}

  bool operator<(const VertexRecord& v) const {
    if (label_ == v.label_) {
      return vid_ < v.vid_;
    } else {
      return label_ < v.label_;
    }
  }
  bool operator==(const VertexRecord& v) const {
    return label_ == v.label_ && vid_ == v.vid_;
  }
  std::string to_string() const {
    return "(" + std::to_string(static_cast<int>(label_)) + ", " +
           std::to_string(vid_) + ")";
  }

  label_t label() const { return label_; }
  vid_t vid() const { return vid_; }
  label_t label_;
  vid_t vid_;
};

struct VertexRecordHash {
  // Hash combine functions copied from Boost.ContainerHash
  // https://github.com/boostorg/container_hash/blob/171c012d4723c5e93cc7cffe42919afdf8b27dfa/include/boost/container_hash/hash.hpp#L311
  // that is based on Peter Dimov's proposal
  // http://www.open-std.org/JTC1/SC22/WG21/docs/papers/2005/n1756.pdf
  // issue 6.18.
  template <typename T>
  static inline void hash_combine(std::size_t& seed, const T& val) {
    std::hash<T> hasher;
    seed ^= hasher(val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }

  std::size_t operator()(const VertexRecord& v) const {
    std::size_t seed = 0;
    hash_combine(seed, v.vid_);
    hash_combine(seed, v.label_);
    return seed;
  }

  std::size_t operator()(const std::pair<VertexRecord, VertexRecord>& p) const {
    std::size_t seed = 0;
    hash_combine(seed, p.first.vid_);
    hash_combine(seed, p.first.label_);
    hash_combine(seed, p.second.vid_);
    hash_combine(seed, p.second.label_);
    return seed;
  }
};
class EdgeRecord {
 public:
  bool operator<(const EdgeRecord& e) const {
    return std::tie(src, dst, label) < std::tie(e.src, e.dst, e.label);
  }
  bool operator==(const EdgeRecord& e) const {
    return std::tie(src, dst, label) == std::tie(e.src, e.dst, e.label);
  }

  VertexRecord start_node() const {
    return dir == Direction::kOut ? VertexRecord{label.src_label, src}
                                  : VertexRecord{label.dst_label, dst};
  }
  VertexRecord end_node() const {
    return dir == Direction::kOut ? VertexRecord{label.dst_label, dst}
                                  : VertexRecord{label.src_label, src};
  }

  LabelTriplet label;
  vid_t src, dst;
  const void* prop;
  Direction dir;
};

class PathImpl : public CObject {
 public:
  static PathImpl* make_path_impl(label_t label, vid_t v, Arena& arena) {
    auto new_path = std::make_unique<PathImpl>();
    new_path->e_label_ = std::numeric_limits<label_t>::max();
    new_path->v_label_ = label;
    new_path->vid_ = v;
    new_path->prev_ = nullptr;
    auto path_ptr = new_path.get();
    arena.emplace_back(std::move(new_path));
    return path_ptr;
  }
  static PathImpl* make_path_impl(
      label_t label, label_t edge_label, const std::vector<vid_t>& path_ids,
      const std::vector<std::pair<Direction, const void*>>& edge_datas,
      Arena& arena) {
    auto cur = std::make_unique<PathImpl>();
    cur->v_label_ = label;
    cur->e_label_ = edge_label;
    cur->prev_ = nullptr;
    cur->vid_ = path_ids[0];
    auto cur_ptr = cur.get();
    arena.emplace_back(std::move(cur));
    for (size_t i = 1; i < path_ids.size(); ++i) {
      auto next = std::make_unique<PathImpl>();
      next->v_label_ = label;
      next->e_label_ = edge_label;
      next->vid_ = path_ids[i];
      next->prev_ = cur_ptr;
      next->direction_ = edge_datas[i - 1].first;
      next->payload_ = edge_datas[i - 1].second;
      cur_ptr = next.get();
      arena.emplace_back(std::move(next));
    }

    return cur_ptr;
  }
  static PathImpl* make_path_impl(
      const std::vector<std::tuple<label_t, Direction, const void*>>&
          edge_datas,
      const std::vector<VertexRecord>& path, Arena& arena) {
    auto cur = std::make_unique<PathImpl>();
    cur->v_label_ = path[0].label_;
    cur->e_label_ = std::numeric_limits<label_t>::max();
    cur->vid_ = path[0].vid_;
    cur->prev_ = nullptr;
    auto cur_ptr = cur.get();
    arena.emplace_back(std::move(cur));
    for (size_t i = 1; i < path.size(); ++i) {
      auto next = std::make_unique<PathImpl>();
      next->v_label_ = path[i].label_;
      next->e_label_ = std::get<0>(edge_datas[i - 1]);
      next->direction_ = std::get<1>(edge_datas[i - 1]);
      next->payload_ = std::get<2>(edge_datas[i - 1]);
      next->vid_ = path[i].vid_;
      next->prev_ = cur_ptr;
      cur_ptr = next.get();
      arena.emplace_back(std::move(next));
    }
    return cur_ptr;
  }
  PathImpl* expand(label_t edge_label, label_t label, vid_t v, Direction dir,
                   const void* payload, Arena& arena) const {
    auto new_path = std::make_unique<PathImpl>();
    new_path->v_label_ = label;
    new_path->e_label_ = edge_label;
    new_path->vid_ = v;
    new_path->prev_ = this;
    new_path->direction_ = dir;
    new_path->payload_ = payload;
    auto new_path_ptr = new_path.get();
    arena.emplace_back(std::move(new_path));
    return new_path_ptr;
  }

  std::string to_string() const {
    std::string str;
    const PathImpl* cur = this;
    std::vector<std::string> parts;
    while (cur != nullptr) {
      parts.push_back(cur->get_end().to_string());
      cur = cur->prev_;
    }
    std::reverse(parts.begin(), parts.end());
    for (size_t i = 0; i < parts.size(); ++i) {
      str += parts[i];
      if (i + 1 < parts.size()) {
        str += "-";
      }
    }
    return str;
  }

  VertexRecord get_end() const { return {v_label_, vid_}; }
  VertexRecord get_start() const {
    const PathImpl* cur = this;
    while (cur->prev_ != nullptr) {
      cur = cur->prev_;
    }
    return {cur->v_label_, cur->vid_};
  }
  bool operator<(const PathImpl& p) const {
    const auto& nodes = this->nodes();
    const auto& p_nodes = p.nodes();
    for (size_t i = 0; i < nodes.size() && i < p_nodes.size(); ++i) {
      if (nodes[i] < p_nodes[i]) {
        return true;
      } else if (p_nodes[i] < nodes[i]) {
        return false;
      }
    }
    if (nodes.size() < p_nodes.size()) {
      return true;
    }
    return false;
  }
  bool operator==(const PathImpl& p) const {
    if ((p.prev_ == nullptr && prev_) || (p.prev_ && prev_ == nullptr)) {
      return false;
    }
    if (v_label_ != p.v_label_ || e_label_ != p.e_label_ ||
        direction_ != p.direction_ || vid_ != p.vid_) {
      return false;
    }
    if (prev_ && p.prev_) {
      return *prev_ == *(p.prev_);
    }
    return true;
  }
  int64_t len() const {
    int64_t length = 1;
    const PathImpl* cur = this;
    while (cur->prev_ != nullptr) {
      length++;
      cur = cur->prev_;
    }
    return length;
  }

  std::vector<VertexRecord> nodes() const {
    std::vector<VertexRecord> result;
    const PathImpl* cur = this;
    while (cur != nullptr) {
      result.emplace_back(cur->v_label_, cur->vid_);
      cur = cur->prev_;
    }
    std::reverse(result.begin(), result.end());
    return result;
  }

  std::vector<EdgeRecord> relationships() const {
    std::vector<EdgeRecord> relations;
    const PathImpl* cur = this;
    std::vector<PathImpl const*> nodes;
    while (cur != nullptr) {
      nodes.push_back(cur);
      cur = cur->prev_;
    }
    std::reverse(nodes.begin(), nodes.end());
    for (size_t i = 0; i + 1 < nodes.size(); ++i) {
      EdgeRecord r;
      r.label = {nodes[i]->v_label_, nodes[i + 1]->v_label_,
                 nodes[i + 1]->e_label_};
      r.src = nodes[i]->vid_;
      r.dst = nodes[i + 1]->vid_;
      r.dir = nodes[i + 1]->direction_;
      r.prop = nodes[i + 1]->payload_;
      relations.push_back(r);
    }
    return relations;
  }

  std::vector<label_t> edge_labels() const {
    std::vector<label_t> edge_labels;
    const PathImpl* cur = this;
    std::vector<label_t> labels;
    while (cur->prev_ != nullptr) {
      labels.push_back(cur->e_label_);
      cur = cur->prev_;
    }
    std::reverse(labels.begin(), labels.end());
    return labels;
  }
  double get_weight() const { return weight_; }
  void set_weight(double weight) { weight_ = weight; }
  label_t v_label_;
  label_t e_label_;
  Direction direction_;
  vid_t vid_;
  const void* payload_;
  const PathImpl* prev_;
  double weight_ = 0.0;
};
class Path {
 public:
  Path() = default;
  explicit Path(PathImpl* impl) : impl_(impl) {}

  std::string to_string() const { return impl_->to_string(); }

  int64_t len() const { return impl_->len(); }
  VertexRecord get_end() const { return impl_->get_end(); }

  std::vector<EdgeRecord> relationships() const {
    return impl_->relationships();
  }
  std::vector<VertexRecord> nodes() { return impl_->nodes(); }

  std::vector<label_t> edge_labels() const { return impl_->edge_labels(); }

  VertexRecord get_start() const { return impl_->get_start(); }
  bool operator<(const Path& p) const { return *impl_ < *(p.impl_); }
  bool operator==(const Path& p) const { return *(impl_) == *(p.impl_); }

  double get_weight() const { return impl_->get_weight(); }

  PathImpl* impl_;
};
class RTAny;

enum class RTAnyType;

class ListImplBase : public CObject {
 public:
  virtual ~ListImplBase() = default;
  virtual bool operator<(const ListImplBase& p) const = 0;
  virtual bool operator==(const ListImplBase& p) const = 0;
  virtual size_t size() const = 0;
  virtual RTAnyType type() const = 0;
  virtual RTAny get(size_t idx) const = 0;
  virtual bool contains(const RTAny& val) const = 0;
};

class List {
 public:
  List() = default;
  explicit List(ListImplBase* impl) : impl_(impl) {}

  bool operator<(const List& p) const { return *impl_ < *(p.impl_); }
  bool operator==(const List& p) const { return *(impl_) == *(p.impl_); }
  size_t size() const { return impl_->size(); }
  bool contains(const RTAny& val) const { return impl_->contains(val); }
  RTAny get(size_t idx) const;
  RTAnyType elem_type() const;
  std::string to_string() const;
  ListImplBase* impl_;
};

class SetImplBase : public CObject {
 public:
  virtual ~SetImplBase() = default;
  virtual bool operator<(const SetImplBase& p) const = 0;
  virtual bool operator==(const SetImplBase& p) const = 0;
  virtual size_t size() const = 0;
  virtual std::vector<RTAny> values() const = 0;
  virtual void insert(const RTAny& val) = 0;
  virtual bool exists(const RTAny& val) const = 0;
  virtual RTAnyType type() const = 0;
};

class Set {
 public:
  Set() = default;
  explicit Set(SetImplBase* impl) : impl_(impl) {}
  void insert(const RTAny& val);
  bool operator<(const Set& p) const;
  bool operator==(const Set& p) const;
  bool exists(const RTAny& val) const;
  std::vector<RTAny> values() const;
  std::string to_string() const;

  RTAnyType elem_type() const;
  size_t size() const;
  SetImplBase* impl_;
};

class TupleImplBase : public CObject {
 public:
  virtual ~TupleImplBase() = default;
  virtual bool operator<(const TupleImplBase& p) const = 0;
  virtual bool operator==(const TupleImplBase& p) const = 0;
  virtual size_t size() const = 0;
  virtual RTAny get(size_t idx) const = 0;
  virtual std::string to_string() const = 0;
};

template <typename... Args>
class TupleImpl : public TupleImplBase {
 public:
  TupleImpl() = default;
  ~TupleImpl() = default;
  explicit TupleImpl(Args&&... args) : values(std::forward<Args>(args)...) {}
  explicit TupleImpl(std::tuple<Args...>&& args) : values(std::move(args)) {}
  bool operator<(const TupleImplBase& p) const override {
    return values < dynamic_cast<const TupleImpl<Args...>&>(p).values;
  }
  bool operator==(const TupleImplBase& p) const override {
    return values == dynamic_cast<const TupleImpl<Args...>&>(p).values;
  }

  RTAny get(size_t idx) const override;

  size_t size() const override {
    return std::tuple_size_v<std::tuple<Args...>>;
  }
  std::string to_string() const override {
    std::stringstream ss;
    ss << "(";
    for (size_t i = 0; i < size(); i++) {
      if (i != 0) {
        ss << ", ";
      }
      ss << get(i).to_string();
    }
    ss << ")";
    return ss.str();
  }
  std::tuple<Args...> values;
};

template <>
class TupleImpl<RTAny> : public TupleImplBase {
 public:
  TupleImpl() = default;
  ~TupleImpl();
  explicit TupleImpl(std::vector<RTAny>&& val);
  bool operator<(const TupleImplBase& p) const override;
  bool operator==(const TupleImplBase& p) const override;
  size_t size() const override;
  RTAny get(size_t idx) const override;
  std::string to_string() const override;

  std::vector<RTAny> values;
};

class Tuple {
 public:
  template <typename... Args>
  static std::unique_ptr<TupleImplBase> make_tuple_impl(
      std::tuple<Args...>&& args) {
    return std::make_unique<TupleImpl<Args...>>(std::move(args));
  }
  static std::unique_ptr<TupleImplBase> make_generic_tuple_impl(
      std::vector<RTAny>&& args) {
    return std::make_unique<TupleImpl<RTAny>>(std::move(args));
  }

  Tuple() = default;
  explicit Tuple(TupleImplBase* impl) : impl_(impl) {}
  bool operator<(const Tuple& p) const { return *impl_ < *(p.impl_); }
  bool operator==(const Tuple& p) const { return *impl_ == *(p.impl_); }
  size_t size() const { return impl_->size(); }
  RTAny get(size_t idx) const;
  std::string to_string() const;
  TupleImplBase* impl_;
};

class MapImpl : public CObject {
 public:
  MapImpl();
  ~MapImpl();
  static std::unique_ptr<MapImpl> make_map_impl(
      const std::vector<RTAny>& keys, const std::vector<RTAny>& values);
  size_t size() const;
  bool operator<(const MapImpl& p) const;
  bool operator==(const MapImpl& p) const;

  std::vector<RTAny> keys;
  std::vector<RTAny> values;
};

class StringImpl : public CObject {
 public:
  std::string_view str_view() const {
    return std::string_view(str.data(), str.size());
  }
  static std::unique_ptr<StringImpl> make_string_impl(const std::string& str) {
    auto new_str = std::make_unique<StringImpl>();
    new_str->str = str;
    return new_str;
  }

  static std::unique_ptr<StringImpl> make_string_impl(
      const std::string_view& str) {
    auto new_str = std::make_unique<StringImpl>();
    new_str->str = std::string(str);
    return new_str;
  }

  std::string str;
};

enum class RTAnyType {
  kVertex = 0,
  kEdge = 1,
  kI64Value = 2,
  kU64Value = 3,
  kI32Value = 4,
  kU32Value = 5,
  kF32Value = 6,
  kF64Value = 7,
  kBoolValue = 8,
  kStringValue = 9,
  kUnknown = 10,
  kDate = 11,
  kDateTime = 12,
  kInterval = 13,
  kPath = 14,
  kNull = 15,
  kTuple = 16,
  kList = 17,
  kMap = 18,
  kSet = 19,
  kEmpty = 20,
};

PropertyType rt_type_to_property_type(RTAnyType type);

RTAnyType arrow_type_to_rt_type(const std::shared_ptr<arrow::DataType>& type);

class Map {
 public:
  static Map make_map(MapImpl* impl);
  std::pair<const std::vector<RTAny>, const std::vector<RTAny>> key_vals()
      const;
  bool operator<(const Map& p) const;
  bool operator==(const Map& p) const;

  size_t size() const;
  std::string to_string() const;

  MapImpl* map_;
};

struct pod_string_view {
  const char* data_;
  size_t size_;
  pod_string_view() = default;
  pod_string_view(const pod_string_view& other) = default;
  explicit pod_string_view(const char* data)
      : data_(data), size_(strlen(data_)) {}
  pod_string_view(const char* data, size_t size) : data_(data), size_(size) {}
  explicit pod_string_view(const std::string& str)
      : data_(str.data()), size_(str.size()) {}
  explicit pod_string_view(const std::string_view& str)
      : data_(str.data()), size_(str.size()) {}
  const char* data() const { return data_; }
  size_t size() const { return size_; }

  std::string to_string() const { return std::string(data_, size_); }
};
// only for pod type
RTAnyType parse_from_data_type(const ::common::DataType& ddt);
RTAnyType parse_from_ir_data_type(const ::common::IrDataType& dt);

union RTAnyValue {
  // TODO(liulexiao) delete it later
  RTAnyValue() {}
  ~RTAnyValue() {}
  VertexRecord vertex;
  EdgeRecord edge;
  int64_t i64_val;
  uint64_t u64_val;
  int i32_val;
  uint32_t u32_val;
  float f32_val;
  double f64_val;
  // Day day;
  Date date_val;
  DateTime dt_val;
  Interval interval_val;
  std::string_view str_val;
  Path p;
  Tuple t;
  List list;
  Map map;
  Set set;
  bool b_val;
};

class RTAny {
 public:
  RTAny();
  explicit RTAny(RTAnyType type);
  explicit RTAny(const Property& val);
  Property to_any() const;
  RTAny(const RTAny& rhs);
  explicit RTAny(const Path& p);
  ~RTAny() = default;
  bool is_null() const { return type_ == RTAnyType::kNull; }

  int numerical_cmp(const RTAny& other) const;

  RTAny& operator=(const RTAny& rhs);

  static RTAny from_vertex(label_t l, vid_t v);
  static RTAny from_vertex(VertexRecord v);
  static RTAny from_edge(const EdgeRecord& v);

  static RTAny from_bool(bool v);
  static RTAny from_int64(int64_t v);
  static RTAny from_uint64(uint64_t v);
  static RTAny from_int32(int v);
  static RTAny from_uint32(uint32_t v);
  static RTAny from_string(const std::string& str);
  static RTAny from_string(const std::string_view& str);
  static RTAny from_date(Date v);
  static RTAny from_datetime(DateTime v);

  static RTAny from_tuple(const Tuple& tuple);
  static RTAny from_list(const List& list);
  static RTAny from_float(float v);
  static RTAny from_double(double v);
  static RTAny from_map(const Map& m);
  static RTAny from_set(const Set& s);
  static RTAny from_interval(const Interval& v);

  bool as_bool() const;
  int as_int32() const;
  uint32_t as_uint32() const;
  int64_t as_int64() const;
  uint64_t as_uint64() const;
  Date as_date() const;
  DateTime as_datetime() const;
  Interval as_interval() const;
  float as_float() const;
  double as_double() const;
  VertexRecord as_vertex() const;
  const EdgeRecord& as_edge() const;
  std::string_view as_string() const;
  Path as_path() const;
  Tuple as_tuple() const;
  List as_list() const;
  Map as_map() const;
  Set as_set() const;

  bool operator<(const RTAny& other) const;
  bool operator==(const RTAny& other) const;

  RTAny operator+(const RTAny& other) const;

  RTAny operator-(const RTAny& other) const;
  RTAny operator*(const RTAny& other) const;
  RTAny operator/(const RTAny& other) const;
  RTAny operator%(const RTAny& other) const;

  void encode_sig(RTAnyType type, Encoder& encoder) const;

  std::string to_string() const;

  RTAnyType type() const;

  const RTAnyValue& value() const { return value_; }

  void sink_impl(::common::Value* collection) const;

 private:
  RTAnyType type_;
  RTAnyValue value_;
};

template <typename T>
struct TypedConverter {};

template <>
struct TypedConverter<bool> {
  static RTAnyType type() { return RTAnyType::kBoolValue; }
  static bool to_typed(const RTAny& val) { return val.as_bool(); }
  static RTAny from_typed(bool val) { return RTAny::from_bool(val); }
  static const std::string name() { return "bool"; }
  static bool typed_from_string(const std::string& str) {
    if (str == "true" || str == "1") {
      return true;
    } else if (str == "false" || str == "0") {
      return false;
    } else {
      LOG(FATAL) << "Invalid boolean string: " << str;
    }
    return false;  // to suppress compiler warning
  }
};
template <>
struct TypedConverter<int32_t> {
  static RTAnyType type() { return RTAnyType::kI32Value; }
  static int32_t to_typed(const RTAny& val) { return val.as_int32(); }
  static RTAny from_typed(int val) { return RTAny::from_int32(val); }
  static const std::string name() { return "int"; }
  static int32_t typed_from_string(const std::string& str) {
    return std::stoi(str);
  }
};

template <>
struct TypedConverter<uint32_t> {
  static RTAnyType type() { return RTAnyType::kU32Value; }
  static uint32_t to_typed(const RTAny& val) { return val.as_uint32(); }
  static RTAny from_typed(uint32_t val) { return RTAny::from_uint32(val); }
  static const std::string name() { return "uint"; }
  static uint32_t typed_from_string(const std::string& str) {
    return std::stoul(str);
  }
};

template <>
struct TypedConverter<std::string_view> {
  static RTAnyType type() { return RTAnyType::kStringValue; }
  static std::string_view to_typed(const RTAny& val) { return val.as_string(); }
  static RTAny from_typed(const std::string_view& val) {
    return RTAny::from_string(val);
  }
  static const std::string name() { return "string_view"; }
  static std::string_view typed_from_string(const std::string& str) {
    return std::string_view(str.data(), str.size());
  }
};

template <>
struct TypedConverter<uint64_t> {
  static RTAnyType type() { return RTAnyType::kU64Value; }
  static uint64_t to_typed(const RTAny& val) { return val.as_uint64(); }
  static RTAny from_typed(uint64_t val) { return RTAny::from_uint64(val); }
  static const std::string name() { return "uint64"; }
};

template <>
struct TypedConverter<int64_t> {
  static RTAnyType type() { return RTAnyType::kI64Value; }
  static int64_t to_typed(const RTAny& val) { return val.as_int64(); }
  static RTAny from_typed(int64_t val) { return RTAny::from_int64(val); }
  static const std::string name() { return "int64"; }
  static int64_t typed_from_string(const std::string& str) {
    return std::stoll(str);
  }
};

template <>
struct TypedConverter<float> {
  static RTAnyType type() { return RTAnyType::kF32Value; }
  static float to_typed(const RTAny& val) { return val.as_float(); }
  static RTAny from_typed(float val) { return RTAny::from_float(val); }
  static const std::string name() { return "float"; }
  static float typed_from_string(const std::string& str) {
    return std::stof(str);
  }
};

template <>
struct TypedConverter<double> {
  static RTAnyType type() { return RTAnyType::kF64Value; }
  static double to_typed(const RTAny& val) { return val.as_double(); }
  static RTAny from_typed(double val) { return RTAny::from_double(val); }
  static double typed_from_string(const std::string& str) {
    return std::stod(str);
  }
  static const std::string name() { return "double"; }
};

template <>
struct TypedConverter<Date> {
  static RTAnyType type() { return RTAnyType::kDate; }
  static Date to_typed(const RTAny& val) { return val.as_date(); }
  static RTAny from_typed(Date val) { return RTAny::from_date(val); }
  static const std::string name() { return "date"; }
  static Date typed_from_string(const std::string& str) {
    int64_t val = std::stoll(str);
    return Date(val);
  }
};

template <>
struct TypedConverter<DateTime> {
  static RTAnyType type() { return RTAnyType::kDateTime; }
  static DateTime to_typed(const RTAny& val) { return val.as_datetime(); }
  static RTAny from_typed(DateTime val) { return RTAny::from_datetime(val); }
  static const std::string name() { return "datetime"; }
  static DateTime typed_from_string(const std::string& str) {
    return DateTime(str);
  }
};

template <>
struct TypedConverter<Interval> {
  static RTAnyType type() { return RTAnyType::kInterval; }
  static Interval to_typed(const RTAny& val) { return val.as_interval(); }
  static RTAny from_typed(Interval val) { return RTAny::from_interval(val); }
  static const std::string name() { return "interval"; }
  static Interval typed_from_string(const std::string& str) {
    int64_t val = std::stoll(str);
    Interval ret;
    ret.from_mill_seconds(val);
    return ret;
  }
};

template <>
struct TypedConverter<Tuple> {
  static RTAnyType type() { return RTAnyType::kTuple; }
  static Tuple to_typed(const RTAny& val) { return val.as_tuple(); }
  static RTAny from_typed(Tuple val) {
    return RTAny::from_tuple(std::move(val));
  }
  static const std::string name() { return "tuple"; }
};

template <>
struct TypedConverter<Map> {
  static RTAnyType type() { return RTAnyType::kMap; }
  static Map to_typed(const RTAny& val) { return val.as_map(); }
  static RTAny from_typed(Map val) { return RTAny::from_map(val); }
  static const std::string name() { return "map"; }
};

template <>
struct TypedConverter<Set> {
  static RTAnyType type() { return RTAnyType::kSet; }
  static Set to_typed(const RTAny& val) { return val.as_set(); }
  static RTAny from_typed(Set val) { return RTAny::from_set(val); }
  static const std::string name() { return "set"; }
};

template <>
struct TypedConverter<VertexRecord> {
  static RTAnyType type() { return RTAnyType::kVertex; }
  static VertexRecord to_typed(const RTAny& val) { return val.as_vertex(); }
  static RTAny from_typed(VertexRecord val) { return RTAny::from_vertex(val); }
  static const std::string name() { return "vertex"; }
};

template <>
struct TypedConverter<EdgeRecord> {
  static RTAnyType type() { return RTAnyType::kEdge; }
  static EdgeRecord to_typed(const RTAny& val) { return val.as_edge(); }
  static RTAny from_typed(EdgeRecord val) { return RTAny::from_edge(val); }
  static const std::string name() { return "edge"; }
};

template <typename... Args>
RTAny TupleImpl<Args...>::get(size_t idx) const {
  if constexpr (sizeof...(Args) == 2) {
    if (idx == 0) {
      return TypedConverter<std::tuple_element_t<0, std::tuple<Args...>>>::
          from_typed(std::get<0>(values));
    } else if (idx == 1) {
      return TypedConverter<std::tuple_element_t<1, std::tuple<Args...>>>::
          from_typed(std::get<1>(values));
    } else {
      return RTAny(RTAnyType::kNull);
    }
  } else if constexpr (sizeof...(Args) == 3) {
    if (idx == 0) {
      return TypedConverter<std::tuple_element_t<0, std::tuple<Args...>>>::
          from_typed(std::get<0>(values));
    } else if (idx == 1) {
      return TypedConverter<std::tuple_element_t<1, std::tuple<Args...>>>::
          from_typed(std::get<1>(values));
    } else if (idx == 2) {
      return TypedConverter<std::tuple_element_t<2, std::tuple<Args...>>>::
          from_typed(std::get<2>(values));
    } else {
      return RTAny(RTAnyType::kNull);
    }
  } else {
    return RTAny(RTAnyType::kNull);
  }
}

template <typename T>
class ListImpl : ListImplBase {
 public:
  ListImpl() = default;
  static std::unique_ptr<ListImplBase> make_list_impl(std::vector<T>&& vals) {
    auto new_list = new ListImpl<T>();
    new_list->list_ = std::move(vals);
    new_list->is_valid_.resize(new_list->list_.size(), true);
    return std::unique_ptr<ListImplBase>(static_cast<ListImplBase*>(new_list));
  }

  static std::unique_ptr<ListImplBase> make_list_impl(
      std::vector<RTAny>&& vals) {
    auto new_list = new ListImpl<T>();
    new_list->list_.reserve(vals.size());
    new_list->is_valid_.reserve(vals.size());
    for (auto& v : vals) {
      if (v.type() == RTAnyType::kNull) {
        new_list->list_.emplace_back();
        new_list->is_valid_.emplace_back(false);
      } else {
        new_list->list_.emplace_back(TypedConverter<T>::to_typed(v));
        new_list->is_valid_.emplace_back(true);
      }
    }
    return std::unique_ptr<ListImplBase>(static_cast<ListImplBase*>(new_list));
  }

  bool operator<(const ListImplBase& p) const override {
    return list_ < (dynamic_cast<const ListImpl<T>&>(p)).list_;
  }
  bool operator==(const ListImplBase& p) const override {
    return list_ == (dynamic_cast<const ListImpl<T>&>(p)).list_;
  }

  bool contains(const RTAny& val) const override {
    T typed_val = TypedConverter<T>::to_typed(val);
    for (size_t i = 0; i < list_.size(); ++i) {
      if (is_valid_[i] && list_[i] == typed_val) {
        return true;
      }
    }
    return false;
  }
  size_t size() const override { return list_.size(); }
  RTAnyType type() const override { return TypedConverter<T>::type(); }
  RTAny get(size_t idx) const override {
    if (is_valid_[idx]) {
      return TypedConverter<T>::from_typed(list_[idx]);
    } else {
      return RTAny(RTAnyType::kNull);
    }
  }

  std::vector<T> list_;
  std::vector<bool> is_valid_;
};

template <typename T>
class SetImpl : public SetImplBase {
 public:
  SetImpl() = default;
  ~SetImpl() {}
  static std::unique_ptr<SetImplBase> make_set_impl(std::set<T>&& vals) {
    auto new_set = new SetImpl<T>();
    new_set->set_ = std::move(vals);
    return std::unique_ptr<SetImplBase>(static_cast<SetImplBase*>(new_set));
  }

  RTAnyType type() const override { return TypedConverter<T>::type(); }

  bool exists(const RTAny& val) const override {
    return set_.find(TypedConverter<T>::to_typed(val)) != set_.end();
  }
  bool exists(const T& val) const { return set_.find(val) != set_.end(); }

  bool operator<(const SetImplBase& p) const override {
    return set_ < (dynamic_cast<const SetImpl<T>&>(p)).set_;
  }

  bool operator==(const SetImplBase& p) const override {
    return set_ == (dynamic_cast<const SetImpl<T>&>(p)).set_;
  }

  void insert(const RTAny& val) override {
    set_.insert(TypedConverter<T>::to_typed(val));
  }
  void insert(const T& val) { set_.insert(val); }
  size_t size() const override { return set_.size(); }

  std::vector<RTAny> values() const override {
    std::vector<RTAny> res;
    for (const auto& v : set_) {
      res.push_back(TypedConverter<T>::from_typed(v));
    }
    return res;
  }
  std::set<T> set_;
};

template <>
class SetImpl<VertexRecord> : public SetImplBase {
 public:
  SetImpl() = default;
  ~SetImpl() {}

  static std::unique_ptr<SetImplBase> make_set_impl(
      std::set<VertexRecord>&& vals) {
    auto new_set = new SetImpl<VertexRecord>();
    for (auto& v : vals) {
      new_set->set_.insert((1ll * v.vid_) << 8 | v.label_);
    }
    return std::unique_ptr<SetImplBase>(static_cast<SetImplBase*>(new_set));
  }

  std::vector<RTAny> values() const override {
    std::vector<RTAny> res;
    for (auto& v : set_) {
      res.push_back(RTAny::from_vertex(VertexRecord{
          static_cast<label_t>(v & 0xff), static_cast<vid_t>(v >> 8)}));
    }
    return res;
  }

  RTAnyType type() const override { return RTAnyType::kVertex; }

  bool exists(const RTAny& val) const override {
    auto v = TypedConverter<VertexRecord>::to_typed(val);
    return set_.find((1ll * v.vid_) << 8 | v.label_) != set_.end();
  }
  bool exists(VertexRecord val) const {
    return set_.find((1ll * val.vid_) << 8 | val.label_) != set_.end();
  }

  bool operator<(const SetImplBase& p) const override {
    LOG(ERROR) << "not support for set of pair";
    return set_.size() <
           (dynamic_cast<const SetImpl<VertexRecord>&>(p)).set_.size();
  }

  bool operator==(const SetImplBase& p) const override {
    return set_ == (dynamic_cast<const SetImpl<VertexRecord>&>(p)).set_;
  }

  void insert(const RTAny& val) override {
    insert(TypedConverter<VertexRecord>::to_typed(val));
  }
  void insert(VertexRecord val) {
    set_.insert((1ll * val.vid_) << 8 | val.label_);
  }
  size_t size() const override { return set_.size(); }
  std::unordered_set<int64_t> set_;
};

template <typename T>
using is_view_type =
    std::disjunction<std::is_same<T, List>, std::is_same<T, Tuple>,
                     std::is_same<T, Map>, std::is_same<T, Set>,
                     std::is_same<T, std::string_view>, std::is_same<T, Path>>;
}  // namespace runtime

}  // namespace gs

namespace std {

inline ostream& operator<<(ostream& os, const gs::runtime::RTAny& any) {
  os << any.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const gs::runtime::Tuple& tuple) {
  os << tuple.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const gs::runtime::List& list) {
  os << list.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const gs::runtime::Map& map) {
  os << map.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const gs::runtime::Set& set) {
  os << set.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const gs::runtime::Path& path) {
  os << path.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const gs::runtime::VertexRecord& v) {
  os << v.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const gs::runtime::EdgeRecord& e) {
  os << e.label.to_string() << " (" << e.src << "-" << e.dst << ")";
  return os;
}

}  // namespace std

#endif  // INCLUDE_NEUG_UTILS_RUNTIME_RT_ANY_H_
