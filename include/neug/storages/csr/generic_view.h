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
#pragma once

#include "neug/storages/csr/nbr.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"

namespace neug {

/**
 * @brief Configuration for neighbor iteration in CSR storage.
 *
 * NbrIterConfig defines the memory layout parameters for iterating over
 * edges in CSR (Compressed Sparse Row) format. It uses bit fields for
 * compact storage.
 *
 * @note This is an internal implementation detail. Users typically don't
 *       need to interact with this directly.
 *
 * @since v0.1.0
 */
struct NbrIterConfig {
  int stride : 16;      ///< Byte stride between neighbor entries
  int ts_offset : 8;    ///< Byte offset to timestamp field (0 if immutable)
  int data_offset : 8;  ///< Byte offset to edge data field
};

/**
 * @brief Iterator for traversing neighbors (edges) in CSR storage.
 *
 * NbrIterator provides efficient iteration over edges connected to a vertex.
 * It supports MVCC (Multi-Version Concurrency Control) by automatically
 * filtering edges based on timestamp visibility.
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Get edges from a GenericView
 * GenericView view = graph.GetGenericOutgoingGraphView(src_label, dst_label,
 * edge_label, ts); NbrList edges = view.get_edges(vertex_id);
 *
 * // Iterate over neighbors
 * for (NbrIterator it = edges.begin(); it != edges.end(); ++it) {
 *     vid_t neighbor = *it;           // Get neighbor vertex ID
 *     vid_t neighbor2 = it.get_vertex();  // Alternative way
 *
 *     // Access edge data (if any)
 *     const void* data_ptr = it.get_data_ptr();
 * }
 * @endcode
 *
 * **MVCC Semantics:**
 * - Only edges with `timestamp <= read_timestamp` are visible
 * - Invisible edges are automatically skipped during iteration
 *
 * @note This is a low-level iterator. For query execution, use Cypher queries
 *       through Connection::Query() which handles iteration internally.
 *
 * @see NbrList For the container that provides begin()/end() iterators
 * @see GenericView For obtaining edge lists
 *
 * @since v0.1.0
 */
struct NbrIterator {
  NbrIterator() = default;
  ~NbrIterator() = default;

  /**
   * @brief Initialize iterator with range and visibility settings.
   *
   * @param ptr Start pointer of neighbor data
   * @param end End pointer of neighbor data
   * @param cfg Memory layout configuration
   * @param timestamp Read timestamp for MVCC visibility
   */
  void init(const void* ptr, const void* end, NbrIterConfig cfg,
            timestamp_t timestamp) {
    cur = ptr;
    this->end = end;
    this->cfg = cfg;
    this->timestamp = timestamp;
    int stride = cfg.stride;
    while (cur != end && get_timestamp() > timestamp) {
      cur = static_cast<const char*>(cur) + stride;
    }
  }

  /** @brief Dereference to get neighbor vertex ID. */
  inline vid_t operator*() const { return get_vertex(); }

  /** @brief Advance to next visible neighbor. */
  __attribute__((always_inline)) NbrIterator& operator++() {
    cur = static_cast<const char*>(cur) + cfg.stride;
    while (cur != end && get_timestamp() > timestamp) {
      cur = static_cast<const char*>(cur) + cfg.stride;
    }
    return *this;
  }

  /** @brief Advance by n positions. */
  __attribute__((always_inline)) NbrIterator& operator+=(size_t n) {
    for (size_t i = 0; i < n; ++i) {
      ++(*this);
    }
    return *this;
  }

  __attribute__((always_inline)) bool operator==(const NbrIterator& rhs) const {
    return (cur == rhs.cur);
  }

  __attribute__((always_inline)) bool operator!=(const NbrIterator& rhs) const {
    return (cur != rhs.cur);
  }

  /** @brief Get the timestamp of current edge. */
  inline timestamp_t get_timestamp() const {
    return *(reinterpret_cast<const timestamp_t*>(
        static_cast<const char*>(cur) + cfg.ts_offset));
  }

  /** @brief Get the neighbor (destination) vertex ID. */
  inline vid_t get_vertex() const {
    return *reinterpret_cast<const vid_t*>(cur);
  }

  /** @brief Get pointer to edge property data. */
  inline const void* get_data_ptr() const {
    return static_cast<const char*>(cur) + cfg.data_offset;
  }

  /** @brief Get raw pointer to neighbor entry. */
  inline const void* get_nbr_ptr() const { return cur; }

  /** @brief Get pointer to timestamp field (nullptr if immutable). */
  inline const timestamp_t* get_timestamp_ptr() const {
    return cfg.ts_offset == 0
               ? nullptr
               : reinterpret_cast<const timestamp_t*>(
                     static_cast<const char*>(cur) + cfg.ts_offset);
  }

  const void* cur;        ///< Current position pointer
  const void* end;        ///< End position pointer
  NbrIterConfig cfg;      ///< Memory layout configuration
  timestamp_t timestamp;  ///< Read timestamp for visibility
};

static_assert(std::is_pod<NbrIterator>::value, "NbrIterator should be POD");

/**
 * @brief Container representing a list of neighbor edges for a vertex.
 *
 * NbrList provides an STL-compatible interface for iterating over edges
 * connected to a specific vertex. It is returned by GenericView::get_edges().
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Get outgoing edges for vertex v
 * GenericView view = graph.GetGenericOutgoingGraphView(
 *     src_label, dst_label, edge_label, timestamp);
 * NbrList neighbors = view.get_edges(v);
 *
 * // Check if vertex has no outgoing edges
 * if (neighbors.empty()) {
 *     std::cout << "No neighbors" << std::endl;
 *     return;
 * }
 *
 * // Iterate with range-based for
 * for (auto it = neighbors.begin(); it != neighbors.end(); ++it) {
 *     vid_t neighbor_id = *it;
 *     // Process neighbor...
 * }
 * @endcode
 *
 * @see NbrIterator For iterator operations
 * @see GenericView::get_edges For obtaining NbrList
 *
 * @since v0.1.0
 */
struct NbrList {
  NbrList() = default;
  ~NbrList() = default;

  /** @brief Get iterator to first visible neighbor. */
  __attribute__((always_inline)) NbrIterator begin() const {
    NbrIterator it;
    it.init(start_ptr, end_ptr, cfg, timestamp);
    return it;
  }

  /** @brief Get iterator to past-the-end position. */
  __attribute__((always_inline)) NbrIterator end() const {
    NbrIterator it;
    it.init(end_ptr, end_ptr, cfg, timestamp);
    return it;
  }

  /** @brief Check if neighbor list is empty. */
  bool empty() const { return start_ptr == end_ptr; }

  const void* start_ptr;  ///< Start of neighbor data
  const void* end_ptr;    ///< End of neighbor data
  NbrIterConfig cfg;      ///< Memory layout configuration
  timestamp_t timestamp;  ///< Read timestamp for MVCC
};

static_assert(std::is_pod<NbrList>::value, "NbrList should be POD");

/**
 * @brief Accessor for reading and writing edge property data.
 *
 * EdgeDataAccessor provides unified access to edge properties regardless
 * of the underlying storage format. Edges can store properties in two ways:
 * - **Bundled**: Property data stored inline with edge structure
 * - **Column-based**: Property data stored in separate column storage
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Get edge data accessor for a property
 * EdgeDataAccessor accessor = graph.GetEdgeDataAccessor(
 *     src_label, dst_label, edge_label, "weight");
 *
 * // Get edges and access their properties
 * GenericView view = graph.GetGenericOutgoingGraphView(...);
 * NbrList edges = view.get_edges(v);
 *
 * for (auto it = edges.begin(); it != edges.end(); ++it) {
 *     // Get property as generic Property type
 *     Property prop = accessor.get_data(it);
 *
 *     // Or get as typed value (faster if type is known)
 *     double weight = accessor.get_typed_data<double>(it);
 * }
 * @endcode
 *
 * @see PropertyGraph::GetEdgeDataAccessor For obtaining accessors
 * @see NbrIterator For edge iteration
 *
 * @since v0.1.0
 */
struct EdgeDataAccessor {
  EdgeDataAccessor() : data_type_(DataTypeId::kEmpty), data_column_(nullptr) {}

  /**
   * @brief Construct accessor with data type and optional column storage.
   * @param data_type The data type of the edge property
   * @param data_column Pointer to column storage (nullptr for bundled data)
   */
  EdgeDataAccessor(DataTypeId data_type, ColumnBase* data_column)
      : data_type_(data_type), data_column_(data_column) {}
  EdgeDataAccessor(const EdgeDataAccessor& other)
      : data_type_(other.data_type_), data_column_(other.data_column_) {}

  /** @brief Check if data is stored inline (bundled) vs column storage. */
  bool is_bundled() const { return data_column_ == nullptr; }

  /**
   * @brief Get property value for current edge as generic Property.
   * @param it Iterator pointing to the edge
   * @return Property containing the edge data
   */
  inline Property get_data(const NbrIterator& it) const {
    return data_column_ == nullptr
               ? get_generic_bundled_data_from_ptr(it.get_data_ptr())
               : get_generic_column_data(
                     *reinterpret_cast<const size_t*>(it.get_data_ptr()));
  }

  template <typename T>
  inline T get_typed_data(const NbrIterator& it) const {
    if constexpr (std::is_same<T, EmptyType>::value) {
      return EmptyType();
    } else {
      return data_column_ == nullptr
                 ? get_bundled_data_from_ptr<T>(it.get_data_ptr())
                 : get_column_data<T>(
                       *reinterpret_cast<const size_t*>(it.get_data_ptr()));
    }
  }

  template <typename T>
  inline T get_typed_data_from_ptr(const void* data_ptr) const {
    if constexpr (std::is_same<T, EmptyType>::value) {
      return EmptyType();
    } else {
      auto ret =
          data_column_ == nullptr
              ? *reinterpret_cast<const T*>(data_ptr)
              : get_column_data<T>(*reinterpret_cast<const size_t*>(data_ptr));
      return ret;
    }
  }

  inline Property get_data_from_ptr(const void* data_ptr) const {
    return data_column_ == nullptr
               ? get_generic_bundled_data_from_ptr(data_ptr)
               : get_generic_column_data(
                     *reinterpret_cast<const size_t*>(data_ptr));
  }

  inline void set_data(const NbrIterator& it, const Property& prop,
                       timestamp_t ts) {
    if (it.cfg.ts_offset != 0) {
      *const_cast<timestamp_t*>(it.get_timestamp_ptr()) = ts;
    }
    if (data_column_ != nullptr) {
      size_t idx = get_bundled_data_from_ptr<size_t>(it.get_data_ptr());
      data_column_->set_prop(idx, prop);
    } else {
      if (data_type_ == DataTypeId::kEmpty) {
        return;
      }
      switch (data_type_) {
#define TYPE_DISPATCHER(enum_val, type)                              \
  case DataTypeId::enum_val: {                                       \
    *reinterpret_cast<type*>(const_cast<void*>(it.get_data_ptr())) = \
        PropUtils<type>::to_typed(prop);                             \
    break;                                                           \
  }
        FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
      default:
        THROW_RUNTIME_ERROR("Could not set bundled data for type " +
                            std::to_string(data_type_));
        break;
      }
    }
  }

 private:
  template <typename T>
  inline T get_bundled_data_from_ptr(const void* data_ptr) const {
    return *reinterpret_cast<const T*>(data_ptr);
  }

  template <typename T>
  inline T get_column_data(size_t idx) const {
    return reinterpret_cast<const TypedColumn<T>*>(data_column_)->get_view(idx);
  }

  inline Property get_generic_bundled_data_from_ptr(
      const void* data_ptr) const {
    if (data_type_ == DataTypeId::kEmpty) {
      return Property::empty();
    }
    switch (data_type_) {
#define TYPE_DISPATCHER(enum_val, type)             \
  case DataTypeId::enum_val: {                      \
    return PropUtils<type>::to_prop(                \
        get_bundled_data_from_ptr<type>(data_ptr)); \
  }
      FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
    default:
      THROW_RUNTIME_ERROR("Could not get bundled data for type " +
                          std::to_string(data_type_));
      return Property::empty();
    }
  }

  inline Property get_generic_column_data(size_t idx) const {
    return data_column_->get_prop(idx);
  }

  DataTypeId data_type_;
  ColumnBase* data_column_;
};

enum class CsrViewType {
  kSingleMutable,
  kMultipleMutable,
  kSingleImmutable,
  kMultipleImmutable,
};

template <typename T, CsrViewType TYPE>
struct TypedView {
  TypedView() = default;
  ~TypedView() = default;
};

template <typename T>
struct TypedView<T, CsrViewType::kMultipleMutable> {
  TypedView(const MutableNbr<T>** adjlists, const int* degrees,
            timestamp_t timestamp, timestamp_t unsorted_since)
      : adjlists(adjlists),
        degrees(degrees),
        timestamp(timestamp),
        unsorted_since(unsorted_since) {}
  ~TypedView() = default;

  template <typename FUNC_T>
  void foreach_nbr_gt(vid_t v, const T& threshold, const FUNC_T& func) const {
    const MutableNbr<T>* ptr = adjlists[v] + degrees[v] - 1;
    const MutableNbr<T>* end = adjlists[v] - 1;
    while (ptr != end) {
      if (ptr->timestamp > timestamp) {
        --ptr;
        continue;
      }
      if (ptr->timestamp < unsorted_since) {
        break;
      }
      if (threshold < ptr->data) {
        func(ptr->neighbor, ptr->data);
      }
      --ptr;
    }
    while (ptr != end) {
      if (threshold < ptr->data) {
        func(ptr->neighbor, ptr->data);
      } else {
        break;
      }
      --ptr;
    }
  }

  template <typename FUNC_T>
  void foreach_nbr_lt(vid_t v, const T& threshold, const FUNC_T& func) const {
    const MutableNbr<T>* ptr = adjlists[v] + degrees[v] - 1;
    const MutableNbr<T>* end = adjlists[v] - 1;
    while (ptr != end) {
      if (ptr->timestamp > timestamp) {
        --ptr;
        continue;
      }
      if (ptr->timestamp < unsorted_since) {
        break;
      }
      if (threshold > ptr->data) {
        func(ptr->neighbor, ptr->data);
      }
      --ptr;
    }
    if (ptr == end) {
      return;
    }
    ptr = std::upper_bound(
              adjlists[v], ptr + 1, threshold,
              [](const T& a, const MutableNbr<T>& b) { return a < b.data; }) -
          1;
    while (ptr != end) {
      func(ptr->neighbor, ptr->data);
      --ptr;
    }
  }

  const MutableNbr<T>** adjlists;
  const int* degrees;
  timestamp_t timestamp;
  timestamp_t unsorted_since;
};

/**
 * @brief Generic view for graph traversal in CSR format.
 *
 * GenericView provides efficient access to edges stored in CSR (Compressed
 * Sparse Row) format. It supports both outgoing and incoming edge traversal
 * with MVCC (Multi-Version Concurrency Control) for transactional consistency.
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Get outgoing edges view
 * GenericView out_view = graph.GetGenericOutgoingGraphView(
 *     person_label,    // source vertex label
 *     person_label,    // neighbor vertex label
 *     knows_label,     // edge label
 *     read_timestamp   // MVCC read timestamp
 * );
 *
 * // Get incoming edges view
 * GenericView in_view = graph.GetGenericIncomingGraphView(
 *     person_label, person_label, knows_label, read_timestamp);
 *
 * // Traverse outgoing edges from vertex v
 * NbrList neighbors = out_view.get_edges(v);
 * for (auto it = neighbors.begin(); it != neighbors.end(); ++it) {
 *     vid_t neighbor_id = *it;
 *     std::cout << "Edge: " << v << " -> " << neighbor_id << std::endl;
 * }
 * @endcode
 *
 * **CSR View Types:**
 * - `kSingleMutable`: At most one edge per vertex pair, with timestamps
 * - `kSingleImmutable`: At most one edge per vertex pair, no timestamps
 * - `kMultipleMutable`: Multiple edges per vertex pair, with timestamps
 * - `kMultipleImmutable`: Multiple edges per vertex pair, no timestamps
 *
 * **Performance Notes:**
 * - GenericView is a lightweight wrapper (no memory allocation)
 * - get_edges() returns a NbrList for efficient iteration
 * - Use typed views (get_typed_view) for additional optimizations
 *
 * @note Obtain GenericView from PropertyGraph or StorageReadInterface.
 * @note Views are read-only and snapshot-based (MVCC).
 *
 * @see PropertyGraph::GetGenericOutgoingGraphView
 * @see PropertyGraph::GetGenericIncomingGraphView
 * @see NbrList For iterating over edges
 * @see EdgeDataAccessor For accessing edge properties
 *
 * @since v0.1.0
 */
struct GenericView {
  /** @brief Default constructor creating an empty view. */
  GenericView()
      : adjlists_(nullptr),
        degrees_(nullptr),
        cfg_({0, 0, 0}),
        timestamp_(0),
        unsorted_since_(0) {}

  /**
   * @brief Construct a GenericView for multiple-edge CSR.
   *
   * @param adjlists Pointer to adjacency list data
   * @param degrees Pointer to degree array (edges per vertex)
   * @param cfg Memory layout configuration
   * @param timestamp Read timestamp for MVCC visibility
   * @param unsorted_since Timestamp since edges may be unsorted
   */
  GenericView(const char* adjlists, const int* degrees, NbrIterConfig cfg,
              timestamp_t timestamp, timestamp_t unsorted_since)
      : adjlists_(adjlists),
        degrees_(degrees),
        cfg_(cfg),
        timestamp_(timestamp),
        unsorted_since_(unsorted_since) {}

  /**
   * @brief Construct a GenericView for single-edge CSR.
   *
   * @param adjlists Pointer to adjacency list data
   * @param cfg Memory layout configuration
   * @param timestamp Read timestamp for MVCC visibility
   * @param unsorted_since Timestamp since edges may be unsorted
   */
  GenericView(const char* adjlists, NbrIterConfig cfg, timestamp_t timestamp,
              timestamp_t unsorted_since)
      : adjlists_(adjlists),
        degrees_(nullptr),
        cfg_(cfg),
        timestamp_(timestamp),
        unsorted_since_(unsorted_since) {}

  /**
   * @brief Get the CSR view type.
   *
   * @return CsrViewType indicating single/multiple and mutable/immutable
   */
  CsrViewType type() const {
    if (degrees_ == nullptr) {
      if (cfg_.ts_offset != 0) {
        return CsrViewType::kSingleMutable;
      } else {
        return CsrViewType::kSingleImmutable;
      }
    } else {
      if (cfg_.ts_offset != 0) {
        return CsrViewType::kMultipleMutable;
      } else {
        return CsrViewType::kMultipleImmutable;
      }
    }
  }

  /**
   * @brief Get edges (neighbors) for a specific vertex.
   *
   * Returns a NbrList containing all visible edges from the specified
   * vertex. The returned list respects MVCC visibility based on the
   * view's read timestamp.
   *
   * @param v Source vertex internal ID (vid_t)
   * @return NbrList for iterating over neighbor edges
   *
   * @note This is the primary method for graph traversal.
   * @note Empty NbrList is returned if vertex has no edges.
   */
  __attribute__((always_inline)) NbrList get_edges(vid_t v) const {
    NbrList ret;
    if (degrees_ == nullptr) {
      // single
      const char* start_ptr = adjlists_ + v * cfg_.stride;
      ret.start_ptr = start_ptr;
      ret.end_ptr = start_ptr + cfg_.stride;
    } else {
      // multiple
      const char* start_ptr = reinterpret_cast<const char*>(
          reinterpret_cast<const int64_t*>(adjlists_)[v]);
      ret.start_ptr = start_ptr;
      ret.end_ptr = start_ptr + degrees_[v] * cfg_.stride;
    }
    ret.cfg = cfg_;
    ret.timestamp = timestamp_;

    return ret;
  }

  template <typename T, CsrViewType TYPE>
  TypedView<T, TYPE> get_typed_view() const {
    if constexpr (TYPE == CsrViewType::kMultipleMutable) {
      assert(cfg_.ts_offset != 0);
      assert(cfg_.stride == sizeof(MutableNbr<T>));
      int64_t val = reinterpret_cast<int64_t>(adjlists_);
      const MutableNbr<T>** lists =
          reinterpret_cast<const MutableNbr<T>**>(val);
      return TypedView<T, TYPE>(lists, degrees_, timestamp_, unsorted_since_);
    } else {
      LOG(FATAL) << "not implemented";
      return TypedView<T, TYPE>();
    }
  }

 private:
  const char* adjlists_;
  const int* degrees_;
  NbrIterConfig cfg_;
  timestamp_t timestamp_;
  timestamp_t unsorted_since_;
};

}  // namespace neug
