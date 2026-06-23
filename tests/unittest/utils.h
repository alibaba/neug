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
#ifndef NEUG_TESTS_UNITTTEST_STORAGES_UTILS_H_
#define NEUG_TESTS_UNITTTEST_STORAGES_UTILS_H_

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/data_chunk.h"
#include "neug/main/connection.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/edge_table.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/storages/module/module_broker.h"
#include "neug/storages/module/module_factory.h"
#include "neug/storages/module/type_name.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

class GeneratedChunkSupplier : public neug::IDataChunkSupplier {
 public:
  explicit GeneratedChunkSupplier(
      std::vector<std::shared_ptr<neug::execution::DataChunk>>&& chunks)
      : chunks_(std::move(chunks)) {}
  ~GeneratedChunkSupplier() override = default;

  std::shared_ptr<neug::execution::DataChunk> GetNextChunk() override {
    if (chunks_.empty()) {
      return nullptr;
    }
    auto chunk = chunks_.back();
    chunks_.pop_back();
    return chunk;
  }

  int64_t RowNum() const override {
    int64_t total_rows = 0;
    for (const auto& chunk : chunks_) {
      if (chunk) {
        total_rows += chunk->row_num();
      }
    }
    return total_rows;
  }

 private:
  std::vector<std::shared_ptr<neug::execution::DataChunk>> chunks_;
};

template <typename T>
std::shared_ptr<neug::execution::IContextColumn> build_value_column_slice(
    const std::vector<T>& data, size_t begin, size_t end) {
  neug::execution::ValueColumnBuilder<T> builder;
  for (size_t i = begin; i < end && i < data.size(); ++i) {
    builder.push_back_elem(neug::execution::Value::CreateValue<T>(data[i]));
  }
  return builder.finish();
}

template <typename T>
std::vector<std::shared_ptr<neug::execution::IContextColumn>>
split_column_to_chunks(const std::vector<T>& data, int num_chunks) {
  size_t chunk_size = (data.size() + num_chunks - 1) / num_chunks;
  std::vector<std::shared_ptr<neug::execution::IContextColumn>> columns;
  for (int i = 0; i < num_chunks; ++i) {
    size_t begin = i * chunk_size;
    size_t end = std::min(begin + chunk_size, data.size());
    if (begin >= end) {
      break;
    }
    columns.push_back(build_value_column_slice(data, begin, end));
  }
  return columns;
}

inline std::vector<std::shared_ptr<neug::execution::DataChunk>>
convert_to_data_chunks(
    const std::vector<
        std::vector<std::shared_ptr<neug::execution::IContextColumn>>>&
        column_chunks) {
  if (column_chunks.empty()) {
    return {};
  }
  std::vector<size_t> chunk_sizes;
  for (const auto& col : column_chunks[0]) {
    chunk_sizes.push_back(col->size());
  }
  for (size_t i = 1; i < column_chunks.size(); ++i) {
    if (column_chunks[i].size() != chunk_sizes.size()) {
      LOG(FATAL) << "All columns must have the same number of chunks";
    }
    for (size_t j = 0; j < column_chunks[i].size(); ++j) {
      if (column_chunks[i][j]->size() != chunk_sizes[j]) {
        LOG(FATAL) << "All columns must have the same chunk sizes";
      }
    }
  }
  std::vector<std::shared_ptr<neug::execution::DataChunk>> chunks;
  for (size_t i = 0; i < chunk_sizes.size(); ++i) {
    neug::execution::DataChunk chunk;
    for (size_t col = 0; col < column_chunks.size(); ++col) {
      chunk.set(static_cast<int>(col), column_chunks[col][i]);
    }
    chunks.push_back(
        std::make_shared<neug::execution::DataChunk>(std::move(chunk)));
  }
  return chunks;
}

template <typename EDATA_T>
std::vector<EDATA_T> generate_random_data(size_t len) {
  if constexpr (std::is_integral<EDATA_T>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<EDATA_T> dis(
        std::numeric_limits<EDATA_T>::min(),
        std::numeric_limits<EDATA_T>::max());
    for (size_t i = 0; i < len; ++i) {
      data.push_back(dis(gen));
    }
    return data;
  } else if constexpr (std::is_floating_point<EDATA_T>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<EDATA_T> dis(-1.0, 1.0);
    for (size_t i = 0; i < len; ++i) {
      data.push_back(dis(gen));
    }
    return data;
  } else if constexpr (std::is_same<EDATA_T, neug::EmptyType>::value) {
    return std::vector<EDATA_T>(len, neug::EmptyType());
  } else if constexpr (std::is_same<EDATA_T, neug::Date>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int32_t> dis(0, 365 * 50);  // 50 years
    for (size_t i = 0; i < len; ++i) {
      data.push_back(EDATA_T(dis(gen)));
    }
    return data;
  } else if constexpr (std::is_same<EDATA_T, neug::DateTime>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(
        0, static_cast<int64_t>(365 * 50) * 24 * 3600 *
               1000);  // 50 years in milliseconds
    for (size_t i = 0; i < len; ++i) {
      data.push_back(EDATA_T(dis(gen)));
    }
    return data;
  } else if constexpr (std::is_same<EDATA_T, std::string>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> len_dis(1, 20);
    std::uniform_int_distribution<> char_dis(97, 122);  // a-z
    for (size_t i = 0; i < len; ++i) {
      int str_len = len_dis(gen);
      std::string str;
      for (int j = 0; j < str_len; ++j) {
        str += static_cast<char>(char_dis(gen));
      }
      data.push_back(str);
    }
    return data;
  } else if constexpr (std::is_same<EDATA_T, neug::Interval>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(
        std::numeric_limits<int64_t>::min(),
        std::numeric_limits<int64_t>::max());
    for (size_t i = 0; i < len; ++i) {
      neug::Interval interval;
      interval.from_mill_seconds(dis(gen));
      data.push_back(interval);
    }
    return data;
  } else {
    LOG(FATAL) << "Unsupported data type";
    return {};
  }
}

template <typename ID_T>
inline std::vector<ID_T> generate_random_vertices(ID_T vnum, size_t len) {
  std::vector<ID_T> vertices;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<ID_T> dis(0, vnum - 1);
  for (size_t i = 0; i < len; ++i) {
    vertices.push_back(dis(gen));
  }
  return vertices;
}

template <typename EDATA_T>
std::vector<std::tuple<neug::vid_t, neug::vid_t, EDATA_T>>
generate_random_edges(neug::vid_t src_num, neug::vid_t dst_num, size_t len,
                      bool single = false) {
  std::vector<std::tuple<neug::vid_t, neug::vid_t, EDATA_T>> edges;
  if (!single) {
    auto src_list = generate_random_vertices<neug::vid_t>(src_num, len);
    auto dst_list = generate_random_vertices<neug::vid_t>(dst_num, len);
    auto data_list = generate_random_data<EDATA_T>(len);
    for (size_t i = 0; i < len; ++i) {
      edges.emplace_back(src_list[i], dst_list[i], data_list[i]);
    }
  } else {
    len = std::min(len, static_cast<size_t>(src_num));
    auto dst_list = generate_random_vertices<neug::vid_t>(dst_num, len);
    std::vector<neug::vid_t> src_list;
    for (neug::vid_t i = 0; i < src_num; ++i) {
      src_list.push_back(i);
    }
    std::shuffle(src_list.begin(), src_list.end(),
                 std::default_random_engine(std::random_device()()));
    src_list.resize(len);
    auto data_list = generate_random_data<EDATA_T>(len);
    for (size_t i = 0; i < len; ++i) {
      edges.emplace_back(src_list[i], dst_list[i], data_list[i]);
    }
  }
  return edges;
}

// Allocates a fresh checkpoint on `ws` and returns a shared_ptr to it.
// Replaces the boilerplate `auto id = ws.CreateCheckpoint(); auto ckp =
// ws.GetCheckpoint(id);` pair that recurs across the storage tests.
inline std::shared_ptr<neug::Checkpoint> make_checkpoint(
    neug::CheckpointManager& ws) {
  return ws.GetCheckpoint(ws.CreateCheckpoint());
}

template <typename ModuleT>
neug::ModuleDescriptor dump_module_descriptor(ModuleT& module,
                                              neug::Checkpoint& ckp,
                                              const std::string& key) {
  neug::CheckpointManifest meta;
  module.Dump(ckp, meta, key);
  auto desc = meta.module(key);
  if (!desc.has_value()) {
    throw std::runtime_error("Module did not write descriptor for key: " + key);
  }
  return std::move(desc.value());
}

// Test fixtures used to round-trip storage objects through encoded paths in a
// single ModuleDescriptor.  The current production path uses ModuleBroker +
// CheckpointManifest: each leaf Module lives as its own entry in
// CheckpointManifest::modules() (keyed by a flat string), scalars live in
// CheckpointManifest::scalars().  The helpers below mirror that flow so test
// code can simulate Dump / Open cycles without spinning up a full
// PropertyGraph.
//
// API contract:
//   * `Dump*Legacy` returns a fresh CheckpointManifest carrying the modules +
//   scalars
//     for that one object.  The source object's leaf shared_ptrs are *shared*
//     into a temporary ModuleBroker and remain usable after Dump returns.
//   * `Open*Legacy` accepts either an empty CheckpointManifest (treated as
//   fresh
//     init — equivalent to the production *::Init flow) or a previously
//     returned CheckpointManifest (restoration via ModuleBroker::Open).
//
// Each helper namespaces its module / scalar keys with a fixed prefix so a
// single CheckpointManifest could in principle host several objects, but the
// tests only ever round-trip one object per meta.

inline constexpr const char* kIndexerKeys = "indexer/keys";
inline constexpr const char* kIndexerIndices = "indexer/indices";
inline constexpr const char* kIndexerNumElements = "indexer/num_elements";
inline constexpr const char* kIndexerNumSlotsMinusOne =
    "indexer/num_slots_minus_one";
inline constexpr const char* kIndexerHashPolicy = "indexer/hash_policy";

template <typename INDEX_T>
inline void OpenIndexerLegacy(neug::LFIndexer<INDEX_T>& idx,
                              neug::Checkpoint& ckp,
                              const neug::DataType& key_type,
                              const neug::CheckpointManifest& meta,
                              neug::MemoryLevel level) {
  {
    neug::LFIndexer<INDEX_T> fresh(key_type);
    idx.swap(fresh);
  }
  if (!meta.has_module(kIndexerKeys)) {
    auto keys = CreateColumn(key_type);
    keys->Open(ckp, neug::ModuleDescriptor{}, level);
    auto indices = std::make_unique<neug::TypedColumn<INDEX_T>>();
    indices->Open(ckp, neug::ModuleDescriptor{}, level);
    idx.Open(ckp, neug::ModuleDescriptor{}, level, std::move(keys),
             std::move(indices));
    return;
  }
  neug::ModuleBroker store;
  store.Open(ckp, meta, level);
  idx.SetKeys(store.TakeModule<neug::ColumnBase>(kIndexerKeys));
  idx.SetIndices(store.TakeModule<neug::TypedColumn<INDEX_T>>(kIndexerIndices));
  idx.SetNumElements(meta.GetScalarAs<size_t>(kIndexerNumElements).value_or(0));
  idx.SetNumSlotsMinusOne(
      meta.GetScalarAs<size_t>(kIndexerNumSlotsMinusOne).value_or(0));
  idx.SetHashPolicyIndex(
      meta.GetScalarAs<size_t>(kIndexerHashPolicy).value_or(0));
}

template <typename INDEX_T>
inline neug::CheckpointManifest DumpIndexerLegacy(neug::LFIndexer<INDEX_T>& idx,
                                                  neug::Checkpoint& ckp) {
  neug::CheckpointManifest meta;
  // Capture scalars before TakeXxx empties the indexer.
  meta.SetScalar(kIndexerNumElements, std::to_string(idx.GetNumElements()));
  meta.SetScalar(kIndexerNumSlotsMinusOne,
                 std::to_string(idx.GetNumSlotsMinusOne()));
  meta.SetScalar(kIndexerHashPolicy, std::to_string(idx.GetHashPolicyIndex()));
  // Move the indexer's leaves into a transient store, which Dumps and then
  // destroys them when it goes out of scope.  The indexer is empty after
  // this helper returns.
  neug::ModuleBroker store;
  store.SetModule(kIndexerKeys, idx.TakeKeys());
  store.SetModule(kIndexerIndices, idx.TakeIndices());
  store.Dump(ckp, meta);
  return meta;
}

// VertexTable round-trip via ModuleBroker.  Keys: nested under "vertex/".
inline constexpr const char* kVertexIndexerKeys = "vertex/indexer/keys";
inline constexpr const char* kVertexIndexerIndices = "vertex/indexer/indices";
inline constexpr const char* kVertexVTs = "vertex/v_ts";
inline std::string VertexPropKey(size_t i) {
  return "vertex/prop_" + std::to_string(i);
}
inline constexpr const char* kVertexNumElements = "vertex/num_elements";
inline constexpr const char* kVertexNumSlotsMinusOne =
    "vertex/num_slots_minus_one";
inline constexpr const char* kVertexHashPolicy = "vertex/hash_policy";

inline void OpenVertexTableLegacy(neug::VertexTable& vt,
                                  std::shared_ptr<neug::Checkpoint> ckp,
                                  const neug::CheckpointManifest& meta,
                                  neug::MemoryLevel level) {
  vt.SetMemoryLevel(level);
  if (!meta.has_module(kVertexIndexerKeys)) {
    vt.Init(ckp, level);
    return;
  }
  auto vs = vt.get_vertex_schema_ptr();
  neug::ModuleBroker store;
  store.Open(*ckp, meta, level);
  auto& idx = vt.get_indexer();
  idx.SetKeys(store.TakeModule<neug::ColumnBase>(kVertexIndexerKeys));
  idx.SetIndices(
      store.TakeModule<neug::TypedColumn<neug::vid_t>>(kVertexIndexerIndices));
  idx.SetNumElements(meta.GetScalarAs<size_t>(kVertexNumElements).value_or(0));
  idx.SetNumSlotsMinusOne(
      meta.GetScalarAs<size_t>(kVertexNumSlotsMinusOne).value_or(0));
  idx.SetHashPolicyIndex(
      meta.GetScalarAs<size_t>(kVertexHashPolicy).value_or(0));

  auto table =
      std::make_unique<neug::Table>(vs->property_names, vs->property_types);
  for (size_t i = 0; i < vs->property_types.size(); ++i) {
    table->SetColumn(static_cast<int>(i),
                     store.TakeModule<neug::ColumnBase>(VertexPropKey(i)));
  }
  vt.SetTable(std::move(table));
  vt.SetVertexTimestamp(store.TakeModule<neug::VertexTimestamp>(kVertexVTs));
}

inline neug::CheckpointManifest DumpVertexTableLegacy(neug::VertexTable& vt,
                                                      neug::Checkpoint& ckp) {
  neug::CheckpointManifest meta;
  auto& idx = vt.get_indexer();
  // Capture scalars before TakeXxx empties the indexer.
  meta.SetScalar(kVertexNumElements, std::to_string(idx.GetNumElements()));
  meta.SetScalar(kVertexNumSlotsMinusOne,
                 std::to_string(idx.GetNumSlotsMinusOne()));
  meta.SetScalar(kVertexHashPolicy, std::to_string(idx.GetHashPolicyIndex()));
  // Table columns are unique_ptr, so they're dumped inline; the unique_ptr
  // leaves transfer ownership into a transient store that Dumps + cleans up.
  auto table = vt.TakeTable();
  for (size_t i = 0; i < table->col_num(); ++i) {
    table->get_column_by_id(i)->Dump(ckp, meta, VertexPropKey(i));
  }
  neug::ModuleBroker store;
  store.SetModule(kVertexIndexerKeys, idx.TakeKeys());
  store.SetModule(kVertexIndexerIndices, idx.TakeIndices());
  store.SetModule(kVertexVTs, vt.TakeVertexTimestamp());
  store.Dump(ckp, meta);
  return meta;
}

// EdgeTable round-trip via ModuleBroker.
inline constexpr const char* kEdgeInCsr = "edge/in_csr";
inline constexpr const char* kEdgeOutCsr = "edge/out_csr";
inline std::string EdgePropKey(size_t i) {
  return "edge/prop_" + std::to_string(i);
}
inline constexpr const char* kEdgeTableIdx = "edge/table_idx";
inline constexpr const char* kEdgeCapacity = "edge/capacity";

inline void OpenEdgeTableLegacy(neug::EdgeTable& et,
                                std::shared_ptr<neug::Checkpoint> ckp,
                                const neug::CheckpointManifest& meta,
                                neug::MemoryLevel level) {
  et.SetMemoryLevel(level);
  if (!meta.has_module(kEdgeOutCsr)) {
    et.Init(ckp, level);
    return;
  }
  auto es = et.get_edge_schema_ptr();
  neug::ModuleBroker store;
  store.Open(*ckp, meta, level);
  et.SetInCsr(store.TakeModule<neug::CsrBase>(kEdgeInCsr));
  et.SetOutCsr(store.TakeModule<neug::CsrBase>(kEdgeOutCsr));
  if (es && !es->is_bundled()) {
    auto table =
        std::make_unique<neug::Table>(es->property_names, es->properties);
    for (size_t i = 0; i < es->properties.size(); ++i) {
      table->SetColumn(static_cast<int>(i),
                       store.TakeModule<neug::ColumnBase>(EdgePropKey(i)));
    }
    et.SetTable(std::move(table));
    et.SetTableIdx(meta.GetScalarAs<uint64_t>(kEdgeTableIdx).value_or(0));
  }
  et.SetCapacity(meta.GetScalarAs<uint64_t>(kEdgeCapacity).value_or(0));
}

inline neug::CheckpointManifest DumpEdgeTableLegacy(neug::EdgeTable& et,
                                                    neug::Checkpoint& ckp) {
  neug::CheckpointManifest meta;
  meta.SetScalar(kEdgeCapacity, std::to_string(et.GetCapacity()));
  auto es = et.get_edge_schema_ptr();
  if (es && !es->is_bundled()) {
    meta.SetScalar(kEdgeTableIdx, std::to_string(et.GetTableIdx()));
    auto table = et.TakeTable();
    for (size_t i = 0; i < table->col_num(); ++i) {
      table->get_column_by_id(i)->Dump(ckp, meta, EdgePropKey(i));
    }
  }
  neug::ModuleBroker store;
  store.SetModule(kEdgeOutCsr, et.TakeOutCsr());
  store.SetModule(kEdgeInCsr, et.TakeInCsr());
  store.Dump(ckp, meta);
  return meta;
}

namespace neug {
namespace test {

inline void load_modern_graph(std::shared_ptr<neug::Connection> conn) {
  const char* csv_dir_ptr = std::getenv("MODERN_GRAPH_DATA_DIR");
  if (csv_dir_ptr == nullptr) {
    throw std::runtime_error(
        "MODERN_GRAPH_DATA_DIR environment variable is not set");
  }
  LOG(INFO) << "CSV data dir: " << csv_dir_ptr;
  std::string csv_dir = csv_dir_ptr;
  auto res = conn->Query(
      "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY "
      "KEY(id));");
  EXPECT_TRUE(res) << res.error().ToString();

  {
    auto res = conn->Query(
        "CREATE NODE TABLE software(id INT64, name STRING, lang STRING, "
        "PRIMARY "
        "KEY(id));");
    EXPECT_TRUE(res) << res.error().ToString();
  }
  {
    auto res = conn->Query(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res = conn->Query(
        "CREATE REL TABLE created(FROM person TO software, weight DOUBLE, "
        "since INT64);");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res = conn->Query("COPY person from \"" + csv_dir + "/person.csv\";");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res =
        conn->Query("COPY software from \"" + csv_dir + "/software.csv\";");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res = conn->Query(
        "COPY knows from \"" + csv_dir +
        "/person_knows_person.csv\" (from=\"person\", to=\"person\");");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res = conn->Query("COPY created from \"" + csv_dir +
                           "/person_created_software.csv\" (from=\"person\", "
                           "to =\"software\");");
    EXPECT_TRUE(res) << res.error().ToString();
  }
}
}  // namespace test
}  // namespace neug

#endif  // NEUG_TESTS_UNITTTEST_STORAGES_UTILS_H_
