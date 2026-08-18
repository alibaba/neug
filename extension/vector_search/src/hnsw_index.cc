/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "hnsw_index.h"

#include <filesystem>
#include <limits>
#include <stdexcept>
#include <utility>
#include <variant>

#include <glog/logging.h>
#include <zvec/core/interface/index_factory.h>
#include <roaring.hh>

#include "neug/common/extra_type_info.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"

namespace neug::vector_search_ext {

namespace {
constexpr const char* kIndexBufferPath = "index_buffer";

struct DenseValueBuffer {
  using Buffer = std::variant<std::vector<float>>;

  Buffer values;

  const void* data() const {
    return std::visit(
        [](const auto& buffer) -> const void* { return buffer.data(); },
        values);
  }
};

result<DenseValueBuffer> ConvertDenseValue(const DataType& property_type,
                                           const Value& value,
                                           size_t dimension) {
  if (property_type.id() != DataTypeId::kArray) {
    RETURN_INVALID_ARGUMENT_ERROR("HNSW index property type must be an ARRAY");
  }
  if (value.IsNull() || value.type().id() != DataTypeId::kArray) {
    RETURN_INVALID_ARGUMENT_ERROR("HNSW vector value must be a non-null ARRAY");
  }
  const auto& children = ArrayValue::GetChildren(value);
  if (children.size() != dimension) {
    RETURN_INVALID_ARGUMENT_ERROR("HNSW vector dimension mismatch: expected " +
                                  std::to_string(dimension) + ", got " +
                                  std::to_string(children.size()));
  }

  auto element_type = ArrayType::GetChildType(property_type).id();
  switch (element_type) {
  case DataTypeId::kFloat: {
    std::vector<float> values;
    values.reserve(dimension);
    for (const auto& child : children) {
      if (child.IsNull() || child.type().id() != DataTypeId::kFloat) {
        RETURN_INVALID_ARGUMENT_ERROR(
            "HNSW FLOAT vector contains an invalid element");
      }
      values.push_back(child.GetValue<float>());
    }
    return DenseValueBuffer{std::move(values)};
  }
  default:
    RETURN_UNSUPPORTED_ERROR("HNSW vector element type is not supported");
  }
}

int ParsePositive(const common::case_insensitive_map_t<std::string>& options,
                  const std::string& key, int default_value) {
  auto iter = options.find(key);
  if (iter == options.end())
    return default_value;
  try {
    auto value = std::stoi(iter->second);
    if (value <= 0)
      throw std::invalid_argument("not positive");
    return value;
  } catch (const std::exception&) {
    THROW_INVALID_ARGUMENT_EXCEPTION("HNSW option '" + key +
                                     "' must be a positive integer");
  }
}
}  // namespace

HNSWVecSource::HNSWVecSource(const VecColumn* column, index_id_t index_id_count)
    : column_(column), vector_byte_size_(0), index_id_count_(index_id_count) {
  if (!column_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("HNSW vector source requires a VecColumn");
  }
  const auto& array_type = column_->array_type();
  if (array_type.id() != DataTypeId::kArray) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "HNSW vector source requires an ARRAY column");
  }
  const auto dimension = column_->array_size();
  if (dimension == 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "HNSW vector source requires a positive vector dimension");
  }
  switch (ArrayType::GetChildType(array_type).id()) {
  case DataTypeId::kFloat:
    vector_byte_size_ = sizeof(float) * dimension;
    break;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "HNSW vector source element type is not supported");
  }
}

const void* HNSWVecSource::get_vector(uint32_t node_id) const {
  if (static_cast<index_id_t>(node_id) >= index_id_count_) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "HNSW vector source node ID " + std::to_string(node_id) +
        " is out of range (index ID count=" + std::to_string(index_id_count_) +
        ")");
  }
  return static_cast<const uint8_t*>(column_->get_buffer_ptr()) +
         node_id * vector_byte_size_;
}

ZVecDumpContainer::ZVecDumpContainer(zvec::core_interface::Index* index,
                                     std::string runtime_path)
    : zvec_index_(index), runtime_path_(std::move(runtime_path)) {}

void ZVecDumpContainer::Sync() {
  if (zvec_index_ && zvec_index_->Flush() != 0) {
    THROW_RUNTIME_ERROR("[zvec] Failed to flush HNSW index");
  }
}

void ZVecDumpContainer::Dump(const std::string& new_path) {
  Sync();
  std::filesystem::rename(runtime_path_, new_path);
  runtime_path_ = new_path;
}

bool ZVecDumpContainer::IsDirty() {
  return zvec_index_ && zvec_index_->IsDirty();
}

std::shared_ptr<IDataContainer> ZVecDumpContainer::Fork(Checkpoint&,
                                                        MemoryLevel) {
  THROW_NOT_SUPPORTED_EXCEPTION("ZVecDumpContainer does not support Fork");
}

HNSWIndex::~HNSWIndex() {
  // Clone() shares the read-only zvec handle until one of the copies is
  // detached. Closing a shared handle here would invalidate the other clone.
  if (zvec_index_ && zvec_index_.use_count() == 1)
    zvec_index_->Close();
}

void HNSWIndex::ParseOptions() {
  if (!meta_)
    THROW_RUNTIME_ERROR("HNSWIndex metadata is not initialized");
  dimension_ = ParsePositive(meta_->options, "dimension", dimension_);
  m_ = ParsePositive(meta_->options, "m", m_);
  ef_construction_ =
      ParsePositive(meta_->options, "ef_construction", ef_construction_);

  auto metric = meta_->options.find("metric");
  if (metric == meta_->options.end() || metric->second == "l2" ||
      metric->second == "l2sq") {
    metric_ = zvec::core_interface::MetricType::kL2sq;
  } else if (metric->second == "cosine") {
    metric_ = zvec::core_interface::MetricType::kCosine;
  } else if (metric->second == "inner_product" || metric->second == "ip") {
    metric_ = zvec::core_interface::MetricType::kInnerProduct;
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Unsupported HNSW metric: " +
                                     metric->second);
  }

  if (meta_->schema.property_type.id() != DataTypeId::kArray) {
    THROW_INVALID_ARGUMENT_EXCEPTION("HNSWIndex requires an ARRAY property");
  }
  dimension_ =
      static_cast<int>(ArrayType::GetNumElements(meta_->schema.property_type));
}

zvec::core_interface::DataType HNSWIndex::ResolveDataType() const {
  auto child = ArrayType::GetChildType(meta_->schema.property_type).id();
  if (child == DataTypeId::kFloat) {
    return zvec::core_interface::DataType::DT_FP32;
  }
  THROW_INVALID_ARGUMENT_EXCEPTION("HNSWIndex supports only FLOAT arrays");
}

void HNSWIndex::Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                     MemoryLevel level) {
  StorageIndex::Open(ckp, descriptor, level);
  ParseOptions();

  zvec::core_interface::HNSWIndexParam param(metric_, dimension_, m_,
                                             ef_construction_);
  param.data_type = ResolveDataType();
  param.use_external_vector = true;
  zvec_index_ = zvec::core_interface::IndexFactory::CreateAndInitIndex(param);
  if (!zvec_index_) {
    THROW_RUNTIME_ERROR("[zvec] Failed to create HNSW index");
  }

  auto runtime_file = ckp.CreateRuntimeFile();
  zvec_runtime_path_ = runtime_file.path();
  zvec_runtime_file_ =
      std::make_unique<CheckpointFileManager::RuntimeFileHandle>(
          std::move(runtime_file));
  auto index_path = descriptor.get_path(kIndexBufferPath);
  bool has_existing = index_path && !index_path->empty() &&
                      std::filesystem::exists(*index_path);
  if (has_existing) {
    file_utils::copy_file(*index_path, zvec_runtime_path_, true);
  } else {
    // CreateRuntimeFile reserves the runtime path with an empty file, while
    // ZVec initializes new storage only when the path does not exist.
    std::error_code ec;
    if (!std::filesystem::remove(zvec_runtime_path_, ec) || ec) {
      THROW_IO_EXCEPTION(
          "Failed to remove reserved runtime file before creating ZVec "
          "index: " +
          zvec_runtime_path_ + ": " + ec.message());
    }
  }

  zvec::core_interface::StorageOptions options;
  options.type = zvec::core_interface::StorageOptions::StorageType::kMMAP;
  options.create_new = !has_existing;
  options.read_only = false;
  options.copy_on_write = false;
  auto ret = zvec_index_->Open(zvec_runtime_path_, options);
  if (ret != 0) {
    THROW_RUNTIME_ERROR("[zvec] Failed to open HNSW index at " +
                        zvec_runtime_path_ +
                        ", error code: " + std::to_string(ret));
  }
  LOG(INFO) << "[zvec] Opened HNSW index at " << zvec_runtime_path_;
}

void HNSWIndex::Dump(Checkpoint& ckp, CheckpointManifest& manifest,
                     const std::string& key) {
  if (key.empty())
    THROW_RUNTIME_ERROR("HNSWIndex::Dump: module key must not be empty");
  if (!zvec_index_)
    THROW_RUNTIME_ERROR("HNSWIndex::Dump: index is not open");

  StorageIndex::Dump(ckp, manifest, key);
  auto descriptor = manifest.mutable_modules().find(key);
  if (descriptor == manifest.mutable_modules().end()) {
    THROW_RUNTIME_ERROR(
        "HNSWIndex::Dump: StorageIndex did not write module descriptor for '" +
        key + "'");
  }
  // HNSW is provided by the vector_search extension. Allow the graph to open
  // before that extension is loaded; the pending index is activated after
  // LOAD vector_search registers the hnsw_index module type.
  descriptor->second.required = false;
  if (zvec_index_->GetDocCount() == 0) {
    return;
  }

  ZVecDumpContainer container(zvec_index_.get(), zvec_runtime_path_);
  auto persisted_path = ckp.Commit(container);
  zvec_runtime_file_.reset();
  zvec_runtime_path_ = persisted_path;

  descriptor->second.set_path(kIndexBufferPath, std::move(persisted_path));
}

Status HNSWIndex::Rebind(const IndexBindContext& context) {
  auto* column = dynamic_cast<const VecColumn*>(context.column);
  if (!column) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSWIndex requires a VecColumn binding");
  }
  if (!meta_ || meta_->schema.property_type.id() != DataTypeId::kArray) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSWIndex metadata does not describe an ARRAY");
  }
  auto element_type = ArrayType::GetChildType(meta_->schema.property_type).id();
  if (element_type != DataTypeId::kFloat) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSWIndex supports only FLOAT arrays");
  }
  auto* offset_accessor = column->get_offset_accessor();
  if (!offset_accessor) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSWIndex requires a bound VecColumn offset accessor");
  }
  auto* buffer_ptr = column->get_buffer_ptr();
  if (!buffer_ptr) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSWIndex requires a bound vector buffer");
  }
  index_id_accessor_ =
      std::make_unique<VecColumnBackedIndexIDAccessor>(*offset_accessor);
  vec_source_ = std::make_unique<HNSWVecSource>(
      column, index_id_accessor_->GetNextIndexID());
  return Status::OK();
}

Status HNSWIndex::BulkBuild(const VertexSet& vertices) {
  if (!zvec_index_ || !vec_source_ || !index_id_accessor_) {
    return Status::RuntimeError(
        "HNSWIndex must be open and bound before bulk build");
  }
  for (auto vid : vertices) {
    auto index_id = index_id_accessor_->GetIndexIDByVID(vid);
    if (index_id == INVALID_INDEX_ID) {
      continue;
    }
    zvec::core_interface::VectorData vector;
    vector.vector =
        zvec::core_interface::DenseVector{vec_source_->get_vector(index_id)};
    auto ret = zvec_index_->AddWithSource(vector, index_id, *vec_source_);
    if (ret != 0) {
      return Status::RuntimeError("ZVec HNSW bulk build failed for vertex " +
                                  std::to_string(vid) + " with error code " +
                                  std::to_string(ret));
    }
  }
  return Status::OK();
}

void HNSWIndex::Detach(Checkpoint& ckp, MemoryLevel level) {
  if (index_id_accessor_) {
    index_id_accessor_->Detach(ckp, level);
  }
}

std::unique_ptr<Module> HNSWIndex::Clone() const {
  auto cloned = std::make_unique<HNSWIndex>();
  if (meta_) {
    cloned->meta_ = std::make_unique<IndexMeta>(*meta_);
  }
  if (index_id_accessor_) {
    auto accessor = index_id_accessor_->Clone();
    cloned->index_id_accessor_.reset(
        static_cast<IndexIDAccessor*>(accessor.release()));
  }
  // ZVec supports concurrent reads of a shared index: search state is held in
  // thread-local contexts and its HNSW streamer protects shared state with
  // internal shared locks. Keep one index instance across read-only clones.
  cloned->zvec_index_ = zvec_index_;
  if (vec_source_) {
    cloned->vec_source_ = std::make_unique<HNSWVecSource>(*vec_source_);
  }
  cloned->zvec_runtime_path_ = zvec_runtime_path_;
  cloned->dimension_ = dimension_;
  cloned->m_ = m_;
  cloned->ef_construction_ = ef_construction_;
  cloned->metric_ = metric_;
  return cloned;
}

result<std::vector<SearchCandidate>> HNSWIndex::SearchImpl(
    const IndexQueryParams& params) {
  const auto* hnsw_params = dynamic_cast<const HNSWIndexQueryParams*>(&params);
  if (!hnsw_params) {
    RETURN_INVALID_ARGUMENT_ERROR(
        "HNSWIndex::Search requires HNSWIndexQueryParams");
  }
  if (!zvec_index_ || !vec_source_) {
    RETURN_INVALID_ARGUMENT_ERROR(
        "HNSWIndex must be open and bound before search");
  }
  if (hnsw_params->topk == 0 || hnsw_params->ef_search == 0) {
    RETURN_INVALID_ARGUMENT_ERROR(
        "HNSW search topk and ef_search must be positive");
  }

  GS_AUTO(target, ConvertDenseValue(meta_->schema.property_type,
                                    hnsw_params->target_value, dimension_));

  auto query_param = std::make_shared<zvec::core_interface::HNSWQueryParam>();
  query_param->topk = hnsw_params->topk;
  query_param->fetch_vector = hnsw_params->fetch_vector;
  query_param->radius = hnsw_params->radius;
  query_param->ef_search = hnsw_params->ef_search;
  query_param->prefetch_offset = hnsw_params->prefetch_offset;
  query_param->prefetch_lines = hnsw_params->prefetch_lines;

  if (hnsw_params->use_scalar_filter) {
    // Build the allowlist from each matching vertex's current index ID. This
    // also filters tombstones: deleted vertices resolve to INVALID_INDEX_ID,
    // while an updated vertex contributes only its latest index ID, not the
    // stale ID left in HNSW.
    auto allowed = std::make_shared<roaring::Roaring>();
    for (auto vid : hnsw_params->scalar_filter) {
      auto index_id = index_id_accessor_->GetIndexIDByVID(vid);
      if (index_id != INVALID_INDEX_ID)
        allowed->add(index_id);
    }
    // An enabled scalar filter with no valid index IDs cannot match anything.
    // Avoid invoking ZVec with a filter that rejects every document.
    if (allowed->isEmpty()) {
      return std::vector<SearchCandidate>{};
    }
    allowed->runOptimize();
    query_param->filter = std::make_shared<zvec::core_interface::IndexFilter>();
    // ZVec's filter predicate returns true to exclude the key.
    query_param->filter->set([allowed](uint64_t key) {
      return key > std::numeric_limits<uint32_t>::max() ||
             !allowed->contains(static_cast<uint32_t>(key));
    });
  } else if (index_id_accessor_) {
    const auto visible_limit = index_id_accessor_->GetVisibleLimit();
    const auto& deleted_index_ids = index_id_accessor_->GetDeletedIndexIDs();
    // Skip the filter in the read-only steady state: every allocated index ID
    // is visible and there are no tombstones to exclude.
    if (visible_limit < index_id_accessor_->GetNextIndexID() ||
        !deleted_index_ids.empty()) {
      auto deleted = std::make_shared<roaring::Roaring>();
      if (!deleted_index_ids.empty()) {
        for (auto index_id : deleted_index_ids) {
          deleted->add(index_id);
        }
        deleted->runOptimize();
      }
      query_param->filter =
          std::make_shared<zvec::core_interface::IndexFilter>();
      query_param->filter->set([visible_limit, deleted](uint64_t key) {
        return key >= visible_limit ||
               deleted->contains(static_cast<uint32_t>(key));
      });
    }
  }

  zvec::core_interface::VectorData query;
  query.vector = zvec::core_interface::DenseVector{target.data()};
  zvec::core_interface::SearchResult search_result;
  auto ret = zvec_index_->SearchWithSource(query, query_param, *vec_source_,
                                           &search_result);
  if (ret != 0) {
    RETURN_ERROR(Status::RuntimeError(
        "ZVec HNSW search failed with error code " + std::to_string(ret)));
  }

  std::vector<SearchCandidate> result;
  result.reserve(search_result.doc_list_.size());
  for (const auto& document : search_result.doc_list_) {
    if (document.key() <= std::numeric_limits<index_id_t>::max()) {
      result.push_back(SearchCandidate{static_cast<index_id_t>(document.key()),
                                       document.score()});
    }
  }
  return result;
}

Status HNSWIndex::AppendImpl(index_id_t index_id, const Value& value) {
  if (!zvec_index_ || !vec_source_) {
    return Status::RuntimeError(
        "HNSWIndex must be open and bound before append");
  }

  auto dense_result =
      ConvertDenseValue(meta_->schema.property_type, value, dimension_);
  if (!dense_result) {
    return dense_result.error();
  }
  auto dense = std::move(dense_result).value();
  vec_source_->UpdateIndexIDCount(index_id_accessor_->GetNextIndexID());

  zvec::core_interface::VectorData vector;
  vector.vector = zvec::core_interface::DenseVector{dense.data()};
  auto ret = zvec_index_->AddWithSource(vector, index_id, *vec_source_);
  if (ret != 0) {
    return Status::RuntimeError("ZVec HNSW append failed with error code " +
                                std::to_string(ret));
  }
  return Status::OK();
}

NEUG_REGISTER_MODULE(HNSWIndex);
}  // namespace neug::vector_search_ext
