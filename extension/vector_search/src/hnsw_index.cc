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

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <functional>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>

#include <glog/logging.h>
#include <zvec/core/framework/index_error.h>
#include <zvec/core/interface/index_factory.h>
#include <roaring.hh>

#include "neug/common/extra_type_info.h"
#include "neug/common/types/container_types.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/parallel_utils.h"

namespace neug::vector_search_ext {

namespace {
constexpr const char* kIndexBufferPath = "index_buffer";
constexpr const char* kFlatOffset = "flat_index_offset";
constexpr const char* kFlatBufferPath = "flat_index_buffer";
constexpr double kDuplicateWarningThresholdPercentage = 20.0;

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
    size_t consumed = 0;
    auto value = std::stoi(iter->second, &consumed);
    if (value <= 0 || consumed != iter->second.size())
      throw std::invalid_argument("not positive");
    return value;
  } catch (const std::exception&) {
    THROW_INVALID_ARGUMENT_EXCEPTION("HNSW option '" + key +
                                     "' must be a positive integer");
  }
}
int ParseConcurrency(
    const common::case_insensitive_map_t<std::string>& options) {
  const auto iter = options.find("concurrency");
  if (iter == options.end())
    return 0;
  try {
    size_t consumed = 0;
    const auto value = std::stoi(iter->second, &consumed);
    if (value < 0 || consumed != iter->second.size())
      throw std::invalid_argument("invalid concurrency");
    return value;
  } catch (const std::exception&) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "HNSW option 'concurrency' must be a non-negative integer; "
        "0 uses the maximum number of CPU cores on the hardware platform. "
        "Large values are capped by the hardware CPU core count and the "
        "number of task chunks.");
  }
}

ParallelOptions BuildParallelOptions(size_t requested, size_t range) {
  constexpr size_t chunk_size = 64;
  const size_t hardware = std::max(1u, std::thread::hardware_concurrency());
  const size_t chunks = range == 0 ? 0 : 1 + (range - 1) / chunk_size;
  return {std::min({requested == 0 ? hardware : requested, hardware, chunks}),
          chunk_size};
}

}  // namespace

HNSWVecSource::HNSWVecSource(const VecColumn* column)
    : column_(column), vector_byte_size_(0) {
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

bool HNSWVecSource::is_l2_normalized() const {
  return column_->is_l2_normalized();
}

const void* HNSWVecSource::get_vector(uint32_t node_id) const {
  // HNSW computes neighbor distances before applying the visibility filter.
  // Newly appended IDs can therefore exceed this reader's visible ID count.
  // Only the capacity of this snapshot's payload determines safe access.
  const auto capacity = column_->buffer_size();
  if (node_id >= capacity) {
    VLOG(1) << "HNSW vector source node ID " << node_id
            << " is out of range (buffer size=" << capacity << ")";
    // A writer may have resized into a new buffer while this reader retains
    // the old one. ZVec skips unavailable external vectors during traversal.
    return nullptr;
  }
  return static_cast<const uint8_t*>(column_->get_buffer_ptr()) +
         static_cast<size_t>(node_id) * vector_byte_size_;
}

FlatVecSource::FlatVecSource(const VecColumn* column, index_id_t index_id_count,
                             index_id_t begin)
    : vector_byte_size_(0), index_id_count_(index_id_count) {
  if (!column || begin > index_id_count ||
      ArrayType::GetChildType(column->array_type()).id() !=
          DataTypeId::kFloat) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid HNSW vector source");
  }
  auto view = column->PinVectorView(begin, index_id_count);
  vector_byte_size_ = view.stride;
  view_ = {view.data, begin, index_id_count - begin, view.stride, view.lease};
}

const void* FlatVecSource::get_vector(uint32_t node_id) const {
  // ZVec skips unavailable external vectors, including nodes appended after
  // this snapshot. Never form an out-of-bounds pointer or throw for them.
  if (node_id < view_.first_id || node_id >= index_id_count_) {
    return nullptr;
  }
  return static_cast<const uint8_t*>(view_.data) +
         size_t(node_id - view_.first_id) * vector_byte_size_;
}

ZVecDumpContainer::ZVecDumpContainer(zvec::core_interface::Index* index,
                                     std::string runtime_path)
    : zvec_index_(index), runtime_path_(std::move(runtime_path)) {}

void ZVecDumpContainer::Sync() {
  if (zvec_index_ && zvec_index_->flush() != 0) {
    THROW_RUNTIME_ERROR("[zvec] Failed to flush index");
  }
}

void ZVecDumpContainer::Dump(const std::string& new_path) {
  Sync();
  std::filesystem::rename(runtime_path_, new_path);
  runtime_path_ = new_path;
}

bool ZVecDumpContainer::IsDirty() {
  return zvec_index_ && zvec_index_->is_dirty();
}

std::shared_ptr<IDataContainer> ZVecDumpContainer::Fork(Checkpoint&,
                                                        MemoryLevel) {
  THROW_NOT_SUPPORTED_EXCEPTION("ZVecDumpContainer does not support Fork");
}

HNSWIndex::~HNSWIndex() {
  // Clone() shares the read-only zvec handle until one of the copies is
  // detached. Closing a shared handle here would invalidate the other clone.
  if (zvec_index_ && zvec_index_.use_count() == 1)
    zvec_index_->close();
  if (flat_index_ && flat_index_.use_count() == 1)
    flat_index_->close();
}

void HNSWIndex::ParseOptions() {
  if (!meta_)
    THROW_RUNTIME_ERROR("HNSWIndex metadata is not initialized");
  dimension_ = ParsePositive(meta_->options, "dimension", dimension_);
  m_ = ParsePositive(meta_->options, "m", m_);
  ef_construction_ =
      ParsePositive(meta_->options, "ef_construction", ef_construction_);
  build_workers_ = ParseConcurrency(meta_->options);

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

  if (meta_->schema.columns.size() != 1 ||
      meta_->schema.columns[0].property_type.id() != DataTypeId::kArray) {
    THROW_INVALID_ARGUMENT_EXCEPTION("HNSWIndex requires an ARRAY property");
  }
  dimension_ = static_cast<int>(
      ArrayType::GetNumElements(meta_->schema.columns[0].property_type));
}

zvec::core_interface::DataType HNSWIndex::ResolveDataType() const {
  if (!meta_ || meta_->schema.columns.size() != 1 ||
      meta_->schema.columns[0].property_type.id() != DataTypeId::kArray) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "HNSWIndex metadata must describe exactly one ARRAY property");
  }
  auto child =
      ArrayType::GetChildType(meta_->schema.columns[0].property_type).id();
  if (child == DataTypeId::kFloat) {
    return zvec::core_interface::DataType::DT_FP32;
  }
  THROW_INVALID_ARGUMENT_EXCEPTION("HNSWIndex supports only FLOAT arrays");
}

void HNSWIndex::Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                     MemoryLevel level) {
  StorageIndex::Open(ckp, descriptor, level);
  ParseOptions();
  const auto offset = descriptor.get(kFlatOffset);
  flat_index_offset_.reset();
  column_ = nullptr;
  if (offset) {
    if (offset->empty() ||
        offset->find_first_not_of("0123456789") != std::string::npos) {
      THROW_RUNTIME_ERROR("Invalid HNSW LocalBuffer offset");
    }
    const auto parsed = std::stoull(*offset);
    if (parsed > std::numeric_limits<index_id_t>::max()) {
      THROW_RUNTIME_ERROR("HNSW LocalBuffer offset is out of range");
    }
    flat_index_offset_ = static_cast<index_id_t>(parsed);
  }

  zvec::core_interface::HNSWIndexParam param(metric_, dimension_, m_,
                                             ef_construction_);
  param.data_type = ResolveDataType();
  param.use_external_vector = true;
  param.params.set("proxima.hnsw.streamer.filter_same_key", true);
  zvec_index_ = zvec::core_interface::IndexFactory::CreateAndInitIndex(param);
  if (!zvec_index_) {
    THROW_RUNTIME_ERROR("[zvec] Failed to create HNSW index");
  }

  auto runtime_file = ckp.CreateRuntimeFile();
  zvec_runtime_path_ = runtime_file.path();
  zvec_runtime_file_ =
      std::make_shared<CheckpointFileManager::RuntimeFileHandle>(
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
  auto ret = zvec_index_->open(zvec_runtime_path_, options);
  if (ret != 0) {
    THROW_RUNTIME_ERROR(
        "ZVec internal open failed for HNSW index at " + zvec_runtime_path_ +
        ", error code: " + std::to_string(ret) + ". See logs for details.");
  }
  zvec::core_interface::FlatIndexParam flat_param;
  flat_param.dimension = dimension_;
  flat_param.data_type = ResolveDataType();
  flat_param.metric_type = metric_ == zvec::core_interface::MetricType::kCosine
                               ? zvec::core_interface::MetricType::kInnerProduct
                               : metric_;
  flat_param.use_external_vector = true;
  flat_index_ =
      zvec::core_interface::IndexFactory::CreateAndInitIndex(flat_param);
  auto flat_file = ckp.CreateRuntimeFile();
  flat_runtime_path_ = flat_file.path();
  flat_runtime_file_ =
      std::make_shared<CheckpointFileManager::RuntimeFileHandle>(
          std::move(flat_file));
  const auto flat_path = descriptor.get_path(kFlatBufferPath);
  const bool has_flat =
      flat_path && !flat_path->empty() && std::filesystem::exists(*flat_path);
  if (has_flat) {
    file_utils::copy_file(*flat_path, flat_runtime_path_, true);
  } else {
    std::filesystem::remove(flat_runtime_path_);
  }
  options.create_new = !has_flat;
  if (!flat_index_ || flat_index_->open(flat_runtime_path_, options) != 0) {
    THROW_RUNTIME_ERROR("Failed to open External Flat");
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
  auto* descriptor = manifest.FindMutableModule(key);
  if (descriptor == nullptr) {
    THROW_RUNTIME_ERROR(
        "HNSWIndex::Dump: StorageIndex did not write module descriptor for '" +
        key + "'");
  }
  // HNSW is provided by the vector_search extension. Allow the graph to open
  // before that extension is loaded; the pending index is activated after
  // LOAD vector_search registers the hnsw_index module type.
  descriptor->required = false;
  descriptor->set(kFlatOffset, std::to_string(flat_index_offset_.value()));
  // Persist registrations even when all vectors are still in LocalBuffer.
  ZVecDumpContainer flat_container(flat_index_.get(), flat_runtime_path_);
  auto flat_persisted_path = ckp.Commit(flat_container);
  flat_runtime_file_.reset();
  flat_runtime_path_ = flat_persisted_path;
  descriptor->set_path(kFlatBufferPath, std::move(flat_persisted_path));
  if (zvec_index_->get_doc_count() == 0) {
    return;
  }

  ZVecDumpContainer container(zvec_index_.get(), zvec_runtime_path_);
  auto persisted_path = ckp.Commit(container);
  zvec_runtime_file_.reset();
  zvec_runtime_path_ = persisted_path;

  descriptor->set_path(kIndexBufferPath, std::move(persisted_path));
}

Status HNSWIndex::Rebind(const IndexBindContext& context) {
  if (context.columns.size() != 1) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSWIndex requires exactly one property column binding");
  }
  auto* column = dynamic_cast<const VecColumn*>(context.columns[0]);
  if (!column) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSWIndex requires a VecColumn binding");
  }
  if (!meta_ || meta_->schema.columns.size() != 1 ||
      meta_->schema.columns[0].property_type.id() != DataTypeId::kArray) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSWIndex metadata does not describe an ARRAY");
  }
  auto element_type =
      ArrayType::GetChildType(meta_->schema.columns[0].property_type).id();
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
  const bool initialize_visible_ids = column_ == nullptr;
  column_ = column;
  const auto limit = index_id_accessor_->GetVisibleLimit();
  local_buffer_enabled_ =
      metric_ != zvec::core_interface::MetricType::kCosine ||
      column->is_l2_normalized();
  if (!flat_index_offset_) {
    flat_index_offset_ = limit;
    LOG(WARNING)
        << "HNSW flat_index_offset is absent; initialize it to visible "
           "limit "
        << limit;
  }
  if (flat_index_offset_ > limit) {
    return Status::RuntimeError(
        "HNSW LocalBuffer offset exceeds visible limit");
  }
  if (initialize_visible_ids)
    InitializeVisibleIDs();
  return Status::OK();
}

void HNSWIndex::InitializeVisibleIDs() {
  deleted_cursor_ = index_id_accessor_->GetDeletedIndexIDs().size();
  hnsw_ids_ = roaring::Roaring();
  flat_ids_ = roaring::Roaring();
  // Reconstruct only actual mappings on first bind after Open. Never insert
  // allocation gaps into either bitmap. Clones already own their snapshot IDs.
  for (size_t vid = 0; vid < index_id_accessor_->size(); ++vid) {
    const auto id = index_id_accessor_->GetIndexIDByVID(vid);
    if (id != INVALID_INDEX_ID) {
      (!local_buffer_enabled_ || id < flat_index_offset_.value() ? hnsw_ids_
                                                                 : flat_ids_)
          .add(id);
    }
  }
}

void HNSWIndex::ConsumeDeletedIDs() {
  const auto& deleted_ids = index_id_accessor_->GetDeletedIndexIDs();
  CHECK_LE(deleted_cursor_, deleted_ids.size());
  while (deleted_cursor_ < deleted_ids.size()) {
    const auto id = deleted_ids[deleted_cursor_];
    hnsw_ids_.remove(id);
    flat_ids_.remove(id);
    ++deleted_cursor_;
  }
}

Status HNSWIndex::Delete(vid_t vid) {
  RETURN_IF_NOT_OK(StorageIndex::Delete(vid));
  ConsumeDeletedIDs();
  return Status::OK();
}

Status HNSWIndex::BulkBuild(const VertexSet& vertices) {
  if (!zvec_index_ || !column_ || !index_id_accessor_) {
    return Status::RuntimeError(
        "HNSWIndex must be open and bound before bulk build");
  }
  struct BuildWorkerState {
    flat_hash_set<uint64_t> fingerprints;
    roaring::Roaring ids;
    size_t indexed_count = 0;
  };
  const auto options = BuildParallelOptions(build_workers_, vertices.size());
  std::vector<BuildWorkerState> states(options.max_workers);
  HNSWVecSource source(column_);
  const auto vector_byte_size = source.GetVectorByteSize();
  auto status =
      vertices.size() == 0
          ? Status::OK()
          : neug::parallel_for(
                0, vertices.size(), options,
                [&](size_t vid, size_t worker_id) -> Status {
                  if (!vertices.valid(vid))
                    return Status::OK();
                  const auto id = index_id_accessor_->GetIndexIDByVID(vid);
                  if (id == INVALID_INDEX_ID)
                    return Status::OK();
                  const auto* data = source.get_vector(id);
                  if (!data)
                    return Status::RuntimeError(
                        "HNSW bulk build requires an available vector for "
                        "vertex " +
                        std::to_string(vid) +
                        " (index ID=" + std::to_string(id) + ")");
                  zvec::core_interface::VectorData vector;
                  vector.vector = zvec::core_interface::DenseVector{data};
                  const auto ret =
                      zvec_index_->add_with_source(vector, id, source);
                  if (ret != 0 && ret != zvec::core::IndexError_Duplicate)
                    return Status::RuntimeError(
                        "ZVec internal bulk build failed for vertex " +
                        std::to_string(vid) + " with error code " +
                        std::to_string(ret) + ". See logs for details.");
                  auto& state = states[worker_id];
                  state.fingerprints.insert(static_cast<uint64_t>(
                      std::hash<std::string_view>{}(std::string_view(
                          static_cast<const char*>(data), vector_byte_size))));
                  state.ids.add(id);
                  ++state.indexed_count;
                  return Status::OK();
                });
  if (!status.ok())
    return status;
  flat_hash_set<uint64_t> duplicate_statistics;
  roaring::Roaring ids, empty;
  size_t indexed_vector_count = 0;
  for (auto& state : states) {
    duplicate_statistics.insert(state.fingerprints.begin(),
                                state.fingerprints.end());
    ids |= state.ids;
    indexed_vector_count += state.indexed_count;
  }
  const auto deleted_cursor = index_id_accessor_->GetDeletedIndexIDs().size();
  const auto limit = index_id_accessor_->GetVisibleLimit();
  const auto duplicate_vector_count =
      indexed_vector_count - duplicate_statistics.size();
  const auto duplicate_percentage =
      indexed_vector_count == 0
          ? 0.0
          : 100.0 * duplicate_vector_count / indexed_vector_count;
  std::ostringstream message_stream;
  message_stream << "HNSW duplicate statistics for index '" << meta_->name
                 << "': " << duplicate_vector_count << " / "
                 << indexed_vector_count << " (" << duplicate_percentage
                 << "%) duplicate vectors";
  const auto message = message_stream.str();
  if (duplicate_percentage >= kDuplicateWarningThresholdPercentage) {
    LOG(WARNING) << message;
  } else {
    LOG(INFO) << message;
  }
  // Physical inserts may survive a failed operation or transaction rollback.
  // Publish snapshot-local visibility only after all workers succeed.
  hnsw_ids_.swap(ids);
  flat_ids_.swap(empty);
  deleted_cursor_ = deleted_cursor;
  flat_index_offset_ = limit;
  return Status::OK();
}

result<ShowIndexStats> HNSWIndex::Show() const {
  const auto buffered_count =
      local_buffer_enabled_
          ? index_id_accessor_->GetVisibleLimit() - flat_index_offset_.value()
          : 0;
  return ShowIndexStats{GetMeta(), IndexState::kActive, buffered_count};
}

result<OptimizeIndexStats> HNSWIndex::Optimize() {
  OptimizeIndexStats stats;
  if (!local_buffer_enabled_)
    return stats;
  const auto limit = index_id_accessor_->GetVisibleLimit();
  const auto begin = flat_index_offset_.value();
  if (limit <= begin)
    return stats;
  struct OptimizeWorkerState {
    size_t indexed_count = 0;
    size_t skipped_count = 0;
  };
  const auto options = BuildParallelOptions(build_workers_, limit - begin);
  std::vector<OptimizeWorkerState> states(options.max_workers);
  HNSWVecSource source(column_);
  auto status = neug::parallel_for(
      begin, limit, options, [&](size_t offset, size_t worker_id) -> Status {
        const auto id = static_cast<index_id_t>(offset);
        auto& state = states[worker_id];
        if (index_id_accessor_->GetVIDByIndexID(id) == INVALID_VID) {
          ++state.skipped_count;
          return Status::OK();
        }
        const auto* data = source.get_vector(id);
        if (!data)
          return Status::RuntimeError(
              "HNSW bulk append requires an available vector for index ID " +
              std::to_string(id));
        zvec::core_interface::VectorData vector;
        vector.vector = zvec::core_interface::DenseVector{data};
        const auto ret = zvec_index_->add_with_source(vector, id, source);
        if (ret == zvec::core::IndexError_Duplicate) {
          LOG(WARNING) << "Skip existing HNSW index_id during bulk append: "
                       << id;
          ++state.skipped_count;
          return Status::OK();
        }
        if (ret != 0)
          return Status::RuntimeError("HNSW bulk append failed: " +
                                      std::to_string(ret));
        ++state.indexed_count;
        return Status::OK();
      });
  if (!status.ok())
    return tl::unexpected(status);
  for (const auto& state : states) {
    stats.indexed_count += state.indexed_count;
    stats.skipped_count += state.skipped_count;
  }
  flat_index_offset_ = limit;
  hnsw_ids_ |= flat_ids_;
  flat_ids_ = roaring::Roaring();
  stats.changed = true;
  return stats;
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
  cloned->flat_index_ = flat_index_;
  cloned->flat_runtime_file_ = flat_runtime_file_;
  cloned->flat_runtime_path_ = flat_runtime_path_;
  cloned->column_ = column_;
  cloned->build_workers_ = build_workers_;
  cloned->flat_index_offset_ = flat_index_offset_;
  cloned->local_buffer_enabled_ = local_buffer_enabled_;
  cloned->hnsw_ids_ = hnsw_ids_;
  cloned->flat_ids_ = flat_ids_;
  cloned->deleted_cursor_ = deleted_cursor_;
  cloned->zvec_runtime_path_ = zvec_runtime_path_;
  cloned->zvec_runtime_file_ = zvec_runtime_file_;
  cloned->dimension_ = dimension_;
  cloned->m_ = m_;
  cloned->ef_construction_ = ef_construction_;
  cloned->metric_ = metric_;
  return cloned;
}

result<std::vector<SearchCandidate>> HNSWIndex::SearchImpl(
    const IndexQueryParams& params) {
  const auto* p = dynamic_cast<const HNSWIndexQueryParams*>(&params);
  if (!p || !zvec_index_ || !flat_index_ || !column_ || !index_id_accessor_) {
    RETURN_INVALID_ARGUMENT_ERROR(
        "HNSW search requires a bound index and query");
  }
  if (p->topk == 0 || p->ef_search == 0) {
    RETURN_INVALID_ARGUMENT_ERROR("HNSW topk and ef_search must be positive");
  }
  const bool cosine_flat = local_buffer_enabled_ &&
                           metric_ == zvec::core_interface::MetricType::kCosine;
  const auto limit = index_id_accessor_->GetVisibleLimit();
  const auto boundary = local_buffer_enabled_
                            ? std::min(flat_index_offset_.value(), limit)
                            : limit;
  Value query_value = p->target_value;
  // Normalize once for both external indexes. HNSW receives an explicit
  // query hint; the Flat IP path consumes the same normalized buffer.
  const bool normalized_query =
      metric_ == zvec::core_interface::MetricType::kCosine &&
      column_->is_l2_normalized();
  if (normalized_query) {
    auto status =
        VectorNormalizer::ValueNormalize(p->target_value, query_value);
    if (!status.ok())
      return tl::unexpected(status);
  }
  GS_AUTO(target, ConvertDenseValue(meta_->schema.columns[0].property_type,
                                    query_value, dimension_));
  roaring::Roaring scalar_hnsw_ids, scalar_flat_ids;
  const auto* hnsw_ids = &hnsw_ids_;
  const auto* flat_ids = &flat_ids_;
  if (p->use_scalar_filter) {
    // Upstream storage has already established visibility. Partition once,
    // without rechecking tombstones/ranges in each distance callback.
    for (auto vid : p->scalar_filter) {
      const auto id = index_id_accessor_->GetIndexIDByVID(vid);
      if (id == INVALID_INDEX_ID)
        continue;
      (id < boundary ? scalar_hnsw_ids : scalar_flat_ids).add(id);
    }
    hnsw_ids = &scalar_hnsw_ids;
    flat_ids = &scalar_flat_ids;
  }
  auto configure_candidates = [](auto& query_param, const roaring::Roaring& ids,
                                 uint64_t document_count, bool needs_filter) {
    // Compare without floating-point rounding; index IDs are uint32_t.
    if (ids.cardinality() * uint64_t{10} < document_count) {
      query_param.bf_pks = std::make_shared<std::vector<uint64_t>>();
      query_param.bf_pks->reserve(ids.cardinality());
      for (auto id : ids)
        query_param.bf_pks->push_back(id);
    } else if (needs_filter) {
      query_param.filter =
          std::make_shared<zvec::core_interface::IndexFilter>();
      query_param.filter->set([ids_ptr = &ids](uint64_t id) {
        return !ids_ptr->contains(static_cast<index_id_t>(id));
      });
    }
  };
  zvec::core_interface::VectorData query;
  query.vector = zvec::core_interface::DenseVector{target.data()};
  query.query_is_l2_normalized = normalized_query;
  std::vector<SearchCandidate> candidates;
  auto append_results = [&](const zvec::core_interface::SearchResult& result,
                            bool convert_cosine) {
    for (const auto& doc : result.doc_list_) {
      const auto score = convert_cosine ? 1.0f - doc.score() : doc.score();
      if (convert_cosine && p->radius > 0 && score > p->radius)
        continue;
      candidates.push_back({static_cast<index_id_t>(doc.key()), score});
    }
  };
  if (!hnsw_ids->isEmpty()) {
    HNSWVecSource source(column_);
    auto qp = std::make_shared<zvec::core_interface::HNSWQueryParam>();
    qp->topk = p->topk;
    qp->ef_search = p->ef_search;
    qp->prefetch_offset = p->prefetch_offset;
    qp->prefetch_lines = p->prefetch_lines;
    qp->fetch_vector = p->fetch_vector;
    qp->radius = p->radius;
    // Shared physical graphs can contain IDs from newer/aborted snapshots.
    const auto document_count = zvec_index_->get_doc_count();
    configure_candidates(*qp, *hnsw_ids, document_count,
                         hnsw_ids->cardinality() != document_count);
    zvec::core_interface::SearchResult result;
    const auto ret =
        zvec_index_->search_with_source(query, qp, source, &result);
    if (ret != 0) {
      RETURN_ERROR(
          Status::RuntimeError("HNSW search failed: " + std::to_string(ret)));
    }
    append_results(result, false);
  }
  if (!flat_ids->isEmpty()) {
    FlatVecSource source(column_, limit, boundary);
    auto qp = std::make_shared<zvec::core_interface::FlatQueryParam>();
    qp->topk = p->topk;
    qp->fetch_vector = p->fetch_vector;
    qp->radius = cosine_flat ? 0 : p->radius;
    // The source bounds enumeration, including when another snapshot appends.
    const uint64_t document_count = uint64_t{limit} - boundary;
    configure_candidates(*qp, *flat_ids, document_count,
                         flat_ids->cardinality() != document_count);
    zvec::core_interface::SearchResult result;
    const auto ret =
        flat_index_->search_with_source(query, qp, source, &result);
    if (ret != 0) {
      RETURN_ERROR(Status::RuntimeError("External Flat search failed: " +
                                        std::to_string(ret)));
    }
    append_results(result, cosine_flat);
  }
  const bool descending =
      metric_ == zvec::core_interface::MetricType::kInnerProduct;
  std::sort(candidates.begin(), candidates.end(),
            [descending](const SearchCandidate& a, const SearchCandidate& b) {
              if (a.score == b.score)
                return a.index_id < b.index_id;
              return descending ? a.score > b.score : a.score < b.score;
            });
  if (candidates.size() > p->topk)
    candidates.resize(p->topk);
  return candidates;
}

Status HNSWIndex::AppendImpl(index_id_t index_id, const IndexValues& values) {
  if (!zvec_index_ || !flat_index_ || !column_ || !index_id_accessor_) {
    return Status::RuntimeError("HNSW must be open and bound before append");
  }
  if (values.size() != 1 || values[0].IsNull()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSW requires one non-null vector");
  }
  // Both Upsert overloads reach AppendImpl after the column mapping changes.
  ConsumeDeletedIDs();
  // VecColumn already validates and stores the value before index maintenance.
  // Register the actual slot without copying/normalizing the vector again.
  HNSWVecSource source(column_);
  const auto* data = source.get_vector(index_id);
  if (!data)
    return Status::RuntimeError("Missing vector slot during append");
  zvec::core_interface::VectorData vector;
  vector.vector = zvec::core_interface::DenseVector{data};
  auto& index = local_buffer_enabled_ ? flat_index_ : zvec_index_;
  const auto ret = index->add_with_source(vector, index_id, source);
  if (ret != 0) {
    return Status::RuntimeError("Vector index append failed: " +
                                std::to_string(ret));
  }
  if (!local_buffer_enabled_) {
    // Direct appends maintain IDs in order; do not advance over other slots
    // that the column may already have allocated for a pending batch.
    flat_index_offset_ = std::max(flat_index_offset_.value(), index_id + 1);
  }
  (local_buffer_enabled_ ? flat_ids_ : hnsw_ids_).add(index_id);
  return Status::OK();
}

Status HNSWIndex::Upsert(vid_t vid, const IndexValue& new_value) {
  if (new_value.column_id != 0) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSWIndex column id is out of range");
  }
  if (!index_id_accessor_) {
    return Status::InternalError("Index ID accessor is not initialized");
  }
  if (new_value.value.IsNull()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "HNSW index does not support NULL vector values");
  }
  auto index_id = index_id_accessor_->UpsertVID(vid);
  return AppendImpl(index_id, IndexValues{new_value.value});
}

NEUG_REGISTER_MODULE(HNSWIndex);
}  // namespace neug::vector_search_ext
