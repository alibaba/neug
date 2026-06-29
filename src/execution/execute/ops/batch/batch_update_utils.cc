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

#include "neug/execution/execute/ops/batch/batch_update_utils.h"

#include <glob.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <stddef.h>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <tuple>
#include "neug/utils/exception/exception.h"

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types/value.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/string_utils.h"

namespace neug {

namespace execution {

namespace ops {

bool check_csv_import_options(
    const std::unordered_map<std::string, std::string>& options) {
  std::unordered_set<std::string> valid_keys = {
      CSV_DELIMITER_KEY,    CSV_DELIM_KEY,  CSV_HEADER_KEY, CSV_QUOTE_KEY,
      CSV_DOUBLE_QUOTE_KEY, CSV_ESCAPE_KEY, CSV_SKIP_KEY,   CSV_PARALLEL_KEY};
  int32_t delim_count = 0;
  for (const auto& [key, value] : options) {
    if (valid_keys.find(key) == valid_keys.end()) {
      LOG(ERROR) << "\"" << key << "\" is not a valid parameter";
      return false;
    }
    if (key == CSV_DELIMITER_KEY || key == CSV_DELIM_KEY) {
      delim_count++;
    }
  }
  if (delim_count >= 2) {
    LOG(ERROR) << "Too many \"DELIMITER\" parameters";
  }
  return true;
}

void add_member(rapidjson::Value& object,
                rapidjson::Document::AllocatorType& allocator,
                const std::string& key, const execution::Value& value) {
  if (value.type().id() == DataTypeId::kBoolean) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<bool>(), allocator);
  } else if (value.type().id() == DataTypeId::kInt32) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<int32_t>(), allocator);
  } else if (value.type().id() == DataTypeId::kUInt32) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<uint32_t>(), allocator);
  } else if (value.type().id() == DataTypeId::kInt64) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<int64_t>(), allocator);
  } else if (value.type().id() == DataTypeId::kUInt64) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<uint64_t>(), allocator);
  } else if (value.type().id() == DataTypeId::kFloat) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<float>(), allocator);
  } else if (value.type().id() == DataTypeId::kDouble) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<double>(), allocator);
  } else if (value.type().id() == DataTypeId::kDate) {
    std::string date = value.GetValue<execution::date_t>().to_string();
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     rapidjson::Value(date.c_str(), allocator).Move(),
                     allocator);
  } else if (value.type().id() == DataTypeId::kTimestampMs) {
    std::string date_time =
        value.GetValue<execution::timestamp_ms_t>().to_string();
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     rapidjson::Value(date_time.c_str(), allocator).Move(),
                     allocator);
  } else if (value.type().id() == DataTypeId::kVarchar) {
    rapidjson::Value valueVal;
    const std::string& str_value = execution::StringValue::Get(value);
    valueVal.SetString(str_value.data(), str_value.size(), allocator);
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(), valueVal,
                     allocator);
  } else if (value.type().id() == DataTypeId::kInterval) {
    std::string interval_str =
        value.GetValue<execution::interval_t>().to_string();
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     rapidjson::Value(interval_str.c_str(), allocator).Move(),
                     allocator);
  } else {
    THROW_RUNTIME_ERROR("Unsupported property type for key: " + key);
  }
}

void add_prop_member(rapidjson::Value& object,
                     rapidjson::Document::AllocatorType& allocator,
                     const std::string& key, const execution::Value& value) {
  if (value.type().id() == DataTypeId::kInt32) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<int32_t>(), allocator);
  } else if (value.type().id() == DataTypeId::kUInt32) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<uint32_t>(), allocator);
  } else if (value.type().id() == DataTypeId::kInt64) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<int64_t>(), allocator);
  } else if (value.type().id() == DataTypeId::kUInt64) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<uint64_t>(), allocator);
  } else if (value.type().id() == DataTypeId::kFloat) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<float>(), allocator);
  } else if (value.type().id() == DataTypeId::kDouble) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<double>(), allocator);
  } else if (value.type().id() == DataTypeId::kDate) {
    std::string date = value.GetValue<execution::date_t>().to_string();
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     rapidjson::Value(date.c_str(), allocator).Move(),
                     allocator);
  } else if (value.type().id() == DataTypeId::kTimestampMs) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.GetValue<execution::timestamp_ms_t>().milli_second,
                     allocator);
  } else if (value.type().id() == DataTypeId::kInterval) {
    std::string interval_str =
        value.GetValue<execution::interval_t>().to_string();
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     rapidjson::Value(interval_str.c_str(), allocator).Move(),
                     allocator);
  } else if (value.type().id() == DataTypeId::kVarchar) {
    rapidjson::Value valueVal;
    const std::string& str_value = execution::StringValue::Get(value);
    valueVal.SetString(str_value.data(), str_value.size(), allocator);
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(), valueVal,
                     allocator);
  } else {
    THROW_NOT_IMPLEMENTED_EXCEPTION("Unsupported property type for key: " +
                                    key);
  }
}

rapidjson::Value build_vertex_object(
    label_t label, vid_t vid, const StorageReadInterface& graph,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value vertex_object(rapidjson::kObjectType);
  std::string internal_id_key = "_ID";
  std::string encoded_id_str =
      std::to_string(label) + ":" + std::to_string(vid);
  execution::Value encoded_id = execution::Value::STRING(encoded_id_str);
  add_member(vertex_object, allocator, internal_id_key, encoded_id);
  std::string internal_label_key = "_LABEL";
  std::string label_name_str = graph.schema().get_vertex_label_name(label);
  execution::Value label_name = execution::Value::STRING(label_name_str);
  add_member(vertex_object, allocator, internal_label_key, label_name);
  std::string primary_key = graph.schema().get_vertex_primary_key_name(label);
  add_member(vertex_object, allocator, primary_key,
             graph.GetVertexId(label, vid));
  auto property_names = graph.schema().get_vertex_property_names(label);
  for (size_t i = 0; i < property_names.size(); i++) {
    add_member(vertex_object, allocator, property_names[i],
               graph.GetVertexProperty(label, vid, i));
  }
  return vertex_object;
}

std::string vertex_to_json_string(label_t label, vid_t vid,
                                  const StorageReadInterface& graph) {
  rapidjson::Document doc;
  auto& allocator = doc.GetAllocator();
  rapidjson::Value vertex_object =
      build_vertex_object(label, vid, graph, allocator);
  vertex_object.Swap(doc);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

rapidjson::Value build_edge_object(
    const EdgeRecord& edge, const StorageReadInterface& graph,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value edge_object(rapidjson::kObjectType);
  label_t src_label = edge.label.src_label;
  label_t dst_label = edge.label.dst_label;
  label_t edge_label = edge.label.edge_label;
  std::string internal_src_id = "_SRC";
  std::string encoded_src_id_str =
      std::to_string(src_label) + ":" + std::to_string(edge.src);
  execution::Value encoded_src_id =
      execution::Value::STRING(encoded_src_id_str);
  add_member(edge_object, allocator, internal_src_id, encoded_src_id);

  std::string internal_dst_id = "_DST";
  std::string encoded_dst_id_str =
      std::to_string(dst_label) + ":" + std::to_string(edge.dst);
  execution::Value encoded_dst_id =
      execution::Value::STRING(encoded_dst_id_str);
  add_member(edge_object, allocator, internal_dst_id, encoded_dst_id);

  std::string internal_src_label_key = "_SRC_LABEL";
  execution::Value src_label_name =
      execution::Value::STRING(graph.schema().get_vertex_label_name(src_label));
  add_member(edge_object, allocator, internal_src_label_key, src_label_name);

  std::string internal_dst_label_key = "_DST_LABEL";
  execution::Value dst_label_name =
      execution::Value::STRING(graph.schema().get_vertex_label_name(dst_label));
  add_member(edge_object, allocator, internal_dst_label_key, dst_label_name);

  std::string internal_label_key = "_LABEL";
  execution::Value edge_label_name =
      execution::Value::STRING(graph.schema().get_edge_label_name(edge_label));
  add_member(edge_object, allocator, internal_label_key, edge_label_name);

  auto property_names =
      graph.schema().get_edge_property_names(src_label, dst_label, edge_label);
  for (size_t i = 0; i < property_names.size(); i++) {
    auto ed_accessor =
        graph.GetEdgeDataAccessor(src_label, dst_label, edge_label, i);
    add_prop_member(edge_object, allocator, property_names[i],
                    ed_accessor.get_data_from_ptr(edge.prop));
  }
  return edge_object;
}

std::string edge_to_json_string(const EdgeRecord& edge,
                                const StorageReadInterface& graph) {
  rapidjson::Document doc;
  auto& allocator = doc.GetAllocator();
  auto edge_object = build_edge_object(edge, graph, allocator);
  edge_object.Swap(doc);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

std::string path_to_json_string(Path& path, const StorageReadInterface& graph) {
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();
  rapidjson::Value vertex_array(rapidjson::kArrayType);
  rapidjson::Value edge_array(rapidjson::kArrayType);
  auto path_vertices = path.nodes();
  auto path_edges = path.relationships();
  for (size_t i = 0; i < path_vertices.size(); i++) {
    auto vertex_object = build_vertex_object(
        path_vertices[i].label_, path_vertices[i].vid_, graph, allocator);
    vertex_array.PushBack(vertex_object, allocator);
    if (i > 0) {
      rapidjson::Value edge_object(rapidjson::kObjectType);
      std::string internal_src_label_key = "_SRC_LABEL";
      execution::Value src_label_name = execution::Value::STRING(
          graph.schema().get_vertex_label_name(path_vertices[i - 1].label_));
      add_member(edge_object, allocator, internal_src_label_key,
                 src_label_name);
      std::string internal_dst_label_key = "_DST_LABEL";
      execution::Value dst_label_name = execution::Value::STRING(
          graph.schema().get_vertex_label_name(path_vertices[i].label_));
      add_member(edge_object, allocator, internal_dst_label_key,
                 dst_label_name);
      std::string internal_label_key = "_LABEL";
      execution::Value edge_label_name =
          execution::Value::STRING(graph.schema().get_edge_label_name(
              path_edges[i - 1].label.edge_label));
      add_member(edge_object, allocator, internal_label_key, edge_label_name);
      edge_array.PushBack(edge_object, allocator);
    }
  }
  doc.AddMember("_nodes", vertex_array, allocator);
  doc.AddMember("_rels", edge_array, allocator);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

/// A supplier that yields pre-projected DataChunks one by one.
class MultiChunkSupplier : public IDataChunkSupplier {
 public:
  explicit MultiChunkSupplier(std::vector<std::shared_ptr<DataChunk>> chunks)
      : chunks_(std::move(chunks)), index_(0) {}

  std::shared_ptr<DataChunk> GetNextChunk() override {
    if (index_ >= chunks_.size())
      return nullptr;
    return chunks_[index_++];
  }

  int64_t RowNum() const override {
    int64_t total = 0;
    for (const auto& chunk : chunks_) {
      total += static_cast<int64_t>(chunk->row_num());
    }
    return total;
  }

 private:
  std::vector<std::shared_ptr<DataChunk>> chunks_;
  size_t index_;
};

std::shared_ptr<IDataChunkSupplier> create_data_chunk_supplier(
    const Context& ctx,
    const std::vector<std::pair<int32_t, std::string>>& prop_mappings) {
  std::vector<std::shared_ptr<DataChunk>> projected_chunks;
  projected_chunks.reserve(ctx.chunk_num());
  for (size_t i = 0; i < ctx.chunk_num(); ++i) {
    const auto& chunk = ctx.chunk(i).chunk();
    auto out_chunk = std::make_shared<DataChunk>();
    for (size_t j = 0; j < prop_mappings.size(); ++j) {
      auto tag_id = prop_mappings[j].first;
      auto column = chunk.get(tag_id);
      if (column == nullptr) {
        THROW_INTERNAL_EXCEPTION("Column not found for tag id: " +
                                 std::to_string(tag_id));
      }
      out_chunk->set(static_cast<int>(j), column);
    }
    projected_chunks.push_back(std::move(out_chunk));
  }
  return std::make_shared<MultiChunkSupplier>(std::move(projected_chunks));
}

std::vector<std::string> match_files_with_pattern(
    const std::string& file_path) {
  std::vector<std::string> result;
  if (file_path.find('*') != std::string::npos ||
      file_path.find('?') != std::string::npos) {
    glob_t glob_result;
    int flags = GLOB_TILDE | GLOB_MARK;
    int ret = glob(file_path.c_str(), flags, nullptr, &glob_result);
    if (ret == 0) {
      for (size_t i = 0; i < glob_result.gl_pathc; ++i) {
        result.push_back(glob_result.gl_pathv[i]);
      }
    }
    globfree(&glob_result);
  } else {
    std::filesystem::path p = std::filesystem::absolute(file_path);
    if (!std::filesystem::exists(p)) {
      THROW_IO_EXCEPTION("Provided path is not a file: " + file_path + ".");
    }
    result.emplace_back(file_path);
  }
  return result;
}

std::vector<std::shared_ptr<IDataChunkSupplier>> create_csv_chunk_suppliers(
    const std::string& file_path, const std::vector<DataType>& column_types,
    const std::unordered_map<std::string, std::string> csv_options) {
  std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers;
  std::vector<std::string> file_paths = match_files_with_pattern(file_path);

  for (auto& path : file_paths) {
    auto config = build_csv_read_config(path, csv_options, column_types);
    suppliers.emplace_back(
        std::make_shared<CSVChunkSupplier>(path, std::move(config)));
  }
  return suppliers;
}

void parse_property_mappings(
    const google::protobuf::RepeatedPtrField<physical::PropertyMapping>&
        property_mappings,
    std::vector<std::pair<int32_t, std::string>>& prop_mappings) {
  for (const auto& mapping : property_mappings) {
    if (mapping.has_property() && mapping.property().has_key()) {
      auto prop_name = mapping.property().key().name();
      if (mapping.data().operators_size() != 1 ||
          !mapping.data().operators(0).has_var()) {
        THROW_INVALID_ARGUMENT_EXCEPTION("Invalid property mapping: " +
                                         prop_name);
      }
      auto tag_id = mapping.data().operators(0).var().tag().id();
      prop_mappings.emplace_back(tag_id, prop_name);
    }
  }
}
}  // namespace ops

}  // namespace execution

}  // namespace neug
