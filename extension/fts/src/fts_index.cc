/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include "fts_index.h"

#include <sqlite3.h>

#include <filesystem>
#include <limits>
#include <memory>
#include <regex>
#include <string>
#include <unordered_set>
#include <utility>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/storages/index/index_id_accessor.h"
#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/property/column.h"

namespace neug::fts_ext {
namespace {

// Help users locate the character that caused an FTS tokenizer parsing error.
std::string EnhanceFTS5Error(const std::string& query,
                             const std::string& error) {
  static const std::regex kSyntaxErrorPattern(
      R"fts(fts5: syntax error near "([^"]+)")fts");
  std::smatch match;
  if (!std::regex_search(error, match, kSyntaxErrorPattern) ||
      match.size() < 2 || match.str(1).empty()) {
    return error;
  }

  const auto character = match.str(1);
  const auto position = query.find(character);
  std::string enhanced = error +
                         ". FTS5 query cannot parse the unquoted character '" +
                         character + "'";
  if (position != std::string::npos) {
    enhanced += " at position " + std::to_string(position);
  }
  enhanced +=
      "; wrap the query in double quotes to form a phrase or escape the "
      "character";
  return enhanced;
}

// Escape SQLite string literals to prevent SQL injection.
std::string QuoteSQLiteLiteral(const std::string& value) {
  std::string quoted{"'"};
  for (char character : value) {
    if (character == '\'') {
      quoted += '\'';
    }
    quoted += character;
  }
  quoted += '\'';
  return quoted;
}

std::string QuoteSQLiteIdentifier(const std::string& value) {
  std::string result = "\"";
  for (char ch : value) {
    result += ch == '\"' ? "\"\"" : std::string(1, ch);
  }
  return result + "\"";
}

std::string FTSPhysicalColumnName(size_t column_index) {
  return "c" + std::to_string(column_index);
}

std::string AddFTS5ColumnFilter(const std::vector<std::string>& column_names,
                                const std::string& query_string) {
  if (column_names.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "FTS search requires at least one property name");
  }
  std::string filter;
  if (column_names.size() == 1) {
    filter = QuoteSQLiteIdentifier(column_names.front());
  } else {
    filter = "{";
    for (size_t i = 0; i < column_names.size(); ++i) {
      if (i != 0) {
        filter += " ";
      }
      filter += QuoteSQLiteIdentifier(column_names[i]);
    }
    filter += "}";
  }
  return filter + " : (" + query_string + ")";
}

}  // namespace

FTSDumpContainer::FTSDumpContainer(
    std::shared_ptr<SQLiteConnection> read_connection,
    std::shared_ptr<SQLiteConnection> write_connection,
    std::string runtime_path)
    : read_connection_(std::move(read_connection)),
      write_connection_(std::move(write_connection)),
      runtime_path_(std::move(runtime_path)) {}

void FTSDumpContainer::Sync() {
  if (!read_connection_ || !read_connection_->IsOpen() || !write_connection_ ||
      !write_connection_->IsOpen()) {
    THROW_RUNTIME_ERROR("FTSDumpContainer: connections are not open");
  }
  read_connection_->Close();
  write_connection_->Execute("PRAGMA wal_checkpoint(TRUNCATE);");
  write_connection_->Flush();
  write_connection_->Close();
}

void FTSDumpContainer::Dump(const std::string& new_path) {
  Sync();
  std::filesystem::rename(runtime_path_, new_path);
  runtime_path_ = new_path;
}

void FTSDumpContainer::Close() {
  if (read_connection_ && read_connection_->IsOpen()) {
    read_connection_->Close();
  }
  if (write_connection_ && write_connection_->IsOpen()) {
    write_connection_->Close();
  }
}

std::shared_ptr<IDataContainer> FTSDumpContainer::Fork(Checkpoint&,
                                                       MemoryLevel) {
  THROW_NOT_SUPPORTED_EXCEPTION("FTSDumpContainer does not support Fork");
}

FTSIndex::~FTSIndex() = default;

void FTSIndex::ParseOptions() {
  if (!meta_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FTSIndex metadata is not initialized");
  }
  if (meta_->name.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FTSIndex name must not be empty");
  }
  if (!std::regex_match(meta_->name, std::regex("[A-Za-z_][A-Za-z0-9_]*"))) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "FTSIndex name must start with a letter or underscore and contain "
        "only letters, digits, or underscores");
  }
  if (meta_->schema.columns.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FTSIndex requires at least one property");
  }
  for (const auto& column : meta_->schema.columns) {
    if (column.property_type.id() != DataTypeId::kVarchar) {
      THROW_INVALID_ARGUMENT_EXCEPTION("FTSIndex properties must be STRING");
    }
  }

  static const std::unordered_set<std::string> kKnownOptions = {
      "tokenizer", "prefix", "jieba_mode", "jieba_dict"};
  for (const auto& [name, value] : meta_->options) {
    if (!kKnownOptions.contains(name)) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Unsupported FTSIndex option: " + name);
    }
  }

  if (auto option = meta_->options.find("prefix");
      option != meta_->options.end()) {
    prefix_ = option->second;
  }
  if (auto option = meta_->options.find("jieba_dict");
      option != meta_->options.end() && !option->second.empty()) {
    std::error_code error;
    auto path = std::filesystem::absolute(option->second, error);
    if (error) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Failed to resolve Jieba user dictionary path " + option->second +
          ": " + error.message());
    }
    option->second = path.lexically_normal().string();
  }
  FTSTokenizerConfig tokenizer_config;
  for (const auto& [name, value] : meta_->options) {
    if (name != "prefix") {
      tokenizer_config.emplace(name, value);
    }
  }
  tokenizer_ = FTSTokenizer::Create(std::move(tokenizer_config));
  table_name_ = "neug_fts_" + meta_->name;
}

void FTSIndex::CreateTable() {
  std::string sql = "CREATE VIRTUAL TABLE " + table_name_ + " USING fts5(";
  for (size_t i = 0; i < meta_->schema.columns.size(); ++i) {
    if (i > 0)
      sql += ", ";
    sql += QuoteSQLiteIdentifier(FTSPhysicalColumnName(i));
  }
  sql += ", content='', tokenize=" +
         QuoteSQLiteLiteral(std::string(tokenizer_->Name()));
  if (!prefix_.empty()) {
    sql += ", prefix=" + QuoteSQLiteLiteral(prefix_);
  }
  sql += ");";
  write_connection_->Execute(sql);
}

void FTSIndex::ValidateExistingTable() {
  auto statement = write_connection_->Prepare(
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1 "
      "AND sql LIKE '%USING fts5(%';");
  statement.BindText(1, table_name_);
  if (statement.Step() != SQLITE_ROW) {
    THROW_RUNTIME_ERROR("FTSIndex persisted FTS5 table is missing: " +
                        table_name_);
  }
}

void FTSIndex::PrepareStatements() {
  std::string column_list;
  std::string placeholders;
  std::string weight_placeholders;
  for (size_t i = 0; i < meta_->schema.columns.size(); ++i) {
    column_list += ", " + QuoteSQLiteIdentifier(FTSPhysicalColumnName(i));
    placeholders += ", ?" + std::to_string(i + 2);
    weight_placeholders += ", ?" + std::to_string(i + 2);
  }
  auto append_sql = "INSERT INTO " + table_name_ + "(rowid" + column_list +
                    ") VALUES (?1" + placeholders + ");";
  auto score = "bm25(" + table_name_ + weight_placeholders + ")";
  auto search_asc_sql = "SELECT rowid, " + score + " AS score FROM " +
                        table_name_ + " WHERE " + table_name_ +
                        " MATCH ?1 ORDER BY score ASC;";
  auto search_desc_sql = "SELECT rowid, " + score + " AS score FROM " +
                         table_name_ + " WHERE " + table_name_ +
                         " MATCH ?1 ORDER BY score DESC;";

  *append_statements_ = write_connection_->Prepare(append_sql);
  *search_asc_statement_ = read_connection_->Prepare(search_asc_sql);
  *search_desc_statement_ = read_connection_->Prepare(search_desc_sql);
}

void FTSIndex::FinalizeStatements() {
  *append_statements_ = SQLiteStatement{};
  *search_asc_statement_ = SQLiteStatement{};
  *search_desc_statement_ = SQLiteStatement{};
}

void FTSIndex::Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                    MemoryLevel level) {
  OpenInternal(ckp, nullptr, descriptor, level);
}

void FTSIndex::Open(Checkpoint& ckp, const CheckpointManifest& manifest,
                    const ModuleDescriptor& descriptor, MemoryLevel level) {
  OpenInternal(ckp, &manifest, descriptor, level);
}

void FTSIndex::OpenInternal(Checkpoint& ckp, const CheckpointManifest* manifest,
                            const ModuleDescriptor& descriptor,
                            MemoryLevel level) {
  StorageIndex::Open(ckp, descriptor, level);
  ParseOptions();

  if (!index_id_accessor_) {
    index_id_accessor_ = std::make_unique<DefaultIndexIDAccessor>();
  }
  ModuleDescriptor accessor_descriptor;
  if (auto accessor_ref = descriptor.get_ref(kAccessorRef)) {
    const auto& resolver = manifest ? *manifest : ckp.manifest();
    const auto* resolved = resolver.FindModule(*accessor_ref);
    if (resolved == nullptr) {
      THROW_RUNTIME_ERROR(
          "FTSIndex::Open: missing index ID accessor descriptor '" +
          *accessor_ref + "'");
    }
    accessor_descriptor = *resolved;
  }
  index_id_accessor_->Open(ckp, accessor_descriptor, level);

  auto index_path = descriptor.get_path(kIndexFilePath);
  const bool has_persisted_path = index_path && !index_path->empty();
  if (has_persisted_path) {
    std::error_code error;
    const bool exists = std::filesystem::exists(*index_path, error);
    if (error) {
      THROW_CHECKPOINT_EXCEPTION("Failed to inspect persisted FTS index file " +
                                 *index_path + ": " + error.message());
    }
    if (!exists) {
      THROW_CHECKPOINT_EXCEPTION("Persisted FTS index file is missing: " +
                                 *index_path);
    }
  }

  runtime_file_ = std::make_shared<CheckpointFileManager::RuntimeFileHandle>(
      ckp.CreateRuntimeFile());
  runtime_path_ = runtime_file_->path();
  read_connection_ = std::make_shared<SQLiteConnection>();
  write_connection_ = std::make_shared<SQLiteConnection>();
  search_asc_statement_ = std::make_shared<SQLiteStatement>();
  search_desc_statement_ = std::make_shared<SQLiteStatement>();
  append_statements_ = std::make_shared<SQLiteStatement>();
  try {
    if (has_persisted_path) {
      file_utils::copy_file(*index_path, runtime_path_, true);
    }
    write_connection_->Open(runtime_path_);
    tokenizer_->Register(*write_connection_);
    if (has_persisted_path) {
      ValidateExistingTable();
    } else {
      CreateTable();
    }
    read_connection_->Open(runtime_path_);
    tokenizer_->Register(*read_connection_);
    PrepareStatements();
  } catch (...) {
    FinalizeStatements();
    read_connection_->Close();
    write_connection_->Close();
    std::error_code error;
    std::filesystem::remove(runtime_path_, error);
    runtime_path_.clear();
    throw;
  }
}

void FTSIndex::Dump(Checkpoint& ckp, CheckpointManifest& manifest,
                    const std::string& key) {
  if (key.empty()) {
    THROW_RUNTIME_ERROR("FTSIndex::Dump: module key must not be empty");
  }
  if (!read_connection_->IsOpen() || !write_connection_->IsOpen()) {
    THROW_RUNTIME_ERROR("FTSIndex::Dump: index is not open");
  }

  std::scoped_lock lock(search_asc_statement_->mutex(),
                        search_desc_statement_->mutex(),
                        append_statements_->mutex());
  FinalizeStatements();
  try {
    StorageIndex::Dump(ckp, manifest, key);
    const auto accessor_key = "fts_accessor_" + meta_->name;
    index_id_accessor_->Dump(ckp, manifest, accessor_key);
    auto* accessor_descriptor = manifest.FindMutableModule(accessor_key);
    if (accessor_descriptor == nullptr) {
      THROW_RUNTIME_ERROR(
          "FTSIndex::Dump: index ID accessor did not write module descriptor "
          "for '" +
          accessor_key + "'");
    }
    accessor_descriptor->mark_as_referenced_module();
    auto* descriptor = manifest.FindMutableModule(key);
    if (descriptor == nullptr) {
      THROW_RUNTIME_ERROR(
          "FTSIndex::Dump: StorageIndex did not write module descriptor for '" +
          key + "'");
    }
    descriptor->set_ref(kAccessorRef, accessor_key);

    FTSDumpContainer container(read_connection_, write_connection_,
                               runtime_path_);
    auto persisted_path = ckp.Commit(container);
    descriptor->set_path(kIndexFilePath, persisted_path);
    runtime_file_.reset();
    runtime_path_.clear();
  } catch (...) {
    FinalizeStatements();
    throw;
  }
}

void FTSIndex::Detach(Checkpoint& ckp, MemoryLevel level) {
  if (index_id_accessor_) {
    index_id_accessor_->Detach(ckp, level);
  }
}

std::unique_ptr<Module> FTSIndex::Clone() const {
  auto cloned = std::make_unique<FTSIndex>();
  if (meta_) {
    cloned->meta_ = std::make_unique<IndexMeta>(*meta_);
  }
  if (index_id_accessor_) {
    auto accessor = index_id_accessor_->Clone();
    cloned->index_id_accessor_.reset(
        static_cast<IndexIDAccessor*>(accessor.release()));
  }
  cloned->read_connection_ = read_connection_;
  cloned->write_connection_ = write_connection_;
  cloned->search_asc_statement_ = search_asc_statement_;
  cloned->search_desc_statement_ = search_desc_statement_;
  cloned->append_statements_ = append_statements_;
  cloned->runtime_file_ = runtime_file_;
  cloned->runtime_path_ = runtime_path_;
  cloned->table_name_ = table_name_;
  cloned->tokenizer_ = tokenizer_;
  cloned->prefix_ = prefix_;
  cloned->bound_columns_ = bound_columns_;
  return cloned;
}

Status FTSIndex::Rebind(const IndexBindContext& context) {
  if (!meta_ || context.columns.size() != meta_->schema.columns.size()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "FTSIndex binding column count does not match metadata");
  }
  if (std::any_of(context.columns.begin(), context.columns.end(),
                  [](const ColumnBase* column) { return column == nullptr; })) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "FTSIndex binding contains a null property column");
  }
  bound_columns_ = context.columns;
  return Status::OK();
}

Status FTSIndex::BulkBuild(const VertexSet& vertices) {
  if (bound_columns_.empty()) {
    return Status::RuntimeError("FTSIndex is not bound");
  }
  if (!write_connection_->IsOpen()) {
    return Status::RuntimeError("FTSIndex must be open before bulk build");
  }
  try {
    write_connection_->Execute("BEGIN TRANSACTION;");
    for (vid_t vid : vertices) {
      IndexValues values;
      values.reserve(bound_columns_.size());
      for (const auto* column : bound_columns_) {
        values.emplace_back(column->get_any(vid));
      }
      auto status = StorageIndex::Upsert(vid, values);
      if (!status.ok()) {
        write_connection_->Execute("ROLLBACK;");
        return status;
      }
    }
    write_connection_->Execute("COMMIT;");
    return Status::OK();
  } catch (const std::exception& error) {
    if (write_connection_->InTransaction()) {
      write_connection_->Execute("ROLLBACK;");
    }
    return Status::RuntimeError("FTSIndex bulk build failed: " +
                                std::string(error.what()));
  }
}

Status FTSIndex::Upsert(vid_t vid, const IndexValue& new_value) {
  if (!index_id_accessor_) {
    return Status::InternalError("Index ID accessor is not initialized");
  }
  if (!meta_ || bound_columns_.size() != meta_->schema.columns.size()) {
    return Status::InternalError("FTSIndex is not bound");
  }
  if (new_value.column_id >= bound_columns_.size()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "FTSIndex column id is out of range");
  }

  IndexValues values;
  values.reserve(bound_columns_.size());
  for (const auto* column : bound_columns_) {
    values.emplace_back(column->get_any(vid));
  }
  values[new_value.column_id] = new_value.value;
  if (std::all_of(values.begin(), values.end(),
                  [](const Value& value) { return value.IsNull(); })) {
    return Delete(vid);
  }
  auto index_id = index_id_accessor_->UpsertVID(vid);
  return AppendImpl(index_id, values);
}

result<std::vector<SearchCandidate>> FTSIndex::SearchImpl(
    const IndexQueryParams& params) {
  const auto* fts_params = dynamic_cast<const FTSQueryParams*>(&params);
  if (!fts_params) {
    RETURN_INVALID_ARGUMENT_ERROR("FTSIndex::Search requires FTSQueryParams");
  }
  if (fts_params->limit && *fts_params->limit == 0) {
    return std::vector<SearchCandidate>{};
  }
  if (!index_id_accessor_) {
    RETURN_ERROR(
        Status::RuntimeError("FTS index ID accessor is not initialized"));
  }
  if (!read_connection_->IsOpen()) {
    RETURN_ERROR(Status::RuntimeError("FTS index is not open"));
  }
  try {
    // Convert the streaming scalar filter to a hash set for fast filtering.
    std::unordered_set<index_id_t> allowed;
    if (fts_params->use_scalar_filter) {
      allowed.reserve(fts_params->scalar_filter.size());
      for (auto vid : fts_params->scalar_filter) {
        auto index_id = index_id_accessor_->GetIndexIDByVID(vid);
        if (index_id != INVALID_INDEX_ID) {
          allowed.insert(index_id);
        }
      }
    }

    const auto& search_statement =
        fts_params->order == FTSScoreOrder::kAscending ? search_asc_statement_
                                                       : search_desc_statement_;
    std::lock_guard lock(search_statement->mutex());
    search_statement->Reset();
    std::vector<std::string> physical_column_names;
    physical_column_names.reserve(fts_params->property_names.size());
    for (const auto& property_name : fts_params->property_names) {
      const auto found =
          std::find_if(meta_->schema.columns.begin(),
                       meta_->schema.columns.end(), [&](const auto& column) {
                         return column.property_name == property_name;
                       });
      if (found == meta_->schema.columns.end()) {
        RETURN_INVALID_ARGUMENT_ERROR("FTS property is not indexed: " +
                                      property_name);
      }
      physical_column_names.push_back(FTSPhysicalColumnName(static_cast<size_t>(
          std::distance(meta_->schema.columns.begin(), found))));
    }
    const auto filtered_query =
        AddFTS5ColumnFilter(physical_column_names, fts_params->query_string);
    search_statement->BindText(1, filtered_query);
    for (size_t i = 0; i < meta_->schema.columns.size(); ++i) {
      const auto& property_name = meta_->schema.columns[i].property_name;
      auto weight = fts_params->weights.find(property_name);
      search_statement->BindDouble(
          static_cast<int>(i + 2),
          weight == fts_params->weights.end() ? 0.0 : weight->second);
    }

    std::vector<SearchCandidate> results;
    while (search_statement->Step() == SQLITE_ROW) {
      auto rowid = search_statement->ColumnInt64(0);
      if (rowid < 0 || static_cast<uint64_t>(rowid) >
                           std::numeric_limits<index_id_t>::max()) {
        continue;
      }
      const auto index_id = static_cast<index_id_t>(rowid);
      // Apply the scalar filter.
      if (fts_params->use_scalar_filter && !allowed.contains(index_id)) {
        continue;
      }
      // Apply the MVCC visibility filter.
      if (index_id_accessor_->GetVIDByIndexID(index_id) == INVALID_VID) {
        continue;
      }
      results.push_back(
          SearchCandidate{index_id, search_statement->ColumnDouble(1)});
      if (fts_params->limit &&
          static_cast<uint64_t>(results.size()) >= *fts_params->limit) {
        break;
      }
    }
    return results;
  } catch (const std::exception& error) {
    RETURN_ERROR(Status::RuntimeError(
        "FTS query failed: " +
        EnhanceFTS5Error(fts_params->query_string, error.what())));
  }
}

Status FTSIndex::AppendImpl(index_id_t index_id, const IndexValues& values) {
  for (const auto& value : values) {
    if (!value.IsNull() && value.type().id() != DataTypeId::kVarchar) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "FTS values must be NULL or STRINGs");
    }
  }
  if (!write_connection_->IsOpen()) {
    return Status::RuntimeError("FTSIndex must be open before append");
  }
  try {
    std::lock_guard lock(append_statements_->mutex());
    auto& statement = *append_statements_;
    statement.Reset();
    statement.BindInt64(1, index_id);
    for (size_t i = 0; i < values.size(); ++i) {
      if (values[i].IsNull()) {
        statement.BindNull(static_cast<int>(i + 2));
      } else {
        statement.BindText(static_cast<int>(i + 2),
                           values[i].GetValue<std::string>());
      }
    }
    statement.Step();
    return Status::OK();
  } catch (const std::exception& error) {
    return Status::RuntimeError("FTSIndex append failed: " +
                                std::string(error.what()));
  }
}

NEUG_REGISTER_MODULE(FTSIndex);

}  // namespace neug::fts_ext
