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

#include "legacy_checkpoint_migrator.h"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>

#ifdef _WIN32
#ifdef GetObject
#undef GetObject
#endif
#endif

#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>

#include "neug/storages/checkpoint.h"
#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/uuid.h"

namespace neug {

namespace {

constexpr std::string_view kLegacyPrefix = "checkpoint-";
constexpr std::string_view kLegacyStagingSuffix = ".next";

struct ParsedLegacyName {
  uint64_t id;
  bool staging;
};

std::optional<ParsedLegacyName> parse_legacy_name(
    const std::filesystem::path& path) {
  std::error_code status_ec;
  if (!std::filesystem::is_directory(
          std::filesystem::symlink_status(path, status_ec)) ||
      status_ec) {
    return std::nullopt;
  }
  const std::string filename = path.filename().string();
  std::string_view name = filename;
  if (!name.starts_with(kLegacyPrefix)) {
    return std::nullopt;
  }
  name.remove_prefix(kLegacyPrefix.size());
  bool staging = false;
  if (name.ends_with(kLegacyStagingSuffix)) {
    staging = true;
    name.remove_suffix(kLegacyStagingSuffix.size());
  }
  if (name.empty()) {
    return std::nullopt;
  }
  uint64_t id = 0;
  const auto [ptr, ec] =
      std::from_chars(name.data(), name.data() + name.size(), id);
  if (ec != std::errc{} || ptr != name.data() + name.size()) {
    return std::nullopt;
  }
  return ParsedLegacyName{id, staging};
}

bool escapes_root(const std::filesystem::path& path) {
  const auto normalized = path.lexically_normal();
  return !normalized.empty() && *normalized.begin() == "..";
}

std::filesystem::path resolve_legacy_path(
    const std::filesystem::path& checkpoint_root, const std::string& value) {
  std::filesystem::path path(value);
  if (!path.is_absolute()) {
    if (escapes_root(path)) {
      THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint path escapes its root: " +
                                 value);
    }
    path = checkpoint_root / path;
  }
  path = path.lexically_normal();
  std::error_code ec;
  if (!std::filesystem::is_regular_file(path, ec) || ec) {
    THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint references missing file: " +
                               path.string());
  }
  return std::filesystem::absolute(path);
}

template <typename AddEntry>
void parse_string_map(const rapidjson::Value& descriptor, const char* field,
                      const std::string& module_key, AddEntry&& add_entry) {
  const auto member = descriptor.FindMember(field);
  if (member == descriptor.MemberEnd()) {
    return;
  }
  if (!member->value.IsObject()) {
    THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint module " + module_key +
                               " has invalid " + std::string(field) +
                               " metadata");
  }
  for (const auto& entry : member->value.GetObject()) {
    if (!entry.value.IsString()) {
      THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint module " + module_key +
                                 " has a non-string " + std::string(field) +
                                 " value");
    }
    add_entry(entry.name.GetString(), entry.value.GetString());
  }
}

ModuleDescriptor parse_v1_module_descriptor(const rapidjson::Value& value,
                                            const std::filesystem::path& root,
                                            const std::string& key) {
  if (!value.IsObject()) {
    THROW_CHECKPOINT_EXCEPTION(
        "Legacy checkpoint contains an invalid module descriptor: " + key);
  }

  ModuleDescriptor descriptor;
  if (!value.HasMember("module_type") || !value["module_type"].IsString()) {
    THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint module " + key +
                               " has no string module_type");
  }
  descriptor.module_type = value["module_type"].GetString();
  if (value.HasMember("required")) {
    if (!value["required"].IsBool()) {
      THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint module " + key +
                                 " has a non-boolean required field");
    }
    descriptor.required = value["required"].GetBool();
  }
  // Some released v1 descriptors are opened directly by their owning graph
  // component and intentionally have no factory type (for example an
  // LFIndexer descriptor). Only non-empty required factory types need to be
  // registered.
  if (descriptor.required && !descriptor.module_type.empty() &&
      ModuleFactory::instance().Create(descriptor.module_type) == nullptr) {
    THROW_CHECKPOINT_EXCEPTION(
        "Legacy checkpoint references unknown required module type " +
        descriptor.module_type + " from module " + key);
  }

  parse_string_map(value, "extra", key,
                   [&](const char* name, const char* extra) {
                     descriptor.set(name, extra);
                   });
  parse_string_map(value, "paths", key,
                   [&](const char* name, const char* value) {
                     std::string path = value;
                     if (!path.empty()) {
                       path = resolve_legacy_path(root, path).string();
                     }
                     descriptor.set_path(name, std::move(path));
                   });
  parse_string_map(value, "refs", key,
                   [&](const char* name, const char* reference) {
                     descriptor.set_ref(name, reference);
                   });
  if (value.HasMember("referenced_module")) {
    if (!value["referenced_module"].IsBool()) {
      THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint module " + key +
                                 " has invalid referenced_module metadata");
    }
    if (value["referenced_module"].GetBool()) {
      descriptor.mark_as_referenced_module();
    }
  }
  return descriptor;
}

CheckpointManifest load_v1_manifest(const std::filesystem::path& root) {
  const auto meta_path = root / "meta";
  std::ifstream input(meta_path);
  if (!input.is_open()) {
    THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint meta is missing: " +
                               meta_path.string());
  }

  rapidjson::IStreamWrapper wrapper(input);
  rapidjson::Document doc;
  doc.ParseStream(wrapper);
  if (doc.HasParseError() || !doc.IsObject()) {
    THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint meta is invalid JSON: " +
                               meta_path.string());
  }
  if (!doc.HasMember("version") || !doc["version"].IsInt()) {
    THROW_CHECKPOINT_EXCEPTION(
        "Legacy checkpoint meta has no integer version: " + meta_path.string());
  }
  if (doc["version"].GetInt() != 1) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported legacy checkpoint meta version " +
        std::to_string(doc["version"].GetInt()) + " in " + meta_path.string());
  }
  if (!doc.HasMember("schema") || !doc["schema"].IsObject()) {
    THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint has no schema: " +
                               meta_path.string());
  }

  CheckpointManifest manifest;
  Schema schema;
  schema.FromJson(doc["schema"].GetObject());
  manifest.SetSchema(schema);

  if (doc.HasMember("modules") && !doc["modules"].IsObject()) {
    THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint modules are invalid: " +
                               meta_path.string());
  }
  if (doc.HasMember("modules")) {
    for (const auto& member : doc["modules"].GetObject()) {
      const std::string key = member.name.GetString();
      manifest.SetModule(key,
                         parse_v1_module_descriptor(member.value, root, key));
    }
  }

  if (doc.HasMember("scalars") && !doc["scalars"].IsObject()) {
    THROW_CHECKPOINT_EXCEPTION("Legacy checkpoint scalars are invalid: " +
                               meta_path.string());
  }
  if (doc.HasMember("scalars")) {
    for (const auto& member : doc["scalars"].GetObject()) {
      if (!member.value.IsString()) {
        THROW_CHECKPOINT_EXCEPTION(
            "Legacy checkpoint contains a non-string scalar: " +
            std::string(member.name.GetString()));
      }
      manifest.SetScalar(member.name.GetString(), member.value.GetString());
    }
  }
  return manifest;
}

void remove_tree(const std::filesystem::path& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    THROW_IO_EXCEPTION("Failed to clean unpublished migration output " +
                       path.string() + ": " + ec.message());
  }
}

void durable_copy(const std::filesystem::path& source,
                  const std::filesystem::path& destination) {
  std::ifstream input(source, std::ios::binary);
  if (!input.is_open()) {
    THROW_IO_EXCEPTION("Failed to open legacy checkpoint file: " +
                       source.string());
  }
  file_utils::AtomicFileWriter writer(destination.string());
  writer.stream() << input.rdbuf();
  if (input.bad()) {
    THROW_IO_EXCEPTION("Failed to read legacy checkpoint file: " +
                       source.string());
  }
  if (writer.Commit() != file_utils::AtomicFileWriter::CommitResult::kDurable) {
    THROW_IO_EXCEPTION("Legacy checkpoint file copy is commit-unknown: " +
                       destination.string());
  }
}

void import_file(const std::filesystem::path& source,
                 const std::filesystem::path& destination) {
  std::error_code ec;
  const bool source_is_symlink = std::filesystem::is_symlink(source, ec);
  if (ec) {
    THROW_IO_EXCEPTION("Failed to inspect legacy checkpoint file " +
                       source.string() + ": " + ec.message());
  }
  if (!source_is_symlink) {
    std::filesystem::create_hard_link(source, destination, ec);
  } else {
    ec = std::make_error_code(std::errc::operation_not_supported);
  }
  if (ec) {
    VLOG(1) << "Legacy checkpoint hardlink failed (" << ec.message()
            << "), falling back to copy for " << source;
    durable_copy(source, destination);
  }
}

std::filesystem::path import_object(const std::filesystem::path& source,
                                    const std::filesystem::path& object_dir) {
  std::filesystem::path destination;
  do {
    destination = object_dir / UUIDGenerator::Generate();
  } while (std::filesystem::exists(destination));
  import_file(source, destination);
  return destination;
}

void import_wal(const std::filesystem::path& source_dir,
                const std::filesystem::path& destination_dir) {
  remove_tree(destination_dir);
  std::error_code ec;
  std::filesystem::create_directories(destination_dir, ec);
  if (ec) {
    THROW_IO_EXCEPTION("Failed to create migrated WAL epoch " +
                       destination_dir.string() + ": " + ec.message());
  }
  if (!std::filesystem::exists(source_dir)) {
    return;
  }
  if (!std::filesystem::is_directory(source_dir)) {
    THROW_CHECKPOINT_EXCEPTION("Legacy WAL path is not a directory: " +
                               source_dir.string());
  }

  for (const auto& entry : std::filesystem::directory_iterator(source_dir)) {
    if (!entry.is_regular_file()) {
      THROW_CHECKPOINT_EXCEPTION("Legacy WAL contains a non-file entry: " +
                                 entry.path().string());
    }
    import_file(entry.path(), destination_dir / entry.path().filename());
  }
}

}  // namespace

bool LegacyCheckpointMigrator::HasLegacyDirectories(
    const std::filesystem::path& database_dir) {
  if (!std::filesystem::is_directory(database_dir)) {
    return false;
  }
  for (const auto& entry : std::filesystem::directory_iterator(database_dir)) {
    if (parse_legacy_name(entry.path()).has_value()) {
      return true;
    }
  }
  return false;
}

void LegacyCheckpointMigrator::RemoveLegacyDirectories(
    const std::filesystem::path& database_dir) {
  bool removed = false;
  for (const auto& entry : std::filesystem::directory_iterator(database_dir)) {
    if (!parse_legacy_name(entry.path()).has_value()) {
      continue;
    }
    std::error_code ec;
    std::filesystem::remove_all(entry.path(), ec);
    if (ec) {
      THROW_IO_EXCEPTION("Failed to remove legacy checkpoint " +
                         entry.path().string() + ": " + ec.message());
    }
    removed = true;
  }
  if (removed && !file_utils::fsync_directory(database_dir.string())) {
    THROW_IO_EXCEPTION(
        "Failed to fsync database directory after removing "
        "legacy checkpoints: " +
        database_dir.string());
  }
}

std::optional<LegacyCheckpointCandidate> LegacyCheckpointMigrator::FindLatest(
    const std::filesystem::path& database_dir) {
  std::vector<std::pair<uint64_t, std::filesystem::path>> candidates;
  bool has_legacy_entries = false;
  for (const auto& entry : std::filesystem::directory_iterator(database_dir)) {
    auto parsed = parse_legacy_name(entry.path());
    if (!parsed.has_value()) {
      continue;
    }
    has_legacy_entries = true;
    if (!parsed->staging) {
      candidates.emplace_back(parsed->id, entry.path());
    }
  }
  if (!has_legacy_entries) {
    return std::nullopt;
  }
  std::sort(
      candidates.begin(), candidates.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.first > rhs.first; });

  std::string last_error = "no published checkpoint-N generation found";
  for (const auto& [id, root] : candidates) {
    try {
      return LegacyCheckpointCandidate{id, root, load_v1_manifest(root)};
    } catch (const std::exception& e) {
      last_error = e.what();
      LOG(WARNING) << "Skipping invalid legacy checkpoint " << root << ": "
                   << e.what();
    }
  }
  THROW_CHECKPOINT_EXCEPTION(
      "No valid legacy checkpoint-N generation can be migrated: " + last_error);
}

void LegacyCheckpointMigrator::Import(
    const LegacyCheckpointCandidate& candidate, Checkpoint& target) {
  if (candidate.id != target.id()) {
    THROW_CHECKPOINT_EXCEPTION("Legacy migration target ID does not match");
  }

  CheckpointManifest manifest = candidate.manifest;
  std::unordered_map<std::string, std::string> imported_objects;
  for (const auto& module : manifest.Modules()) {
    auto* descriptor = manifest.FindMutableModule(module.first);
    for (auto& path : descriptor->mutable_paths()) {
      auto& source = path.second;
      if (source.empty()) {
        continue;
      }
      std::error_code ec;
      const auto canonical_source =
          std::filesystem::weakly_canonical(source, ec).string();
      if (ec) {
        THROW_IO_EXCEPTION("Failed to canonicalize legacy object " + source +
                           ": " + ec.message());
      }
      auto [it, inserted] = imported_objects.try_emplace(canonical_source);
      if (inserted) {
        it->second = import_object(source, target.object_dir_)
                         .lexically_normal()
                         .string();
      }
      source = it->second;
    }
  }

  import_wal(candidate.root / "wal", target.wal_dir());
  target.SetManifest(std::move(manifest));
}

}  // namespace neug
