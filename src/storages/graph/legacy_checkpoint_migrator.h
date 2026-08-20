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

#include <cstdint>
#include <filesystem>
#include <optional>

#include "neug/storages/checkpoint_manifest.h"

namespace neug {

class Checkpoint;

struct LegacyCheckpointCandidate {
  uint64_t id;
  std::filesystem::path root;
  CheckpointManifest manifest;
};

/** Internal one-shot upgrader for the released checkpoint-N v1 layout. */
class LegacyCheckpointMigrator {
 public:
  static bool HasLegacyDirectories(const std::filesystem::path& database_dir);

  /// Remove strictly named legacy checkpoint-N and checkpoint-N.next
  /// directories, then durably sync the database root.
  static void RemoveLegacyDirectories(
      const std::filesystem::path& database_dir);

  /// Return the highest valid published generation, or nullopt when no legacy
  /// entries exist. Throws when entries exist but none is a complete v1
  /// checkpoint.
  static std::optional<LegacyCheckpointCandidate> FindLatest(
      const std::filesystem::path& database_dir);

  /// Import candidate files into target and install a complete in-memory v2
  /// manifest. The caller publishes target through CheckpointManager.
  static void Import(const LegacyCheckpointCandidate& candidate,
                     Checkpoint& target);
};

}  // namespace neug
