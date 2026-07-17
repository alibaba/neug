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

#include <gtest/gtest.h>

#include "neug/storages/graph/modification_tracker.h"

namespace neug {

TEST(ModificationTrackerTest, TracksChangesSincePersistedRevision) {
  ModificationTracker tracker;
  EXPECT_FALSE(tracker.HasChanges());

  tracker.MarkModified();
  const auto revision = tracker.CurrentRevision();
  EXPECT_TRUE(tracker.HasChanges());

  tracker.MarkPersisted(revision);
  EXPECT_FALSE(tracker.HasChanges());

  tracker.MarkModified();
  tracker.MarkPersisted(revision);
  EXPECT_TRUE(tracker.HasChanges());
}

TEST(ModificationTrackerTest, CopiesAndSwapsRevisionState) {
  ModificationTracker changed;
  changed.MarkModified();

  ModificationTracker clean;
  clean.CopyFrom(changed);
  EXPECT_TRUE(clean.HasChanges());

  ModificationTracker persisted;
  persisted.MarkModified();
  persisted.MarkPersisted(persisted.CurrentRevision());
  clean.Swap(persisted);

  EXPECT_FALSE(clean.HasChanges());
  EXPECT_TRUE(persisted.HasChanges());
}

}  // namespace neug
