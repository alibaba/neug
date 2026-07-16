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
