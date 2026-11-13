/** Copyright 2026 Alibaba Group Holding Limited.
 * Licensed under the Apache License, Version 2.0.
 */
#pragma once

#include <map>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>
#include "neug/common/types/value.h"
#include "neug/utils/api.h"

namespace neug {
class PropertyGraph;
class ColumnBase;
#ifdef NEUG_BUILD_TEST
// Fail the next N reservations on this thread, without modifying the ledger.
NEUG_API void SetInsertReservationFailuresForTest(int count);
#endif

// Owns only logical identities and counts, never references the discarded
// transaction's graph or redo. Reservations are tracked separately per
// database.
class NEUG_API InsertCapacityRequest {
 public:
  // Append counts are additive across outstanding transactions. LIST/ARRAY
  // have one child requirement; STRUCT has one per field, recursively.
  struct ColumnNeed {
    size_t count = 0;
    size_t bytes = 0;
    std::vector<ColumnNeed> children;
  };
  struct Need {
    size_t rows = 0;
    size_t key_bytes = 0;
    std::vector<ColumnNeed> columns;
    std::vector<DataType> types;
  };
  void AddVertex(const std::string& label, const Value& key,
                 const std::vector<Value>& props);
  void AddEdge(const std::string& src, const std::string& dst,
               const std::string& edge, const std::vector<Value>& props);
  static InsertCapacityRequest FromWal(char* data, size_t length);
  bool Fits(PropertyGraph& graph) const;
  // Exclusive/quiescent access required. All object identities are retained.
  void Grow(PropertyGraph& graph) const;
  bool empty() const { return vertices_.empty() && edges_.empty(); }

 private:
  friend class InsertCapacityReservations;
  void ValidateGrowthSupport(PropertyGraph& graph) const;
  void Merge(const InsertCapacityRequest& other);
  void Subtract(const InsertCapacityRequest& other) noexcept;
  static void Add(Need& need, const std::vector<Value>& props);
  static void AddValue(ColumnNeed& need, const Value& value);
  static void MergeColumn(ColumnNeed& target, const ColumnNeed& source);
  static void SubtractColumn(ColumnNeed& target,
                             const ColumnNeed& source) noexcept;
  static bool ColumnFits(ColumnBase& col, size_t rows, const ColumnNeed& need);
  static void GrowColumn(ColumnBase& col, size_t rows, const ColumnNeed& need);
  std::map<std::string, Need> vertices_;
  std::map<std::tuple<std::string, std::string, std::string>, Need> edges_;
};
// Per database, short critical section only; never held across execution,
// WAL append, admission waits or resize. Applied rows may temporarily be
// counted twice, deliberately preferring an early retry to overcommit.
class NEUG_API InsertCapacityReservations {
 public:
  bool TryReserve(PropertyGraph& graph, const InsertCapacityRequest& request,
                  InsertCapacityRequest& growth_request);
  void Release(const InsertCapacityRequest& request) noexcept;

 private:
  std::mutex mutex_;
  InsertCapacityRequest outstanding_;
};
}  // namespace neug
