/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <carquet/carquet.h>

#include <memory>

#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/utils/io/read/common/schema.h"

namespace neug::parquet {

// Compiles a scan filter into conservative row-group decisions. A true result
// only means that the group may match; the shared row filter remains the source
// of exact SQL semantics.
class CarquetRowGroupPruner final {
 public:
  struct Node;

  static std::unique_ptr<CarquetRowGroupPruner> create(
      const carquet_reader_t* reader, const reader::EntrySchema& schema,
      const ::common::Expression* filter);

  ~CarquetRowGroupPruner();

  CarquetRowGroupPruner(const CarquetRowGroupPruner&) = delete;
  CarquetRowGroupPruner& operator=(const CarquetRowGroupPruner&) = delete;

  bool mightMatch(int32_t rowGroup) const;

 private:
  CarquetRowGroupPruner(const carquet_reader_t* reader,
                        std::unique_ptr<Node> root);

  const carquet_reader_t* reader_;
  std::unique_ptr<Node> root_;
};

}  // namespace neug::parquet
