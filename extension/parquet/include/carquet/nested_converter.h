/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#pragma once

#include <carquet/carquet.h>

#include <memory>
#include <string>

#include "neug/common/types/data_chunk.h"
#include "neug/utils/result.h"

namespace neug::parquet {

// Converts scalar, LIST, LARGE_LIST and fixed ARRAY data recursively into
// owned NeuG columns. MAP and general STRUCT fields remain unsupported.
result<std::shared_ptr<IContextColumn>> convertArrowCColumn(
    const ArrowSchema& schema, const ArrowArray& array,
    const std::string& path = "column");

result<std::shared_ptr<DataChunk>> convertArrowCStruct(
    const ArrowSchema& schema, const ArrowArray& array);

}  // namespace neug::parquet
