/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#pragma once

#include <memory>

#include "neug/execution/common/context.h"
#include "neug/utils/io/read/common/read_state.h"
#include "neug/utils/io/stream/input_stream.h"
#include "neug/utils/result.h"

namespace neug {

class IDataChunkSupplier;

}  // namespace neug

namespace neug::parquet {

result<std::shared_ptr<reader::EntrySchema>> sniffCarquet(
    io::InputStreamFactory inputFactory);

std::shared_ptr<IDataChunkSupplier> createCarquetChunkSupplier(
    const std::shared_ptr<reader::ReadSharedState>& state);

void scanCarquet(const std::shared_ptr<reader::ReadSharedState>& state,
                 execution::Context& output);

}  // namespace neug::parquet
