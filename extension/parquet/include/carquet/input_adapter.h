/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#pragma once

#include <carquet/carquet.h>

#include <memory>

#include "neug/utils/io/stream/input_stream.h"

namespace neug::parquet {

/**
 * Opens a Carquet reader over an existing NeuG input stream.
 *
 * Ownership transfers to Carquet when this function is called. The stream is
 * closed and destroyed exactly once when the reader closes or opening fails.
 */
carquet_reader_t* openCarquetReader(std::unique_ptr<io::InputStream> input,
                                    const carquet_reader_options_t* options,
                                    carquet_error_t* error);

}  // namespace neug::parquet
