/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#pragma once

#include <carquet/carquet.h>

#include <memory>

#include "neug/utils/io/stream/output_stream.h"

namespace neug::parquet {

/**
 * Creates a Carquet writer over an existing NeuG output stream.
 *
 * Ownership transfers to Carquet when this function is called. A successful
 * writer close closes and destroys the stream exactly once. Any creation,
 * write, or close failure aborts and destroys it exactly once.
 */
carquet_writer_t* createCarquetWriter(std::unique_ptr<io::OutputStream> output,
                                      const carquet_schema_t* schema,
                                      const carquet_writer_options_t* options,
                                      carquet_error_t* error);

}  // namespace neug::parquet
