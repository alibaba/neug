/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#pragma once

#include <carquet/carquet.h>

#include <memory>
#include <string>

#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/result.h"

namespace neug::parquet {

result<std::shared_ptr<::common::DataType>> convertArrowCScalarSchemaType(
    const ArrowSchema& schema, const std::string& path = "column");

result<std::shared_ptr<reader::EntrySchema>> convertArrowCStructSchema(
    const ArrowSchema& schema);

result<std::shared_ptr<reader::EntrySchema>> convertCarquetSchema(
    const carquet_schema_t& schema);

}  // namespace neug::parquet
