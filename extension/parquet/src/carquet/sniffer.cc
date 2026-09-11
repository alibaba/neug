/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "carquet/sniffer.h"

#include <string>

#include "carquet/schema_converter.h"
#include "carquet/input_adapter.h"

namespace neug::parquet {

result<std::shared_ptr<reader::EntrySchema>> CarquetSniffer::sniff() {
  if (!inputFactory_) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Parquet input stream factory is null");
  }
  auto input = inputFactory_();
  if (!input) {
    RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                        "Parquet input stream factory returned null");
  }

  carquet_error_t error = CARQUET_ERROR_INIT;
  std::unique_ptr<carquet_reader_t, decltype(&carquet_reader_close)>
      parquetReader(openCarquetReader(std::move(input), nullptr, &error),
                    carquet_reader_close);
  if (!parquetReader) {
    RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                        "Failed to open Parquet input for schema sniffing: " +
                            std::string(error.message));
  }
  return convertCarquetSchema(*carquet_reader_schema(parquetReader.get()));
}

}  // namespace neug::parquet
