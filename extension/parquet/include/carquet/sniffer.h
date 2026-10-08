/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#pragma once

#include <utility>

#include "neug/utils/io/read/common/sniffer.h"
#include "neug/utils/io/stream/input_stream.h"

namespace neug::parquet {

class CarquetSniffer final : public reader::Sniffer {
 public:
  explicit CarquetSniffer(io::InputStreamFactory inputFactory)
      : inputFactory_(std::move(inputFactory)) {}

  result<std::shared_ptr<reader::EntrySchema>> sniff() override;

 private:
  io::InputStreamFactory inputFactory_;
};

}  // namespace neug::parquet
