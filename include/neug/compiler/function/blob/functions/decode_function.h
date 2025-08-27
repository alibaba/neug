/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "neug/compiler/common/types/blob.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/utils/exception/exception.h"
#include "utf8proc_wrapper.h"

namespace gs {
namespace function {

struct Decode {
  static inline void operation(common::blob_t& input,
                               common::ku_string_t& result,
                               common::ValueVector& resultVector) {
    if (utf8proc::Utf8Proc::analyze(
            reinterpret_cast<const char*>(input.value.getData()),
            input.value.len) == utf8proc::UnicodeType::INVALID) {
      THROW_RUNTIME_ERROR(
          "Failure in decode: could not convert blob to UTF8 string, "
          "the blob contained invalid UTF8 characters");
    }
    common::StringVector::addString(&resultVector, result, input.value);
  }
};

}  // namespace function
}  // namespace gs
