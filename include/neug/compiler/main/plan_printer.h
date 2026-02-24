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

#include <sstream>
#include <string>

#include "kuzu_fwd.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/profiler.h"

namespace neug {
namespace main {

class OpProfileBox {
 public:
  OpProfileBox(std::string opName, const std::string& paramsName,
               std::vector<std::string> attributes);

  inline std::string getOpName() const { return opName; }

  inline uint32_t getNumParams() const { return paramsNames.size(); }

  std::string getParamsName(uint32_t idx) const;

  std::string getAttribute(uint32_t idx) const;

  inline uint32_t getNumAttributes() const { return attributes.size(); }

  uint32_t getAttributeMaxLen() const;

 private:
  std::string opName;
  std::vector<std::string> paramsNames;
  std::vector<std::string> attributes;
};

}  // namespace main
}  // namespace neug
