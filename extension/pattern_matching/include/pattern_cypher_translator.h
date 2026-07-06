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

#pragma once

#include <string>
#include <string_view>

namespace neug {
namespace pattern_matching {

// Translates the supported NeuG Cypher MATCH subset into the JSON pattern
// format consumed by PATTERN_MATCH and SAMPLED_PATTERN_MATCH. Returns "" when
// the input is syntactically invalid or outside the supported subset.
std::string translate_pattern_cypher_to_json(std::string_view cypher);

}  // namespace pattern_matching
}  // namespace neug
