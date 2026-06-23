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
 */

#pragma once

#include <string>
#include <string_view>

namespace neug {
namespace pattern_matching {

// Translates a pattern_matching pattern DSL string into the JSON pattern format
// the existing matcher consumes. Returns the JSON document text on success
// or "" on parse/validation failure (the cause is logged via glog).
//
// The matcher (SampledSubgraphMatcher::PatternJsonText) consumes the result
// directly — no tempfile round-trip.
std::string TranslatePatternDslToJson(std::string_view dsl);

}  // namespace pattern_matching
}  // namespace neug
