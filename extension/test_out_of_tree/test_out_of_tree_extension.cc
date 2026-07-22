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

#include "neug/compiler/extension/extension_api.h"

extern "C" {

void Init() {
  neug::extension::ExtensionAPI::registerExtension(
      neug::extension::ExtensionInfo{
          "test_out_of_tree",
          "Test extension for validating out-of-tree extension builds."});
}

const char* Name() { return "TEST_OUT_OF_TREE"; }

}  // extern "C"
