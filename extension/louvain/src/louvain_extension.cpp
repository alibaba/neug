/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "glog/logging.h"
#include "neug/compiler/extension/extension_api.h"
#include "neug/utils/exception/exception.h"

#include "louvain_function.h"

extern "C" {

void Init() {
  LOG(INFO) << "[louvain extension] init called";

  try {
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::LouvainFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    neug::extension::ExtensionAPI::registerExtension(
        neug::extension::ExtensionInfo{
            "louvain",
            "Louvain community detection algorithm for graph clustering."});

    LOG(INFO) << "[louvain extension] functions registered successfully";
  } catch (const std::exception& e) {
    THROW_EXCEPTION_WITH_FILE_LINE("[louvain extension] registration failed: " +
                                   std::string(e.what()));
  } catch (...) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[louvain extension] registration failed: unknown exception");
  }
}

const char* Name() { return "LOUVAIN"; }

}  // extern "C"
