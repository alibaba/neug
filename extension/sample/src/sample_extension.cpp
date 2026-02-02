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

 #include "glog/logging.h"
 #include "neug/compiler/extension/extension_api.h"
 #include "neug/utils/exception/exception.h"
 
 #include "sample_functions.h"
 
 extern "C" {
 
 /**
  * @brief Extension initialization function
  *
  * This function is called when the extension is loaded.
  * It registers the subgraph matching functions with the catalog.
  */
 void Init() {
   LOG(INFO) << "[sample extension] init called";
 
  try {
    // 注册 SAMPLED_MATCH 函数 (NeugCallFunction with graph access)
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::SampledMatchFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // 注册 extension 元数据
    neug::extension::ExtensionAPI::registerExtension(
        neug::extension::ExtensionInfo{
            "sample",
            "Provides subgraph matching functions using FaSTest algorithm. "
            "Functions: CALL SAMPLED_MATCH(pattern_file). "
            "Input: JSON file describing the pattern to match. "
            "Output: Estimated embedding count and sampled results."});

    LOG(INFO) << "[sample extension] functions registered successfully";
   } catch (const std::exception& e) {
     THROW_EXCEPTION_WITH_FILE_LINE(
         "[sample extension] registration failed: " + std::string(e.what()));
   } catch (...) {
     THROW_EXCEPTION_WITH_FILE_LINE(
         "[sample extension] registration failed: unknown exception");
   }
 }
 
 /**
  * @brief Extension name function
  *
  * Returns the display name of this extension.
  */
 const char* Name() { return "SAMPLED_MATCH"; }
 
 }  // extern "C"
 