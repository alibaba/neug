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
   std::cout << "[sampled_match extension] init called" << std::endl;
 
  try {
    // 注册 INITIALIZE 函数 (图初始化)
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::InitializeGraphFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
    
    // 注册 SAMPLED_MATCH 函数 (NeugCallFunction with graph access)
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::SampledMatchFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
    
    // 注册 GET_VERTEX_PROPERTY 函数
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::GetVertexPropertyFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
    
    // 注册 GET_EDGE_PROPERTY 函数
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::GetEdgePropertyFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // 注册 extension 元数据
    neug::extension::ExtensionAPI::registerExtension(
        neug::extension::ExtensionInfo{
            "sampled_match",
            "Provides subgraph matching and property access functions. "
            "Functions: CALL INITIALIZE() - initializes graph data cache, "
            "CALL SAMPLED_MATCH(pattern_file, sample_size), "
            "CALL GET_VERTEX_PROPERTY(vertex_ids_json, vertex_label, prop_names_json), "
            "CALL GET_EDGE_PROPERTY(edge_keys_json, edge_label, prop_names_json). "
            "SAMPLED_MATCH returns estimated embedding count and sampled results with edge keys."});

    std::cout << "[sampled_match extension] functions registered successfully" << std::endl;
   } catch (const std::exception& e) {
     THROW_EXCEPTION_WITH_FILE_LINE(
         "[sampled_match extension] registration failed: " + std::string(e.what()));
   } catch (...) {
     THROW_EXCEPTION_WITH_FILE_LINE(
         "[sampled_match extension] registration failed: unknown exception");
   }
 }
 
 /**
  * @brief Extension name function
  *
  * Returns the display name of this extension.
  */
 const char* Name() { return "SAMPLED_MATCH"; }
 
 }  // extern "C"
 