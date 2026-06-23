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

#include "neug/compiler/extension/extension_api.h"
#include "neug/utils/exception/exception.h"

#include "pattern_matching_functions.h"

extern "C" {

// Entry point invoked by the extension loader. Registers the
// pattern_matching table functions with the catalog and publishes the
// extension metadata so SQL callers can invoke them.
void Init() {
  std::cout << "[pattern_matching extension] init called" << std::endl;

  try {
    // Graph cache bootstrap: loads / prepares the in-memory data graph.
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::InitializeGraphFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Exact subgraph matching entry backed by DAF.
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::PatternMatchFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Sampled subgraph matching entry backed by FaSTest.
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::SampledPatternMatchFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Vertex property lookup for matched vertices.
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::GetVertexPropertyFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Edge property lookup for matched edges.
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::GetEdgePropertyFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Persists the prepared graph cache to disk for faster restarts.
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::SaveSampledmatchCheckpointFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    neug::extension::ExtensionAPI::registerExtension(
        neug::extension::ExtensionInfo{
            "pattern_matching",
            "Provides subgraph matching and property access functions. "
            "Functions: CALL INITIALIZE([checkpoint_dir]) - initializes graph data cache, "
            "CALL SAVE_SAMPLEDMATCH_CHECKPOINT(checkpoint_dir) - saves graph cache to files, "
            "CALL PATTERN_MATCH(cypher_text_or_file, limit), "
            "CALL SAMPLED_PATTERN_MATCH(cypher_text_or_file, sample_size), "
            "CALL GET_VERTEX_PROPERTY(vertex_ids_json, vertex_label, prop_names_json), "
            "CALL GET_EDGE_PROPERTY(edge_keys_json, edge_label, prop_names_json). "
            "Both match functions accept mini-Cypher text, mini-Cypher files, "
            "JSON text, or JSON pattern files."});

    std::cout << "[pattern_matching extension] functions registered successfully" << std::endl;
  } catch (const std::exception& e) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[pattern_matching extension] registration failed: " + std::string(e.what()));
  } catch (...) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[pattern_matching extension] registration failed: unknown exception");
  }
}

// Display name surfaced by the extension loader.
const char* Name() { return "PATTERN_MATCHING"; }

}  // extern "C"
