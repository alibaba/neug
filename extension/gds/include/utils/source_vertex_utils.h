#pragma once

#include <cstdlib>
#include <stdexcept>
#include <string>

#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace gds {

vid_t parse_source_vertex(const StorageReadInterface& graph,
                          label_t vertex_label, const std::string& source_str);

}  // namespace gds
}  // namespace neug
