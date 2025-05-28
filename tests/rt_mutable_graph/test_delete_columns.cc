#include <iostream>
#include <string>
#include "src/main/nexg_db.h"
#include "src/storages/rt_mutable_graph/file_names.h"
#include "src/storages/rt_mutable_graph/schema.h"

void print_schema(const gs::Schema& schema) {
  std::cout << "Vertex labels: " << schema.vertex_label_num() << "\n";
  for (size_t i = 0; i < schema.vertex_label_num(); ++i) {
    std::cout << "Vertex label: " << schema.get_vertex_label_name(i) << "\n";
    auto properties = schema.get_vertex_properties(i);
    auto prop_names = schema.get_vertex_property_names(i);
    for (size_t j = 0; j < properties.size(); ++j) {
      std::cout << "  Property: " << prop_names[j]
                << ", Type: " << properties[j].ToString() << "\n";
    }
  }
  std::cout << "Edge labels: " << schema.edge_label_num() << "\n";
  for (size_t src_label = 0; src_label < schema.vertex_label_num();
       ++src_label) {
    for (size_t dst_label = 0; dst_label < schema.vertex_label_num();
         ++dst_label) {
      for (size_t edge_label = 0; edge_label < schema.edge_label_num();
           ++edge_label) {
        if (schema.has_edge_label(src_label, dst_label, edge_label)) {
          std::cout << "Edge label: " << schema.get_edge_label_name(edge_label)
                    << " from " << schema.get_vertex_label_name(src_label)
                    << " to " << schema.get_vertex_label_name(dst_label)
                    << "\n";
          auto properties =
              schema.get_edge_properties(src_label, dst_label, edge_label);
          auto prop_names =
              schema.get_edge_property_names(src_label, dst_label, edge_label);
          for (size_t j = 0; j < properties.size(); ++j) {
            std::cout << "  Property: " << prop_names[j]
                      << ", Type: " << properties[j].ToString() << "\n";
          }
        }
      }
    }
  }
}

int main(int argc, char** argv) {
  // Expect 1 args, data path
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <data_path>" << std::endl;
    return 1;
  }

  gs::setup_signal_handler();

  std::string data_path = argv[1];
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  // Test create vertex, add vertex properties, update vertex properties,
  // And delete vertex properties, and add vertex properties again.

  gs::MutablePropertyFragment fragment;
  fragment.Open(data_path, 1);

  {
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Any>> properties;
    std::string default_value = "";
    std::string_view default_value_view = "";
    properties.emplace_back(
        std::make_tuple(gs::PropertyType::Int64(), "id", gs::Any(0)));
    properties.emplace_back(std::make_tuple(
        gs::PropertyType::StringView(), "name", gs::Any(default_value_view)));
    properties.emplace_back(
        std::make_tuple(gs::PropertyType::Int64(), "age", gs::Any(0)));

    fragment.create_vertex_type("person", properties,
                                {"id"});  // primary key names
    print_schema(fragment.schema());
  }

  {
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Any>> properties;
    properties.emplace_back(
        std::make_tuple(gs::PropertyType::Int64(), "height", gs::Any(0)));
    properties.emplace_back(std::make_tuple(gs::PropertyType::StringView(),
                                            "city", gs::Any(std::string(""))));
    fragment.add_vertex_properties("person", properties);  // add new properties
    print_schema(fragment.schema());
  }

  {
    fragment.rename_vertex_properties(
        "person", {{"height", "tallness"}, {"city", "location"}});
    // rename properties
    LOG(INFO) << "After renaming properties:";
    print_schema(fragment.schema());
  }

  {
    fragment.delete_vertex_properties("person", {"age", "location"});
    LOG(INFO) << "After deleting age property:";
    print_schema(fragment.schema());
  }

  {
    std::string_view default_value_view = "";
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Any>> properties;
    properties.emplace_back(std::make_tuple(gs::PropertyType::StringView(),
                                            "country",
                                            gs::Any(default_value_view)));
    properties.emplace_back(
        std::make_tuple(gs::PropertyType::Int64(), "hobby_count", gs::Any(0)));
    fragment.add_vertex_properties("person",
                                   properties);  // add new properties again
    LOG(INFO) << "After adding new properties again:";
    print_schema(fragment.schema());
  }

  // TODO: Load vertices and delete

  return 0;
}