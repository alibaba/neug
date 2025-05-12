#include <iostream>
#include <string>
#include "src/main/nexg_db.h"
#include "src/storages/rt_mutable_graph/file_names.h"
#include "src/storages/rt_mutable_graph/schema.h"

gs::StorageStrategy StringToStorageStrategy(const std::string& str) {
  if (str == "None") {
    return gs::StorageStrategy::kNone;
  } else if (str == "Mem") {
    return gs::StorageStrategy::kMem;
  } else if (str == "Disk") {
    return gs::StorageStrategy::kDisk;
  } else {
    return gs::StorageStrategy::kMem;
  }
}

void push_strategies(std::vector<gs::StorageStrategy>& strategies) {
  for (int i = 0; i < 10; ++i) {
    std::string stragety_str = "Mem";
    strategies.push_back(StringToStorageStrategy(stragety_str));
  }
}

int main(int argc, char** argv) {
  // Expect 1 args, graph yaml
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <graph yaml>" << std::endl;
    return 1;
  }

  {
    std::vector<gs::StorageStrategy> strategies;
    push_strategies(strategies);
  }
  auto graph_yaml = std::string(argv[1]);

  YAML::Node graph_node = YAML::LoadFile(graph_yaml);

  auto schema_res = gs::Schema::LoadFromYaml(graph_yaml);
  if (!schema_res.ok()) {
    std::cerr << "Failed to load graph schema: "
              << schema_res.status().ToString() << std::endl;
    return 1;
  }
  auto& schema = schema_res.value();
  LOG(INFO) << "Empty: " << schema.Empty();

  gs::NexgDB db(data_path, 1, "r", "jni", java_class_path, planner_config_path);
  auto conn = db.connect();
  auto res = conn->query("MATCH (v) RETURN v;");
  LOG(INFO) << "Query result: " << res.ok() << ", "
            << res.status().error_message();
  return 0;
}