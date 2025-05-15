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
  // Expect 1 args, data path
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <data_path>" << std::endl;
    return 1;
  }

  {
    std::vector<gs::StorageStrategy> strategies;
    push_strategies(strategies);
  }
  std::string data_path = argv[1];
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NexgDB db(data_path, 1, "w", "dummy", "", "", "");
  auto conn = db.connect();

  auto res = conn->query("MATCH (v) RETURN v;");
  LOG(INFO) << "Query result: " << res.ok() << ", "
            << res.status().error_message();
  return 0;
}