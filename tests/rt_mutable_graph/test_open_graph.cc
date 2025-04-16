#include <iostream>
#include <string>
#include "src/engines/graph_db/database/graph_db.h"

int main(int argc, char** argv) {
  // Expect one args, data_path
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <data_path>" << std::endl;
    return 1;
  }
  std::string data_path = argv[1];

  gs::GraphDB db;
  gs::GraphDBConfig config;
  config.data_dir = data_path;
  auto res = db.Open(config);
  if (!res.ok()) {
    LOG(ERROR) << "Open directory failed: " << res.status().error_message();
  } else {
    LOG(INFO) << "Open directory success";
  }
  return 0;
}