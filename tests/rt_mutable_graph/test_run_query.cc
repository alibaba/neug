#include <iostream>
#include <string>
#include "src/main/neug_db.h"
#include "src/storages/rt_mutable_graph/file_names.h"
#include "src/storages/rt_mutable_graph/schema.h"

int main(int argc, char** argv) {
  // Expect 1 args, data path
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <data_path> <query_string>"
              << std::endl;
    std::cerr << " Got " << argc - 1 << " arguments." << std::endl;
    return 1;
  }

  std::string data_path = argv[1];
  std::string query_string = argv[2];

  gs::NeugDB db(data_path, 1, "r", "gopt", "");
  auto conn = db.connect();
  LOG(INFO) << "Running query: " << query_string;

  auto res = conn->query(query_string);
  if (!res.ok()) {
    LOG(ERROR) << "Query failed: " << res.status().ToString();
    return 1;
  }
  LOG(INFO) << "Query result: " << res.value().length() << " records found.";
  while (res.value().hasNext()) {
    auto record = res.value().next();
    LOG(INFO) << "Record: " << record.ToString();
  }
  return 0;
}