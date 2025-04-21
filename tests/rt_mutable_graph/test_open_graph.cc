#include <iostream>
#include <string>
#include "src/main/nexg_db.h"

int main(int argc, char** argv) {
  // Expect 3 args, data_path, planner_config_path, java_class_path
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <data_path> <planner_config_path>"
              << " <java_class_path>" << std::endl;
    return 1;
  }
  std::string data_path = argv[1];
  std::string planner_config_path = argv[2];
  std::string java_class_path = argv[3];

  gs::NexgDB db(data_path, 1, "r", "jni", java_class_path, planner_config_path);
  auto conn = db.connect();
  auto res = conn->query("MATCH (v) RETURN v;");
  LOG(INFO) << "Query result: " << res.DebugString();
  return 0;
}