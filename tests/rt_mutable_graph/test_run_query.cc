#include <iostream>
#include <string>
#include "src/main/nexg_db.h"
#include "src/storages/rt_mutable_graph/file_names.h"
#include "src/storages/rt_mutable_graph/schema.h"

int main(int argc, char** argv) {
  // Expect 1 args, data path
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <data_path>" << std::endl;
    return 1;
  }

  std::string data_path = argv[1];

  LOG(INFO) << "Current source file path: " << __FILE__;
  LOG(INFO) << "Current source file directory: "
            << std::filesystem::path(__FILE__).parent_path();
  std::string cur_dir = std::filesystem::current_path().string();
  std::string compiler_jar_path =
      cur_dir + "/../tools/python_bind/nexg/resources/compiler.jar";
  LOG(INFO) << "Compiler JAR path: " << compiler_jar_path;
  std::string planner_config_path =
      cur_dir + "/../tools/python_bind/nexg/resources/planner_config.yaml";
  LOG(INFO) << "Planner config path: " << planner_config_path;
  std::string resource_path = cur_dir + "/../tools/python_bind/nexg/resources";
  LOG(INFO) << "Resource path: " << resource_path;

  gs::NexgDB db(data_path, 1, "w", "jni", compiler_jar_path,
                planner_config_path, resource_path);
  auto conn = db.connect();

  auto res = conn->query("MATCH (v) RETURN v;");
  LOG(INFO) << "Query result: " << res.ok() << ", "
            << res.status().error_message();
  auto records = res.value();
  while (records.hasNext()) {
    auto record = records.next();
    LOG(INFO) << "Record: " << record.ToString();
  }
  return 0;
}