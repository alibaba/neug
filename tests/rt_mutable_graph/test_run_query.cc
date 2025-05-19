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

  gs::NexgDB db(
      data_path, 1, "r", "jni",
      "/Users/zhanglei/code/nexg/tools/python_bind/nexg/resources/compiler.jar",
      "/Users/zhanglei/code/nexg/tools/python_bind/nexg/resources/"
      "planner_config.yaml",
      "/Users/zhanglei/code/nexg/tools/python_bind/nexg/resources/");
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