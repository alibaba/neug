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

  gs::setup_signal_handler();

  {
    std::vector<gs::StorageStrategy> strategies;
    push_strategies(strategies);
  }
  // Test parse date
  std::string date_str = "1900-01-01";
  gs::Date date(date_str);
  LOG(INFO) << "date str: " << date.to_string();

  std::string data_path = argv[1];
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NexgDB db(data_path, 1, "w", "dummy", "", "", "");
  auto conn = db.connect();
  {
    auto res = conn->query(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY "
        "KEY(id));");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to create node table: " << res.status().ToString();
      return 1;
    }
  }
  {
    auto res = conn->query(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to create edge table: " << res.status().ToString();
      return 1;
    }
  }
  {
    auto res = conn->query("COPY person from \"person.csv\"");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy node table: " << res.status().ToString();
      return 1;
    }
  }
  {
    auto res = conn->query(
        "COPY COPY knows [person->person] from \"person_knows_person.csv\"");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy edge table: " << res.status().ToString();
      return 1;
    }
  }
  auto res = conn->query("MATCH (v) RETURN v;");
  LOG(INFO) << "Query result: " << res.ok() << ", "
            << res.status().error_message();
  auto res_val = res.value();
  while (res_val.hasNext()) {
    auto row = res_val.next();
    LOG(INFO) << "Row: " << row.ToString();
  }
  return 0;
}