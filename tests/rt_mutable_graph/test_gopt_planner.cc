#include <iostream>
#include <string>
#include "neug/main/neug_db.h"
#include "neug/storages/rt_mutable_graph/file_names.h"
#include "neug/storages/rt_mutable_graph/schema.h"

bool test_gopt_planner(const std::string& data_path) {
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }

  // Get env variable FLEX_DATA_DIR
  const char* env_p = std::getenv("FLEX_DATA_DIR");
  if (env_p == nullptr) {
    LOG(FATAL) << "FLEX_DATA_DIR is not set.";
  }
  std::string flex_data_dir(env_p);
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NeugDB db(data_path, 1, "w", "gopt", "");
  auto conn = db.connect();
  {
    auto res = conn->query(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY "
        "KEY(id));");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to create node table: " << res.status().ToString();
      return false;
    }
  }

  {
    auto res = conn->query(
        "CREATE REL TABLE knows(FROM person TO person, weight "
        "DOUBLE);");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to create edge table: " << res.status().ToString();
      return false;
    }
  }

  {
    std::string person_csv_path = flex_data_dir + "/person.csv";
    auto res = conn->query("COPY person from \"" + person_csv_path + "\"");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy node table: " << res.status().ToString();
      return false;
    }
  }
  {
    std::string knows_csv_path = flex_data_dir + "/person_knows_person.csv";
    auto res = conn->query("COPY knows from \"" + knows_csv_path + "\"" +
                           " (from=\"person\", to=\"person\")");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy edge table: " << res.status().ToString();
      return false;
    }
  }
  auto res = conn->query("MATCH (v) RETURN v.name;");
  LOG(INFO) << "Query result: " << res.ok() << ", "
            << res.status().error_message();
  auto res_val = res.value();
  while (res_val.hasNext()) {
    auto row = res_val.next();
    LOG(INFO) << "Row: " << row.ToString();
  }
  return true;
}

int main(int argc, char** argv) {
  // Expect 1 args, data path
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <data_path>" << std::endl;
    return 1;
  }

  gs::setup_signal_handler();

  // Test parse date
  std::string date_str = "1900-01-01";
  gs::Date date(date_str);
  LOG(INFO) << "date str: " << date.to_string();

  std::string data_path = argv[1];
  CHECK(test_gopt_planner(data_path)) << "Database test failed.";
  LOG(INFO) << "------------------------------------";

  LOG(INFO) << "All tests passed successfully.";
  return 0;
}