#include <iostream>
#include <string>
#include "src/main/neug_db.h"
#include "src/storages/rt_mutable_graph/file_names.h"
#include "src/storages/rt_mutable_graph/schema.h"

int main(int argc, char** argv) {
  // Expect 1 args, data path
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <data_path>" << std::endl;
    return 1;
  }

  // gs::setup_signal_handler();

  std::string data_path = argv[1];
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NeugDB db(data_path, 1, "w", "dummy", "");
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
    auto res = conn->query("ALTER TABLE person ADD birthday DATE;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to alter table: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res =
        conn->query("ALTER TABLE person ADD IF NOT EXISTS birthday DATE;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to alter table: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query("ALTER TABLE person ADD name STRING;");
    if (res.ok()) {
      LOG(ERROR) << "Altered table successfully, but it should have failed "
                 << "because the column already exists.";
      return 1;
    }
  }

  {
    auto res = conn->query("ALTER TABLE knows ADD registion DATE;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to alter table: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query("ALTER TABLE person DROP non_existing_column;");
    if (res.ok()) {
      LOG(ERROR) << "Altered table successfully, but it should have failed "
                 << "because the column does not exist.";
      return 1;
    }
  }

  {
    auto res = conn->query("ALTER TABLE person DROP birthday;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to alter table: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query("ALTER TABLE person DROP IF EXISTS birthday;");
    if (res.ok()) {
      LOG(ERROR) << "Failed to alter table: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query("ALTER TABLE person RENAME name TO username;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to alter table: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query("DROP TABLE knows;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to drop edge type: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query("DROP TABLE person;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to drop vertex type: " << res.status().ToString();
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