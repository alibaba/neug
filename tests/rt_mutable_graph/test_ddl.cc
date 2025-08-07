#include <iostream>
#include <string>
#include "neug/main/neug_db.h"
#include "neug/storages/rt_mutable_graph/file_names.h"
#include "neug/storages/rt_mutable_graph/schema.h"

int main(int argc, char** argv) {
  std::string data_path = "/tmp/test_batch_loading";
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NeugDB db(data_path, 1, "w", "gopt", "");

  std::string flex_data_dir = std::getenv("FLEX_DATA_DIR");
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
        "CREATE NODE TABLE software(id INT64, name STRING, lang STRING, "
        "PRIMARY "
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
    auto res = conn->query(
        "CREATE REL TABLE created(FROM person TO software, weight DOUBLE, "
        "since INT64);");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to create edge table: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res =
        conn->query("COPY person from \"" + flex_data_dir + "/person.csv\";");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to load person vertex: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query("COPY software from \"" + flex_data_dir +
                           "/software.csv\";");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to load software vertex: "
                 << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query(
        "COPY knows from \"" + flex_data_dir +
        "/person_knows_person.csv\" (from=\"person\", to=\"person\");");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to load person-knows->person edge: "
                 << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query(
        "COPY created from \"" + flex_data_dir +
        "/person_created_software.csv\" (from=\"person\", to=\"software\");");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to load person-knows->software edge: "
                 << res.status().ToString();
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
    bool exception_thrown = false;
    try {
      auto res = conn->query("ALTER TABLE person ADD name STRING;");
    } catch (const std::runtime_error& e) {
      LOG(INFO) << "Expected error: " << e.what();
      exception_thrown = true;
    }
    if (!exception_thrown) {
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
    bool exception_thrown = false;
    try {
      auto res = conn->query("ALTER TABLE person DROP non_existing_column;");
    } catch (const std::runtime_error& e) {
      LOG(INFO) << "Expected error: " << e.what();
      exception_thrown = true;
    }
    if (!exception_thrown) {
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
    if (!res.ok()) {
      LOG(ERROR) << "Failed to alter table: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query("ALTER TABLE person DROP age;");
    if (!res.ok()) {
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
    auto res =
        conn->query("MATCH (v:person)-[e:created]->(:software) DELETE e;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to drop edge type: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query("MATCH (v:person) DELETE v;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to drop vertex type: " << res.status().ToString();
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
    auto res = conn->query("MATCH (v:person) RETURN v;");
    LOG(INFO) << "Query result: " << res.ok() << ", "
              << res.status().error_message();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      LOG(INFO) << "Row: " << row.ToString();
    }
  }

  {
    auto res =
        conn->query("MATCH (v:software)<-[e:created]-(:person) RETURN e;");
    LOG(INFO) << "Query result: " << res.ok() << ", "
              << res.status().error_message();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      LOG(INFO) << "Row: " << row.ToString();
    }
  }

  {
    auto res = conn->query("COPY person from \"" + flex_data_dir +
                           "/person_after_alter.csv\";");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to load person vertex: " << res.status().ToString();
      return 1;
    }
  }

  {
    auto res = conn->query("MATCH (v:person) RETURN count(v);");
    LOG(INFO) << "Query result: " << res.ok() << ", "
              << res.status().error_message();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      LOG(INFO) << "Row: " << row.ToString();
    }
  }

  return 0;
}