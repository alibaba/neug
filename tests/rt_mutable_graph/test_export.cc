#include <iostream>
#include <string>
#include "src/main/neug_db.h"
#include "src/storages/rt_mutable_graph/file_names.h"
#include "src/storages/rt_mutable_graph/schema.h"

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
    auto res = conn->query("COPY (MATCH (v:person) RETURN v) to 'person.csv';");
    LOG(INFO) << "Query result: " << res.ok() << ", "
              << res.status().error_message();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      LOG(INFO) << "Row: " << row.ToString();
    }
  }

  {
    auto res = conn->query(
        "COPY (MATCH (:person)-[e:knows]->(:person) RETURN e) to "
        "'person_knows_person.csv' (HEADER = true);");
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