#include <neug/main/neug_db.h>
#include <iostream>

int main() {
    std::string db_path = "/Users/louyk/Desktop/Projects/neug/sampling/dataset/small";
    SetupDatabase(db_path);
  
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(db_path));
    auto conn = db.Connect();
    ASSERT_TRUE(conn != nullptr);
  
    // load JSON extension
    auto load_res = conn->Query("LOAD SAMPLED_MATCH");
    ASSERT_TRUE(load_res.has_value())
        << "LOAD sampled_match failed: " << load_res.error().ToString();
  
    auto show_res = conn->Query("CALL show_loaded_extensions() RETURN *;");
    ASSERT_TRUE(show_res.has_value())
        << "CALL show_loaded_extensions failed: " << show_res.error().ToString();
  
    if (show_res.has_value()) {
      auto& rs = show_res.value();
      int row_idx = 0;
      while (rs.hasNext()) {
        auto row = rs.next();
        std::string name, desc;
        name = std::string(row.entries()[0]->element().object().str());
        desc = std::string(row.entries()[1]->element().object().str());
  
        std::cout << name << " " << desc << std::endl;
      }
    }
  
    auto res = conn->Query("CALL SAMPLED_MATCH('/Users/louyk/Desktop/Projects/neug/sampling/pattern/pattern_with_constraints.json')");
    ASSERT_TRUE(res.has_value())
        << "CALL SAMPLED_MATCH failed: " << res.error().ToString();

    auto rs = res.value();
    while (rs.hasNext()) {
      auto row = rs.next();
      std::cout << row.ToString() << std::endl;
    }

    return 0;
}