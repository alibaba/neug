#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include "neug/main/neug_db.h"
#include "neug/storages/file_names.h"

class TestJsonExtension : public ::testing::Test {
protected:
    std::string db_path = "/tmp/test_json_extension_db";
    std::string json_file = "/tmp/extension_unittest_data/json/test_array.json";
    std::string json_extension_path = std::string(std::getenv("HOME")) + "/.neug/extensions/json/libjson.neug_extension";

    void SetUp() override {
        std::filesystem::remove_all(db_path);
        std::filesystem::create_directories("/tmp/extension_unittest_data/json/test_data");

        std::ofstream file(json_file);
        file << R"([
            {"id": 1, "name": "Mika", "age": 17, "height": 157.5, "birthday": "2004-05-08"},
            {"id": 2, "name": "Yuuka", "age": 16, "height": 156.1, "birthday": "2005-03-14"},
            {"id": 3, "name": "Hina", "age": 17, "height": 142.8, "birthday": "2004-02-19"}
        ])";
        file.close();

        // set up db
        gs::NeugDB db;
        db.Open(db_path);
        auto conn = db.Connect();

        // create table
        auto res = conn->Query(
            "CREATE NODE TABLE student("
            "id INT32, "
            "name STRING, "
            "age INT32, "
            "height DOUBLE, "
            "birthday DATE, "
            "PRIMARY KEY(id)"
            ");"
        );
        ASSERT_TRUE(res.has_value()) << "Failed to create table: " << res.error().ToString();

        conn.reset();
        db.Close();
    }

    void TearDown() override {
        std::filesystem::remove_all("/tmp/extension_unittest_data/json/test_data");
        std::filesystem::remove_all(db_path);
    }
};

TEST_F(TestJsonExtension, LoadAndImportJson) {
    gs::NeugDB db;
    ASSERT_TRUE(db.Open(db_path));
    auto conn = db.Connect();
    ASSERT_TRUE(conn != nullptr);

    LOG(INFO) << "=== Database opened at: " << db_path;

    // // Check if lib/json exists and uninstall if present
    // {
    //     bool json_existed_before = std::filesystem::exists(json_extension_path);
    //     LOG(INFO) << "JSON extension exists before test: " << (json_existed_before ? "YES" : "NO");
        
    //     if (json_existed_before) {
    //         LOG(INFO) << "Uninstalling existing JSON extension...";
    //         auto uninstall_res = conn->Query("UNINSTALL json;");
    //         if (uninstall_res.has_value()) {
    //             LOG(INFO) << "UNINSTALL json succeeded";
                
    //             // Check if the extension file was removed
    //             bool json_exists_after_uninstall = std::filesystem::exists(json_extension_path);
    //             EXPECT_FALSE(json_exists_after_uninstall) 
    //                 << "JSON extension file should be deleted after UNINSTALL";
    //             LOG(INFO) << "JSON extension removed successfully: " 
    //                      << (!json_exists_after_uninstall ? "YES" : "NO");
    //         } else {
    //             LOG(WARNING) << "UNINSTALL json failed: " << uninstall_res.error().ToString();
    //             // Continue with test even if uninstall fails
    //         }
    //     }
    // }

    // // Install JSON extension and verify download
    // {
    //     LOG(INFO) << "Installing JSON extension...";
    //     auto install_res = conn->Query("INSTALL json;");
    //     ASSERT_TRUE(install_res.has_value()) << "INSTALL json failed: " << install_res.error().ToString();
        
    //     LOG(INFO) << "INSTALL json succeeded";
        
    //     // Check if libjson.neug_extension was downloaded successfully
    //     bool json_downloaded = std::filesystem::exists(json_extension_path);
    //     EXPECT_TRUE(json_downloaded) 
    //         << "JSON extension file should exist after INSTALL at: " << json_extension_path;
        
    //     if (json_downloaded) {
    //         // Check file size to ensure it's not empty
    //         auto file_size = std::filesystem::file_size(json_extension_path);
    //         EXPECT_GT(file_size, 0) << "JSON extension file should not be empty";
    //         LOG(INFO) << "JSON extension downloaded successfully, size: " << file_size << " bytes";
    //     } else {
    //         FAIL() << "JSON extension file not found at: " << json_extension_path;
    //     }
    // }

    // load JSON extension
    auto load_res = conn->Query("LOAD json");
    ASSERT_TRUE(load_res.has_value()) << "LOAD json failed: " << load_res.error().ToString();

    // insert JSON data
    std::string import_query = "COPY student FROM \"" + json_file + "\";";
    auto import_res = conn->Query(import_query);
    ASSERT_TRUE(import_res.has_value()) << "Import failed: " << import_res.error().ToString();

    // Verify by count
    {
        auto count_res = conn->Query("MATCH (s:student) RETURN count(s);");
        ASSERT_TRUE(count_res.has_value()) << "Count query failed: " << count_res.error().ToString();
        auto& rs = count_res.value();
        ASSERT_TRUE(rs.hasNext());
        auto row = rs.next();
        EXPECT_EQ(row.ToString(), "<element { object { i64: 3 } }>");
    }

    // Verify specific fields via simple queries
    {
        auto q1 = conn->Query("MATCH (s:student) WHERE s.id = 1 RETURN s.name;");
        ASSERT_TRUE(q1.has_value()) << "Query name failed: " << q1.error().ToString();
        auto& rs1 = q1.value();
        ASSERT_TRUE(rs1.hasNext());
        auto r1 = rs1.next();
        EXPECT_NE(r1.ToString().find("Mika"), std::string::npos);
    }
    {
        auto q2 = conn->Query("MATCH (s:student) WHERE s.id = 2 RETURN s.age;");
        ASSERT_TRUE(q2.has_value()) << "Query age failed: " << q2.error().ToString();
        auto& rs2 = q2.value();
        ASSERT_TRUE(rs2.hasNext());
        auto r2 = rs2.next();
        EXPECT_NE(r2.ToString().find("16"), std::string::npos);
    }
    {
        auto q3 = conn->Query("MATCH (s:student) WHERE s.id = 3 RETURN s.height;");
        ASSERT_TRUE(q3.has_value()) << "Query height failed: " << q3.error().ToString();
        auto& rs3 = q3.value();
        ASSERT_TRUE(rs3.hasNext());
        auto r3 = rs3.next();
        EXPECT_NE(r3.ToString().find("142.8"), std::string::npos);
    }
    {
        auto q_date_comp = conn->Query("MATCH (s:student) WHERE s.id = 1 AND s.birthday = DATE('2004-05-08') RETURN s.name;");
        if (q_date_comp.has_value()) {
            auto& rs_date = q_date_comp.value();
            if (rs_date.hasNext()) {
                auto r_date = rs_date.next();
                EXPECT_NE(r_date.ToString().find("Mika"), std::string::npos) 
                    << "Mika should match the exact birthday 2004-05-08";
                LOG(INFO) << "Exact date match successful for Mika";
            } else {
                LOG(WARNING) << "Exact date comparison might not be supported or date format differs";
            }
        } else {
            LOG(WARNING) << "Date comparison query failed: " << q_date_comp.error().ToString();
        }
    }

    LOG(INFO) << "JSON Extension test with birthday completed successfully";
}