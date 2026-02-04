#include <neug/main/neug_db.h>
#include <neug/compiler/common/file_system/virtual_file_system.h>
#include <neug/compiler/gopt/g_vfs_holder.h>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <cassert>
#include <dlfcn.h>

// // Path to sample extension library
// static std::string getSampleExtensionPath() {
//     // First try build directory
//     std::string build_path = "/mnt/lyk/neug/build/extension/sample/libsample.neug_extension";
//     if (std::filesystem::exists(build_path)) {
//         return build_path;
//     }
//     // Fallback to user install directory
//     const char* home = std::getenv("HOME");
//     if (home) {
//         return std::string(home) + "/.neug/extensions/sample/libsample.neug_extension";
//     }
//     return build_path;
// }

// // Directly load extension using dlopen (bypass SQL LOAD command)
// bool loadExtensionDirectly(const std::string& lib_path) {
//     std::cout << "Loading extension from: " << lib_path << std::endl;
    
//     if (!std::filesystem::exists(lib_path)) {
//         std::cerr << "Extension file not found: " << lib_path << std::endl;
//         return false;
//     }
    
//     void* handle = dlopen(lib_path.c_str(), RTLD_NOW | RTLD_GLOBAL);
//     if (!handle) {
//         std::cerr << "dlopen failed: " << dlerror() << std::endl;
//         return false;
//     }
    
//     dlerror(); // Clear errors
//     typedef void (*init_func_t)();
//     init_func_t init_func = (init_func_t)dlsym(handle, "Init");
//     const char* dlsym_error = dlerror();
//     if (dlsym_error) {
//         std::cerr << "dlsym failed: " << dlsym_error << std::endl;
//         dlclose(handle);
//         return false;
//     }
    
//     try {
//         (*init_func)();
//         std::cout << "Extension initialized successfully" << std::endl;
//         return true;
//     } catch (const std::exception& e) {
//         std::cerr << "Extension init failed: " << e.what() << std::endl;
//         dlclose(handle);
//         return false;
//     }
// }

// Execute a query and check for errors
bool executeQuery(neug::Connection* conn, const std::string& query, const std::string& description = "") {
    if (!description.empty()) {
        std::cout << "  " << description << std::endl;
    }
    auto res = conn->Query(query);
    if (!res.has_value()) {
        std::cerr << "Query failed: " << res.error().ToString() << std::endl;
        std::cerr << "Query was: " << query << std::endl;
        return false;
    }
    return true;
}

// Create the graph schema
bool createSchema(neug::Connection* conn) {
    std::cout << "\n=== Creating Schema ===" << std::endl;
    
    // Create Person node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Person(
            id INT32 PRIMARY KEY,
            firstName STRING,
            lastName STRING,
            gender STRING,
            birthday STRING,
            creationDate STRING,
            locationIP STRING,
            browserUsed STRING
        )
    )", "Creating Person node table")) {
        return false;
    }
    
    // Create Comment node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Comment(
            id INT32 PRIMARY KEY,
            creationDate STRING,
            content STRING,
            length INT32
        )
    )", "Creating Comment node table")) {
        return false;
    }
    
    // Create Post node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Post(
            id INT32 PRIMARY KEY,
            imageFile STRING,
            creationDate STRING,
            content STRING,
            length INT32
        )
    )", "Creating Post node table")) {
        return false;
    }
    
    // Create edge tables
    if (!executeQuery(conn, R"(
        CREATE REL TABLE person_knows_person(
            FROM Person TO Person,
            creationDate STRING
        )
    )", "Creating person_knows_person edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE comment_hasCreator_person(
            FROM Comment TO Person
        )
    )", "Creating comment_hasCreator_person edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE post_hasCreator_person(
            FROM Post TO Person
        )
    )", "Creating post_hasCreator_person edge table")) {
        return false;
    }
    
    std::cout << "Schema created successfully!" << std::endl;
    return true;
}

// Insert sample data
bool insertData(neug::Connection* conn) {
    std::cout << "\n=== Inserting Data ===" << std::endl;
    
    // Insert Person nodes
    std::cout << "  Inserting Person nodes..." << std::endl;
    
    if (!executeQuery(conn, R"(
        CREATE (n:Person {
            id: 0,
            firstName: 'Alice',
            lastName: 'Anderson',
            gender: 'female',
            birthday: '1990-01-15',
            creationDate: '2020-01-01',
            locationIP: '192.168.1.1',
            browserUsed: 'Chrome'
        })
    )")) return false;
    
    if (!executeQuery(conn, R"(
        CREATE (n:Person {
            id: 1,
            firstName: 'Bob',
            lastName: 'Brown',
            gender: 'male',
            birthday: '1985-06-20',
            creationDate: '2020-02-15',
            locationIP: '192.168.1.2',
            browserUsed: 'Firefox'
        })
    )")) return false;
    
    if (!executeQuery(conn, R"(
        CREATE (n:Person {
            id: 2,
            firstName: 'Carol',
            lastName: 'Chen',
            gender: 'female',
            birthday: '1992-03-10',
            creationDate: '2020-03-20',
            locationIP: '192.168.1.3',
            browserUsed: 'Safari'
        })
    )")) return false;
    
    std::cout << "  Inserted 3 Person nodes" << std::endl;
    
    // Insert Comment nodes
    std::cout << "  Inserting Comment nodes..." << std::endl;
    
    if (!executeQuery(conn, R"(
        CREATE (n:Comment {
            id: 3,
            creationDate: '2020-04-01',
            content: 'Great post!',
            length: 11
        })
    )")) return false;
    
    std::cout << "  Inserted 1 Comment node" << std::endl;
    
    // Insert Post nodes
    std::cout << "  Inserting Post nodes..." << std::endl;
    
    if (!executeQuery(conn, R"(
        CREATE (n:Post {
            id: 4,
            imageFile: 'photo1.jpg',
            creationDate: '2020-03-25',
            content: 'My first post!',
            length: 15
        })
    )")) return false;
    
    if (!executeQuery(conn, R"(
        CREATE (n:Post {
            id: 5,
            imageFile: 'photo2.jpg',
            creationDate: '2020-03-26',
            content: 'Another post!',
            length: 13
        })
    )")) return false;
    
    std::cout << "  Inserted 2 Post nodes" << std::endl;
    
    // Insert person_knows_person edges (forming a triangle: 0->1, 1->2, 2->0)
    std::cout << "  Inserting person_knows_person edges (triangle)..." << std::endl;
    
    if (!executeQuery(conn, R"(
        MATCH (a:Person), (b:Person) 
        WHERE a.id = 0 AND b.id = 1
        CREATE (a)-[:person_knows_person {creationDate: '2020-05-01'}]->(b)
    )")) return false;
    
    if (!executeQuery(conn, R"(
        MATCH (a:Person), (b:Person) 
        WHERE a.id = 1 AND b.id = 2
        CREATE (a)-[:person_knows_person {creationDate: '2020-05-15'}]->(b)
    )")) return false;
    
    if (!executeQuery(conn, R"(
        MATCH (a:Person), (b:Person) 
        WHERE a.id = 2 AND b.id = 0
        CREATE (a)-[:person_knows_person {creationDate: '2020-06-01'}]->(b)
    )")) return false;
    
    std::cout << "  Inserted 3 person_knows_person edges" << std::endl;
    
    // Insert comment_hasCreator_person edges
    std::cout << "  Inserting comment_hasCreator_person edges..." << std::endl;
    
    if (!executeQuery(conn, R"(
        MATCH (c:Comment), (p:Person) 
        WHERE c.id = 3 AND p.id = 0
        CREATE (c)-[:comment_hasCreator_person]->(p)
    )")) return false;
    
    std::cout << "  Inserted 1 comment_hasCreator_person edge" << std::endl;
    
    // Insert post_hasCreator_person edges
    std::cout << "  Inserting post_hasCreator_person edges..." << std::endl;
    
    if (!executeQuery(conn, R"(
        MATCH (post:Post), (person:Person) 
        WHERE post.id = 4 AND person.id = 0
        CREATE (post)-[:post_hasCreator_person]->(person)
    )")) return false;
    
    if (!executeQuery(conn, R"(
        MATCH (post:Post), (person:Person) 
        WHERE post.id = 5 AND person.id = 1
        CREATE (post)-[:post_hasCreator_person]->(person)
    )")) return false;
    
    std::cout << "  Inserted 2 post_hasCreator_person edges" << std::endl;
    
    std::cout << "Data inserted successfully!" << std::endl;
    return true;
}

bool loadExtension(neug::Connection* conn) {
  std::cout << "\n=== Loading Extension ===" << std::endl;
  auto res = conn->Query("LOAD sample;");
  if (!res.has_value()) {
    std::cerr << "Failed to load extension" << std::endl;
    return false;
  }
  std::cout << "Extension loaded successfully" << std::endl;
  return true;
}

// Verify inserted data
void verifyData(neug::Connection* conn) {
    std::cout << "\n=== Verifying Data ===" << std::endl;
    
    // Count Person nodes
    auto res = conn->Query("MATCH (p:Person) RETURN count(*)");
    if (res.has_value()) {
        auto& rs = res.value();
        if (rs.hasNext()) {
            auto row = rs.next();
            std::cout << "  Person count: " << row.ToString() << std::endl;
        }
    }
    
    // Count edges
    res = conn->Query("MATCH (a:Person)-[k:person_knows_person]->(b:Person) RETURN count(*)");
    if (res.has_value()) {
        auto& rs = res.value();
        if (rs.hasNext()) {
            auto row = rs.next();
            std::cout << "  person_knows_person count: " << row.ToString() << std::endl;
        }
    }
    
    // Show knows relationships
    std::cout << "\n  Person knows Person relationships:" << std::endl;
    res = conn->Query(R"(
        MATCH (a:Person)-[k:person_knows_person]->(b:Person)
        RETURN a.firstName, b.firstName, k.creationDate
    )");
    if (res.has_value()) {
        auto& rs = res.value();
        while (rs.hasNext()) {
            auto row = rs.next();
            std::cout << "    " << row.ToString() << std::endl;
        }
    }
}

int main() {
    std::cout << "============================================" << std::endl;
    std::cout << "NeuG SAMPLED_MATCH Extension Test" << std::endl;
    std::cout << "============================================" << std::endl;
    
    // Setup VFS (required for neug)
    auto vfs = std::make_unique<neug::common::VirtualFileSystem>();
    neug::common::VFSHolder::setVFS(vfs.get());
    
    // Use a temporary database path
    std::string db_path = "/tmp/neug_sample_test_db";
    std::string pattern_file = "/tmp/p/t.json";  // Keep path short (< 48 chars)
    
    // Clean up old database if exists
    if (std::filesystem::exists(db_path)) {
        std::cout << "\nRemoving existing database: " << db_path << std::endl;
        std::filesystem::remove_all(db_path);
    }
    
    // Create database
    std::cout << "\n=== Creating Database ===" << std::endl;
    std::cout << "Database path: " << db_path << std::endl;
    
    neug::NeugDB db;
    if (!db.Open(db_path)) {
        std::cerr << "Failed to create database: " << db_path << std::endl;
        return 1;
    }
    std::cout << "Database created successfully" << std::endl;
    
    auto conn = db.Connect();
    if (!conn) {
        std::cerr << "Failed to connect to database" << std::endl;
        return 1;
    }
    std::cout << "Connected to database" << std::endl;
    
    // Create schema
    if (!createSchema(conn.get())) {
        std::cerr << "Failed to create schema" << std::endl;
        conn.reset();
        db.Close();
        return 1;
    }
    
    // Insert data
    if (!insertData(conn.get())) {
        std::cerr << "Failed to insert data" << std::endl;
        conn.reset();
        db.Close();
        return 1;
    }
    
    // Verify data
    // verifyData(conn.get());
    
    // Load extension directly using dlopen
    std::cout << "\n=== Loading Extension ===" << std::endl;
    if (!loadExtension(conn.get())) {
        std::cerr << "Failed to load sample extension" << std::endl;
        conn.reset();
        db.Close();
        return 1;
    }
    
    // Verify extension is loaded
    auto show_res = conn->Query("CALL show_loaded_extensions() RETURN *;");
    if (show_res.has_value()) {
        std::cout << "\nLoaded extensions:" << std::endl;
        auto& rs = show_res.value();
        while (rs.hasNext()) {
            auto row = rs.next();
            std::string name = std::string(row.entries()[0]->element().object().str());
            std::string desc = std::string(row.entries()[1]->element().object().str());
            std::cout << "  - " << name << ": " << desc << std::endl;
        }
    }
    
    // Call SAMPLED_MATCH function
    std::cout << "\n=== Calling SAMPLED_MATCH ===" << std::endl;
    // std::cout << "Pattern: Triangle (3 people who all know each other)" << std::endl;
    
    std::string query = "CALL SAMPLED_MATCH('" + pattern_file + "')";
    std::cout << "Query: " << query << std::endl;
    
    auto res = conn->Query(query);
    if (!res.has_value()) {
        std::cerr << "SAMPLED_MATCH failed: " << res.error().ToString() << std::endl;
        conn.reset();
        db.Close();
        return 1;
    }

    std::cout << "\n=== Results ===" << std::endl;
    auto& result_rs = res.value();
    int result_count = 0;
    while (result_rs.hasNext()) {
        auto row = result_rs.next();
        std::cout << "  " << row.ToString() << std::endl;
        result_count++;
    }
    
    if (result_count == 0) {
        std::cout << "  No matches found." << std::endl;
    } else {
        std::cout << "\nTotal: " << result_count << " match(es) found." << std::endl;
    }

    std::cout << "\n============================================" << std::endl;
    std::cout << "Test completed successfully!" << std::endl;
    std::cout << "============================================" << std::endl;
    
    // Cleanup
    conn.reset();
    db.Close();
    
    // Remove temporary files
    if (std::filesystem::exists(pattern_file)) {
        std::filesystem::remove(pattern_file);
    }
    // Optionally remove the database
    // std::filesystem::remove_all(db_path);
    
    return 0;
}
