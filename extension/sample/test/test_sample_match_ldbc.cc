#include <neug/main/neug_db.h>
#include <neug/compiler/common/file_system/virtual_file_system.h>
#include <neug/compiler/gopt/g_vfs_holder.h>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <cassert>
#include <chrono>
#include <dlfcn.h>

// LDBC SF01 dataset path
const std::string LDBC_DATA_PATH = "/mnt/lyk/neug/sampling/dataset/sf01/";

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

// Create the graph schema for LDBC SF01
bool createSchema(neug::Connection* conn) {
    std::cout << "\n=== Creating Schema ===" << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    
    // Create Person node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Person(
            id INT64 PRIMARY KEY,
            label STRING,
            firstName STRING,
            lastName STRING,
            gender STRING,
            birthday STRING,
            creationDate STRING,
            locationIP STRING,
            browserUsed STRING,
            language STRING,
            email STRING
        )
    )", "Creating Person node table")) {
        return false;
    }
    
    // Create Comment node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Comment(
            id INT64 PRIMARY KEY,
            label STRING,
            creationDate STRING,
            locationIP STRING,
            browserUsed STRING,
            content STRING,
            length INT32
        )
    )", "Creating Comment node table")) {
        return false;
    }
    
    // Create Post node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Post(
            id INT64 PRIMARY KEY,
            label STRING,
            imageFile STRING,
            creationDate STRING,
            locationIP STRING,
            browserUsed STRING,
            language STRING,
            content STRING,
            length INT32
        )
    )", "Creating Post node table")) {
        return false;
    }
    
    // Create Forum node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Forum(
            id INT64 PRIMARY KEY,
            label STRING,
            title STRING,
            creationDate STRING
        )
    )", "Creating Forum node table")) {
        return false;
    }
    
    // Create Tag node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Tag(
            id INT64 PRIMARY KEY,
            label STRING,
            name STRING,
            url STRING
        )
    )", "Creating Tag node table")) {
        return false;
    }
    
    // Create TagClass node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE TagClass(
            id INT64 PRIMARY KEY,
            label STRING,
            name STRING,
            url STRING
        )
    )", "Creating TagClass node table")) {
        return false;
    }
    
    // Create Place node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Place(
            id INT64 PRIMARY KEY,
            label STRING,
            name STRING,
            url STRING,
            type STRING
        )
    )", "Creating Place node table")) {
        return false;
    }
    
    // Create Organisation node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Organisation(
            id INT64 PRIMARY KEY,
            label STRING,
            type STRING,
            name STRING,
            url STRING
        )
    )", "Creating Organisation node table")) {
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
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE forum_hasMember_person(
            FROM Forum TO Person,
            joinDate STRING
        )
    )", "Creating forum_hasMember_person edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE forum_hasModerator_person(
            FROM Forum TO Person
        )
    )", "Creating forum_hasModerator_person edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE forum_containerOf_post(
            FROM Forum TO Post
        )
    )", "Creating forum_containerOf_post edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE comment_replyOf_post(
            FROM Comment TO Post
        )
    )", "Creating comment_replyOf_post edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE comment_replyOf_comment(
            FROM Comment TO Comment
        )
    )", "Creating comment_replyOf_comment edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE person_likes_post(
            FROM Person TO Post,
            creationDate STRING
        )
    )", "Creating person_likes_post edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE person_likes_comment(
            FROM Person TO Comment,
            creationDate STRING
        )
    )", "Creating person_likes_comment edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE person_hasInterest_tag(
            FROM Person TO Tag
        )
    )", "Creating person_hasInterest_tag edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE person_isLocatedIn_place(
            FROM Person TO Place
        )
    )", "Creating person_isLocatedIn_place edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE person_studyAt_organisation(
            FROM Person TO Organisation,
            classYear INT32
        )
    )", "Creating person_studyAt_organisation edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE person_workAt_organisation(
            FROM Person TO Organisation,
            workFrom INT32
        )
    )", "Creating person_workAt_organisation edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE post_hasTag_tag(
            FROM Post TO Tag
        )
    )", "Creating post_hasTag_tag edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE comment_hasTag_tag(
            FROM Comment TO Tag
        )
    )", "Creating comment_hasTag_tag edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE forum_hasTag_tag(
            FROM Forum TO Tag
        )
    )", "Creating forum_hasTag_tag edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE post_isLocatedIn_place(
            FROM Post TO Place
        )
    )", "Creating post_isLocatedIn_place edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE comment_isLocatedIn_place(
            FROM Comment TO Place
        )
    )", "Creating comment_isLocatedIn_place edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE organisation_isLocatedIn_place(
            FROM Organisation TO Place
        )
    )", "Creating organisation_isLocatedIn_place edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE place_isPartOf_place(
            FROM Place TO Place
        )
    )", "Creating place_isPartOf_place edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE tag_hasType_tagclass(
            FROM Tag TO TagClass
        )
    )", "Creating tag_hasType_tagclass edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE tagclass_isSubclassOf_tagclass(
            FROM TagClass TO TagClass
        )
    )", "Creating tagclass_isSubclassOf_tagclass edge table")) {
        return false;
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Schema created successfully in " << duration.count() << "ms!" << std::endl;
    return true;
}

// Load data from CSV files
bool loadData(neug::Connection* conn) {
    std::cout << "\n=== Loading Data from CSV Files ===" << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    
    // Load vertex data
    std::cout << "\n  Loading vertex data..." << std::endl;
    
    if (!executeQuery(conn, "COPY Person FROM '" + LDBC_DATA_PATH + "person.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Person nodes")) return false;
    
    if (!executeQuery(conn, "COPY Comment FROM '" + LDBC_DATA_PATH + "comment.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Comment nodes")) return false;
    
    if (!executeQuery(conn, "COPY Post FROM '" + LDBC_DATA_PATH + "post.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Post nodes")) return false;
    
    if (!executeQuery(conn, "COPY Forum FROM '" + LDBC_DATA_PATH + "forum.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Forum nodes")) return false;
    
    if (!executeQuery(conn, "COPY Tag FROM '" + LDBC_DATA_PATH + "tag.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Tag nodes")) return false;
    
    if (!executeQuery(conn, "COPY TagClass FROM '" + LDBC_DATA_PATH + "tagclass.csv' (HEADER=true, DELIMITER=',')", 
        "Loading TagClass nodes")) return false;
    
    if (!executeQuery(conn, "COPY Place FROM '" + LDBC_DATA_PATH + "place.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Place nodes")) return false;
    
    if (!executeQuery(conn, "COPY Organisation FROM '" + LDBC_DATA_PATH + "organisation.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Organisation nodes")) return false;
    
    // Load edge data
    std::cout << "\n  Loading edge data..." << std::endl;
    
    if (!executeQuery(conn, "COPY person_knows_person FROM '" + LDBC_DATA_PATH + "person_knows_person.csv' (HEADER=true, DELIMITER=',')", 
        "Loading person_knows_person edges")) return false;
    
    if (!executeQuery(conn, "COPY comment_hasCreator_person FROM '" + LDBC_DATA_PATH + "comment_hasCreator_person.csv' (HEADER=true, DELIMITER=',')", 
        "Loading comment_hasCreator_person edges")) return false;
    
    if (!executeQuery(conn, "COPY post_hasCreator_person FROM '" + LDBC_DATA_PATH + "post_hasCreator_person.csv' (HEADER=true, DELIMITER=',')", 
        "Loading post_hasCreator_person edges")) return false;
    
    if (!executeQuery(conn, "COPY forum_hasMember_person FROM '" + LDBC_DATA_PATH + "forum_hasMember_person.csv' (HEADER=true, DELIMITER=',')", 
        "Loading forum_hasMember_person edges")) return false;
    
    if (!executeQuery(conn, "COPY forum_hasModerator_person FROM '" + LDBC_DATA_PATH + "forum_hasModerator_person.csv' (HEADER=true, DELIMITER=',')", 
        "Loading forum_hasModerator_person edges")) return false;
    
    if (!executeQuery(conn, "COPY forum_containerOf_post FROM '" + LDBC_DATA_PATH + "forum_containerOf_post.csv' (HEADER=true, DELIMITER=',')", 
        "Loading forum_containerOf_post edges")) return false;
    
    if (!executeQuery(conn, "COPY comment_replyOf_post FROM '" + LDBC_DATA_PATH + "comment_replyOf_post.csv' (HEADER=true, DELIMITER=',')", 
        "Loading comment_replyOf_post edges")) return false;
    
    if (!executeQuery(conn, "COPY comment_replyOf_comment FROM '" + LDBC_DATA_PATH + "comment_replyOf_comment.csv' (HEADER=true, DELIMITER=',')", 
        "Loading comment_replyOf_comment edges")) return false;
    
    if (!executeQuery(conn, "COPY person_likes_post FROM '" + LDBC_DATA_PATH + "person_likes_post.csv' (HEADER=true, DELIMITER=',')", 
        "Loading person_likes_post edges")) return false;
    
    if (!executeQuery(conn, "COPY person_likes_comment FROM '" + LDBC_DATA_PATH + "person_likes_comment.csv' (HEADER=true, DELIMITER=',')", 
        "Loading person_likes_comment edges")) return false;
    
    if (!executeQuery(conn, "COPY person_hasInterest_tag FROM '" + LDBC_DATA_PATH + "person_hasInterest_tag.csv' (HEADER=true, DELIMITER=',')", 
        "Loading person_hasInterest_tag edges")) return false;
    
    if (!executeQuery(conn, "COPY person_isLocatedIn_place FROM '" + LDBC_DATA_PATH + "person_isLocatedIn_place.csv' (HEADER=true, DELIMITER=',')", 
        "Loading person_isLocatedIn_place edges")) return false;
    
    if (!executeQuery(conn, "COPY person_studyAt_organisation FROM '" + LDBC_DATA_PATH + "person_studyAt_organisation.csv' (HEADER=true, DELIMITER=',')", 
        "Loading person_studyAt_organisation edges")) return false;
    
    if (!executeQuery(conn, "COPY person_workAt_organisation FROM '" + LDBC_DATA_PATH + "person_workAt_organisation.csv' (HEADER=true, DELIMITER=',')", 
        "Loading person_workAt_organisation edges")) return false;
    
    if (!executeQuery(conn, "COPY post_hasTag_tag FROM '" + LDBC_DATA_PATH + "post_hasTag_tag.csv' (HEADER=true, DELIMITER=',')", 
        "Loading post_hasTag_tag edges")) return false;
    
    if (!executeQuery(conn, "COPY comment_hasTag_tag FROM '" + LDBC_DATA_PATH + "comment_hasTag_tag.csv' (HEADER=true, DELIMITER=',')", 
        "Loading comment_hasTag_tag edges")) return false;
    
    if (!executeQuery(conn, "COPY forum_hasTag_tag FROM '" + LDBC_DATA_PATH + "forum_hasTag_tag.csv' (HEADER=true, DELIMITER=',')", 
        "Loading forum_hasTag_tag edges")) return false;
    
    if (!executeQuery(conn, "COPY post_isLocatedIn_place FROM '" + LDBC_DATA_PATH + "post_isLocatedIn_place.csv' (HEADER=true, DELIMITER=',')", 
        "Loading post_isLocatedIn_place edges")) return false;
    
    if (!executeQuery(conn, "COPY comment_isLocatedIn_place FROM '" + LDBC_DATA_PATH + "comment_isLocatedIn_place.csv' (HEADER=true, DELIMITER=',')", 
        "Loading comment_isLocatedIn_place edges")) return false;
    
    if (!executeQuery(conn, "COPY organisation_isLocatedIn_place FROM '" + LDBC_DATA_PATH + "organisation_isLocatedIn_place.csv' (HEADER=true, DELIMITER=',')", 
        "Loading organisation_isLocatedIn_place edges")) return false;
    
    if (!executeQuery(conn, "COPY place_isPartOf_place FROM '" + LDBC_DATA_PATH + "place_isPartOf_place.csv' (HEADER=true, DELIMITER=',')", 
        "Loading place_isPartOf_place edges")) return false;
    
    if (!executeQuery(conn, "COPY tag_hasType_tagclass FROM '" + LDBC_DATA_PATH + "tag_hasType_tagclass.csv' (HEADER=true, DELIMITER=',')", 
        "Loading tag_hasType_tagclass edges")) return false;
    
    if (!executeQuery(conn, "COPY tagclass_isSubclassOf_tagclass FROM '" + LDBC_DATA_PATH + "tagclass_isSubclassOf_tagclass.csv' (HEADER=true, DELIMITER=',')", 
        "Loading tagclass_isSubclassOf_tagclass edges")) return false;
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
    std::cout << "\nData loaded successfully in " << duration.count() << "s!" << std::endl;
    return true;
}

// Load extension
bool loadExtension(neug::Connection* conn) {
    std::cout << "\n=== Loading Extension ===" << std::endl;
    auto res = conn->Query("LOAD sample;");
    if (!res.has_value()) {
        std::cerr << "Failed to load extension: " << res.error().ToString() << std::endl;
        return false;
    }
    std::cout << "Extension loaded successfully" << std::endl;
    return true;
}

int main(int argc, char* argv[]) {
    std::cout << "============================================" << std::endl;
    std::cout << "NeuG SAMPLED_MATCH Extension Test (LDBC SF01)" << std::endl;
    std::cout << "============================================" << std::endl;
    
    // Setup VFS (required for neug)
    auto vfs = std::make_unique<neug::common::VirtualFileSystem>();
    neug::common::VFSHolder::setVFS(vfs.get());
    
    // Use a temporary database path
    std::string db_path = "/tmp/neug_ldbc_sf01_db";
    std::string pattern_file = "/tmp/p/ldbc.json";  // Keep path short (< 48 chars)
    
    // Check if we should skip data loading (use existing DB)
    bool reload_data = true;
    if (argc > 1 && std::string(argv[1]) == "--no-reload") {
        reload_data = false;
        std::cout << "\nUsing existing database (--no-reload flag)" << std::endl;
    }
    
    if (reload_data) {
        // Clean up old database if exists
        if (std::filesystem::exists(db_path)) {
            std::cout << "\nRemoving existing database: " << db_path << std::endl;
            std::filesystem::remove_all(db_path);
        }
    }
    
    // Create/Open database
    std::cout << "\n=== Opening Database ===" << std::endl;
    std::cout << "Database path: " << db_path << std::endl;
    
    neug::NeugDB db;
    if (!db.Open(db_path)) {
        std::cerr << "Failed to open database: " << db_path << std::endl;
        return 1;
    }
    std::cout << "Database opened successfully" << std::endl;
    
    auto conn = db.Connect();
    if (!conn) {
        std::cerr << "Failed to connect to database" << std::endl;
        return 1;
    }
    std::cout << "Connected to database" << std::endl;
    
    if (reload_data) {
        // Create schema
        if (!createSchema(conn.get())) {
            std::cerr << "Failed to create schema" << std::endl;
            conn.reset();
            db.Close();
            return 1;
        }
        
        // Load data
        if (!loadData(conn.get())) {
            std::cerr << "Failed to load data" << std::endl;
            conn.reset();
            db.Close();
            return 1;
        }
    }
    
    // Verify data
    // verifyData(conn.get());
    
    // Load extension
    if (!loadExtension(conn.get())) {
        std::cerr << "Failed to load sample extension" << std::endl;
        conn.reset();
        db.Close();
        return 1;
    }
    
    // Verify extension is loaded
    {
        auto show_res = conn->Query("CALL show_loaded_extensions() RETURN *;");
        if (show_res.has_value()) {
            std::cout << "\nLoaded extensions:" << std::endl;
            auto tbl = show_res.value().table();
            if (tbl) {
                for (int64_t i = 0; i < tbl->num_rows(); i++) {
                    auto name_col = std::static_pointer_cast<arrow::StringArray>(tbl->column(0)->chunk(0));
                    auto desc_col = std::static_pointer_cast<arrow::StringArray>(tbl->column(1)->chunk(0));
                    std::cout << "  - " << name_col->GetString(i) << ": " << desc_col->GetString(i) << std::endl;
                }
            }
        }
    }
    
    // Create pattern file
    // if (!createPatternFile(pattern_file)) {
    //     conn.reset();
    //     db.Close();
    //     return 1;
    // }
    
    // Call SAMPLED_MATCH function
    std::cout << "\n=== Calling SAMPLED_MATCH ===" << std::endl;
    std::cout << "Pattern: Triangle (3 people who all know each other)" << std::endl;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::string query = "CALL SAMPLED_MATCH('" + pattern_file + "', 100000) RETURN *;";
    std::cout << "Query: " << query << std::endl;
    
    auto res = conn->Query(query);
    if (!res.has_value()) {
        std::cerr << "SAMPLED_MATCH failed: " << res.error().ToString() << std::endl;
        conn.reset();
        db.Close();
        return 1;
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "\n=== Results ===" << std::endl;
    auto& result_rs = res.value();
    int64_t result_count = result_rs.length();
    std::cout << result_rs.ToString() << std::endl;
    
    if (result_count == 0) {
        std::cout << "  No matches found." << std::endl;
    } else {
        std::cout << "\nTotal: " << result_count << " match(es) found." << std::endl;
    }
    
    std::cout << "Query execution time: " << duration.count() << "ms" << std::endl;

    std::cout << "\n============================================" << std::endl;
    std::cout << "Test completed successfully!" << std::endl;
    std::cout << "============================================" << std::endl;
    
    // Cleanup
    conn.reset();
    db.Close();
    
    // Remove temporary pattern file
    if (std::filesystem::exists(pattern_file)) {
        std::filesystem::remove(pattern_file);
    }
    
    return 0;
}
