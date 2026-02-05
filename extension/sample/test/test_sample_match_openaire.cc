#include <neug/main/neug_db.h>
#include <neug/compiler/common/file_system/virtual_file_system.h>
#include <neug/compiler/gopt/g_vfs_holder.h>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <cassert>
#include <chrono>
#include <dlfcn.h>

// OpenAIRE dataset path (converted to LDBC-like format)
const std::string OPENAIRE_DATA_PATH = "/mnt/lyk/openaire_scripts/output/";

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

// Create the graph schema for OpenAIRE
// Synced with process_data.py VERTEX_FILES and EDGE_FILES
bool createSchema(neug::Connection* conn) {
    std::cout << "\n=== Creating Schema ===" << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    
    // Create Publication node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Publication(
            id INT64 PRIMARY KEY,
            label STRING,
            type STRING,
            originalIds STRING,
            mainTitle STRING,
            subTitle STRING,
            publicationDate STRING,
            year STRING,
            publisher STRING,
            FOS STRING,
            container_name STRING,
            countries STRING
        )
    )", "Creating Publication node table")) {
        return false;
    }
    
    // Create Project node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Project(
            id INT64 PRIMARY KEY,
            label STRING,
            acronym STRING,
            title STRING,
            startDate STRING,
            endDate STRING,
            callIdentifier STRING
        )
    )", "Creating Project node table")) {
        return false;
    }
    
    // Create Funding node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Funding(
            id INT64 PRIMARY KEY,
            label STRING,
            shortName STRING,
            name STRING,
            jurisdiction STRING
        )
    )", "Creating Funding node table")) {
        return false;
    }
    
    // Create Organization node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Organization(
            id INT64 PRIMARY KEY,
            label STRING,
            legalShortName STRING,
            legalName STRING,
            country_code STRING,
            country_label STRING
        )
    )", "Creating Organization node table")) {
        return false;
    }
    
    // Create Author node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Author(
            id INT64 PRIMARY KEY,
            label STRING,
            fullName STRING,
            name STRING,
            surname STRING,
            orcid STRING,
            idtype STRING,
            organization STRING,
            country STRING
        )
    )", "Creating Author node table")) {
        return false;
    }
    
    // Create Datasource node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Datasource(
            id INT64 PRIMARY KEY,
            label STRING,
            type_value STRING,
            officialName STRING,
            subjects STRING
        )
    )", "Creating Datasource node table")) {
        return false;
    }
    
    // Create Community node table
    if (!executeQuery(conn, R"(
        CREATE NODE TABLE Community(
            id INT64 PRIMARY KEY,
            label STRING,
            acronym STRING,
            name STRING,
            type STRING,
            subjects STRING
        )
    )", "Creating Community node table")) {
        return false;
    }
    
    // Create edge tables - synced with process_data.py EDGE_FILES (non-commented)
    
    // Publication edges
    if (!executeQuery(conn, R"(
        CREATE REL TABLE publication_hasAuthorInstitution_organization(
            FROM Publication TO Organization
        )
    )", "Creating publication_hasAuthorInstitution_organization edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE publication_isProducedBy_project(
            FROM Publication TO Project
        )
    )", "Creating publication_isProducedBy_project edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE publication_isProvidedBy_datasource(
            FROM Publication TO Datasource
        )
    )", "Creating publication_isProvidedBy_datasource edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE publication_IsRelatedTo_community(
            FROM Publication TO Community
        )
    )", "Creating publication_IsRelatedTo_community edge table")) {
        return false;
    }
    
    // Project edges
    if (!executeQuery(conn, R"(
        CREATE REL TABLE project_hasParticipant_organization(
            FROM Project TO Organization
        )
    )", "Creating project_hasParticipant_organization edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE project_IsRelatedTo_community(
            FROM Project TO Community
        )
    )", "Creating project_IsRelatedTo_community edge table")) {
        return false;
    }
    
    // Funding edges
    if (!executeQuery(conn, R"(
        CREATE REL TABLE funding_for_project(
            FROM Funding TO Project
        )
    )", "Creating funding_for_project edge table")) {
        return false;
    }
    
    // Organization edges
    if (!executeQuery(conn, R"(
        CREATE REL TABLE organization_provides_datasource(
            FROM Organization TO Datasource
        )
    )", "Creating organization_provides_datasource edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE organization_IsParentOf_organization(
            FROM Organization TO Organization
        )
    )", "Creating organization_IsParentOf_organization edge table")) {
        return false;
    }
    
    if (!executeQuery(conn, R"(
        CREATE REL TABLE organization_IsRelatedTo_community(
            FROM Organization TO Community
        )
    )", "Creating organization_IsRelatedTo_community edge table")) {
        return false;
    }
    
    // Datasource edges
    if (!executeQuery(conn, R"(
        CREATE REL TABLE datasource_IsRelatedTo_community(
            FROM Datasource TO Community
        )
    )", "Creating datasource_IsRelatedTo_community edge table")) {
        return false;
    }
    
    // Publication-Author edges
    if (!executeQuery(conn, R"(
        CREATE REL TABLE publication_authoredBy_author(
            FROM Author TO Publication
        )
    )", "Creating publication_authoredBy_author edge table")) {
        return false;
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Schema created successfully in " << duration.count() << "ms!" << std::endl;
    return true;
}

// Load data from CSV files - synced with process_data.py
bool loadData(neug::Connection* conn) {
    std::cout << "\n=== Loading Data from CSV Files ===" << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    
    // Load vertex data
    std::cout << "\n  Loading vertex data..." << std::endl;
    
    if (!executeQuery(conn, "COPY Publication FROM '" + OPENAIRE_DATA_PATH + "publication.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Publication nodes")) return false;
    
    if (!executeQuery(conn, "COPY Project FROM '" + OPENAIRE_DATA_PATH + "project.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Project nodes")) return false;
    
    if (!executeQuery(conn, "COPY Funding FROM '" + OPENAIRE_DATA_PATH + "funding.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Funding nodes")) return false;
    
    if (!executeQuery(conn, "COPY Organization FROM '" + OPENAIRE_DATA_PATH + "organization.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Organization nodes")) return false;
    
    if (!executeQuery(conn, "COPY Author FROM '" + OPENAIRE_DATA_PATH + "author.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Author nodes")) return false;
    
    if (!executeQuery(conn, "COPY Datasource FROM '" + OPENAIRE_DATA_PATH + "datasource.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Datasource nodes")) return false;
    
    if (!executeQuery(conn, "COPY Community FROM '" + OPENAIRE_DATA_PATH + "community.csv' (HEADER=true, DELIMITER=',')", 
        "Loading Community nodes")) return false;
    
    // Load edge data - synced with process_data.py EDGE_FILES
    std::cout << "\n  Loading edge data..." << std::endl;
    
    // Publication edges
    if (!executeQuery(conn, "COPY publication_hasAuthorInstitution_organization FROM '" + OPENAIRE_DATA_PATH + "publication_hasAuthorInstitution_organization.csv' (HEADER=true, DELIMITER=',')", 
        "Loading publication_hasAuthorInstitution_organization edges")) return false;
    
    if (!executeQuery(conn, "COPY publication_isProducedBy_project FROM '" + OPENAIRE_DATA_PATH + "publication_isProducedBy_project.csv' (HEADER=true, DELIMITER=',')", 
        "Loading publication_isProducedBy_project edges")) return false;
    
    if (!executeQuery(conn, "COPY publication_isProvidedBy_datasource FROM '" + OPENAIRE_DATA_PATH + "publication_isProvidedBy_datasource.csv' (HEADER=true, DELIMITER=',')", 
        "Loading publication_isProvidedBy_datasource edges")) return false;
    
    if (!executeQuery(conn, "COPY publication_IsRelatedTo_community FROM '" + OPENAIRE_DATA_PATH + "publication_IsRelatedTo_community.csv' (HEADER=true, DELIMITER=',')", 
        "Loading publication_IsRelatedTo_community edges")) return false;
    
    // Project edges
    if (!executeQuery(conn, "COPY project_hasParticipant_organization FROM '" + OPENAIRE_DATA_PATH + "project_hasParticipant_organization.csv' (HEADER=true, DELIMITER=',')", 
        "Loading project_hasParticipant_organization edges")) return false;
    
    if (!executeQuery(conn, "COPY project_IsRelatedTo_community FROM '" + OPENAIRE_DATA_PATH + "project_IsRelatedTo_community.csv' (HEADER=true, DELIMITER=',')", 
        "Loading project_IsRelatedTo_community edges")) return false;
    
    // Funding edges
    if (!executeQuery(conn, "COPY funding_for_project FROM '" + OPENAIRE_DATA_PATH + "funding_for_project.csv' (HEADER=true, DELIMITER=',')", 
        "Loading funding_for_project edges")) return false;
    
    // Organization edges
    if (!executeQuery(conn, "COPY organization_provides_datasource FROM '" + OPENAIRE_DATA_PATH + "organization_provides_datasource.csv' (HEADER=true, DELIMITER=',')", 
        "Loading organization_provides_datasource edges")) return false;
    
    if (!executeQuery(conn, "COPY organization_IsParentOf_organization FROM '" + OPENAIRE_DATA_PATH + "organization_IsParentOf_organization.csv' (HEADER=true, DELIMITER=',')", 
        "Loading organization_IsParentOf_organization edges")) return false;
    
    if (!executeQuery(conn, "COPY organization_IsRelatedTo_community FROM '" + OPENAIRE_DATA_PATH + "organization_IsRelatedTo_community.csv' (HEADER=true, DELIMITER=',')", 
        "Loading organization_IsRelatedTo_community edges")) return false;
    
    // Datasource edges
    if (!executeQuery(conn, "COPY datasource_IsRelatedTo_community FROM '" + OPENAIRE_DATA_PATH + "datasource_IsRelatedTo_community.csv' (HEADER=true, DELIMITER=',')", 
        "Loading datasource_IsRelatedTo_community edges")) return false;
    
    // Publication-Author edges
    if (!executeQuery(conn, "COPY publication_authoredBy_author FROM '" + OPENAIRE_DATA_PATH + "publication_authoredBy_author.csv' (HEADER=true, DELIMITER=',')", 
        "Loading publication_authoredBy_author edges")) return false;
    
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

// Create pattern file for OpenAIRE queries
bool createPatternFile(const std::string& pattern_file, int pattern_type) {
    std::cout << "\n=== Creating Pattern File ===" << std::endl;
    
    // Create directory if needed
    std::filesystem::path p(pattern_file);
    std::filesystem::create_directories(p.parent_path());
    
    std::ofstream f(pattern_file);
    if (!f.is_open()) {
        std::cerr << "Failed to create pattern file: " << pattern_file << std::endl;
        return false;
    }
    
    if (pattern_type == 1) {
        // Pattern 1: Publication -> Organization <- Project
        // Find publications that are related to organizations which participate in projects
        f << R"({
    "vertices": [
        {"id": 0, "label": "Publication"},
        {"id": 1, "label": "Organization"},
        {"id": 2, "label": "Project"}
    ],
    "edges": [
        {"src": 0, "dst": 1, "label": "publication_hasAuthorInstitution_organization"},
        {"src": 2, "dst": 1, "label": "project_hasParticipant_organization"}
    ]
})";
        std::cout << "Pattern 1: Publication -> Organization <- Project" << std::endl;
    } else if (pattern_type == 2) {
        // Pattern 2: Funding -> Project -> Organization
        f << R"({
    "vertices": [
        {"id": 0, "label": "Funding"},
        {"id": 1, "label": "Project"},
        {"id": 2, "label": "Organization"}
    ],
    "edges": [
        {"src": 0, "dst": 1, "label": "funding_for_project"},
        {"src": 1, "dst": 2, "label": "project_hasParticipant_organization"}
    ]
})";
        std::cout << "Pattern 2: Funding -> Project -> Organization" << std::endl;
    } else if (pattern_type == 3) {
        // Pattern 3: Publication -> Project <- Funding
        f << R"({
    "vertices": [
        {"id": 0, "label": "Publication"},
        {"id": 1, "label": "Project"},
        {"id": 2, "label": "Funding"}
    ],
    "edges": [
        {"src": 0, "dst": 1, "label": "publication_isProducedBy_project"},
        {"src": 2, "dst": 1, "label": "funding_for_project"}
    ]
})";
        std::cout << "Pattern 3: Publication -> Project <- Funding" << std::endl;
    } else if (pattern_type == 4) {
        // Pattern 4: Author -> Publication -> Datasource
        f << R"({
    "vertices": [
        {"id": 0, "label": "Author"},
        {"id": 1, "label": "Publication"},
        {"id": 2, "label": "Datasource"}
    ],
    "edges": [
        {"src": 0, "dst": 1, "label": "publication_authoredBy_author"},
        {"src": 1, "dst": 2, "label": "publication_isProvidedBy_datasource"}
    ]
})";
        std::cout << "Pattern 4: Author -> Publication -> Datasource" << std::endl;
    } else if (pattern_type == 5) {
        // Pattern 5: Publication -> Community <- Project
        f << R"({
    "vertices": [
        {"id": 0, "label": "Publication"},
        {"id": 1, "label": "Community"},
        {"id": 2, "label": "Project"}
    ],
    "edges": [
        {"src": 0, "dst": 1, "label": "publication_IsRelatedTo_community"},
        {"src": 2, "dst": 1, "label": "project_IsRelatedTo_community"}
    ]
})";
        std::cout << "Pattern 5: Publication -> Community <- Project" << std::endl;
    } else {
        // Default Pattern 0: Publication -> Organization
        f << R"({
    "vertices": [
        {"id": 0, "label": "Publication"},
        {"id": 1, "label": "Organization"}
    ],
    "edges": [
        {"src": 0, "dst": 1, "label": "publication_hasAuthorInstitution_organization"}
    ]
})";
        std::cout << "Pattern 0: Publication -> Organization" << std::endl;
    }
    
    f.close();
    std::cout << "Pattern file created: " << pattern_file << std::endl;
    return true;
}

int main(int argc, char* argv[]) {
    std::cout << "================================================" << std::endl;
    std::cout << "NeuG SAMPLED_MATCH Extension Test (OpenAIRE)" << std::endl;
    std::cout << "================================================" << std::endl;
    
    // Setup VFS (required for neug)
    auto vfs = std::make_unique<neug::common::VirtualFileSystem>();
    neug::common::VFSHolder::setVFS(vfs.get());
    
    // Use a temporary database path
    std::string db_path = "/tmp/neug_openaire_db";
    std::string pattern_file = "/tmp/p/openaire.json";  // Keep path short (< 48 chars)
    
    // Check command line arguments
    bool reload_data = true;
    
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
    
    // Load extension
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
    
    // Create pattern file
    // if (!createPatternFile(pattern_file, pattern_type)) {
    //     conn.reset();
    //     db.Close();
    //     return 1;
    // }
    
    // Call SAMPLED_MATCH function
    std::cout << "\n=== Calling SAMPLED_MATCH ===" << std::endl;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::string query = "CALL SAMPLED_MATCH('" + pattern_file + "')";
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
    int result_count = 0;
    while (result_rs.hasNext()) {
        auto row = result_rs.next();
        if (result_count < 10) {  // Only show first 10 results
            std::cout << "  " << row.ToString() << std::endl;
        }
        result_count++;
    }
    
    if (result_count == 0) {
        std::cout << "  No matches found." << std::endl;
    } else {
        if (result_count > 10) {
            std::cout << "  ... (" << (result_count - 10) << " more results)" << std::endl;
        }
        std::cout << "\nTotal: " << result_count << " match(es) found." << std::endl;
    }
    
    std::cout << "Query execution time: " << duration.count() << "ms" << std::endl;

    std::cout << "\n================================================" << std::endl;
    std::cout << "Test completed successfully!" << std::endl;
    std::cout << "================================================" << std::endl;
    
    // Cleanup
    conn.reset();
    db.Close();
    
    // Remove temporary pattern file
    if (std::filesystem::exists(pattern_file)) {
        std::filesystem::remove(pattern_file);
    }
    
    return 0;
}
