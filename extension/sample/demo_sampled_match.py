#!/usr/bin/env python3
"""
Demo script for the SAMPLED_MATCH extension.

This script demonstrates:
1. Creating a NeuG database
2. Defining graph schema (node and edge tables)
3. Inserting sample data
4. Loading the sample extension
5. Using SAMPLED_MATCH for subgraph pattern matching

Usage:
    python3 demo_sampled_match.py

Requirements:
    - NeuG Python binding installed (pip install neug or build from source)
    - Sample extension built (make neug_sample_extension)
"""

import os
import sys
import json
import tempfile
import shutil

# Try to import neug with helpful error messages
NEUG_AVAILABLE = False
try:
    import neug
    NEUG_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import neug module: {e}")
    print("\nTo use this script, you need to:")
    print("1. Build NeuG: cd /mnt/lyk/neug/build && cmake .. && make -j")
    print("2. Build Python binding: cd /mnt/lyk/neug/tools/python_bind && python3 setup.py build")
    print("3. Install: pip install /mnt/lyk/neug/tools/python_bind/dist/neug-*.whl")
    print("\nAlternatively, use the C++ test program:")
    print("  /mnt/lyk/neug/build/extension/sample/test/test_sample_match")
    print("\nThis script will now print the Cypher commands that would be executed.")
    print("=" * 60)


def get_extension_path():
    """Get the path to the sample extension library."""
    # Try build directory first
    build_path = "/mnt/lyk/neug/build/extension/sample/libsample.neug_extension"
    if os.path.exists(build_path):
        return build_path
    
    # Fallback to user install directory
    home = os.environ.get("HOME", "")
    if home:
        user_path = os.path.join(home, ".neug/extensions/sample/libsample.neug_extension")
        if os.path.exists(user_path):
            return user_path
    
    return build_path


def create_schema(conn):
    """Create the graph schema with node and edge tables."""
    print("\n=== Creating Schema ===")
    
    # Create Person node table
    conn.execute("""
        CREATE NODE TABLE Person(
            id INT32 PRIMARY KEY,
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
    """)
    print("Created Person node table")
    
    # Create Comment node table
    conn.execute("""
        CREATE NODE TABLE Comment(
            id INT32 PRIMARY KEY,
            creationDate STRING,
            locationIP STRING,
            browserUsed STRING,
            content STRING,
            length INT32
        )
    """)
    print("Created Comment node table")
    
    # Create Post node table
    conn.execute("""
        CREATE NODE TABLE Post(
            id INT32 PRIMARY KEY,
            imageFile STRING,
            creationDate STRING,
            locationIP STRING,
            browserUsed STRING,
            language STRING,
            content STRING,
            length INT32
        )
    """)
    print("Created Post node table")
    
    # Create edge tables
    conn.execute("""
        CREATE REL TABLE person_knows_person(
            FROM Person TO Person,
            creationDate STRING
        )
    """)
    print("Created person_knows_person edge table")
    
    conn.execute("""
        CREATE REL TABLE comment_hasCreator_person(
            FROM Comment TO Person
        )
    """)
    print("Created comment_hasCreator_person edge table")
    
    conn.execute("""
        CREATE REL TABLE post_hasCreator_person(
            FROM Post TO Person
        )
    """)
    print("Created post_hasCreator_person edge table")
    
    print("Schema created successfully!")


def insert_data(conn):
    """Insert sample data into the graph."""
    print("\n=== Inserting Data ===")
    
    # Insert Person nodes
    persons = [
        (0, "Alice", "Anderson", "female", "1990-01-15", "2020-01-01", "192.168.1.1", "Chrome", "en", "alice@example.com"),
        (1, "Bob", "Brown", "male", "1985-06-20", "2020-02-15", "192.168.1.2", "Firefox", "en", "bob@example.com"),
        (2, "Carol", "Chen", "female", "1992-03-10", "2020-03-20", "192.168.1.3", "Safari", "zh;en", "carol@example.com"),
    ]
    
    for p in persons:
        conn.execute(f"""
            CREATE (n:Person {{
                id: {p[0]},
                firstName: '{p[1]}',
                lastName: '{p[2]}',
                gender: '{p[3]}',
                birthday: '{p[4]}',
                creationDate: '{p[5]}',
                locationIP: '{p[6]}',
                browserUsed: '{p[7]}',
                language: '{p[8]}',
                email: '{p[9]}'
            }})
        """)
    print(f"Inserted {len(persons)} Person nodes")
    
    # Insert Comment nodes
    comments = [
        (3, "2020-04-01", "192.168.1.4", "Chrome", "This is a great post!", 21),
    ]
    
    for c in comments:
        conn.execute(f"""
            CREATE (n:Comment {{
                id: {c[0]},
                creationDate: '{c[1]}',
                locationIP: '{c[2]}',
                browserUsed: '{c[3]}',
                content: '{c[4]}',
                length: {c[5]}
            }})
        """)
    print(f"Inserted {len(comments)} Comment nodes")
    
    # Insert Post nodes
    posts = [
        (4, "photo1.jpg", "2020-03-25", "192.168.1.5", "Firefox", "en", "My first post!", 15),
        (5, "photo2.jpg", "2020-03-26", "192.168.1.6", "Chrome", "en", "Another post!", 13),
    ]
    
    for p in posts:
        conn.execute(f"""
            CREATE (n:Post {{
                id: {p[0]},
                imageFile: '{p[1]}',
                creationDate: '{p[2]}',
                locationIP: '{p[3]}',
                browserUsed: '{p[4]}',
                language: '{p[5]}',
                content: '{p[6]}',
                length: {p[7]}
            }})
        """)
    print(f"Inserted {len(posts)} Post nodes")
    
    # Insert person_knows_person edges (forming a triangle: 0->1, 1->2, 2->0)
    knows_edges = [
        (0, 1, "2020-05-01"),
        (1, 2, "2020-05-15"),
        (2, 0, "2020-06-01"),
    ]
    
    for e in knows_edges:
        conn.execute(f"""
            MATCH (a:Person), (b:Person) 
            WHERE a.id = {e[0]} AND b.id = {e[1]}
            CREATE (a)-[:person_knows_person {{creationDate: '{e[2]}'}}]->(b)
        """)
    print(f"Inserted {len(knows_edges)} person_knows_person edges")
    
    # Insert comment_hasCreator_person edges
    comment_edges = [(3, 0)]
    for e in comment_edges:
        conn.execute(f"""
            MATCH (c:Comment), (p:Person) 
            WHERE c.id = {e[0]} AND p.id = {e[1]}
            CREATE (c)-[:comment_hasCreator_person]->(p)
        """)
    print(f"Inserted {len(comment_edges)} comment_hasCreator_person edges")
    
    # Insert post_hasCreator_person edges
    post_edges = [(4, 0), (5, 1)]
    for e in post_edges:
        conn.execute(f"""
            MATCH (p:Post), (person:Person) 
            WHERE p.id = {e[0]} AND person.id = {e[1]}
            CREATE (p)-[:post_hasCreator_person]->(person)
        """)
    print(f"Inserted {len(post_edges)} post_hasCreator_person edges")
    
    print("Data inserted successfully!")


def create_pattern_file(pattern_path):
    """Create a pattern file for SAMPLED_MATCH query."""
    # Pattern: Find a triangle of three people who all know each other
    pattern = {
        "description": "Find a triangle of three people who all know each other",
        "vertices": [
            {"id": 0, "label": "person"},
            {"id": 1, "label": "person"},
            {"id": 2, "label": "person"}
        ],
        "edges": [
            {"source": 0, "target": 1, "label": "person_knows_person"},
            {"source": 1, "target": 2, "label": "person_knows_person"},
            {"source": 2, "target": 0, "label": "person_knows_person"}
        ]
    }
    
    os.makedirs(os.path.dirname(pattern_path), exist_ok=True)
    with open(pattern_path, 'w') as f:
        json.dump(pattern, f, indent=2)
    
    print(f"Created pattern file: {pattern_path}")
    return pattern


def run_sampled_match(conn, pattern_path):
    """Run the SAMPLED_MATCH query."""
    print("\n=== Running SAMPLED_MATCH ===")
    print(f"Pattern file: {pattern_path}")
    
    # Read and display the pattern
    with open(pattern_path, 'r') as f:
        pattern = json.load(f)
    print(f"Pattern description: {pattern.get('description', 'N/A')}")
    print(f"Vertices: {len(pattern['vertices'])}, Edges: {len(pattern['edges'])}")
    
    # Execute SAMPLED_MATCH query
    query = f"CALL SAMPLED_MATCH('{pattern_path}')"
    print(f"Query: {query}")
    
    result = conn.execute(query)
    
    print("\n=== Results ===")
    row_count = 0
    for row in result:
        print(row)
        row_count += 1
    
    if row_count == 0:
        print("No results found.")
    else:
        print(f"Total: {row_count} result(s)")
    
    return row_count


def verify_data(conn):
    """Verify the inserted data with some simple queries."""
    print("\n=== Verifying Data ===")
    
    # Count nodes
    result = conn.execute("MATCH (p:Person) RETURN count(*)")
    for row in result:
        print(f"Person count: {row[0]}")
    
    result = conn.execute("MATCH (c:Comment) RETURN count(*)")
    for row in result:
        print(f"Comment count: {row[0]}")
    
    result = conn.execute("MATCH (p:Post) RETURN count(*)")
    for row in result:
        print(f"Post count: {row[0]}")
    
    # Show knows relationships
    print("\nPerson knows Person relationships:")
    result = conn.execute("""
        MATCH (a:Person)-[k:person_knows_person]->(b:Person)
        RETURN a.firstName, b.firstName, k.creationDate
    """)
    for row in result:
        print(f"  {row[0]} knows {row[1]} (since {row[2]})")


def print_cypher_commands():
    """Print all Cypher commands that would be executed (for reference)."""
    print("\n" + "=" * 60)
    print("Cypher Commands Reference")
    print("=" * 60)
    
    print("\n-- 1. Create Schema --")
    print("""
CREATE NODE TABLE Person(
    id INT32 PRIMARY KEY,
    firstName STRING,
    lastName STRING,
    gender STRING,
    birthday STRING,
    creationDate STRING,
    locationIP STRING,
    browserUsed STRING,
    language STRING,
    email STRING
);

CREATE NODE TABLE Comment(
    id INT32 PRIMARY KEY,
    creationDate STRING,
    locationIP STRING,
    browserUsed STRING,
    content STRING,
    length INT32
);

CREATE NODE TABLE Post(
    id INT32 PRIMARY KEY,
    imageFile STRING,
    creationDate STRING,
    locationIP STRING,
    browserUsed STRING,
    language STRING,
    content STRING,
    length INT32
);

CREATE REL TABLE person_knows_person(
    FROM Person TO Person,
    creationDate STRING
);

CREATE REL TABLE comment_hasCreator_person(
    FROM Comment TO Person
);

CREATE REL TABLE post_hasCreator_person(
    FROM Post TO Person
);
""")
    
    print("\n-- 2. Insert Data --")
    print("""
-- Insert Person nodes
CREATE (n:Person {id: 0, firstName: 'Alice', lastName: 'Anderson', gender: 'female', 
    birthday: '1990-01-15', creationDate: '2020-01-01', locationIP: '192.168.1.1', 
    browserUsed: 'Chrome', language: 'en', email: 'alice@example.com'});

CREATE (n:Person {id: 1, firstName: 'Bob', lastName: 'Brown', gender: 'male',
    birthday: '1985-06-20', creationDate: '2020-02-15', locationIP: '192.168.1.2',
    browserUsed: 'Firefox', language: 'en', email: 'bob@example.com'});

CREATE (n:Person {id: 2, firstName: 'Carol', lastName: 'Chen', gender: 'female',
    birthday: '1992-03-10', creationDate: '2020-03-20', locationIP: '192.168.1.3',
    browserUsed: 'Safari', language: 'zh;en', email: 'carol@example.com'});

-- Insert edges (forming a triangle: 0->1, 1->2, 2->0)
MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 1
CREATE (a)-[:person_knows_person {creationDate: '2020-05-01'}]->(b);

MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2
CREATE (a)-[:person_knows_person {creationDate: '2020-05-15'}]->(b);

MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 0
CREATE (a)-[:person_knows_person {creationDate: '2020-06-01'}]->(b);
""")
    
    print("\n-- 3. Load Extension and Run Query --")
    ext_path = get_extension_path()
    print(f"""
-- Load the sample extension
LOAD '{ext_path}';

-- Run SAMPLED_MATCH with a triangle pattern
-- Pattern file content (save as /tmp/pattern.json):
-- {{
--   "vertices": [
--     {{"id": 0, "label": "person"}},
--     {{"id": 1, "label": "person"}},
--     {{"id": 2, "label": "person"}}
--   ],
--   "edges": [
--     {{"source": 0, "target": 1, "label": "person_knows_person"}},
--     {{"source": 1, "target": 2, "label": "person_knows_person"}},
--     {{"source": 2, "target": 0, "label": "person_knows_person"}}
--   ]
-- }}

CALL SAMPLED_MATCH('/tmp/pattern.json');
""")


def main():
    """Main function to run the demo."""
    print("=" * 60)
    print("NeuG SAMPLED_MATCH Extension Demo")
    print("=" * 60)
    
    # If neug is not available, just print the commands
    if not NEUG_AVAILABLE:
        print_cypher_commands()
        return 0
    
    # Use a temporary directory for the database
    db_path = tempfile.mkdtemp(prefix="neug_sample_demo_")
    pattern_path = "/tmp/pattern_demo.json"
    
    print(f"\nDatabase path: {db_path}")
    
    try:
        # Create database
        print("\n=== Creating Database ===")
        db = neug.Database(db_path)
        conn = db.connect()
        print("Database created and connected")
        
        # Create schema
        create_schema(conn)
        
        # Insert data
        insert_data(conn)
        
        # Verify data
        verify_data(conn)
        
        # Load extension
        print("\n=== Loading Extension ===")
        ext_path = get_extension_path()
        if not os.path.exists(ext_path):
            print(f"Warning: Extension not found at {ext_path}")
            print("Please build the extension first: cd /mnt/lyk/neug/build && make neug_sample_extension")
            print("Skipping SAMPLED_MATCH demo...")
        else:
            try:
                # Load extension using LOAD command
                conn.execute(f"LOAD '{ext_path}'")
                print(f"Extension loaded from: {ext_path}")
                
                # Create pattern file
                create_pattern_file(pattern_path)
                
                # Run SAMPLED_MATCH
                run_sampled_match(conn, pattern_path)
                
            except Exception as e:
                print(f"Error loading/running extension: {e}")
                print("You may need to use the C++ test program for extension testing.")
        
        # Close connection and database
        conn.close()
        db.close()
        print("\n=== Demo completed ===")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        # Cleanup temporary files
        if os.path.exists(pattern_path):
            os.remove(pattern_path)
        if os.path.exists(db_path):
            shutil.rmtree(db_path, ignore_errors=True)
        print(f"\nCleaned up temporary files")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
