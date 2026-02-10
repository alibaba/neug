# Exploring Social Networks with NeuG

Welcome to this comprehensive tutorial using the **TinySNB** (Tiny Social Network Benchmark) dataset! This Python tutorial will guide you through exploring a small social network graph database, demonstrating the power of graph queries for social network analysis with NeuG.

## What is TinySNB?

TinySNB, given by [Kuzu](https://kuzudb.com/) for test purpose, is a small social network dataset that models relationships between people, organizations, and movies. It's perfect for learning graph database concepts and testing queries against synthetic, real-life use cases.

The dataset contains:
- **People**: Individuals with personal information (name, age, occupation status, etc.)
- **Organizations**: Universities and companies where people study or work
- **Movies**: Films with ratings and descriptions
- **Relationships**: Social connections, work relationships, academic affiliations, and more


## Getting Started

Let's begin by loading the TinySNB dataset and exploring its structure.
[Install NeuG](../../installation/installation) if you have not done so. 

### Loading the Dataset

```python
import neug
import os

db_path = '/path/to/database'

if not os.path.exists(db_path):
    # First, let's load the builtin TinySNB dataset into a new database
    db = neug.Database(db_path)
    db.load_builtin_dataset('tinysnb')
else:
    # if the path exists, directly open the database without extra loading
    db = neug.Database(db_path)

conn = db.connect()

print("TinySNB dataset loaded successfully!")
```

### Exploring the Schema

Let's understand what types of nodes and relationships exist in our social network:

```python
# Get basic statistics about our graph
result = list(conn.execute("MATCH (n) RETURN count(n) as total_nodes"))
total_nodes = result[0][0]
print(f"Total nodes in the graph: {total_nodes}")

# Count nodes by type
result = list(conn.execute("MATCH (p:person) RETURN count(p) as people_count"))
people_count = result[0][0]
print(f"Number of people: {people_count}")

result = list(conn.execute("MATCH (o:organisation) RETURN count(o) as org_count"))
org_count = result[0][0]
print(f"Number of organizations: {org_count}")

result = list(conn.execute("MATCH (m:movies) RETURN count(m) as movie_count"))
movie_count = result[0][0]
print(f"Number of movies: {movie_count}")
```

## Exploring People in Our Social Network

### Basic Person Queries

Let's start by exploring the people in our social network:

```python
# Get all people with their basic information
print("=== People in our social network ===")
result = conn.execute("""
    MATCH (p:person) 
    RETURN p.fName, p.age, p.isStudent, p.isWorker 
    ORDER BY p.age
""")

for record in result:
    name, age, is_student, is_worker = record
    status = []
    if is_student:
        status.append("Student")
    if is_worker:
        status.append("Worker")
    status_str = " & ".join(status) if status else "Neither student nor worker"
    print(f"{name} (age {age}): {status_str}")
```

### Filtering and Conditional Queries

```python
# Find all students
print("\n=== Students in our network ===")
result = conn.execute("""
    MATCH (p:person) 
    WHERE p.isStudent = true
    RETURN p.fName, p.age
    ORDER BY p.age
""")

for record in result:
    print(f"{record[0]} (age {record[1]})")

# Find working adults (workers who are not students)
print("\n=== Working adults (non-students) ===")
result = conn.execute("""
    MATCH (p:person) 
    WHERE p.isWorker = true AND p.isStudent = false
    RETURN p.fName, p.age
    ORDER BY p.age DESC
""")

for record in result:
    print(f"{record[0]} (age {record[1]})")

# Find people in their thirties
print("\n=== People in their thirties ===")
result = conn.execute("""
    MATCH (p:person) 
    WHERE p.age >= 30 AND p.age < 40
    RETURN p.fName, p.age
    ORDER BY p.age
""")

for record in result:
    print(f"{record[0]} is {record[1]} years old")
```

## Social Network Analysis: Relationships

Now let's explore the relationships between people - this is where graph databases really shine!

### Who Knows Whom?

```python
# Explore the "knows" relationships
print("=== Social connections (who knows whom) ===")
result = conn.execute("""
    MATCH (p1:person)-[k:knows]->(p2:person)
    RETURN p1.fName, p2.fName, k.date
    ORDER BY p1.fName, p2.fName
""")

for record in result:
    print(f"{record[0]} knows {record[1]} (since {record[2]})")
```

### Finding Popular People

```python
# Who has the most connections?
print("\n=== Most connected people ===")
result = conn.execute("""
    MATCH (p:person)-[k:knows]->(friend:person)
    RETURN p.fName, count(friend) as friend_count
    ORDER BY friend_count DESC
    LIMIT 5
""")

for record in result:
    print(f"{record[0]} knows {record[1]} people")

# Who is known by the most people?
print("\n=== Most popular people (known by others) ===")
result = conn.execute("""
    MATCH (p:person)<-[k:knows]-(friend:person)
    RETURN p.fName, count(friend) as known_by_count
    ORDER BY known_by_count DESC
    LIMIT 5
""")

for record in result:
    print(f"{record[0]} is known by {record[1]} people")
```

### Mutual Connections

```python
print("\n=== Mutual friendships ===")
result = conn.execute("""
    MATCH (p1:person)-[k1:knows]->(p2:person),
          (p2:person)-[k2:knows]->(p1:person)
    WHERE p1.id < p2.id  // Avoid duplicates
    RETURN p1.fName, p2.fName
    ORDER BY p1.fName
""")

for record in result:
    print(f"{record[0]} and {record[1]} know each other")
```

## Professional Networks: Work and Education

### Academic Connections

```python
# Who studies where?
print("=== Academic affiliations ===")
result = conn.execute("""
    MATCH (p:person)-[s:studyAt]->(o:organisation)
    RETURN p.fName, o.name, s.year
    ORDER BY s.year DESC
""")

for record in result:
    print(f"{record[0]} studied at {record[1]} in {record[2]}")

# Which organizations have the most students?
print("\n=== Most popular educational institutions ===")
result = conn.execute("""
    MATCH (p:person)-[s:studyAt]->(o:organisation)
    RETURN o.name, count(p) as student_count
    ORDER BY student_count DESC
""")

for record in result:
    print(f"{record[0]}: {record[1]} students")
```

### Professional Connections

```python
# Who works where?
print("\n=== Professional affiliations ===")
result = conn.execute("""
    MATCH (p:person)-[w:workAt]->(o:organisation)
    RETURN p.fName, o.name, w.year, w.rating
    ORDER BY w.year DESC
""")

for record in result:
    rating = record[3] if record[3] else "N/A"
    print(f"{record[0]} works at {record[1]} (since {record[2]}, rating: {rating})")
```

## Advanced Pattern Matching

### Multi-hop Relationships

```python
# Find friends of friends (2-degree connections)
print("=== Friends of friends (2-degree connections) ===")
result = conn.execute("""
    MATCH (p1:person)-[:knows]->(mutual:person)-[:knows]->(p2:person)
    WHERE p1.id <> p2.id  // Different people
    AND NOT (p1)-[:knows]-(p2)  // Not direct friends
    RETURN p1.fName, p2.fName, mutual.fName
    ORDER BY p1.fName
""")

for record in result:
    print(f"{record[0]} could meet {record[1]} through {record[2]}")
```

### Colleagues and Classmates

```python
# Find people who work at the same organization
print("\n=== Colleagues (people working at the same organization) ===")
result = conn.execute("""
    MATCH (p1:person)-[:workAt]->(o:organisation)<-[:workAt]-(p2:person)
    WHERE p1.id < p2.id  // Avoid duplicates
    RETURN p1.fName, p2.fName, o.name
    ORDER BY o.name
""")

for record in result:
    print(f"{record[0]} and {record[1]} work together at {record[2]}")

# Find people who studied at the same organization
print("\n=== Alumni/Classmates (people who studied at the same institution) ===")
result = conn.execute("""
    MATCH (p1:person)-[s1:studyAt]->(o:organisation)<-[s2:studyAt]-(p2:person)
    WHERE p1.id < p2.id
    RETURN p1.fName, p2.fName, o.name, s1.year, s2.year
    ORDER BY o.name
""")

for record in result:
    if record[3] == record[4]:
        print(f"{record[0]} and {record[1]} were classmates at {record[2]} in {record[3]}")
    else:
        print(f"{record[0]} and {record[1]} both studied at {record[2]} ({record[3]} and {record[4]})")
```


## Social Network Analytics

### Network Density and Connectivity

```python
# Calculate basic network metrics
print("=== Network Statistics ===")

# Total possible connections vs actual connections
result = list(conn.execute("MATCH (p:person) RETURN count(p) as person_count"))
person_count = result[0][0]

result = list(conn.execute("MATCH ()-[k:knows]->() RETURN count(k) as connections"))
actual_connections = result[0][0]

max_possible = person_count * (person_count - 1)  # Directed graph
density = (actual_connections / max_possible) * 100 if max_possible > 0 else 0

print(f"People in network: {person_count}")
print(f"Actual connections: {actual_connections}")
print(f"Maximum possible connections: {max_possible}")
print(f"Network density: {density:.2f}%")
```

### Identifying Network Hubs

```python
# Find the most connected individuals (network hubs)
print("\n=== Network Hubs (most connected individuals) ===")
result = conn.execute("""
    MATCH (p:person)
    OPTIONAL MATCH (p)-[out:knows]->()
    OPTIONAL MATCH (p)<-[i:knows]-()
    RETURN p.fName, 
           count(DISTINCT out) as outgoing,
           count(DISTINCT i) as incoming,
           count(DISTINCT out) + count(DISTINCT i) as total_connections
    ORDER BY total_connections DESC
    LIMIT 5
""")

for record in result:
    print(f"{record[0]}: {record[3]} total connections ({record[1]} outgoing, {record[2]} incoming)")
```

### Age-based Social Analysis

```python
# Analyze social connections by age groups
print("\n=== Social connections across age groups ===")
result = conn.execute("""
    MATCH (p1:person)-[:knows]->(p2:person)
    WITH p1, p2,
         CASE 
             WHEN p1.age < 25 THEN "Young (< 25)"
             WHEN p1.age < 35 THEN "Adult (25-34)" 
             WHEN p1.age < 50 THEN "Middle-aged (35-49)"
             ELSE "Senior (50+)"
         END as age_group1,
         CASE 
             WHEN p2.age < 25 THEN "Young (< 25)"
             WHEN p2.age < 35 THEN "Adult (25-34)"
             WHEN p2.age < 50 THEN "Middle-aged (35-49)" 
             ELSE "Senior (50+)"
         END as age_group2
    RETURN age_group1, age_group2, count(*) as connection_count
    ORDER BY connection_count DESC
""")

for record in result:
    print(f"{record[0]} → {record[1]}: {record[2]} connections")
```

## Conclusion

In this tutorial, you've learned how to:

1. **Load builtin datasets** using NeuG's dataset functionality
2. **Explore graph schema** and understand the structure of your data
3. **Perform basic queries** to find and filter nodes
4. **Analyze relationships** between entities in your graph
5. **Use pattern matching** to find complex relationships and paths
6. **Calculate network metrics** to understand social network properties
7. **Combine multiple relationship types** to gain insights from interconnected data

### Key Takeaways

- **Graph databases excel at relationship queries**: Finding patterns like "friends of friends" or "colleagues who are also friends" is natural and efficient
- **Pattern matching is powerful**: Complex queries that would require multiple joins in SQL become intuitive graph patterns
- **Social network analysis**: Graph databases provide built-in support for analyzing network structures, connectivity, and influence

### Next Steps

To continue your NeuG journey:

1. **Try loading your own data**: Use the schema patterns you've learned to model your own relationships
2. **Explore larger datasets**: Test your queries on bigger social networks
3. **Learn advanced Cypher**: Dive deeper into aggregations, path algorithms, and graph analytics
4. **Performance optimization**: Learn about indexing and query optimization for larger graphs

### Cleanup

Don't forget to clean up your resources:

```python
# Close the connection and database
conn.close()
db.close()
```

Happy NeuG querying! 🚀

