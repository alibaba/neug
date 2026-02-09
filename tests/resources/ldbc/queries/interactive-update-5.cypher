MATCH (f:FORUM {id: $forumId}), (p:PERSON {id: $personId})
CREATE (f:FORUM)-[:HASMEMBER {joinDate: $joinDate}]->(p:PERSON)