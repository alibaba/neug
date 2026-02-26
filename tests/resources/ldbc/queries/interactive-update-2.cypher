MATCH (person:PERSON {id: $personId}), (post:POST {id: $postId})
CREATE (person:PERSON)-[:LIKES {creationDate: $creationDate}]->(post:POST)