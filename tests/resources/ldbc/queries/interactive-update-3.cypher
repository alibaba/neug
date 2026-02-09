MATCH (person: PERSON {id: $personId}), (comment:COMMENT {id:$commentId})
CREATE (person)-[:LIKES {creationDate: $creationDate}]->(comment)