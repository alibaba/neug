WITH $personId as personId, $commentId as commentId, $creationDate as date
CREATE (person:PERSON {id: personId})-[:LIKES {creationDate: date}]->(comment:COMMENT {id: commentId})