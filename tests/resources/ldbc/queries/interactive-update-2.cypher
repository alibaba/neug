WITH $personId as personId, $postId as postId, $creationDate as date
CREATE (person:PERSON {id: personId})-[:LIKES {creationDate: date}]->(post:POST {id: postId})