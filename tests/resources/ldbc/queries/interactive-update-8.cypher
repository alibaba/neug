MATCH(p1: PERSON {id: $person1Id}), (p2 :PERSON {id: $person2Id})
CREATE (p1)-[:KNOWS {creationDate: $creationDate}]->(p2)