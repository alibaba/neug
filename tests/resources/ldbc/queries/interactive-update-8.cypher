WITH $person1Id as person1Id, $person2Id as person2Id, $creationDate as date
CREATE (p1:PERSON {id: person1Id})-[:KNOWS {creationDate: date}]->(p2:PERSON {id: person2Id})