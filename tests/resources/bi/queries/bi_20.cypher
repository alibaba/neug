MATCH
  (company:ORGANISATION {name: $company})<-[:WORKAT]-(person1:PERSON)
WITH person1 AS person1
MATCH
  (person2:PERSON {id: $person2Id})
CALL shortestPath.dijkstra.stream(
'(personA:PERSON)-[knows:KNOWS]-(personB:PERSON)',
person1,
person2,
'knows.bi20_precompute'
)
WITH person1.id AS person1Id, person2.id AS person2Id, totalCost AS totalWeight
RETURN person1Id, person2Id, totalWeight
ORDER BY totalWeight ASC, person1Id ASC, person2Id ASC
LIMIT 1