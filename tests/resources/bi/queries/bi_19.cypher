MATCH
  (person1:PERSON)-[:ISLOCATEDIN]->(city1Id:PLACE {id: $city1Id})
WITH person1 AS person1
MATCH
  (person2:PERSON)-[:ISLOCATEDIN]->(city2Id:PLACE {id: $city2Id})
CALL shortestPath.dijkstra.stream(
'(personA:PERSON)-[knows:KNOWS]-(personB:PERSON)',
person1,
person2,
'max(round(40 - sqrt(knows.interaction1_count + knows.interaction2_count)), 1)'
)
WITH person1.id AS person1Id, person2.id AS person2Id, totalCost AS totalWeight
RETURN totalWeight, collect(person1Id, person2Id) AS personIds
ORDER BY totalWeight ASC
LIMIT 1