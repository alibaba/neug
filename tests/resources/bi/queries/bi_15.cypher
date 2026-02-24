MATCH
  (person1:PERSON {id: $person1Id})
WITH person1 AS person1
MATCH
  (person2:PERSON {id: $person2Id})
CALL shortestPath.dijkstra.stream(
'(personA:PERSON)-[knows:KNOWS]-(personB:PERSON)',
person1,
person2,
'OPTIONAL MATCH (personA)<-[:HASCREATOR]-(m1:POST|COMMENT)-[r:REPLYOF]-(m2:POST|COMMENT)-[:HASCREATOR]->(personB)
 OPTIONAL MATCH (m1)-[:REPLYOF*0..]->(:POST)<-[:CONTAINEROF]-(forum:FORUM)
  WHERE forum.creationDate >= $startDate AND forum.creationDate <= $endDate
 WITH sum(CASE forum IS NOT NULL
  WHEN true THEN
      CASE (m1:POST OR m2:POST) WHEN true THEN 1.0
      ELSE 0.5 END
  ELSE 0.0 END)'
)
WITH totalCost AS totalWeight
RETURN totalWeight
  ORDER BY totalWeight
  LIMIT 1