MATCH shortestPath((p1:PERSON {id : $personId})-[:KNOWS*3..5]-(expert:PERSON))
MATCH (expert)-[:ISLOCATEDIN]->(:PLACE)-[:ISPARTOF]->(country:PLACE {name: $country}),
      (expert)<-[:HASCREATOR]-(message)-[:HASTAG]->(:TAG)-[:HASTYPE]->(:TAGCLASS {name: $tagClass})
WITH DISTINCT expert, message
MATCH (message)-[:HASTAG]->(tag:TAG)
RETURN
  expert.id as id,
  tag.name as name,
  count(DISTINCT message) AS messageCount
ORDER BY
  messageCount DESC,
  name ASC,
  id ASC
LIMIT 100