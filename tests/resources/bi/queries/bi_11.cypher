MATCH (a:PERSON)-[:ISLOCATEDIN]->(:PLACE)-[:ISPARTOF]->(country:PLACE {name: $country}),
      (a)-[k1:KNOWS]-(b:PERSON)
WHERE a.id < b.id
  AND $startDate <= k1.creationDate AND k1.creationDate <=  $endDate
WITH DISTINCT country, a, b
MATCH (b)-[:ISLOCATEDIN]->(:PLACE)-[:ISPARTOF]->(country)
WITH DISTINCT country, a, b
MATCH (b)-[k2:KNOWS]-(c:PERSON)
WHERE $startDate <= k2.creationDate AND k2.creationDate <= $endDate
WITH a,b, c, country
WHERE b.id < c.id
MATCH (c)-[:ISLOCATEDIN]->(:PLACE)-[:ISPARTOF]->(country)
WITH DISTINCT a, b, c
MATCH (c)-[k3:KNOWS]-(a)
WHERE $startDate <= k3.creationDate AND k3.creationDate <= $endDate
WITH DISTINCT a, b, c
RETURN count(a) AS count

