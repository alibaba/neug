MATCH
  (country1:PLACE {name: $country1})<-[:ISPARTOF]-(city1:PLACE)<-[:ISLOCATEDIN]-(person1:PERSON),
  (country2:PLACE {name: $country2})<-[:ISPARTOF]-(city2:PLACE)<-[:ISLOCATEDIN]-(person2:PERSON),
  (person1)-[knows:KNOWS]-(person2)

WITH person1, person2, knows, city1, 0 AS score

WITH DISTINCT person1, person2, city1, knows,
              score + (CASE WHEN knows.interaction2_count=0 THEN 0
                ELSE  4
                END) AS score

WITH DISTINCT person1, person2, city1, knows,
              score + (CASE WHEN knows.interaction1_count=0 THEN 0
                ELSE  1
                END) AS score

OPTIONAL MATCH (person1)-[:LIKES]->(m:COMMENT|POST)-[:HASCREATOR]->(person2)

WITH DISTINCT person1, person2, city1,
              score + (CASE WHEN m IS NULL THEN 0
                ELSE 10
                END) AS score

OPTIONAL MATCH (person1)<-[:HASCREATOR]-(m:COMMENT|POST)<-[:LIKES]-(person2)

WITH DISTINCT person1, person2, city1,
              score + (CASE WHEN m IS NULL THEN 0
                ELSE  1
                END) AS score
ORDER BY
  city1.name  ASC,
  score DESC,
  person1.id ASC,
  person2.id ASC
WITH city1, head(collect(person1.id)) AS person1Id, head(collect(person2.id)) AS person2Id, head(collect(score)) AS score
RETURN
  person1Id,
  person2Id,
  city1.name AS cityName,
  score
  ORDER BY
  score DESC,
  person1Id ASC,
  person2Id ASC
  LIMIT 100