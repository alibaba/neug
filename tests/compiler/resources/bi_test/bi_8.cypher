MATCH (tag:TAG {name: "Henry_Ford"})
OPTIONAL MATCH (tag)<-[interest:HASINTEREST]-(person:PERSON)
OPTIONAL MATCH (tag)<-[:HASTAG]-(message:POST:COMMENT)-[:HASCREATOR]->(person:PERSON)
WHERE DATE("2010-06-07") < message.creationDate
AND message.creationDate < DATE("2010-06-18")
WITH
  person,
  CASE WHEN interest IS NOT NULL THEN 1
    ELSE 0
    END AS tag1,
  CASE WHEN message IS NOT NULL THEN 1
    ELSE 0
    END AS tag2
WITH person,
     100 * count(tag1) + count(tag2) AS score
OPTIONAL MATCH (person)-[:KNOWS]-(friend:PERSON)
OPTIONAL MATCH (friend)-[interest:HASINTEREST]->(tag {name: "Henry_Ford"})
OPTIONAL MATCH (friend)<-[:HASCREATOR]-(message:POST:COMMENT)-[:HASTAG]->(tag {name: "Henry_Ford"})
WITH
  person,
  score,
  friend,
  CASE WHEN interest IS NOT NULL THEN 1
    ELSE 0
    END AS tag1,
  CASE WHEN message IS NOT NULL THEN 1
    ELSE 0
    END AS tag2
RETURN
  person.id as id,
  score AS score,
  100 * count(tag1) + count(tag2) AS friendsScore
  ORDER BY
  score + friendsScore DESC,
  id ASC
  LIMIT 100