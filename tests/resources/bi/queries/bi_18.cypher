MATCH (tag:TAG {name: $tag})<-[:HASINTEREST]-(person1:PERSON)-[:KNOWS]-(friend:PERSON)
WITH friend, collect(person1) as persons
UNWIND persons as person1
UNWIND persons as person2
WITH person1, person2, friend
WHERE person1 <> person2 AND NOT (person1)-[:KNOWS]-(person2)
RETURN person1.id AS person1Id, person2.id AS person2Id, count(DISTINCT friend) AS friendCount
ORDER BY friendCount DESC, person1Id ASC, person2Id ASC
LIMIT 20