MATCH (country:PLACE {name: "India"})<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(zombie:PERSON)
  WHERE zombie.creationDate < $date
WITH country, zombie
OPTIONAL MATCH (zombie)<-[:HASCREATOR]-(message:POST:COMMENT)
  WHERE message.creationDate < $date
WITH
  country,
  zombie,
  CAST($date, 'TIMESTAMP') AS idate,
  zombie.creationDate AS zdate,
  count(message) AS messageCount
WITH
  country,
  zombie,
  12 * ( date_part('year', idate)  - date_part('year', zdate) )
  + ( date_part('month', idate) - date_part('month', zdate) )
  + 1 AS months,
  messageCount
WHERE messageCount / months < 1
WITH
  country,
  collect(zombie) AS zombies
UNWIND zombies AS zombie
OPTIONAL MATCH
  (zombie)<-[:HASCREATOR]-(message:POST:COMMENT)<-[:LIKES]-(likerZombie:PERSON)
WITH zombie, likerZombie, zombies
WHERE likerZombie IN zombies
WITH
  zombie,
  count(likerZombie) AS zombieLikeCount
OPTIONAL MATCH
  (zombie)<-[:HASCREATOR]-(message:POST:COMMENT)<-[:LIKES]-(likerPerson:PERSON)
  WHERE likerPerson.creationDate < $date
WITH
  zombie,
  zombieLikeCount,
  count(likerPerson) AS totalLikeCount
RETURN
  zombie.id AS zid,
  zombieLikeCount,
  totalLikeCount,
  CASE totalLikeCount
    WHEN 0 THEN 0.0
    ELSE 1.0 * zombieLikeCount / totalLikeCount
    END AS zombieScore
  ORDER BY
  zombieScore DESC,
  zid ASC
  LIMIT 100