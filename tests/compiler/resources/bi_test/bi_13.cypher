MATCH (country:PLACE {name: "India"})<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(zombie:PERSON)
  WHERE zombie.creationDate < DATE("2012-11-09")
WITH country, zombie
OPTIONAL MATCH (zombie)<-[:HASCREATOR]-(message:POST:COMMENT)
  WHERE message.creationDate < DATE("2012-11-09")
WITH
  country,
  zombie,
  DATE("2012-11-09") AS idate,
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
  WHERE likerPerson.creationDate < DATE("2012-11-09")
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
    ELSE zombieLikeCount / totalLikeCount
    END AS zombieScore
  ORDER BY
  zombieScore DESC,
  zid ASC
  LIMIT 100