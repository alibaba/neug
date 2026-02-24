MATCH (country:PLACE {name: $country})<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(zombie:PERSON)
WHERE zombie.creationDate < $endDate
OPTIONAL MATCH (zombie)<-[:HASCREATOR]-(message:POST|COMMENT)
WHERE message.creationDate < $endDate
WITH
  country,
  zombie,
  gs.function.datetime($endDate) AS idate,
  zombie.creationDate AS zdate,
  count(message) AS messageCount
WITH
  country,
  zombie,
  12 * (idate.year  - zdate.year )
  + (idate.month - zdate.month)
  + 1 AS months,
  messageCount
WHERE messageCount / months < 1
WITH
  country,
  collect(zombie) AS zombies
UNWIND zombies AS zombie
OPTIONAL MATCH
  (zombie)<-[:HASCREATOR]-(message:POST|COMMENT)<-[:LIKES]-(likerPerson:PERSON)
WHERE likerPerson.creationDate < $endDate
WITH
  zombie,
  likerPerson,
  CASE WHEN likerPerson IN zombies THEN 1
  ELSE 0
  END as likerZombie
WITH
  zombie,
  sum(likerZombie) AS zombieLikeCount,
  count(likerPerson) as totalLikeCount
RETURN
  zombie.id AS zid,
  zombieLikeCount,
  totalLikeCount,
  CASE totalLikeCount
    WHEN 0 THEN gs.function.toFloat(0L)
    ELSE zombieLikeCount / gs.function.toFloat(totalLikeCount)
    END AS zombieScore
ORDER BY
  zombieScore DESC,
  zid ASC
LIMIT 100