
MATCH 
    (p:PERSON {id: $personId})-[:KNOWS*1..2]-(otherP:PERSON)
WITH distinct otherP
WHERE otherP.id <> $personId
MATCH (country:PLACE)<-[:ISLOCATEDIN]-(message : POST : COMMENT)-[:HASCREATOR]->(otherP:PERSON)-[ISLOCATEDIN]->(city:PLACE)-[:ISPARTOF]-> (country2:PLACE)
WHERE 
     (country.name = $countryXName OR country.name = $countryYName)
      AND message.creationDate >= $startDate
      AND message.creationDate < $endDate
WITH 

     message,
    otherP, 
    country,country2
WHERE (country2.name <> $countryXName AND country2.name <> $countryYName)
    
WITH otherP,
     CASE WHEN country.name=$countryXName THEN 1 ELSE 0 END AS messageX,
     CASE WHEN country.name=$countryYName THEN 1 ELSE 0 END AS messageY
WITH otherP, sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount > 0 AND yCount > 0
RETURN
    otherP.id as id,
    otherP.firstName as firstName,
    otherP.lastName as lastName,
    xCount,
    yCount,
    xCount + yCount as total 
ORDER BY total DESC, id ASC LIMIT 20;