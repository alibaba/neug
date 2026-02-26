MATCH (p_:PERSON {id: $personId})-[:KNOWS*1..2]-(other:PERSON)
WITH distinct other
WHERE  other.id <> $personId

MATCH (other)<-[:HASCREATOR]-(p:POST)-[:HASTAG]->(t:TAG {name: $tagName})
WITH p,t
MATCH    (p:POST)-[:HASTAG]->(otherTag:TAG)

WITH otherTag, t,count(distinct p) as postCnt
WHERE 
    otherTag <> t 
RETURN
    otherTag.name as name,
    postCnt 
ORDER BY 
    postCnt desc, 
    name asc 
LIMIT 10;