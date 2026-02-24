MATCH (p:PERSON {id: $personId})-[:KNOWS*1..2]-(friend:PERSON)
WITH distinct friend
WHERE 
     friend.id <> $personId     

MATCH (friend:PERSON)-[wa:WORKAT]->(com:ORGANISATION)-[:ISLOCATEDIN]->(:PLACE {name: $countryName}) 
WHERE wa.workFrom < $workFromYear
WITH
    friend as friend, 
    com AS com, 
    wa.workFrom as organizationWorkFromYear 
ORDER BY 
    organizationWorkFromYear ASC, 
    friend.id ASC, com.name DESC 
LIMIT 10 
return 
    friend.id AS personId, 
    friend.firstName AS personFirstName, 
    friend.lastName AS personLastName, 
    com.name as organizationName, 
    organizationWorkFromYear as organizationWorkFromYear;