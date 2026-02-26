MATCH (p:PERSON {id: $personId})-[:KNOWS*1..2]-(friend:PERSON)
WITH distinct friend
where friend.id <> $personId
MATCH  (message:POST:COMMENT)-[e:HASCREATOR]->(friend)
where e.creationDate < $maxDate
WITH friend, message

RETURN 
    friend.id AS personId, 
    friend.firstName AS personFirstName, 
    friend.lastName AS personLastName, 
    message.id AS commentOrPostId, 
    message.content AS messageContent, 
    message.imageFile AS messageImageFile, 
    message.creationDate AS commentOrPostCreationDate
ORDER BY 
    commentOrPostCreationDate DESC, 
    commentOrPostId ASC 
LIMIT 20
