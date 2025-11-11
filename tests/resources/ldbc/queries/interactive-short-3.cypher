MATCH (n:PERSON {id: $personId })-[r:KNOWS]-(friend:PERSON)
RETURN
    friend.id AS personId,
    r.creationDate AS friendshipCreationDate,
    friend.firstName AS firstName,
    friend.lastName AS lastName
ORDER BY
    friendshipCreationDate DESC,
    personId ASC