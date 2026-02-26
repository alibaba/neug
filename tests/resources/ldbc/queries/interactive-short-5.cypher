MATCH (m:POST : COMMENT {id:  $messageId })-[:HASCREATOR]->(p:PERSON)
RETURN
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName