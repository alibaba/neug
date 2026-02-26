MATCH (person:PERSON {id: $personId})<-[:HASCREATOR]-(message: POST : COMMENT)<-[like:LIKES]-(liker:PERSON)
WITH liker, message, like.creationDate AS likeTime
OPTIONAL MATCH (liker: PERSON)-[k:KNOWS]-(person: PERSON {id: $personId})
WITH liker, message, likeTime,
  CASE
      WHEN k is null THEN true
      ELSE false
     END AS isNew
ORDER BY likeTime DESC, message.id ASC
WITH liker, collect(message)[0] as message, collect(likeTime)[0] AS likeTime, isNew
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    likeTime,
    message.id AS commentOrPostId,
    message.content AS messageContent,
    message.imageFile AS messageImageFile,
    CAST(likeTime - message.creationDate, 'INT64')/1000/60 AS minutesLatency,
  	isNew
ORDER BY
    likeTime DESC,
    personId ASC
LIMIT 20;