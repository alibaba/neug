CALL {
  MATCH (p2:PERSON)
  WITH count(p2) as personCnt
  RETURN 0L as msgCnt, personCnt
}
UNION
CALL {
  MATCH (person:PERSON)<-[:HASCREATOR]-(message:COMMENT|POST),
        (message)-[:REPLYOF * 0..30]->(post:POST)
  WHERE message.content <> "" AND message.length < $lengthThreshold AND message.creationDate > $startDate
        AND post.language IN $languages
  WITH person, count(message) as msgCnt
  WITH msgCnt, count(person) as personCnt
  CALL {
    RETURN msgCnt, personCnt
  }
  UNION
  CALL {
    RETURN 0L as msgCnt, -1L * sum(personCnt) as personCnt
  }
  RETURN msgCnt, personCnt
}
RETURN msgCnt, sum(personCnt) as personCnt
ORDER BY
  personCnt DESC,
  msgCnt DESC