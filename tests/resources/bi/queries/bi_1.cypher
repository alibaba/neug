MATCH (message:COMMENT|POST)
WHERE message.creationDate < $datetime
WITH count(message) AS totalMessageCount

MATCH (message:COMMENT|POST)
WHERE message.creationDate < $datetime
AND message.length > 0
WITH
   totalMessageCount,
  message,
  message.creationDate AS date
WITH
  totalMessageCount,
  date.year AS year,
  'COMMENT' in labels(message) AS isComment,
  CASE
    WHEN message.length <  40 THEN 0
    WHEN message.length <  80 THEN 1
    WHEN message.length < 160 THEN 2
    ELSE                           3
    END AS lengthCategory,
  count(message) AS messageCount,
  sum(message.length) AS sumMessageLength

RETURN
  year,
  isComment,
  lengthCategory,
  messageCount,
   sumMessageLength / gs.function.toFloat(messageCount) AS averageMessageLength,
  sumMessageLength,
  messageCount / gs.function.toFloat(totalMessageCount) AS percentageOfMessages
  ORDER BY
  year DESC,
  isComment ASC,
  lengthCategory ASC