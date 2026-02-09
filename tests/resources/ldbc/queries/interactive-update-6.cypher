MATCH (author:PERSON {id: $authorPersonId}), (country:PLACE {id: $countryId}), (forum:Forum {id: $forumId})
CREATE (author)<-[:HASCREATOR {creationDate: $creationDate}]-(p:POST {
    id: $postId,
    creationDate: $creationDate,
    locationIP: $locationIP,
    browserUsed: $browserUsed,
    language: $language,
    content: $content,
    imageFile: $imageFile,
    length: $length
  })<-[:CONTAINEROF]-(forum), (p)-[:ISLOCATEDIN]->(country)
WITH p
UNWIND CAST($tagIds, 'INT64[]') AS tagId
MATCH (t:TAG {id: tagId})
CREATE (p)-[:HASTAG]->(t)