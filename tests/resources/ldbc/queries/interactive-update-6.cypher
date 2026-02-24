WITH
  $authorPersonId as personId,
  $countryId as countryId,
  $forumId as forumId,
  $postId as postId,
  $creationDate as creationDate,
  $locationIP as locationIP,
  $browserUsed as browserUsed,
  $language as language,
  $content as content,
  $imageFile as image,
  $length as length,
  $tagIds as tagIds
CREATE (p:POST {
    id: postId,
    creationDate: creationDate,
    locationIP: locationIP,
    browserUsed: browserUsed,
    language: language,
    content: content,
    imageFile: image,
    length: length
  })
CREATE (author:PERSON {id: personId})<-[:HASCREATOR {creationDate: creationDate} ]-(p:POST {id: postId})
CREATE (p: POST {id: postId})<-[:CONTAINEROF]-(forum:FORUM {id: forumId})
CREATE (p: POST {id: postId})-[:ISLOCATEDIN]->(country:PLACE {id: countryId})
UNWIND tagIds AS tagId
CREATE (p:POST {id: postId})-[:HASTAG]->(t:TAG {id: tagId})