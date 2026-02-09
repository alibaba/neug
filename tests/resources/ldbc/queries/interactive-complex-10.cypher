MATCH (person:PERSON {id: $personId})-[:KNOWS*2..2]-(friend: PERSON)
WHERE 
        NOT friend=person 
        AND NOT (friend:PERSON)-[:KNOWS]-(person :PERSON {id: $personId})
WITH distinct
        friend
WITH friend,        friend.birthday as birthday
WHERE  (date_part('month', birthday)=$month AND date_part('day', birthday)>=21) OR (date_part('month',birthday)=($month%12) + 1 AND date_part('day',birthday)<22)

OPTIONAL MATCH (friend : PERSON)<-[:HASCREATOR]-(post:POST)
WITH friend,  count(post) as postCount

CALL  (friend, postCount) {
        MATCH (friend: PERSON) <- [:HASCREATOR]- (post: POST) - [:HASTAG] -> (tag:TAG)
        WITH friend, postCount, post, tag
        MATCH (tag:TAG)<-[:HASINTEREST]-(person: PERSON {id: $personId})
        WITH friend, postCount, count(distinct post) as commonPostCount
        return friend, commonPostCount - (postCount - commonPostCount) AS commonInterestScore, friend.id as friendId
        ORDER BY commonInterestScore DESC, friendId ASC
        LIMIT 10

UNION ALL
        return friend, (0-postCount) AS commonInterestScore, friend.id as friendId
        ORDER BY commonInterestScore DESC, friendId ASC
        LIMIT 10
}
WITH friend, max(commonInterestScore) AS score
ORDER BY score DESC, friend.id ASC
LIMIT 10
MATCH (friend: PERSON)-[:ISLOCATEDIN]->(city:PLACE)
RETURN friend.id,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       score,
       friend.gender AS personGender,
       city.name AS personCityName

