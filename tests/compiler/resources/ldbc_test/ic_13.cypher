MATCH  (person1: PERSON {id: $person1Id})
with person1
OPTIONAL MATCH 
   (person1: PERSON {id:$person1Id})-[k:KNOWS* SHORTEST 1..]-(person2: PERSON {id: $person2Id})
WITH
    CASE 
        WHEN k is null THEN -1
        ELSE length(k)
    END as len
RETURN len