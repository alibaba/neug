MATCH (p1:person)-[k:knows]-(:person)-[:person_islocatedin]->(pl:place) WHERE p1.p_personid = 933 
RETURN k.k_creationDate; 
