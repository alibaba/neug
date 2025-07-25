Match (p1:person {p_personid:1})-[k:knows]->(p2:person) 
Where k.k_creationDate > 10000 
Return p1.p_personid;
