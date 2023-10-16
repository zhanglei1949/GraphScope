MATCH (unused:PERSON {id: $personId})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(comments:COMMENT)-[:REPLYOF]->
(:POST)-[:HASTAG]->(tags:TAG)-[:HASTYPE]->(:TAGCLASS)-
[:ISSUBCLASSOF*0..10]->(:TAGCLASS {name: $tagClassName})
 with friend AS friend, collect(DISTINCT tags.name) AS tagNames, count(DISTINCT comments) AS replyCount 
 ORDER BY 
    replyCount DESC, 
    friend.id ASC
LIMIT 20
return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, tagNames, replyCount