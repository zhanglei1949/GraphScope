MATCH (person:PERSON { id: $personId })-[:KNOWS*1..3]-(otherPerson: PERSON)<-[membership:HASMEMBER]-(forum: FORUM)
WHERE
    otherPerson.id <> $personId AND membership.joinDate > $minDate
WITH DISTINCT otherPerson, forum
OPTIONAL MATCH (person:PERSON { id: $personId })-[:KNOWS*1..3]-(otherPerson: PERSON)<-[:HASCREATOR]-(post:POST)<-[:CONTAINEROF]-(forum:FORUM)
return forum.id AS forumId, head(collect(forum.title)) AS forumTitle, count(post) AS postCount
ORDER BY
    postCount DESC,
    forumId ASC
LIMIT 20;

