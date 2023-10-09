MATCH (person:PERSON { id: $personId })-[:KNOWS*1..3]-(otherPerson: PERSON)<-[membership:HASMEMBER]-(forum: FORUM)
WHERE
    person <> otherPerson AND membership.joinDate > $minDate
WITH DISTINCT otherPerson, forum
OPTIONAL MATCH (otherPerson:PERSON)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum:FORUM)
return forum.id AS forumId, head(collect(forum.title)) AS forumTitle, count(post) AS postCount
ORDER BY
    postCount DESC,
    forumId ASC
LIMIT 20;

