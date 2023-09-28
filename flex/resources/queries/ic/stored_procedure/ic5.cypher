MATCH (person:PERSON { id: $personId })-[:KNOWS*1..3]-(otherPerson: PERSON)
WHERE
    person <> otherPerson
WITH DISTINCT otherPerson
MATCH (otherPerson:PERSON)<-[membership:HASMEMBER]-(forum: FORUM)
WHERE
    membership.joinDate > $minDate
WITH forum, otherPerson
OPTIONAL MATCH (otherPerson:PERSON)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum:FORUM)
return forum.id AS forumId, head(collect(forum.title)) AS forumTitle, count(post) AS postCount
ORDER BY
    postCount DESC,
    forumId ASC
LIMIT 20;

