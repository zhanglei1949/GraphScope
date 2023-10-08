MATCH (person:PERSON {id: $personId})-[:KNOWS*2..3]-(friend: PERSON)
WHERE NOT friend=person AND NOT (friend:PERSON)-[:KNOWS]-(person :PERSON {id: $personId})
WITH  friend, friend.birthday as birthday
WHERE  (birthday.month=$month AND birthday.day>=21) OR
        ((birthday.month =($month % 12) + 1) AND birthday.day<22)
WITH DISTINCT friend

OPTIONAL MATCH (friend : PERSON)<-[:HASCREATOR]-(post:POST)
WITH friend, count(post) as postCount

OPTIONAL MATCH (friend)<-[:HASCREATOR]-(post1:POST)-[:HASTAG]->(tag:TAG)<-[:HASINTEREST]-(person: PERSON {id: $personId})
WITH friend, postCount, count(DISTINCT post1) as commonPostCount

MATCH (friend:PERSON)-[:ISLOCATEDIN]->(city:PLACE)
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10;