Match (p1:Person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->(post:Post)-[:HAS_CREATOR]->(p2:Person) Return count(p1);