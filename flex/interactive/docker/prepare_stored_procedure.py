# -*- coding: utf-8 -*-
#
# Copyright 2023 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *


def create_procedure(sess: Session, graph_id: str, name: str, desc: str, query: str): 
    create_proc_request = CreateProcedureRequest(
        name=name,
        description=desc,
        query=query,
        type="cypher",
    )
    resp = sess.create_procedure(graph_id, create_proc_request)
    print("create procedure resp: ",resp)
    assert resp.is_ok()


if __name__ == "__main__":

    driver = Driver()
    with driver.session() as sess:
        create_procedure(sess, "1", "count_papers", "count the number of papers", "MATCH (p:Paper) RETURN COUNT(p) AS cnt;")
        create_procedure(sess, "1", "count_topics", "count the number of topics" , "MATCH (t: Topic) RETURN t.topic AS topic;")
        create_procedure(sess, "1", "get_topic_papers_hist", "get the topic and the number of papers working on the topic",
                         "MATCH (p:Paper)-[:WorkOn]->(:Task)-[:Belong]->(t:Topic) WITH DISTINCT t, COUNT(p) AS paperCount RETURN t.topic AS topic, paperCount ORDER BY paperCount DESC;")
        create_procedure(sess, "1", "get_challenges", "count the number of challenges", "MATCH (c: Challenge) RETURN c.challenge AS challenge;")
        create_procedure(sess, "1", "get_solutions_of_challenge", "get all solutions of a challenge", "MATCH (s: Solution)-[:ApplyOn]->(c: Challenge) WHERE c.challenge = $challenge RETURN c.challenge AS challenge, s.solution AS solution;")
        create_procedure(sess, "1", "get_challenge_paper_hist_of_topic", "get the challenges and the number of papers working on the topic", """
                         MATCH (t:Topic)<-[:Belong]-(ta:Task),
                            (ta)<-[:WorkOn]-(p:Paper)-[:Use]->(s:Solution),
                            (s)-[:ApplyOn]->(ch:Challenge)
                        WHERE t.topic = $topic
                        WITH t, ch, count(p) AS num
                        RETURN t.topic AS topic, ch.challenge AS challenge, num
                        ORDER BY num DESC;""")
        # restart the service
        resp = sess.start_service(start_service_request=StartServiceRequest(graph_id="1"))
        print(resp)
        assert resp.is_ok()


