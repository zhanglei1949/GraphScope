#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
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
import sys
import threading
from time import sleep

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session

from gs_interactive.tests.conftest import create_procedure
from gs_interactive.tests.conftest import start_service_on_graph
from gs_interactive.tests.conftest import ensure_compiler_schema_ready
from gs_interactive.tests.conftest import call_procedure


def test_admin_service_in_replication_deployment(primary_driver, primary_session, standby_driver, standby_session, create_modern_graph_in_primary):
    """
    With primary and standby server launched locally, we test whether the two admin service works as expected.
    Note that we suppose two admin service launches on different port, connecting to the same etcd endpoint, and the
    etcd endpoint is launched locally.
    """
    # create graph and load graph in primary session, and check whether the graph exists in standby session
    assert standby_session.get_graph_meta(create_modern_graph_in_primary).is_ok()
    
    primary_proc_id = create_procedure(
        primary_session,
        create_modern_graph_in_primary,
        "test_proc",
        "MATCH(n: person) return count(n);",
    )
    start_service_on_graph(primary_session, create_modern_graph_in_primary)
    primary_neo4j_session = primary_driver.getNeo4jSession()
    ensure_compiler_schema_ready(
        primary_session, primary_neo4j_session, create_modern_graph_in_primary
    )
    call_procedure(primary_neo4j_session, create_modern_graph_in_primary, primary_proc_id)
    
    sleep(1) # wait for the procedure to be ready in standby server

    standby_neo4j_session = standby_driver.getNeo4jSession()
    call_procedure(
        standby_neo4j_session, create_modern_graph_in_primary, primary_proc_id
    )
        
    
    
    
    
    

