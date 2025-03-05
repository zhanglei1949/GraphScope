#!/bin/bash
# Copyright 2020 Alibaba Group Holding Limited.
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
set -e
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
FLEX_HOME=${SCRIPT_DIR}/../../
SERVER_BIN=${FLEX_HOME}/build/bin/interactive_server
PRIMARY_CONFIG_FILE=${FLEX_HOME}/tests/hqps/interactive_config_test_primary.yaml
SECONDARY_CONFIG_FILE=${FLEX_HOME}/tests/hqps/interactive_config_test_standby.yaml

if [ ! $# -eq 1 ]; then
  echo "only receives: $# args, need 1"
  echo "Usage: $0 <INTERACTIVE_WORKSPACE>"
  exit 1
fi

INTERACTIVE_WORKSPACE=$1

if [ ! -d ${INTERACTIVE_WORKSPACE} ]; then
  echo "INTERACTIVE_WORKSPACE: ${INTERACTIVE_WORKSPACE} not exists"
  exit 1
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color
err() {
  echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] -ERROR- $* ${NC}" >&2
}

info() {
  echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] -INFO- $* ${NC}"
}

kill_service(){
    info "Kill Service first"
    ps -ef | grep "interactive_server" |  awk '{print $2}' | xargs kill -9  || true
    sleep 3
    # check if service is killed
    info "Kill Service success"
}

# kill service when exit
trap kill_service EXIT

# start engine service
start_engine_service(){
    # expect one argument
    if [ ! $# -eq 2 ]; then
        err "start_engine_service need two arguments"
        err "Usage: start_engine_service <config_path> <log_path>"
        exit 1
    fi
    local config_path=$1
    local log_path=$2
    #check SERVER_BIN exists
    if [ ! -f ${SERVER_BIN} ]; then
        err "SERVER_BIN not found"
        exit 1
    fi

    cmd="${SERVER_BIN} -c ${config_path} --enable-admin-service true "
    cmd="${cmd}  -w ${INTERACTIVE_WORKSPACE} --start-compiler true > ${log_path} 2>&1 & "

    echo "Start engine service with command: ${cmd}"
    eval ${cmd} 
    sleep 10
    #check interactive_server is running, if not, exit
    ps -ef | grep "interactive_server" | grep -v grep

    info "Start engine service success"
}


run_etcd_meta_test(){
    export PRIMARY_ADMIN_SERVICE_ENDPOINT=localhost:7777
    export STANDBY_ADMIN_SERVICE_ENDPOINT=localhost:7778
    pushd ${FLEX_HOME}/interactive/sdk/python/gs_interactive
    cmd="python3 -m pytest -s tests/test_admin_service_replication.py"
    echo "Run robust test with command: ${cmd}"
    eval ${cmd} || (err "Run robust test failed"; exit 1)
    info "Run robust test success"
    popd
}

kill_service
start_engine_service ${PRIMARY_CONFIG_FILE} /tmp/engine_1.log
start_engine_service ${SECONDARY_CONFIG_FILE} /tmp/engine_2.log
run_etcd_meta_test
kill_service