/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/graph_db/database/graph_db_update_server.h"
#include "flex/engines/http_server/options.h"
#include "flex/engines/http_server/service/graph_db_update_service.h"
#include "flex/storages/rt_mutable_graph/schema.h"

#include <boost/program_options.hpp>
#include <seastar/core/alien.hh>

#include <glog/logging.h>

using namespace server;
namespace bpo = boost::program_options;

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")("version,v",
                                                     "Display version")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "http-port,p", bpo::value<uint16_t>()->default_value(10000),
      "http port of query handler");
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }
  if (vm.count("version")) {
    std::cout << "GraphScope/Flex version " << FLEX_VERSION << std::endl;
    return 0;
  }
  std::string graph_schema_path = "";
  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();
  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  server::GraphDBUpdateServer::get().init(schema);

  bool enable_dpdk = false;
  constexpr uint32_t shard_num = 1;
  uint16_t http_port = vm["http-port"].as<uint16_t>();

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  // start service
  LOG(INFO) << "GraphScope http server start to listen on port " << http_port;
  server::GraphDBUpdateService::get().init(shard_num, http_port, enable_dpdk);
  server::GraphDBUpdateService::get().run_and_wait_for_exit();

  return 0;
}