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

#include "grape/util.h"

#include <sys/wait.h>  // for waitpid()
#include <unistd.h>    // for fork() and execvp()
#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
#include <vector>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

namespace bpo = boost::program_options;
bool generate_plan(const std::string& query,
                   physical::PhysicalPlan& plan_cache) {
  // dump query to file
  static const char* const GRAPHSCOPE_DIR = "/data/GraphScope/";
  static const char* const COMPILER_CONFIG_PATH =
      "/data/GraphScope/flex/tests/hqps/engine_config_test.yaml";
  static const char* const COMPILER_GRAPH_SCHEMA =
      "/data/flex_ldbc_snb/configs/graph_for_compiler.yaml";

  auto id = std::this_thread::get_id();
  std::stringstream ss;
  ss << id;
  std::string thread_id = ss.str();
  const std::string query_file = "/tmp/temp" + thread_id + ".cypher";
  const std::string output_file = "/tmp/temp" + thread_id + ".pb";
  const std::string jar_path = std::string(GRAPHSCOPE_DIR) +
                               "/interactive_engine/compiler/target/"
                               "compiler-0.0.1-SNAPSHOT.jar:" +
                               std::string(GRAPHSCOPE_DIR) +
                               "/interactive_engine/compiler/target/libs/*";
  const std::string djna_path =
      std::string("-Djna.library.path=") + std::string(GRAPHSCOPE_DIR) +
      "/interactive_engine/executor/ir/target/release/";
  const std::string schema_path =
      "-Dgraph.schema=" + std::string(COMPILER_GRAPH_SCHEMA);
  auto raw_query = query;  // decompress(query);
  {
    std::ofstream out(query_file);
    out << query;
    out.close();
  }

  // call compiler to generate plan
  {
    pid_t pid = fork();

    if (pid == -1) {
      std::cerr << "Fork failed!" << std::endl;
      return false;
    } else if (pid == 0) {
      const char* const args[] = {
          "java",
          "-cp",
          jar_path.c_str(),
          schema_path.c_str(),
          djna_path.c_str(),
          "com.alibaba.graphscope.common.ir.tools.GraphPlanner",
          COMPILER_CONFIG_PATH,
          query_file.c_str(),
          output_file.c_str(),
          "/tmp/temp.cypher.yaml",
          nullptr  // execvp expects a null-terminated array
      };

      execvp(args[0], const_cast<char* const*>(args));
      std::cerr << "Exec failed!" << std::endl;
      return false;
    } else {
      int status;
      waitpid(pid, &status, 0);
      if (WIFEXITED(status)) {
        std::cout << "Child exited with status " << WEXITSTATUS(status)
                  << std::endl;
      }
    }
  }

  {
    std::ifstream file(output_file, std::ios::binary);

    if (!file.is_open()) {
      return false;
    }

    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::string buffer;
    buffer.resize(size);

    file.read(&buffer[0], size);

    file.close();
    physical::PhysicalPlan plan;
    if (!plan.ParseFromString(std::string(buffer))) {
      return false;
    }

    plan_cache = plan;
  }
  // clean up temp files
  {
    unlink(output_file.c_str());
    unlink(query_file.c_str());
    // unlink("/tmp/temp.cypher.yaml");
    // unlink("/tmp/temp.cypher.yaml_extra_config.yaml");
  }

  return true;
}
std::string read_pb(const std::string& filename) {
  std::ifstream file(filename, std::ios::binary);

  if (!file.is_open()) {
    LOG(FATAL) << "open pb file: " << filename << " failed...";
    return "";
  }

  file.seekg(0, std::ios::end);
  size_t size = file.tellg();
  file.seekg(0, std::ios::beg);

  std::string buffer;
  buffer.resize(size);

  file.read(&buffer[0], size);

  file.close();

  return buffer;
}

void load_params(const std::string& filename,
                 std::vector<std::map<std::string, std::string>>& map) {
  std::ifstream in(filename);
  if (!in.is_open()) {
    LOG(FATAL) << "open params file: " << filename << " failed...";
    return;
  }
  std::string line;
  std::vector<std::string> keys;
  std::getline(in, line);
  std::stringstream ss(line);
  std::string key;
  while (std::getline(ss, key, '|')) {
    keys.push_back(key);
    LOG(INFO) << key;
  }
  while (std::getline(in, line)) {
    std::map<std::string, std::string> m;
    std::stringstream ss(line);
    std::string value;
    for (auto& key : keys) {
      std::getline(ss, value, '|');
      m[key] = value;
    }
    map.push_back(m);
  }
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("shard-num,s",
                                      bpo::value<uint32_t>()->default_value(1),
                                      "shard number of actor system")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "query-file,q", bpo::value<std::string>(), "query file")(
      "params_file,p", bpo::value<std::string>(), "params file")(
      "query-num,n", bpo::value<int>()->default_value(0))(
      "output-file,o", bpo::value<std::string>(), "output file")(
      "log-path,l", bpo::value<std::string>(), "log file path")(
      "thread-num,t", bpo::value<int>()->default_value(1))(
      "memory-level,m", bpo::value<int>()->default_value(1));

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

  uint32_t shard_num = vm["shard-num"].as<uint32_t>();

  std::string graph_schema_path = "";
  std::string data_path = "";
  std::string output_path = "";
  std::string log_path = "";
  int query_num = vm["query-num"].as<int>();

  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();
  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  if (vm.count("output-file")) {
    output_path = vm["output-file"].as<std::string>();
  }
  if (vm.count("log-path")) {
    log_path = vm["log-path"].as<std::string>();
  }

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  if (!schema.ok()) {
    LOG(ERROR) << "Failed to load graph schema from " << graph_schema_path;
    return -1;
  }
  int memory_level = vm["memory-level"].as<int>();
  gs::GraphDBConfig config(schema.value(), data_path, shard_num);
  config.memory_level = memory_level;
  if (config.memory_level >= 2) {
    config.enable_auto_compaction = true;
  }
  db.Open(config);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";
  std::string req_file = vm["query-file"].as<std::string>();
  physical::PhysicalPlan pb;
  generate_plan(read_pb(req_file), pb);
  auto txn = db.GetReadTransaction();
  std::vector<std::map<std::string, std::string>> map;
  load_params(vm["params_file"].as<std::string>(), map);
  size_t params_num = map.size();

  // physical::PhysicalPlan pb;
  // pb.ParseFromString(query);

  if (query_num == 0) {
    query_num = params_num;
  }

  int thread_num = vm["thread-num"].as<int>();
  {
    std::vector<std::vector<char>> outputs(query_num);
    for (int i = 0; i < query_num; ++i) {
      auto& m = map[i % params_num];
      auto ctx = gs::runtime::runtime_eval(pb, txn, m);
      gs::Encoder output(outputs[i]);
      gs::runtime::eval_sink_encoder(ctx, txn, output);
    }
  }
  LOG(INFO) << "warm up done\n";
  double t1 = -grape::GetCurrentTime();
  std::vector<std::thread> threads;
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([&]() {
      std::vector<std::vector<char>> outputs(query_num);
      for (int i = 0; i < query_num; ++i) {
        auto& m = map[i % params_num];
        auto ctx = gs::runtime::runtime_eval(pb, txn, m);
        gs::Encoder output(outputs[i]);
        gs::runtime::eval_sink_encoder(ctx, txn, output);
      }
      double t2 = t1 + grape::GetCurrentTime();
      LOG(INFO) << "Finished run " << query_num << " queries, elapsed " << t2
                << " s, avg " << t2 / static_cast<double>(query_num) * 1000000
                << " us";
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  // gs::runtime::OpCost::get().clear();
  // double t2 = -grape::GetCurrentTime();
  // for (int i = 0; i < query_num; ++i) {
  //   auto& m = map[i % params_num];
  //   auto ctx = gs::runtime::runtime_eval(pb, txn, m);
  //   outputs[i].clear();
  //   gs::Encoder output(outputs[i]);
  //   gs::runtime::eval_sink_beta(ctx, txn, output);
  // }
  // t2 += grape::GetCurrentTime();

  // double t3 = -grape::GetCurrentTime();
  // for (int i = 0; i < query_num; ++i) {
  //   auto& m = map[i % params_num];
  //   auto ctx = gs::runtime::runtime_eval(pb, txn, m);
  //   outputs[i].clear();
  //   gs::Encoder output(outputs[i]);
  //   gs::runtime::eval_sink(ctx, txn, output);
  // }
  // t3 += grape::GetCurrentTime();

  // LOG(INFO) << "Finished run " << query_num << " queries, elapsed " << t2
  //           << " s, avg " << t2 / static_cast<double>(query_num) * 1000000
  //           << " us";
  // LOG(INFO) << "Finished run " << query_num << " queries, elapsed " << t3
  //           << " s, avg " << t3 / static_cast<double>(query_num) * 1000000
  //           << " us";

  /**if (!output_path.empty()) {
    FILE* fout = fopen(output_path.c_str(), "a");
    for (auto& output : outputs) {
      fwrite(output.data(), sizeof(char), output.size(), fout);
    }
    fflush(fout);
    fclose(fout);
  }
  if (!log_path.empty()) {
    gs::runtime::OpCost::get().output(log_path);
  }
*/
  return 0;
}
