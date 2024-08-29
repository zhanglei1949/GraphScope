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

#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
#include <vector>

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

namespace bpo = boost::program_options;

std::string read_pb(const std::string& filename) {
  std::ifstream file(filename, std::ios::binary);

  if (!file.is_open()) {
    std::cerr << "open pb file: " << filename << " failed..." << std::endl;
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

void write_pb(const std::string& filename, const std::string& str) {
  std::ofstream file(filename, std::ios::binary);

  if (!file.is_open()) {
    std::cerr << "open pb file: " << filename << " failed..." << std::endl;
    return;
  }

  file.write(str.data(), str.size());
  file.close();
}

void process_ic5(const std::string& input_dir, const std::string& output_dir) {
  std::string input_name = input_dir + "/ic5_adhoc.cypher.pb";
  std::string output_name = output_dir + "/ic5_adhoc.cypher.pb.out";

  std::string query = read_pb(input_name);
  physical::PhysicalPlan pb;
  pb.ParseFromString(query);
  physical::EdgeExpand& opr = *pb.mutable_plan(1)
                                   ->mutable_opr()
                                   ->mutable_join()
                                   ->mutable_left_plan()
                                   ->mutable_plan(0)
                                   ->mutable_opr()
                                   ->mutable_join()
                                   ->mutable_right_plan()
                                   ->mutable_plan(1)
                                   ->mutable_opr()
                                   ->mutable_edge();
  opr.mutable_alias()->set_value(-1);
  std::string query_out;
  pb.SerializeToString(&query_out);

  write_pb(output_name, query_out);
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "query-dir,q", bpo::value<std::string>(), "query dir")(
      "output-dir,o", bpo::value<std::string>(), "output dir");

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string query_dir = vm["query-dir"].as<std::string>();
  std::string output_dir = vm["output-dir"].as<std::string>();

  process_ic5(query_dir, output_dir);

  return 0;
}