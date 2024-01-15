/**icensed under the Apache License, Version 2.0 (the "License");
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
#include <chrono>
#include <fstream>
#include "flex/engines/graph_db/database/graph_db.h"

#include <iostream>
#include <vector>

namespace bpo = boost::program_options;
using namespace std::chrono_literals;

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("data-path,d", bpo::value<std::string>(),
                                      "data directory path")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file");

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

  uint32_t shard_num = 1;

  std::string graph_schema_path = "";
  std::string data_path = "";

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

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  db.Open(schema, data_path, shard_num);

  auto txn = db.GetReadTransaction();
  gs::label_t user_label_id = db.graph().schema().get_vertex_label_id("User");
   auto& user_subIndustry_col=(
		                           *(std::dynamic_pointer_cast<gs::StringMapColumn<uint16_t>>(db.
							   get_vertex_property_column(user_label_id,"subIndustry"))));

  //gs::label_t org_label_id = db.graph().schema().get_vertex_label_id("DingEduOrg");
  size_t num = db.graph().vertex_num(user_label_id);
  std::map<std::string_view,size_t> mp;
  std::vector<uint32_t> vec;
  size_t null_count = 0;
  for(size_t i = 0; i < num; ++i){
	  auto sw = user_subIndustry_col.get_view(i);
	  if(sw == "")null_count++;
	  else {mp[sw] += 1;
	  vec.emplace_back(i);}

  }
  std::cout << "null count: " << null_count << "\n";
  //gs::label_t workat_label_id = db.graph().schema().get_edge_label_id("StudyAt");

  //auto workat_oe = txn.GetOutgoingGraphView<gs::char_array<16>>(
    //  user_label_id, org_label_id, workat_label_id);
 // std::vector<std::pair<int, gs::vid_t>> vec;
  //for (size_t i = 0; i < num; ++i) {
    //vec.emplace_back(workat_oe.get_edges(i).estimated_degree(), i);
  //}
  //std::sort(vec.begin(), vec.end());
  size_t len = vec.size();
  std::vector<int64_t> tmp;
  while (len) {
    auto& b = vec[--len];
    //if (a == 0)
     // break;
    //else 
    {
      int64_t oid = db.graph().get_oid(user_label_id, b).AsInt64();
      tmp.emplace_back(oid);
    }
    if (tmp.size() == 1000000) {
      std::cout << tmp.size() << "\n";
      break;
    }

  }
  {
  std::ofstream out("./subIndustry_count");
	  std::vector<std::pair<size_t, std::string_view>> vec;
  for(auto&[a,b]: mp){
	  vec.emplace_back(b,a);
	  //std::cout << a << " " << b << "\n"; 
  }
  std::sort(vec.begin(),vec.end());
  size_t sum = 0;
  size_t count = 0;
  for(auto&[b,a] : vec){
	  out << a << " " << b << "\n";
	  sum += 1ull*b*b;
	  count += b;
  }
  //int n = vec.size();
  std::cout << count << "\n";
  std::cout <<sum << " avg: "<< (sum / count) << "\n";
  size_t cc = 0;
  for(auto&[b,a] : vec){
	  cc += b;
	  if(cc*2>= count&& (cc-b)*2 < count){
		  std::cout << "50%: " << b << "\n";
	  }else if(cc*4/3 >= count && (cc - b)*4/3 < count){
		  std::cout << "75%: " << b << "\n";
	  }else if(cc*10/9 >= count && (cc-b)*10/9 < count){
		  std::cout << "90% " << b << "\n";
	  }else if(cc*100/95 >= count && (cc-b)*100/95 < count){
		  std::cout << "95%: " << b << "\n";
	  }else if(cc*100/99 >= count && (cc-b)*100/99 < count){
		  std::cout << "99%: " << b << "\n";
	  }
  }
  //std::cout << "50%: " << vec[n/2].first << "\n";
  //std::cout << "75%: " << vec[n*3/4].first << "\n";
  //std::cout << "90%: " << vec[n*9/10].first << "\n";
  //std::cout << "95%: " << vec[n*95/100].first << "\n";
  //std::cout << "99%: " << vec[n*99/100].first << "\n";
  //std::cout << "99.5%: " << vec[n*995/1000].first << "\n";
  }

  //std::ofstream out("./subIndustry");
//  for(auto oid: tmp){
//	  out << oid << "\n";
 // }
 // out.close();
  //fwrite(tmp.data(), sizeof(int64_t), tmp.size(), fp);
 // fflush(fp);
  //fclose(fp);

  // std::cout << "max : " << deg2.back() << "\n";

  // std::cout << timer.get_time() / 1us << " microseconds\n";
}

