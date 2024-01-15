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
  gs::label_t user_label_id = db.graph().schema().get_vertex_label_id("User");
  gs::StringMapColumn<uint16_t>& user_subIndustry_col = *(std::dynamic_pointer_cast<gs::StringMapColumn<uint16_t>>
		  (db.get_vertex_property_column(user_label_id,"subIndustry")));
  size_t vnum = db.graph().vertex_num(user_label_id);
  const auto& mp = user_subIndustry_col.get_meta_map();
  size_t dnum = mp.size();
  std::cout <<"vnum: "<< vnum << " dnum: " << dnum << "\n";
  std::vector<std::vector<uint32_t>> G(dnum);
  for(size_t i = 0; i < vnum; ++i){
	  auto industry = user_subIndustry_col.get_view(i);
	  if(industry == "")continue;
	  uint16_t vid;
	  if(!mp.get_index(industry,vid)){
		  std::cout << "error: " << industry << "\n";
	  }
	  G[vid].emplace_back(i);
  }
  std::vector<int> deg(dnum,0);
  { FILE*fp = fopen("ie_User_SubIndustry.nbr","wb");
  
  for(size_t i = 0; i < dnum; ++i){
	  fwrite(G[i].data(),sizeof(uint32_t),G[i].size(),fp);
	  deg[i] = G[i].size();
  }
  fflush(fp);
  fclose(fp);
}
{
	FILE*fp = fopen("ie_User_SubIndustry.deg","wb");
	fwrite(deg.data(),sizeof(int),deg.size(),fp);
	fflush(fp);
	fclose(fp);
}
  /**
  auto txn = db.GetReadTransaction();
  gs::label_t user_label_id = db.graph().schema().get_vertex_label_id("User");
  gs::label_t studyat_label_id = db.graph().schema().get_edge_label_id("StudyAt");
  gs::label_t ding_edu_org_label_id = db.graph().schema().get_vertex_label_id("DingEduOrg");
  auto edges = txn.GetOutgoingGraphView<gs::char_array<16>>(user_label_id,ding_edu_org_label_id,studyat_label_id);
  size_t vnum = db.graph().vertex_num(user_label_id);
  std::vector<std::pair<int,size_t>> vec;
  std::cout << vnum << "\n";
  for(size_t i = 0; i < vnum; ++i){
	 const auto& oe = edges.get_edges(i);
	  int deg = 0;
	  for(auto& e: oe){
		  deg++;
	  }
	  vec.emplace_back(deg,i);
	  if(i% 10000000 == 0){
		  std::cout << i << " " << deg << "\n";
	  }
  }
  std::sort(vec.begin(),vec.end());
  std::vector<int64_t> tmp;
  for(size_t i = 0; i < 1000000; ++i){
	  tmp.emplace_back(db.graph().get_oid(user_label_id,vec[vnum-i-1].second).AsInt64());
  }
  std::ofstream out("./studyat");
  for(auto v: tmp){
	  out << v << "\n";
  }*/
}
