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

#include "flex/engines/http_server/executor.act.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/engines/http_server/stored_procedure.h"

// Temporally included for benchmark
#include "flex/tests/hqps/ic/ic1.h"
#include "flex/tests/hqps/ic/ic10.h"
// #include "flex/tests/hqps/ic/ic11.h"
// #include "flex/tests/hqps/ic/ic11_v2.h"
// #include "flex/tests/hqps/ic/ic12.h"
// #include "flex/tests/hqps/ic/ic2.h"
// #include "flex/tests/hqps/ic/ic3_v2.h"
// #include "flex/tests/hqps/ic/ic4.h"
// #include "flex/tests/hqps/ic/ic5_v2.h"
// #include "flex/tests/hqps/ic/ic6.h"
// #include "flex/tests/hqps/ic/ic7.h"
// #include "flex/tests/hqps/ic/ic8.h"
// #include "flex/tests/hqps/ic/ic9.h"

#include <seastar/core/print.hh>

namespace server {

executor::~executor() {
  // finalization
  // ...
}

executor::executor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr)
    : hiactor::actor(exec_ctx, addr) {
  set_max_concurrency(1);  // set max concurrency for task reentrancy (stateful)
  // initialization
  // ...
}

seastar::future<query_result> executor::run_graph_db_query(
    query_param&& param) {
  auto ret = gs::GraphDB::get()
                 .GetSession(hiactor::local_shard_id())
                 .Eval(param.content);
  seastar::sstring content(ret.data(), ret.size());
  return seastar::make_ready_future<query_result>(std::move(content));
}

// run_query_for stored_procedure
seastar::future<query_result> executor::run_hqps_benchmark(
    query_param&& param) {
  gs::MutableCSRInterface graph_store(
      gs::GraphDB::get().GetSession(hiactor::local_shard_id()));
  auto& str = param.content;
  const char* str_data = str.data();
  size_t str_length = str.size();
  VLOG(1) << "Receive pay load: " << str_length
          << " bytes, on shard: " << hiactor::local_shard_id();
  uint8_t type = str.back();
  size_t payload_length = str_length - 1;

  std::vector<char> result_buffer;

  gs::Decoder decoder(str_data, payload_length);
  gs::Encoder encoder(result_buffer);

  switch (type) {
    // case 1:
    //   thread_local gs::IC1 ic1;
    //   LOG(INFO) << "Run IC1";
    //   ic1.Query(graph_store, decoder, encoder);
    //   break;
    // case 2:
    //   thread_local gs::IC2 ic2;
    //   LOG(INFO) << "Run IC2";
    //   ic2.Query(graph_store, decoder, encoder);
    //   break;
    // case 3:
    //   thread_local gs::IC3 ic3;
    //   LOG(INFO) << "Run IC3";
    //   ic3.Query(graph_store, decoder, encoder);
    //   break;
    // case 4:
    //   thread_local gs::IC4 ic4;
    //   LOG(INFO) << "Run IC4";
    //   ic4.Query(graph_store, decoder, encoder);
    //   break;
    // case 5:
    //   thread_local gs::IC5 ic5;
    //   LOG(INFO) << "Run IC5";
    //   ic5.Query(graph_store, decoder, encoder);
    //   break;

  // case 6:
  //   thread_local gs::IC6 ic6;
  //   LOG(INFO) << "Run IC6";
  //   ic6.Query(graph_store, decoder, encoder);
  //   break;

  // case 7:
  //   thread_local gs::IC7 ic7;
  //   LOG(INFO) << "Run IC7";
  //   ic7.Query(graph_store, decoder, encoder);
  //   break;
  // case 8:
  //   thread_local gs::IC8 ic8;
  //   LOG(INFO) << "Run IC8";
  //   ic8.Query(graph_store, decoder, encoder);
  //   break;
  // case 9:
  //   thread_local gs::IC9 ic9;
  //   LOG(INFO) << "Run IC9";
  //   ic9.Query(graph_store, decoder, encoder);
  //   break;
  case 10:
    thread_local gs::IC10 ic10;
    LOG(INFO) << "Run IC10";
    ic10.Query(graph_store, decoder, encoder);
    break;
    // case 11:
    //   thread_local gs::IC11 ic11;
    //   LOG(INFO) << "Run IC11";
    //   ic11.Query(graph_store, decoder, encoder);
    //   break;
    // case 12:
    //   thread_local gs::IC12 ic12;
    //   LOG(INFO) << "Run IC12";
    //   ic12.Query(graph_store, decoder, encoder);
    //   break;

  default:
    LOG(INFO) << "Not registered app: " << type;
    break;
  }
  return seastar::make_ready_future<query_result>(
      seastar::sstring(result_buffer.data(), result_buffer.size()));
}

// run_query_for stored_procedure
seastar::future<query_result> executor::run_hqps_procedure_query(
    query_param&& param) {
  auto& str = param.content;
  const char* str_data = str.data();
  size_t str_length = str.size();
  LOG(INFO) << "Receive pay load: " << str_length << " bytes";

  query::Query cur_query;
  {
    CHECK(cur_query.ParseFromArray(str.data(), str.size()));
    LOG(INFO) << "Parse query: " << cur_query.DebugString();
  }
  auto& store_procedure_manager = server::StoredProcedureManager::get();
  return store_procedure_manager.Query(cur_query).then(
      [&cur_query](results::CollectiveResults&& hqps_result) {
        LOG(INFO) << "Finish running query: " << cur_query.DebugString();
        LOG(INFO) << "Query results" << hqps_result.DebugString();

        auto tem_str = hqps_result.SerializeAsString();

        seastar::sstring content(tem_str.data(), tem_str.size());
        return seastar::make_ready_future<query_result>(std::move(content));
      });
}

seastar::future<query_result> executor::run_hqps_adhoc_query(
    query_param&& param) {
  LOG(INFO) << "Run adhoc query";
  // The received query's pay load shoud be able to deserialze to physical plan
  auto& str = param.content;
  if (str.size() <= 0) {
    LOG(INFO) << "Empty query";
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Empty query string"));
  }

  const char* str_data = str.data();
  size_t str_length = str.size();
  LOG(INFO) << "Deserialize physical job request" << str_length;

  physical::PhysicalPlan plan;
  bool ret = plan.ParseFromArray(str_data, str_length);
  if (ret) {
    VLOG(10) << "Parse physical plan: " << plan.DebugString();
  } else {
    LOG(ERROR) << "Fail to parse physical plan";
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Fail to parse physical plan"));
  }

  // 0. do codegen gen.
  std::string lib_path = "";
  int32_t job_id = -1;
  auto& codegen_proxy = server::CodegenProxy::get();
  if (codegen_proxy.Initialized()) {
    auto ret = codegen_proxy.do_gen(plan);
    if (ret.has_value()) {
      auto& v = ret.value();
      job_id = v.first;
      lib_path = v.second;
    }
  } else {
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Codegen proxy not initialized"));
  }
  if (job_id == -1) {
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Fail to parse job id from codegen proxy"));
  }
  // 1. load and run.
  LOG(INFO) << "Okay, try to run the query of lib path: " << lib_path
            << ", job id: " << job_id
            << "local shard id: " << hiactor::local_shard_id();

  seastar::sstring content = server::load_and_run(job_id, lib_path);
  return seastar::make_ready_future<query_result>(std::move(content));
}

}  // namespace server
