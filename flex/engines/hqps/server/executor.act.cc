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
#include "flex/engines/hqps/server/executor.act.h"
#include "flex/engines/hqps/server/service.h"
#include "flex/engines/hqps/server/stored_procedure.h"

#include "flex/engines/hqps/server/codegen_proxy.h"
#include "flex/engines/hqps/server/stored_procedure.h"
#include "flex/utils/app_utils.h"

#include <seastar/core/print.hh>
#include "proto_generated_gie/physical.pb.h"
#include "proto_generated_gie/results.pb.h"

#define RECEIVE_JOB_REQUEST

namespace hqps_server {

executor::~executor() {}

executor::executor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr)
    : hiactor::actor(exec_ctx, addr) {}

// run_query_for stored_procedure
seastar::future<::server::query_result> executor::run_query(
    ::server::query_param&& param) {
  auto& str = param.content;
  const char* str_data = str.data();
  size_t str_length = str.size();
  LOG(INFO) << "Receive pay load: " << str_length << " bytes";

  query::Query cur_query;
  {
    CHECK(cur_query.ParseFromArray(str.data(), str.size()));
    LOG(INFO) << "Parse query: " << cur_query.DebugString();
  }
  auto& store_procedure_manager = gs::StoredProcedureManager::get();
  results::CollectiveResults hqps_result =
      store_procedure_manager.Query(cur_query);
  LOG(INFO) << "Finish running query: " << cur_query.DebugString();
  LOG(INFO) << "Query results" << hqps_result.DebugString();

  auto tem_str = hqps_result.SerializeAsString();

  seastar::sstring content(tem_str.data(), tem_str.size());
  return seastar::make_ready_future<::server::query_result>(std::move(content));
}

seastar::future<::server::query_result> executor::run_adhoc_query(
    ::server::query_param&& param) {
  LOG(INFO) << "Run adhoc query";
  // The received query's pay load shoud be able to deserialze to physical plan
  auto& str = param.content;
  if (str.size() <= 0) {
    LOG(INFO) << "Empty query";
    seastar::sstring reply;
    return seastar::make_ready_future<::server::query_result>(std::move(reply));
  }

  const char* str_data = str.data();
  size_t str_length = str.size();
  LOG(INFO) << "Deserialize physical job request" << str_length;

  // Currenly compilers sends the jobRequest, but we expect the physical plan.
  // protocol::JobRequest job_request;
  // physical::PhysicalPlan physical_plan;
  // bool ret = physical_plan.ParseFromArray(str.data(), str.size());
  // if (ret) {
  //   LOG(INFO) << "Successfully parse physical plan: "
  //             << physical_plan.plan_size();
  // } else {
  //   LOG(ERROR) << "Fail to parse job request";
  //   seastar::sstring reply;
  //   return seastar::make_ready_future<query_result>(std::move(reply));
  // }
  physical::PhysicalPlan plan;
  bool ret = plan.ParseFromArray(str_data, str_length);
  if (ret) {
    LOG(INFO) << "Parse physical plan: " << plan.DebugString();
  } else {
    LOG(ERROR) << "Fail to parse physical plan";
    seastar::sstring reply;
    return seastar::make_ready_future<::server::query_result>(std::move(reply));
  }

  // 0. do codegen gen.
  std::string lib_path = "";
  int32_t job_id = -1;
  auto& codegen_proxy = hqps_server::CodegenProxy::get();
  if (codegen_proxy.Initialized()) {
    auto ret = codegen_proxy.do_gen(plan);
    if (ret.has_value()) {
      auto& v = ret.value();
      job_id = v.first;
      lib_path = v.second;
    }
  } else {
    LOG(ERROR) << "Codegen proxy not initialized";
  }
  if (job_id == -1) {
    seastar::sstring reply;
    return seastar::make_ready_future<::server::query_result>(std::move(reply));
  }
  // 1. load and run.
  LOG(INFO) << "Okay, try to run the query of lib path: " << lib_path
            << ", job id: " << job_id;

  seastar::sstring content =
      gs::load_and_run(job_id, lib_path, hiactor::local_shard_id());
  return seastar::make_ready_future<::server::query_result>(std::move(content));
}

}  // namespace hqps_server