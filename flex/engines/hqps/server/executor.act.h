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

#ifndef ENGINES_HQPS_SERVER_EXECUTOR_ACT_H_
#define ENGINES_HQPS_SERVER_EXECUTOR_ACT_H_

#include "flex/engines/graph_db/server/types.h"

#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>

namespace hqps_server {

class ANNOTATION(actor:impl) executor : public hiactor::actor {
 public:
  executor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr);
  ~executor() override;

  seastar::future<::server::query_result> ANNOTATION(actor:method) run_query(::server::query_param&& param);

  seastar::future<::server::query_result> ANNOTATION(actor:method) run_adhoc_query(::server::query_param&& param);

  // DECLARE_RUN_QUERYS;
  /// Declare `do_work` func here, no need to implement.
  ACTOR_DO_WORK()

 private:
  int32_t your_private_members_ = 0;
};

}  // namespace hqps_server

#endif  // ENGINES_HQPS_SERVER_EXECUTOR_ACT_H_
