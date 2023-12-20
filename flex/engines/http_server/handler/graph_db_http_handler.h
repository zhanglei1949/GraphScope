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

#ifndef ENGINES_HTTP_SERVER_HANDLER_GRAPH_DB_HTTP_HANDLER_H_
#define ENGINES_HTTP_SERVER_HANDLER_GRAPH_DB_HTTP_HANDLER_H_

#include <seastar/core/alien.hh>
#include <seastar/core/print.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include "flex/engines/http_server/generated/actor/executor_ref.act.autogen.h"
#include "flex/engines/http_server/types.h"

namespace server {

class graph_db_ic_handler : public seastar::httpd::handler_base {
 public:
  graph_db_ic_handler(uint32_t group_id, uint32_t shard_concurrency);
  ~graph_db_ic_handler() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override;

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<executor_ref> executor_refs_;
};

class graph_db_http_handler {
 public:
  graph_db_http_handler(uint16_t http_port);

  void start();
  void stop();

 private:
  seastar::future<> set_routes();

 private:
  const uint16_t http_port_;
  seastar::httpd::http_server_control server_;
};

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_HANDLER_GRAPH_DB_HTTP_HANDLER_H_
