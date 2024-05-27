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

#ifndef GRAPHSCOPE_DATABASE_GRAPH_DB_SESSION_H_
#define GRAPHSCOPE_DATABASE_GRAPH_DB_SESSION_H_

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/compact_transaction.h"
#include "flex/engines/graph_db/database/insert_transaction.h"
#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/database/single_edge_insert_transaction.h"
#include "flex/engines/graph_db/database/single_vertex_insert_transaction.h"
#include "flex/engines/graph_db/database/transaction_utils.h"
#include "flex/engines/graph_db/database/update_transaction.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/column.h"
#include "flex/utils/result.h"

namespace gs {

class GraphDB;
class WalWriter;

class GraphDBSession {
 public:
  enum class InputFormat : uint8_t {
    kCppEncoder = 0,
    kCypherJson = 1,               // External usage format
    kCypherInternalAdhoc = 2,      // Internal format for adhoc query
    kCypherInternalProcedure = 3,  // Internal format for procedure
  };

  static constexpr int32_t MAX_RETRY = 3;
  static constexpr int32_t MAX_PLUGIN_NUM = 256;  // 2^(sizeof(uint8_t)*8)
  static constexpr const char* kCypherJson = "\x01";
  static constexpr const char* kCypherInternalAdhoc = "\x02";
  static constexpr const char* kCypherInternalProcedure = "\x03";
  GraphDBSession(GraphDB& db, Allocator& alloc, WalWriter& logger,
                 const std::string& work_dir, int thread_id)
      : db_(db),
        alloc_(alloc),
        logger_(logger),
        work_dir_(work_dir),
        thread_id_(thread_id),
        eval_duration_(0),
        query_num_(0) {
    for (auto& app : apps_) {
      app = nullptr;
    }
  }
  ~GraphDBSession() {}

  ReadTransaction GetReadTransaction();

  InsertTransaction GetInsertTransaction();

  SingleVertexInsertTransaction GetSingleVertexInsertTransaction();

  SingleEdgeInsertTransaction GetSingleEdgeInsertTransaction();

  UpdateTransaction GetUpdateTransaction();

  CompactTransaction GetCompactTransaction();

  bool BatchUpdate(UpdateBatch& batch);

  const MutablePropertyFragment& graph() const;
  MutablePropertyFragment& graph();
  const GraphDB& db() const;

  const Schema& schema() const;

  std::shared_ptr<ColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const;

  // Get vertex id column.
  std::shared_ptr<RefColumnBase> get_vertex_id_column(uint8_t label) const;

  Result<std::vector<char>> Eval(const std::string& input);

  void GetAppInfo(Encoder& result);

  int SessionId() const;

  bool Compact();

  double eval_duration() const;

  const AppMetric& GetAppMetric(int idx) const;

  int64_t query_num() const;

  AppBase* GetApp(int idx);

 private:
  /**
   * @brief Parse the input format of the query.
   *        There are four formats:
   *       0. CppEncoder: This format will be used by interactive-sdk to submit
   * c++ stored prcoedure queries. The second last byte is the query id.
   *       1. CypherJson: This format will be sended by interactive-sdk, the
   *        input is a json string + '\x01'
   *         {
   *            "query_name": "example",
   *            "arguments": {
   *               "value": 1,
   *               "type": {
   *                "primitive_type": "DT_SIGNED_INT32"
   *                }
   *            }
   *          }
   *       2. CypherInternalAdhoc: This format will be used by compiler to
   *        submit adhoc query, the input is a string + '\x02', the string is
   *        the path to the dynamic library.
   *       3. CypherInternalProcedure: This format will be used by compiler to
   *        submit procedure query, the input is a proto-encoded string +
   *        '\x03', the string is the path to the dynamic library.
   * @param input The input query.
   * @param str_len The length of the valid payload(other than the format and
   * type bytes)
   * @return The id of the query.
   */
  Result<uint8_t> parse_query_type(const std::string& input, size_t& str_len);
  GraphDB& db_;
  Allocator& alloc_;
  WalWriter& logger_;
  std::string work_dir_;
  int thread_id_;

  std::array<AppWrapper, MAX_PLUGIN_NUM> app_wrappers_;
  std::array<AppBase*, MAX_PLUGIN_NUM> apps_;
  std::array<AppMetric, MAX_PLUGIN_NUM> app_metrics_;

  std::atomic<int64_t> eval_duration_;
  std::atomic<int64_t> query_num_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_GRAPH_DB_SESSION_H_
