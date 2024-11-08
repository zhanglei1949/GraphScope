#include "flex/engines/graph_db/app/cypher_read_app.h"
#include "flex/engines/graph_db/app/cypher_app_utils.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

namespace gs {

bool CypherReadApp::Query(const GraphDBSession& graph, Decoder& input,
                          Encoder& output) {
  auto txn = graph.GetReadTransaction();
  std::string_view bytes = input.get_bytes();

  size_t sep = bytes.find_first_of("&?");
  auto query_str = bytes.substr(0, sep);
  auto params_str = bytes.substr(sep + 2);
  std::map<std::string, std::string> params;
  parse_params(params_str, params);
  auto query = std::string(query_str.data(), query_str.size());
  const std::string statistics = db_.work_dir() + "/statistics.json";
  const std::string& compiler_yaml = db_.work_dir() + "/.graph.yaml";
  if (plan_cache_.count(query)) {
    // LOG(INFO) << "Hit cache for query ";
  } else {
    auto& query_cache = db_.getQueryCache();
    std::string_view plan_str;
    if (query_cache.get(query, plan_str)) {
      physical::PhysicalPlan plan;
      if (!plan.ParseFromString(std::string(plan_str))) {
        return false;
      }
      plan_cache_[query] = plan;
    } else {
      for (int i = 0; i < 3; ++i) {
        if (!generate_plan(query, statistics, compiler_yaml, plan_cache_)) {
          LOG(ERROR) << "Generate plan failed for query: " << query;
        } else {
          query_cache.put(query, plan_cache_[query].SerializeAsString());
          break;
        }
      }
    }
  }

  const auto& plan = plan_cache_[query];

  // LOG(INFO) << "plan: " << plan.DebugString();

  gs::runtime::GraphReadInterface gri(txn);
  auto ctx = runtime::runtime_eval(plan, gri, params, timer_);

  runtime::eval_sink_encoder(ctx, gri, output);
  return true;
}
AppWrapper CypherReadAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new CypherReadApp(db), NULL);
}
}  // namespace gs