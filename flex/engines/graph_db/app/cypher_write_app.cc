#include "flex/engines/graph_db/app/cypher_write_app.h"
#include "flex/engines/graph_db/app/cypher_app_utils.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

namespace gs {

bool CypherWriteApp::Query(GraphDBSession& graph, Decoder& input,
                           Encoder& output) {
  auto txn = graph.GetInsertTransaction();
  std::string_view bytes = input.get_bytes();

  size_t sep = bytes.find_first_of("&?");
  auto query_str = bytes.substr(0, sep);
  auto params_str = bytes.substr(sep + 2);
  std::map<std::string, std::string> params;
  parse_params(params_str, params);
  auto query = std::string(query_str.data(), query_str.size());
  if (plan_cache_.count(query)) {
    // LOG(INFO) << "Hit cache for query ";
  } else {
    for (int i = 0; i < 3; ++i) {
      if (!generate_plan(query, plan_cache_)) {
        LOG(ERROR) << "Generate plan failed for query: " << query;
      } else {
        break;
      }
    }
  }

  const auto& plan = plan_cache_[query];

  // LOG(INFO) << "plan: " << plan.DebugString();

  auto ctx = runtime::runtime_eval(plan, txn, params);

  // runtime::eval_sink_encoder(ctx, txn, output);
  return true;
}
AppWrapper CypherWriteAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new CypherWriteApp(), NULL);
}
}  // namespace gs