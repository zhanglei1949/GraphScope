#include "flex/engines/graph_db/app/adhoc_app.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

bool AdhocReadApp::Query(const GraphDBSession& graph, Decoder& input,
                         Encoder& output) {
#if 1
  auto txn = graph.GetReadTransaction();
#else
  runtime::graph_interface_impl::DummyGraph g;
  runtime::GraphReadInterface txn(g);
#endif

  std::string_view plan_str = input.get_bytes();
  physical::PhysicalPlan plan;
  if (!plan.ParseFromString(std::string(plan_str))) {
    LOG(ERROR) << "Parse plan failed...";
    return false;
  }

  LOG(INFO) << "plan: " << plan.DebugString();

  auto ctx = runtime::runtime_eval(plan, txn, {}, timer_);

  runtime::eval_sink(ctx, txn, output);

  return true;
}
AppWrapper AdhocReadAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new AdhocReadApp(), NULL);
}
}  // namespace gs