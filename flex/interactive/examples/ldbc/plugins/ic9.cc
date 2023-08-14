// Generated by query_generator.h
// This file is generated by codegen/query_generator.h
// DO NOT EDIT

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "interactive_utils.h"

namespace gs {
// Auto generated expression class definition
struct Query0expr1 {
 public:
  using result_t = bool;
  static constexpr bool filter_null = true;
  Query0expr1(Date maxDate) : maxDate(maxDate) {}

  inline auto operator()(Date creationDate) const {
    return creationDate < maxDate;
  }

 private:
  Date maxDate;
};

struct Query0expr2 {
 public:
  using result_t = bool;
  static constexpr bool filter_null = true;
  Query0expr2() {}

  inline auto operator()(GlobalId var5, GlobalId var6) const {
    return var5 != var6;
  }

 private:
};

// Auto generated query class definition
class Query0 : public AppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  using gid_t = typename gs::MutableCSRInterface::gid_t;
  // constructor
  Query0(const GraphDBSession& session) : graph(session) {}
  // Query function for query class
  results::CollectiveResults Query(int64_t personId, int64_t maxDate) const {
    auto ctx0 =
        Engine::template ScanVertexWithOid<gs::AppendOpt::Persist, int64_t>(
            graph, 1, std::vector<int64_t>{personId});

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto get_v_opt0 = make_getv_opt(gs::VOpt::Other,
                                    std::array<label_id_t, 1>{(label_id_t) 1});

    auto path_opt2 = gs::make_path_expandv_opt(
        std::move(edge_expand_opt1), std::move(get_v_opt0), gs::Range(1, 3));
    auto ctx1 = Engine::PathExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
        graph, std::move(ctx0), std::move(path_opt2));
    auto edge_expand_opt3 = gs::make_edge_expand_multiv_opt(
        gs::Direction::In, std::vector<std::array<label_id_t, 3>>{
                               std::array<label_id_t, 3>{2, 1, 0},
                               std::array<label_id_t, 3>{3, 1, 0}});
    auto ctx2 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(ctx1), std::move(edge_expand_opt3));

    auto expr2 = gs::make_filter(Query0expr1(maxDate),
                                 gs::PropertySelector<Date>("creationDate"));
    auto get_v_opt4 =
        make_getv_opt(gs::VOpt::Itself,
                      std::array<label_id_t, 2>{(label_id_t) 2, (label_id_t) 3},
                      std::move(expr2));
    auto ctx3 = Engine::template GetV<gs::AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx2), std::move(get_v_opt4));
    auto expr3 =
        gs::make_filter(Query0expr2(), gs::PropertySelector<GlobalId>("None"),
                        gs::PropertySelector<GlobalId>("None"));
    auto ctx4 = Engine::template Select<INPUT_COL_ID(1), INPUT_COL_ID(0)>(
        graph, std::move(ctx3), std::move(expr3));

    auto ctx5 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx4),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    auto ctx6 = Engine::Sort(
        graph, std::move(ctx5), gs::Range(0, 20),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 1, Date>("creationDate"),
            gs::OrderingPropPair<gs::SortOrder::ASC, 1, int64_t>("id")});
    auto ctx7 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx6),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<std::string_view>("content")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<std::string_view>("imageFile")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<Date>("creationDate"))});
    return Engine::Sink(graph, ctx7,
                        std::array<int32_t, 7>{3, 4, 5, 6, 7, 8, 9});
  }
  // Wrapper query function for query class
  bool Query(Decoder& decoder, Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    int64_t var0 = decoder.get_long();

    int64_t var1 = decoder.get_long();

    auto res = Query(var0, var1);
    // dump results to string
    encode_ic9_result(res, encoder);
    return true;
  }
  // private members
 private:
  gs::MutableCSRInterface graph;
};
}  // namespace gs

// extern c interfaces
extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::Query0* app = new gs::Query0(db);
  return static_cast<void*>(app);
}
void DeleteApp(void* app) {
  if (app != nullptr) {
    gs::Query0* casted = static_cast<gs::Query0*>(app);
    delete casted;
  }
}
}
