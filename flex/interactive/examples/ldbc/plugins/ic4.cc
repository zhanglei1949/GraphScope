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
  using result_t = int32_t;
  static constexpr bool filter_null = true;
  Query0expr1(Date endDate, Date startDate)
      : endDate(endDate), startDate(startDate) {}

  inline auto operator()(Date creationDate, Date creationDate_0) const {
    if (creationDate < endDate && creationDate_0 >= startDate) {
      return 1;
    }

    return 0;
  }

 private:
  Date endDate;
  Date startDate;
};

struct Query0expr5 {
 public:
  using result_t = int32_t;
  static constexpr bool filter_null = true;
  Query0expr5(Date startDate) : startDate(startDate) {}

  inline auto operator()(Date creationDate) const {
    if (startDate > creationDate) {
      return 1;
    }

    return 0;
  }

 private:
  Date startDate;
};

struct Query0expr9 {
 public:
  using result_t = bool;
  static constexpr bool filter_null = true;
  Query0expr9() {}

  inline auto operator()(int32_t var6, int32_t var7) const {
    return var6 > 0 && var7 == 0;
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
  results::CollectiveResults Query(int64_t personId, Date endDate,
                                   Date startDate) const {
    auto ctx0 =
        Engine::template ScanVertexWithOid<gs::AppendOpt::Persist, int64_t>(
            graph, 1, std::vector<int64_t>{personId});

    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);
    auto ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expand_multiv_opt(
        gs::Direction::In, std::vector<std::array<label_id_t, 3>>{
                               std::array<label_id_t, 3>{2, 1, 0},
                               std::array<label_id_t, 3>{3, 1, 0}});
    auto ctx2 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(ctx1), std::move(edge_expand_opt1));

    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 1, (label_id_t) 7);
    auto ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(2)>(
            graph, std::move(ctx2), std::move(edge_expand_opt2));

    auto ctx4 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx3),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(3)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    auto ctx5 = Engine::template Dedup<0, 1>(
        graph, std::move(ctx4),
        std::tuple{GlobalIdSelector(), GlobalIdSelector()});
    auto ctx6 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx5),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_expr<1, 1>(
                       Query0expr1(endDate, startDate),
                       gs::PropertySelector<Date>("creationDate"),
                       gs::PropertySelector<Date>("creationDate")),
                   gs::make_mapper_with_expr<1>(
                       Query0expr5(startDate),
                       gs::PropertySelector<Date>("creationDate"))});
    GroupKey<0, grape::EmptyType> group_key3(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto agg_func4 = gs::make_aggregate_prop<gs::AggFunc::SUM>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 1>{});

    auto agg_func5 = gs::make_aggregate_prop<gs::AggFunc::SUM>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 2>{});

    auto ctx7 = Engine::GroupBy(graph, std::move(ctx6), std::tuple{group_key3},
                                std::tuple{agg_func4, agg_func5});
    auto expr3 =
        gs::make_filter(Query0expr9(), gs::PropertySelector<int32_t>("None"),
                        gs::PropertySelector<int32_t>("None"));
    auto ctx8 = Engine::template Select<INPUT_COL_ID(1), INPUT_COL_ID(2)>(
        graph, std::move(ctx7), std::move(expr3));

    auto ctx9 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx8),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("name")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    auto ctx10 = Engine::Sort(
        graph, std::move(ctx9), gs::Range(0, 10),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 1, int32_t>(""),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view>("")});
    return Engine::Sink(graph, ctx10, std::array<int32_t, 2>{8, 6});
  }
  // Wrapper query function for query class
  bool Query(Decoder& decoder, Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    int64_t var0 = decoder.get_long();

    int64_t var1 = decoder.get_long();

    int32_t days = decoder.get_int();
    const int64_t milli_sec_per_day = 24 * 60 * 60 * 1000l;
    int64_t end_date = var1 + days * milli_sec_per_day;

    LOG(INFO) << "Query0: personId=" << var0 << ", start=" << var1
              << ", end=" << end_date;

    auto res = Query(var0, end_date, var1);
    encode_ic4_result(res, encoder);
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