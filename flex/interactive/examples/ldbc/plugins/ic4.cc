// Generated by query_generator.h
// This file is generated by codegen/query_generator.h
// DO NOT EDIT

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

namespace gs {
// Auto generated expression class definition
struct Query0expr0 {
 public:
  using result_t = bool;
  static constexpr bool filter_null = true;
  Query0expr0(int64_t personId) : personId(personId) {}

  inline auto operator()(LabelKey label, int64_t id) const {
    return ((label<WithIn> std::array<int64_t, 1>{1}) && (id == personId));
  }

 private:
  int64_t personId;
};

struct Query0expr1 {
 public:
  using result_t = int32_t;
  static constexpr bool filter_null = true;
  Query0expr1(int64_t endDate, int64_t startDate)
      : endDate(endDate), startDate(startDate) {}

  inline auto operator()(int64_t creationDate, int64_t creationDate_0) const {
    if (creationDate < endDate && creationDate_0 >= startDate) {
      return 1;
    }

    return 0;
  }

 private:
  int64_t endDate;
  int64_t startDate;
};

struct Query0expr5 {
 public:
  using result_t = int32_t;
  static constexpr bool filter_null = true;
  Query0expr5(int64_t startDate) : startDate(startDate) {}

  inline auto operator()(int64_t creationDate) const {
    if (startDate > creationDate) {
      return 1;
    }

    return 0;
  }

 private:
  int64_t startDate;
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
  results::CollectiveResults Query(int64_t personId, int64_t endDate,
                                   int64_t startDate) const {
    auto expr0 = gs::make_filter(Query0expr0(personId),
                                 gs::PropertySelector<LabelKey>("label"),
                                 gs::PropertySelector<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);
    auto ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0, (label_id_t) 3);
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
                       gs::PropertySelector<int64_t>("creationDate"),
                       gs::PropertySelector<int64_t>("creationDate")),
                   gs::make_mapper_with_expr<1>(
                       Query0expr5(startDate),
                       gs::PropertySelector<int64_t>("creationDate"))});
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
    auto expr4 =
        gs::make_filter(Query0expr9(), gs::PropertySelector<int32_t>("None"),
                        gs::PropertySelector<int32_t>("None"));
    auto ctx8 = Engine::template Select<INPUT_COL_ID(1), INPUT_COL_ID(2)>(
        graph, std::move(ctx7), std::move(expr4));

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

    int64_t var3 = decoder.get_long();

    auto res = Query(var0, var1, var3);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    if (!res_str.empty()) {
      encoder.put_string_view(res_str);
    }
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
