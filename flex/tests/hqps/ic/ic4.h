
#ifndef IC4_H
#define IC4_H

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {
// Auto generated expression class definition
struct IC4expr0 {
 public:
  using result_t = bool;
  IC4expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC4expr1 {
 public:
  using result_t = int32_t;
  IC4expr1(int64_t startDate, int64_t endDate)
      : startDate_(startDate), endDate_(endDate) {}

  inline auto operator()(int64_t creationDate, int64_t creationDate_0) const {
    if (creationDate >= startDate_ && creationDate_0 < endDate_) {
      return 1;
    }

    return 0;
  }

 private:
  int64_t startDate_;
  int64_t endDate_;
};

struct IC4expr5 {
 public:
  using result_t = int32_t;
  IC4expr5(int64_t startDate) : startDate_(startDate) {}

  inline auto operator()(int64_t creationDate) const {
    if (creationDate < startDate_) {
      return 1;
    }

    return 0;
  }

 private:
  int64_t startDate_;
};

struct IC4expr9 {
 public:
  using result_t = bool;
  IC4expr9() {}

  inline auto operator()(int32_t var6, int32_t var7) const {
    return var6 > 0 && var7 == 0;
  }

 private:
};

// Auto generated query class definition
class IC4 {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  // Query function for query class
  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) const {
    auto personId = input.get_long();
    auto startDate = input.get_long();
    int32_t durationDays = input.get_int();
    // auto endDate = input.get_long();
    int64_t milli_sec_per_day = 24 * 60 * 60 * 1000l;
    auto endDate = startDate + milli_sec_per_day * durationDays;

    auto expr0 = gs::make_filter(IC4expr0(personId),
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
    auto ctx5 = Engine::template Dedup<0, 1>(std::move(ctx4));
    auto ctx6 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx5),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_expr<INPUT_COL_ID(1), INPUT_COL_ID(1)>(
                       IC4expr1(startDate, endDate),
                       gs::PropertySelector<int64_t>("creationDate"),
                       gs::PropertySelector<int64_t>("creationDate")),
                   gs::make_mapper_with_expr<INPUT_COL_ID(1)>(
                       IC4expr5(startDate),
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
        gs::make_filter(IC4expr9(), gs::PropertySelector<int32_t>("None"),
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

    for (auto iter : ctx10) {
      auto eles = iter.GetAllElement();
      output.put_string_view(std::get<0>(eles));
      output.put_int(std::get<1>(eles));
    }
  }

  void Query(const MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    int64_t id = input.get<int64_t>("personIdQ4");
    int64_t start_date = input.get<int64_t>("startDate");
    int64_t durationDays = input.get<int32_t>("durationDays");
    // auto end_date = start_date + durationDays * 24 * 3600 * 1000;

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_long(start_date);
    input_encoder.put_int(durationDays);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("tagName", output_decoder.get_string());  // id
      node.put("postCount", output_decoder.get_int());   // dist

      output.push_back(std::make_pair("", node));
    }
  }
};
}  // namespace gs

#endif  // IC4_H
