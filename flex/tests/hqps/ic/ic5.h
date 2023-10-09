#ifndef IC5_H
#define IC5_H

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {
// Auto generated expression class definition
struct IC5left_left_expr0 {
 public:
  using result_t = bool;
  IC5left_left_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC5left_left_expr1 {
 public:
  using result_t = bool;
  IC5left_left_expr1() {}
  template <typename vertex_id_t>
  inline auto operator()(vertex_id_t var3, vertex_id_t var4) const {
    return var3 != var4;
  }

 private:
};

struct IC5left_right_expr0 {
 public:
  using result_t = bool;
  IC5left_right_expr0(int64_t minDate) : minDate_(minDate) {}

  inline auto operator()(int64_t joinDate) const { return joinDate > minDate_; }

 private:
  int64_t minDate_;
};

struct IC5left_right_expr1 {
 public:
  using result_t = bool;
  IC5left_right_expr1() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC5right_expr0 {
 public:
  using result_t = bool;
  IC5right_expr0() {}

  inline auto operator()() const { return true; }

 private:
};

// Auto generated query class definition
class IC5 {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  // Query function for query class
  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) const {
    int64_t personId = input.get_long();
    int64_t minDate = input.get_long();
    auto left_left_expr0 = gs::make_filter(IC5left_left_expr0(personId),
                                           gs::PropertySelector<int64_t>("id"));
    auto left_left_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, std::move(left_left_expr0));

    auto left_left_edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto left_left_get_v_opt0 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{(label_id_t) 1});

    auto left_left_path_opt2 = gs::make_path_expandv_opt(
        std::move(left_left_edge_expand_opt1), std::move(left_left_get_v_opt0),
        gs::Range(1, 3));
    auto left_left_ctx1 =
        Engine::PathExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(left_left_ctx0), std::move(left_left_path_opt2));
    auto left_left_expr2 = gs::make_filter(
        IC5left_left_expr1(), gs::PropertySelector<grape::EmptyType>("None"),
        gs::PropertySelector<grape::EmptyType>("None"));
    auto left_left_ctx2 =
        Engine::template Select<INPUT_COL_ID(0), INPUT_COL_ID(1)>(
            graph, std::move(left_left_ctx1), std::move(left_left_expr2));

    auto left_left_ctx3 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_ctx2),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
            gs::PropertySelector<grape::EmptyType>(""))});
    auto left_left_ctx4 = Engine::template Dedup<0>(std::move(left_left_ctx3));

    auto left_right_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 4, Filter<TruePredicate>());

    auto left_right_expr0 =
        gs::make_filter(IC5left_right_expr0(minDate),
                        gs::PropertySelector<int64_t>("joinDate"));
    auto left_right_edge_expand_opt0 = gs::make_edge_expande_opt<int64_t>(
        gs::PropNameArray<int64_t>{"joinDate"}, gs::Direction::Out,
        (label_id_t) 4, (label_id_t) 1, std::move(left_right_expr0));
    auto left_right_ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(left_right_ctx0),
            std::move(left_right_edge_expand_opt0));

    auto left_right_get_v_opt1 =
        make_getv_opt(gs::VOpt::End, std::array<label_id_t, 0>{});
    auto left_right_ctx2 =
        Engine::template GetV<gs::AppendOpt::Temp, INPUT_COL_ID(-1)>(
            graph, std::move(left_right_ctx1),
            std::move(left_right_get_v_opt1));
    auto left_right_expr1 = gs::make_filter(IC5left_right_expr1());
    auto left_right_get_v_opt2 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 0>{},
                      std::move(left_right_expr1));
    auto left_right_ctx3 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_right_ctx2),
            std::move(left_right_get_v_opt2));

    auto left_left_ctx5 = Engine::template Join<0, 2, gs::JoinKind::InnerJoin>(
        std::move(left_left_ctx4), std::move(left_right_ctx3));
    auto left_left_ctx6 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_ctx5),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<grape::EmptyType>(""))});

    auto right_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 4, Filter<TruePredicate>());

    auto right_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 3, (label_id_t) 3);
    auto right_ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(right_ctx0), std::move(right_edge_expand_opt0));

    auto right_edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);
    auto right_ctx2 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(1)>(
            graph, std::move(right_ctx1), std::move(right_edge_expand_opt1));

    auto right_expr2 = gs::make_filter(IC5right_expr0());
    auto right_get_v_opt2 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(right_expr2));
    auto right_ctx3 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(right_ctx2), std::move(right_get_v_opt2));

    auto left_left_ctx7 =
        Engine::template Join<0, 1, 0, 2, gs::JoinKind::LeftOuterJoin>(
            std::move(left_left_ctx6), std::move(right_ctx3));
    GroupKey<0, int64_t> left_left_group_key5(
        gs::PropertySelector<int64_t>("id"));

    auto left_left_agg_func6 = gs::make_aggregate_prop<gs::AggFunc::FIRST>(
        std::tuple{gs::PropertySelector<std::string_view>("title")},
        std::integer_sequence<int32_t, 0>{});

    auto left_left_agg_func7 = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 2>{});

    auto left_left_ctx8 = Engine::GroupBy(
        graph, std::move(left_left_ctx7), std::tuple{left_left_group_key5},
        std::tuple{left_left_agg_func6, left_left_agg_func7});
    auto left_left_ctx9 = Engine::Sort(
        graph, std::move(left_left_ctx8), gs::Range(0, 20),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::DESC, 2, int64_t>(""),
                   gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("")});
    for (auto iter : left_left_ctx9) {
      auto eles = iter.GetAllElement();
      output.put_string_view(std::get<1>(eles));
      // id is calculated but not used
      output.put_int(std::get<2>(eles));
    }
  }

  void Query(const MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ5");
    int64_t min_date = input.get<int64_t>("minDate");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_long(min_date);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("forumTitle", output_decoder.get_string());  // id
      node.put("postCount", output_decoder.get_int());      // post cnt

      output.push_back(std::make_pair("", node));
    }
  }
};
}  // namespace gs

#endif  // IC5_H
