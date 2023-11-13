#ifndef IC5_H
#define IC5_H

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {
// Auto generated expression class definition
struct IC5left_expr0 {
 public:
  using result_t = bool;
  IC5left_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC5left_expr1 {
 public:
  using result_t = bool;
  IC5left_expr1(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const { return id != personId_; }

 private:
  int64_t personId_;
};

struct IC5left_expr2 {
 public:
  using result_t = bool;
  IC5left_expr2(int64_t minDate) : minDate_(minDate) {}

  inline auto operator()(int64_t joinDate) const { return joinDate > minDate_; }

 private:
  int64_t minDate_;
};

struct IC5right_expr0 {
 public:
  using result_t = bool;
  IC5right_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

// Auto generated query class definition
class IC5 {
 private:
  mutable double left_outer_join_time = 0.0;
  mutable double scan_time = 0.0;
  mutable double group_by_time = 0.0;
  mutable double inner_join_time = 0.0;
  mutable double useless_get_v_time = 0.0;
  mutable double get_all_forum_post_time = 0.0;

 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  ~IC5() {
    LOG(INFO) << "IC5 left_outer_join_time: " << left_outer_join_time;
    LOG(INFO) << "group_by_time: " << group_by_time << "s";
    LOG(INFO) << "inner_join_time: " << inner_join_time << "s";
    LOG(INFO) << "useless_get_v_time: " << useless_get_v_time << "s";
    LOG(INFO) << "get_all_forum_post_time: " << get_all_forum_post_time << "s";
    LOG(INFO) << "scan_time: " << scan_time << "s";
  }
  // Query function for query class
  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) const {
    int64_t personId = input.get_long();
    int64_t minDate = input.get_long();

    auto left_expr0 = gs::make_filter(IC5left_expr0(personId),
                                      gs::PropertySelector<int64_t>("id"));
    auto left_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, std::move(left_expr0));

    auto left_edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto left_get_v_opt0 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{(label_id_t) 1});

    auto left_path_opt2 =
        gs::make_path_expandv_opt(std::move(left_edge_expand_opt1),
                                  std::move(left_get_v_opt0), gs::Range(1, 3));
    auto left_ctx1 = Engine::PathExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(0)>(
        graph, std::move(left_ctx0), std::move(left_path_opt2));
    auto left_expr2 = gs::make_filter(IC5left_expr1(personId),
                                      gs::PropertySelector<int64_t>("id"));
    auto left_get_v_opt3 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{(label_id_t) 1},
        std::move(left_expr2));
    auto left_ctx2 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_ctx1), std::move(left_get_v_opt3));
    auto left_expr3 = gs::make_filter(
        IC5left_expr2(minDate), gs::PropertySelector<int64_t>("joinDate"));
    auto left_edge_expand_opt4 = gs::make_edge_expande_opt<int64_t>(
        gs::PropNameArray<int64_t>{"joinDate"}, gs::Direction::In,
        (label_id_t) 4, (label_id_t) 4, std::move(left_expr3));
    auto left_ctx3 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(left_ctx2), std::move(left_edge_expand_opt4));

    auto left_get_v_opt5 = make_getv_opt(
        gs::VOpt::Start, std::array<label_id_t, 1>{(label_id_t) 4});
    auto left_ctx4 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_ctx3), std::move(left_get_v_opt5));
    auto left_ctx5 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_ctx4),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(3)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    auto left_ctx6 = Engine::template Dedup<0, 1>(std::move(left_ctx5));

    auto right_expr0 = gs::make_filter(IC5right_expr0(personId),
                                       gs::PropertySelector<int64_t>("id"));
    auto right_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, std::move(right_expr0));

    auto right_edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto right_get_v_opt0 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{(label_id_t) 1});

    auto right_path_opt2 =
        gs::make_path_expandv_opt(std::move(right_edge_expand_opt1),
                                  std::move(right_get_v_opt0), gs::Range(1, 3));
    auto right_ctx1 =
        Engine::PathExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(right_ctx0), std::move(right_path_opt2));
    auto right_edge_expand_opt3 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0, (label_id_t) 3);
    auto right_ctx2 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(right_ctx1), std::move(right_edge_expand_opt3));

    auto right_edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 3, (label_id_t) 4);
    auto right_ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(2)>(
            graph, std::move(right_ctx2), std::move(right_edge_expand_opt4));

    auto left_ctx7 =
        Engine::template Join<0, 1, 1, 3, gs::JoinKind::LeftOuterJoin>(
            std::move(left_ctx6), std::move(right_ctx3));
    GroupKey<1, int64_t> left_group_key6(gs::PropertySelector<int64_t>("id"));

    auto left_agg_func7 = gs::make_aggregate_prop<gs::AggFunc::FIRST>(
        std::tuple{gs::PropertySelector<std::string_view>("title")},
        std::integer_sequence<int32_t, 1>{});

    auto left_agg_func8 = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 3>{});

    auto left_ctx8 = Engine::GroupBy(
        graph, std::move(left_ctx7), std::tuple{left_group_key6},
        std::tuple{left_agg_func7, left_agg_func8});
    auto left_ctx9 = Engine::Sort(
        graph, std::move(left_ctx8), gs::Range(0, 20),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::DESC, 2, int64_t>(""),
                   gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("")});

    for (auto iter : left_ctx9) {
      auto eles = iter.GetAllElement();
      output.put_string_view(std::get<1>(eles));
      // id is calculated but not used
      output.put_int(std::get<2>(eles));
    }
  }

  void Query(const MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    int64_t id = input.get<int64_t>("personIdQ5");
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
