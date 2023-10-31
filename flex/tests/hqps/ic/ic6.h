#ifndef IC6_H
#define IC6_H

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {
// Auto generated expression class definition
struct IC6left_expr0 {
 public:
  using result_t = bool;
  IC6left_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC6right_expr0 {
 public:
  using result_t = bool;
  IC6right_expr0(std::string_view tagName) : tagName_(tagName) {}

  inline auto operator()(std::string_view name) const {
    return (true) && (name == tagName_);
  }

 private:
  std::string_view tagName_;
};

struct IC6right_expr1 {
 public:
  using result_t = bool;
  IC6right_expr1(std::string_view tagName) : tagName_(tagName) {}

  inline auto operator()(std::string_view name) const {
    return name != tagName_;
  }

 private:
  std::string_view tagName_;
};

// Auto generated query class definition
class IC6 {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  // Query function for query class
  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) const {
    int64_t personId = input.get_long();
    std::string_view tagName = input.get_string();

    auto left_expr0 = gs::make_filter(IC6left_expr0(personId),
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
    auto left_ctx1 =
        Engine::PathExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(left_ctx0), std::move(left_path_opt2));
    auto left_edge_expand_opt3 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0, (label_id_t) 3);
    auto left_ctx2 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(left_ctx1), std::move(left_edge_expand_opt3));

    auto left_ctx3 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_ctx2),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
            gs::PropertySelector<grape::EmptyType>(""))});

    auto right_expr0 =
        gs::make_filter(IC6right_expr0(tagName),
                        gs::PropertySelector<std::string_view>("name"));
    auto right_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 7, std::move(right_expr0));

    auto right_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 1, (label_id_t) 3);
    auto right_ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(right_ctx0), std::move(right_edge_expand_opt0));

    auto right_edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 1, (label_id_t) 7);
    auto right_ctx2 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(1)>(
            graph, std::move(right_ctx1), std::move(right_edge_expand_opt1));

    auto right_expr3 =
        gs::make_filter(IC6right_expr1(tagName),
                        gs::PropertySelector<std::string_view>("name"));
    auto right_get_v_opt2 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{(label_id_t) 7},
        std::move(right_expr3));
    auto right_ctx3 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(right_ctx2), std::move(right_get_v_opt2));

    auto left_ctx4 = Engine::template Join<0, 1, gs::JoinKind::InnerJoin>(
        std::move(left_ctx3), std::move(right_ctx3));
    GroupKey<2, std::string_view> left_group_key4(
        gs::PropertySelector<std::string_view>("name"));

    auto left_agg_func5 = gs::make_aggregate_prop<gs::AggFunc::COUNT_DISTINCT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 0>{});

    auto left_ctx5 = Engine::GroupBy(graph, std::move(left_ctx4),
                                     std::tuple{left_group_key4},
                                     std::tuple{left_agg_func5});
    auto left_ctx6 = Engine::Sort(
        graph, std::move(left_ctx5), gs::Range(0, 10),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t>(""),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view>("")});
    for (auto iter : left_ctx6) {
      auto eles = iter.GetAllElement();
      output.put_string_view(std::get<0>(eles));
      output.put_int(std::get<1>(eles));
    }
  }
  void Query(const MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ6");
    std::string tagName = input.get<std::string>("tagName");
    int32_t limit = 10;

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(tagName);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("tagName", output_decoder.get_string());  // id
      node.put("postCount", output_decoder.get_long());  // post cnt

      output.push_back(std::make_pair("", node));
    }
  }
};
}  // namespace gs

#endif  // IC6_H
