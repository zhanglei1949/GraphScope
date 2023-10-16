#ifndef FLEX_TEST_HQPS_IC_IC12_H_
#define FLEX_TEST_HQPS_IC_IC12_H_

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {
// Auto generated expression class definition
struct IC12expr0 {
 public:
  using result_t = bool;
  IC12expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC12expr1 {
 public:
  using result_t = bool;
  IC12expr1(std::string_view tagClassName) : tagClassName_(tagClassName) {}

  inline auto operator()(std::string_view name) const {
    return name == tagClassName_;
  }

 private:
  std::string_view tagClassName_;
};

// Auto generated query class definition
class IC12 {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  // Query function for query class
  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) const {
    int64_t personId = input.get_long();
    std::string_view tagClassName = input.get_string();
    // auto expr0 = gs::make_filter(IC12expr0(personId),
    //                              gs::PropertySelector<int64_t>("id"));
    // auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
    //     graph, 1, std::move(expr0));
    auto ctx0 = Engine::template ScanVertexWithOid<gs::AppendOpt::Persist>(
        graph, 1, personId);

    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);
    auto ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0, (label_id_t) 2);
    auto ctx2 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(ctx1), std::move(edge_expand_opt1));

    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 2, (label_id_t) 3);
    auto ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(2)>(
            graph, std::move(ctx2), std::move(edge_expand_opt2));

    auto edge_expand_opt3 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 1, (label_id_t) 7);
    auto ctx4 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(3)>(
            graph, std::move(ctx3), std::move(edge_expand_opt3));

    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 12, (label_id_t) 6);
    auto ctx5 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(4)>(
            graph, std::move(ctx4), std::move(edge_expand_opt4));

    auto edge_expand_opt6 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 13, (label_id_t) 6);

    auto get_v_opt5 = make_getv_opt(gs::VOpt::Itself,
                                    std::array<label_id_t, 1>{(label_id_t) 6});

    auto path_opt7 = gs::make_path_expandv_opt(
        std::move(edge_expand_opt6), std::move(get_v_opt5), gs::Range(0, 10));
    auto ctx6 = Engine::PathExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(5)>(
        graph, std::move(ctx5), std::move(path_opt7));
    auto expr7 =
        gs::make_filter(IC12expr1(tagClassName),
                        gs::PropertySelector<std::string_view>("name"));
    auto get_v_opt8 = make_getv_opt(gs::VOpt::Itself,
                                    std::array<label_id_t, 1>{(label_id_t) 6},
                                    std::move(expr7));
    auto ctx7 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx6), std::move(get_v_opt8));
    auto ctx8 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx7),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(4)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    GroupKey<1, grape::EmptyType> group_key9(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto agg_func10 = gs::make_aggregate_prop<gs::AggFunc::TO_SET>(
        std::tuple{gs::PropertySelector<std::string_view>("name")},
        std::integer_sequence<int32_t, 3>{});

    auto agg_func11 = gs::make_aggregate_prop<gs::AggFunc::COUNT_DISTINCT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 2>{});

    auto ctx9 = Engine::GroupBy(graph, std::move(ctx8), std::tuple{group_key9},
                                std::tuple{agg_func10, agg_func11});
    auto ctx10 = Engine::Sort(
        graph, std::move(ctx9), gs::Range(0, 20),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::DESC, 2, int64_t>(""),
                   gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("id")});
    auto ctx11 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx10),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    for (auto iter : ctx11) {
      auto eles = iter.GetAllElement();
      output.put_long(std::get<0>(eles));
      output.put_string_view(std::get<1>(eles));
      output.put_string_view(std::get<2>(eles));
      auto tag_names = std::get<3>(eles);
      output.put_int(tag_names.size());
      for (auto tag_name : tag_names) {
        output.put_string_view(tag_name);
      }
      output.put_long(std::get<4>(eles));
    }
  }

  void Query(const MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ12");
    std::string tag_class_name = input.get<std::string>("tagClassName");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(tag_class_name);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("personId", output_decoder.get_long());           // id
      node.put("personFirstName", output_decoder.get_string());  // post cnt
      node.put("personLastName", output_decoder.get_string());
      // new node for list.
      boost::property_tree::ptree tag_names;
      int32_t size = output_decoder.get_int();
      for (auto i = 0; i < size; ++i) {
        std::string str{output_decoder.get_string()};
        tag_names.push_back(
            std::make_pair("", boost::property_tree::ptree(str)));
      }
      node.add_child("tagNames", tag_names);
      node.put("replyCount", output_decoder.get_long());
      output.push_back(std::make_pair("", node));
    }
  }
};
}  // namespace gs

#endif  // FLEX_TEST_HQPS_IC_IC12_H_