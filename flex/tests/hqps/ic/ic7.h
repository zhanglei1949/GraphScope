#ifndef IC7_H
#define IC7_H

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {
// Auto generated expression class definition
struct IC7left_expr0 {
 public:
  using result_t = bool;
  IC7left_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC7right_expr0 {
 public:
  using result_t = bool;
  IC7right_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const { return id == personId_; }

 private:
  int64_t personId_;
};

struct IC7right_expr1 {
 public:
  using result_t = bool;
  IC7right_expr1() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC7left_expr1 {
 public:
  using result_t = bool;
  IC7left_expr1() {}
  template <typename vertex_id_t>
  inline auto operator()(const DefaultEdge<vertex_id_t>& var3) const {
    return NONE == var3;
  }

 private:
};

struct IC7left_expr2 {
 public:
  using result_t = int;
  IC7left_expr2() {}

  inline auto operator()(const int64_t& a, const int64_t b) const {
    return (a - b) / 60000;
  }

 private:
};

// Auto generated query class definition
class IC7 {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  // Query function for query class
  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) const {
    auto personId = input.get_long();
    // auto left_expr0 = gs::make_filter(IC7left_expr0(personId),
    //                                   gs::PropertySelector<int64_t>("id"));
    // auto left_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
    //     graph, 1, std::move(left_expr0));
    auto left_ctx0 = Engine::template ScanVertexWithOid<gs::AppendOpt::Persist>(
        graph, 1, personId);

    auto left_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0,
        std::array<label_id_t, 2>{(label_id_t) 3, (label_id_t) 2});
    auto left_ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(left_ctx0), std::move(left_edge_expand_opt0));

    auto left_edge_expand_opt1 = gs::make_edge_expande_opt<int64_t>(
        gs::PropNameArray<int64_t>{"creationDate"}, gs::Direction::In,
        (label_id_t) 9, (label_id_t) 1);
    auto left_ctx2 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(left_ctx1), std::move(left_edge_expand_opt1));

    auto left_get_v_opt2 = make_getv_opt(
        gs::VOpt::Start, std::array<label_id_t, 1>{(label_id_t) 1});
    auto left_ctx3 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_ctx2), std::move(left_get_v_opt2));

    // auto right_expr0 = gs::make_filter(IC7right_expr0(personId),
    //    gs::PropertySelector<int64_t>("id"));
    // auto right_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
    //     graph, 1, std::move(right_expr0));
    auto right_ctx0 =
        Engine::template ScanVertexWithOid<gs::AppendOpt::Persist>(graph, 1,
                                                                   personId);

    auto right_edge_expand_opt0 = gs::make_edge_expande_opt<int64_t>(
        gs::PropNameArray<int64_t>{"creationDate"}, gs::Direction::Both,
        (label_id_t) 8, (label_id_t) 1);
    auto right_ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(right_ctx0), std::move(right_edge_expand_opt0));

    auto right_get_v_opt1 =
        make_getv_opt(gs::VOpt::Other, std::array<label_id_t, 0>{});
    auto right_ctx2 =
        Engine::template GetV<gs::AppendOpt::Temp, INPUT_COL_ID(-1)>(
            graph, std::move(right_ctx1), std::move(right_get_v_opt1));
    auto right_expr2 = gs::make_filter(IC7right_expr1());
    auto right_get_v_opt2 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(right_expr2));
    auto right_ctx3 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(right_ctx2), std::move(right_get_v_opt2));

    auto left_ctx4 =
        Engine::template Join<0, 3, 0, 2, gs::JoinKind::LeftOuterJoin>(
            std::move(left_ctx3), std::move(right_ctx3));

    auto left_ctx5 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_ctx4),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(3)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<int64_t>("creationDate")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_expr<INPUT_COL_ID(4)>(
                       IC7left_expr1(),
                       gs::PropertySelector<grape::EmptyType>("None"))});

    auto left_ctx6 = Engine::Sort(
        graph, std::move(left_ctx5), gs::Range(0, 2147483647),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::DESC, 2, int64_t>(""),
                   gs::OrderingPropPair<gs::SortOrder::ASC, 1, int64_t>("id")});
    GroupKey<0, grape::EmptyType> left_group_key4(
        gs::PropertySelector<grape::EmptyType>("None"));

    GroupKey<3, grape::EmptyType> left_group_key5(
        gs::PropertySelector<grape::EmptyType>("None"));

    GroupKey<4, grape::EmptyType> left_group_key6(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto left_agg_func7 = gs::make_aggregate_prop<gs::AggFunc::FIRST>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 1>{});

    auto left_agg_func8 = gs::make_aggregate_prop<gs::AggFunc::FIRST>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 2>{});

    auto left_ctx7 = Engine::GroupBy(
        graph, std::move(left_ctx6),
        std::tuple{left_group_key4, left_group_key5, left_group_key6},
        std::tuple{left_agg_func7, left_agg_func8});
    auto left_ctx8 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_ctx7),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(4)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(3)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(3)>(
                       gs::PropertySelector<std::string_view>("content")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(3)>(
                       gs::PropertySelector<std::string_view>("imageFile")),
                   gs::make_mapper_with_expr<INPUT_COL_ID(4), INPUT_COL_ID(3)>(
                       IC7left_expr2(), gs::PropertySelector<int64_t>("None"),
                       gs::PropertySelector<int64_t>("creationDate")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    auto left_ctx9 = Engine::Sort(
        graph, std::move(left_ctx8), gs::Range(0, 20),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::DESC, 3, int64_t>(""),
                   gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("")});
    for (auto iter : left_ctx9) {
      auto eles = iter.GetAllElement();
      output.put_long(std::get<0>(eles));
      output.put_string_view(std::get<1>(eles));
      output.put_string_view(std::get<2>(eles));
      output.put_time_stamp(std::get<3>(eles));
      output.put_long(std::get<4>(eles));
      if (std::get<5>(eles).empty()) {
        output.put_string_view(std::get<6>(eles));
      } else {
        output.put_string_view(std::get<5>(eles));
      }
      output.put_int(std::get<7>(eles));
      if (std::get<8>(eles)) {
        output.put_byte((char) true);
      } else {
        output.put_byte((char) false);
      }
    }
  }
  void Query(const MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    int64_t id = input.get<int64_t>("personIdQ7");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("personId", output_decoder.get_long());           // id
      node.put("personFirstName", output_decoder.get_string());  // post cnt
      node.put("personLastName", output_decoder.get_string());
      node.put("likeCreationDate", output_decoder.get_long());
      node.put("messageId", output_decoder.get_long());
      node.put("messageContent", output_decoder.get_string());
      node.put("minutesLatency", output_decoder.get_int());
      node.put("isNew", (bool) output_decoder.get_byte());

      output.push_back(std::make_pair("", node));
    }
  }
};
}  // namespace gs

#endif  // IC7_H