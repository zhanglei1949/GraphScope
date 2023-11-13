#ifndef IC2_H
#define IC2_H

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {
// Auto generated expression class definition
struct IC2expr0 {
 public:
  using result_t = bool;
  IC2expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC2expr1 {
 public:
  using result_t = bool;
  IC2expr1(int64_t maxDate) : maxDate_(maxDate) {}

  inline auto operator()(int64_t creationDate) const {
    return creationDate < maxDate_;
  }

 private:
  int64_t maxDate_;
};

// Auto generated query class definition
class IC2 {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  // Query function for query class
  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) const {
    auto personId = input.get_long();
    auto maxDate = input.get_long();
    auto expr0 = gs::make_filter(IC2expr0(personId),
                                 gs::PropertySelector<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);
    auto ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0,
        std::array<label_id_t, 2>{(label_id_t) 3, (label_id_t) 2});
    auto ctx2 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(1)>(
            graph, std::move(ctx1), std::move(edge_expand_opt1));

    auto expr3 = gs::make_filter(IC2expr1(maxDate),
                                 gs::PropertySelector<int64_t>("creationDate"));
    auto get_v_opt2 =
        make_getv_opt(gs::VOpt::Itself,
                      std::array<label_id_t, 2>{(label_id_t) 2, (label_id_t) 3},
                      std::move(expr3));
    auto ctx3 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx2), std::move(get_v_opt2));
    auto ctx4 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx3),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    auto ctx5 = Engine::Sort(
        graph, std::move(ctx4), gs::Range(0, 20),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t>(
                       "creationDate"),
                   gs::OrderingPropPair<gs::SortOrder::ASC, 1, int64_t>("id")});
    auto ctx6 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx5),
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
                       gs::PropertySelector<int64_t>("creationDate"))});
    for (auto iter : ctx6) {
      auto tuple = iter.GetAllElement();
      output.put_long(std::get<0>(tuple));
      output.put_string_view(std::get<1>(tuple));
      output.put_string_view(std::get<2>(tuple));
      output.put_long(std::get<3>(tuple));
      if (std::get<4>(tuple).empty()) {
        output.put_string_view(std::get<5>(tuple));
      } else {
        output.put_string_view(std::get<4>(tuple));
      }
      output.put_date(std::get<6>(tuple));
    }
  }

  void Query(const MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    int64_t id = input.get<int64_t>("personIdQ2");
    int64_t maxDate = input.get<int64_t>("maxDate");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_long(maxDate);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("personId", output_decoder.get_long());
      node.put("personFirstName", output_decoder.get_string());
      node.put("personLastName", output_decoder.get_string());
      node.put("messageId", output_decoder.get_long());
      node.put("messageContent", output_decoder.get_string());
      node.put("messageCreationDate", output_decoder.get_long());

      output.push_back(std::make_pair("", node));
    }
  }
};
}  // namespace gs

#endif  // IC2_H