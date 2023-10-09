#ifndef FLEX_TEST_HQPS_IC_IC11_H_
#define FLEX_TEST_HQPS_IC_IC11_H_

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {
// Auto generated expression class definition
struct IC11expr0 {
 public:
  using result_t = bool;
  IC11expr0(std::string_view countryName) : countryName_(countryName) {}

  inline auto operator()(std::string_view name) const {
    return name == countryName_;
  }

 private:
  std::string_view countryName_;
};

struct IC11expr1 {
 public:
  using result_t = bool;
  IC11expr1(int32_t workFromYear) : workFromYear_(workFromYear) {}

  inline auto operator()(int32_t workFrom) const {
    return workFrom < workFromYear_;
  }

 private:
  int32_t workFromYear_;
};

struct IC11expr2 {
 public:
  using result_t = bool;
  IC11expr2(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC11expr3 {
 public:
  using result_t = bool;
  IC11expr3() {}
  template <typename vertex_id_t>
  inline auto operator()(vertex_id_t var7, vertex_id_t var8) const {
    return var7 != var8;
  }

 private:
};

// Auto generated query class definition
class IC11 {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  // Query function for query class
  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) const {
    int64_t personId = input.get_long();
    std::string_view countryName = input.get_string();
    int32_t workFromYear = input.get_int();

    auto expr0 = gs::make_filter(
        IC11expr0(countryName), gs::PropertySelector<std::string_view>("name"));
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 0, std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 7, (label_id_t) 5);
    auto ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto expr2 = gs::make_filter(IC11expr1(workFromYear),
                                 gs::PropertySelector<int32_t>("workFrom"));
    auto edge_expand_opt1 = gs::make_edge_expande_opt<int32_t>(
        gs::PropNameArray<int32_t>{"workFrom"}, gs::Direction::In,
        (label_id_t) 10, (label_id_t) 1, std::move(expr2));
    auto ctx2 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(ctx1), std::move(edge_expand_opt1));

    auto get_v_opt2 = make_getv_opt(gs::VOpt::Start,
                                    std::array<label_id_t, 1>{(label_id_t) 1});
    auto ctx3 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx2), std::move(get_v_opt2));
    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto get_v_opt3 = make_getv_opt(gs::VOpt::Itself,
                                    std::array<label_id_t, 1>{(label_id_t) 1});

    auto path_opt5 = gs::make_path_expandv_opt(
        std::move(edge_expand_opt4), std::move(get_v_opt3), gs::Range(1, 3));
    auto ctx4 = Engine::PathExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(3)>(
        graph, std::move(ctx3), std::move(path_opt5));

    auto expr4 = gs::make_filter(IC11expr2(personId),
                                 gs::PropertySelector<int64_t>("id"));

    auto get_v_opt6 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr4));
    auto ctx5 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx4), std::move(get_v_opt6));
    for (auto iter : ctx5) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << "after select: " << gs::to_string(eles);
    }
    // I0928 16:44:04.964781 324254 ic11.h:118] after select:
    // tuple<27,815,tuple<815,974,tuple<2004,>,>,815,1055,>
    auto ctx6 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx5),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(4)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(3)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    auto expr5 = gs::make_filter(
        IC11expr3(), gs::PropertySelector<grape::EmptyType>("None"),
        gs::PropertySelector<grape::EmptyType>("None"));
    auto ctx7 = Engine::template Select<INPUT_COL_ID(0), INPUT_COL_ID(1)>(
        graph, std::move(ctx6), std::move(expr5));

    auto ctx8 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx7),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(3)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<int32_t>("workFrom"))});

    for (auto iter : ctx8) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << "before dedup: " << gs::to_string(eles);
    }
    auto ctx9 = Engine::template Dedup<0, 1, 2>(std::move(ctx8));

    for (auto iter : ctx9) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << "after dedup: " << gs::to_string(eles);
    }
    auto ctx10 = Engine::Sort(
        graph, std::move(ctx9), gs::Range(0, 10),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::ASC, 2, int32_t>(""),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("id"),
            gs::OrderingPropPair<gs::SortOrder::DESC, 1, std::string_view>(
                "name")});
    auto ctx11 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx10),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<std::string_view>("name")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    for (auto iter : ctx11) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << "res: " << gs::to_string(eles);
      output.put_long(std::get<0>(eles));
      output.put_string_view(std::get<1>(eles));
      output.put_string_view(std::get<2>(eles));
      output.put_string_view(std::get<3>(eles));
      output.put_int(std::get<4>(eles));
    }
  }

  // 6597069767668
  // 19791209300507
  void Query(const MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ11");
    std::string country_name = input.get<std::string>("countryName");
    int32_t work_from_year = input.get<int32_t>("workFromYear");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(country_name);
    input_encoder.put_int(work_from_year);
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
      node.put("organizationName", output_decoder.get_string());
      node.put("organizationWorkFromYear", output_decoder.get_int());

      output.push_back(std::make_pair("", node));
    }
  }
};
}  // namespace gs

#endif  // FLEX_TEST_HQPS_IC_IC11_H_
