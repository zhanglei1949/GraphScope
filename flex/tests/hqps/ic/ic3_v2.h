// MATCh(p : PERSON
// {id:$personId})-[:KNOWS*1..3]-(otherP:PERSON)-[:ISLOCATEDIN]->(city:PLACE)-[:ISPARTOF]->(countryCity:PLACE)
// WHERE countryCity.name <> $countryXName AND countryCity.name <> $countryYName
// WITH distinct otherP AS otherP

// MATCH(otherP: PERSON) <-[:HASCREATOR]-(messageX: POST |
// COMMENT)-[:ISLOCATEDIN]->(countryX: PLACE) WHERE countryX.name =
// $countryXName AND messageX.creationDate >= $startDate AND
// messageX.creationDate < $endDate with otherP AS otherP, COUNT(messageX) as
// xCount

// MATCH(otherP: PERSON)<-[:HASCREATOR]-(messageY: POST |
// COMMENT)-[:ISLOCATEDIN]->(countryY: PLACE) where countryY.name =
// $countryYName AND messageY.creationDate >= $startDate AND
// messageY.creationDate < $endDate with otherP AS otherP, xCount as xCount,
// COUNT(messageY) as yCount RETURN otherP.id as id,otherP.firstName as
// firstName, otherP.lastName as lastName, xCount, yCount, xCount + yCount as
// total ORDER BY total DESC, id ASC LIMIT 20;

#ifndef IC3_H
#define IC3_H

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {

struct IC3left_left_expr0 {
 public:
  using result_t = bool;
  IC3left_left_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC3left_left_expr1 {
 public:
  using result_t = bool;
  IC3left_left_expr1(std::string_view countryXName,
                     std::string_view countryYName)
      : countryXName_(countryXName), countryYName_(countryYName) {}

  inline auto operator()(std::string_view name, std::string_view name_0) const {
    return name != countryXName_ && name_0 != countryYName_;
  }

 private:
  std::string_view countryXName_;
  std::string_view countryYName_;
};

struct IC3left_right_expr0 {
 public:
  using result_t = bool;
  IC3left_right_expr0(std::string_view countryXName)
      : countryXName_(countryXName) {}

  inline auto operator()(std::string_view name) const {
    return name == countryXName_;
  }

 private:
  std::string_view countryXName_;
};

struct IC3left_right_expr1 {
 public:
  using result_t = bool;
  IC3left_right_expr1(int64_t startDate, int64_t endDate)
      : startDate_(startDate), endDate_(endDate) {}

  inline auto operator()(int64_t creationDate, int64_t creationDate_0) const {
    return creationDate >= startDate_ && creationDate_0 < endDate_;
  }

 private:
  int64_t startDate_;
  int64_t endDate_;
};

struct IC3left_right_expr2 {
 public:
  using result_t = bool;
  IC3left_right_expr2() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC3right_expr0 {
 public:
  using result_t = bool;
  IC3right_expr0(std::string_view countryYName) : countryYName_(countryYName) {}

  inline auto operator()(std::string_view name) const {
    return name == countryYName_;
  }

 private:
  std::string_view countryYName_;
};

struct IC3right_expr1 {
 public:
  using result_t = bool;
  IC3right_expr1(int64_t startDate, int64_t endDate)
      : startDate_(startDate), endDate_(endDate) {}

  inline auto operator()(int64_t creationDate, int64_t creationDate_0) const {
    return creationDate >= startDate_ && creationDate_0 < endDate_;
  }

 private:
  int64_t startDate_;
  int64_t endDate_;
};

struct IC3right_expr2 {
 public:
  using result_t = bool;
  IC3right_expr2() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC3left_left_expr2 {
 public:
  using result_t = int64_t;
  IC3left_left_expr2() {}

  inline auto operator()(int64_t var11, int64_t var12) const {
    return var11 + var12;
  }

 private:
};

// Auto generated query class definition
class IC3 {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  // Query function for query class
  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) const {
    int64_t personId = input.get_long();
    std::string_view countryXName = input.get_string();
    std::string_view countryYName = input.get_string();
    int64_t startDate = input.get_long();
    int32_t durationDays = input.get_int();
    int64_t milli_sec_per_day = 24 * 60 * 60 * 1000l;
    auto endDate = startDate + durationDays * milli_sec_per_day;

    auto left_left_expr0 = gs::make_filter(IC3left_left_expr0(personId),
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
    auto left_left_edge_expand_opt3 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);
    auto left_left_ctx2 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(left_left_ctx1),
            std::move(left_left_edge_expand_opt3));

    auto left_left_edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 11, (label_id_t) 0);
    auto left_left_ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(2)>(
            graph, std::move(left_left_ctx2),
            std::move(left_left_edge_expand_opt4));

    auto left_left_expr4 =
        gs::make_filter(IC3left_left_expr1(countryXName, countryYName),
                        gs::PropertySelector<std::string_view>("name"),
                        gs::PropertySelector<std::string_view>("name"));
    auto left_left_get_v_opt5 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{(label_id_t) 0},
        std::move(left_left_expr4));
    auto left_left_ctx4 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_left_ctx3), std::move(left_left_get_v_opt5));
    auto left_left_ctx5 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_ctx4),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
            gs::PropertySelector<grape::EmptyType>(""))});
    auto left_left_ctx6 = Engine::template Dedup<0>(std::move(left_left_ctx5));

    auto left_right_expr0 =
        gs::make_filter(IC3left_right_expr0(countryXName),
                        gs::PropertySelector<std::string_view>("name"));
    auto left_right_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 0, std::move(left_right_expr0));

    auto left_right_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 7,
        std::array<label_id_t, 2>{(label_id_t) 3, (label_id_t) 2});
    auto left_right_ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(0)>(
            graph, std::move(left_right_ctx0),
            std::move(left_right_edge_expand_opt0));

    auto left_right_expr2 =
        gs::make_filter(IC3left_right_expr1(startDate, endDate),
                        gs::PropertySelector<int64_t>("creationDate"),
                        gs::PropertySelector<int64_t>("creationDate"));
    auto left_right_get_v_opt1 =
        make_getv_opt(gs::VOpt::Itself,
                      std::array<label_id_t, 2>{(label_id_t) 2, (label_id_t) 3},
                      std::move(left_right_expr2));
    auto left_right_ctx2 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_right_ctx1),
            std::move(left_right_get_v_opt1));
    auto left_right_edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);
    auto left_right_ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(1)>(
            graph, std::move(left_right_ctx2),
            std::move(left_right_edge_expand_opt2));

    auto left_right_expr4 = gs::make_filter(IC3left_right_expr2());
    auto left_right_get_v_opt3 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 0>{},
                      std::move(left_right_expr4));
    auto left_right_ctx4 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_right_ctx3),
            std::move(left_right_get_v_opt3));

    auto left_left_ctx7 = Engine::template Join<0, 2, gs::JoinKind::InnerJoin>(
        std::move(left_left_ctx6), std::move(left_right_ctx4));
    GroupKey<0, grape::EmptyType> left_left_group_key6(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto left_left_agg_func7 = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 2>{});

    auto left_left_ctx8 = Engine::GroupBy(graph, std::move(left_left_ctx7),
                                          std::tuple{left_left_group_key6},
                                          std::tuple{left_left_agg_func7});

    auto right_expr0 =
        gs::make_filter(IC3right_expr0(countryYName),
                        gs::PropertySelector<std::string_view>("name"));
    auto right_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 0, std::move(right_expr0));

    auto right_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 7,
        std::array<label_id_t, 2>{(label_id_t) 3, (label_id_t) 2});
    auto right_ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(0)>(
            graph, std::move(right_ctx0), std::move(right_edge_expand_opt0));

    auto right_expr2 =
        gs::make_filter(IC3right_expr1(startDate, endDate),
                        gs::PropertySelector<int64_t>("creationDate"),
                        gs::PropertySelector<int64_t>("creationDate"));
    auto right_get_v_opt1 =
        make_getv_opt(gs::VOpt::Itself,
                      std::array<label_id_t, 2>{(label_id_t) 2, (label_id_t) 3},
                      std::move(right_expr2));
    auto right_ctx2 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(right_ctx1), std::move(right_get_v_opt1));
    auto right_edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);
    auto right_ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(1)>(
            graph, std::move(right_ctx2), std::move(right_edge_expand_opt2));

    auto right_expr4 = gs::make_filter(IC3right_expr2());
    auto right_get_v_opt3 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(right_expr4));
    auto right_ctx4 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(right_ctx3), std::move(right_get_v_opt3));

    auto left_left_ctx9 = Engine::template Join<0, 2, gs::JoinKind::InnerJoin>(
        std::move(left_left_ctx8), std::move(right_ctx4));
    GroupKey<0, grape::EmptyType> left_left_group_key8(
        gs::PropertySelector<grape::EmptyType>("None"));

    GroupKey<1, grape::EmptyType> left_left_group_key9(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto left_left_agg_func10 = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 3>{});

    auto left_left_ctx10 =
        Engine::GroupBy(graph, std::move(left_left_ctx9),
                        std::tuple{left_left_group_key8, left_left_group_key9},
                        std::tuple{left_left_agg_func10});
    auto left_left_ctx11 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_ctx10),
        std::tuple{
            gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                gs::PropertySelector<int64_t>("id")),
            gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                gs::PropertySelector<std::string_view>("firstName")),
            gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                gs::PropertySelector<std::string_view>("lastName")),
            gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                gs::PropertySelector<grape::EmptyType>("")),
            gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                gs::PropertySelector<grape::EmptyType>("")),
            gs::make_mapper_with_expr<INPUT_COL_ID(1), INPUT_COL_ID(2)>(
                IC3left_left_expr2(), gs::PropertySelector<int64_t>("None"),
                gs::PropertySelector<int64_t>("None"))});
    auto left_left_ctx12 = Engine::Sort(
        graph, std::move(left_left_ctx11), gs::Range(0, 20),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::DESC, 5, int64_t>(""),
                   gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("")});

    for (auto iter : left_left_ctx12) {
      auto eles = iter.GetAllElement();
      output.put_long(std::get<0>(eles));
      output.put_string_view(std::get<1>(eles));
      output.put_string_view(std::get<2>(eles));
      output.put_long(std::get<3>(eles));
      output.put_long(std::get<4>(eles));
      output.put_long(std::get<5>(eles));
    }
  }

  void Query(const MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    int64_t id = input.get<int64_t>("personIdQ3");

    int64_t start_date = input.get<int64_t>("startDate");
    int32_t duration_days = input.get<int32_t>("durationDays");
    std::string country_x = input.get<std::string>("countryXName");
    std::string country_y = input.get<std::string>("countryYName");
    int32_t limit = 20;

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(country_x);
    input_encoder.put_string(country_y);
    input_encoder.put_long(start_date);
    input_encoder.put_int(duration_days);

    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("personId", output_decoder.get_long());           // id
      node.put("personFirstName", output_decoder.get_string());  // dist
      node.put("personLastName", output_decoder.get_string());   // lastName"
      node.put("xCount", output_decoder.get_long());             // xcount
      node.put("yCount",
               output_decoder.get_long());  // ycount
      node.put("count",
               output_decoder.get_long());  // gender

      output.push_back(std::make_pair("", node));
    }
  }
};
}  // namespace gs

#endif  // IC3_H
