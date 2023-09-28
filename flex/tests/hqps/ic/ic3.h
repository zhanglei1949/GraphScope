#ifndef IC3_H
#define IC3_H

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

namespace gs {
// Auto generated expression class definition
struct IC3expr0 {
 public:
  using result_t = bool;
  IC3expr0(std::string_view countryYName) : countryYName_(countryYName) {}

  inline auto operator()(std::string_view name) const {
    return (true) && (name == countryYName_);
  }

 private:
  std::string_view countryYName_;
};

struct IC3expr1 {
 public:
  using result_t = bool;
  IC3expr1(Date startDate, Date endDate)
      : startDate_(startDate), endDate_(endDate) {}

  inline auto operator()(Date creationDate, Date creationDate_0) const {
    return creationDate >= startDate_ && creationDate_0 < endDate_;
  }

 private:
  Date startDate_;
  Date endDate_;
};

struct IC3expr2 {
 public:
  using result_t = bool;
  IC3expr2() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC3expr3 {
 public:
  using result_t = bool;
  IC3expr3(Date startDate, Date endDate)
      : startDate_(startDate), endDate_(endDate) {}

  inline auto operator()(Date creationDate, Date creationDate_0) const {
    return creationDate >= startDate_ && creationDate_0 < endDate_;
  }

 private:
  Date startDate_;
  Date endDate_;
};

struct IC3expr4 {
 public:
  using result_t = bool;
  IC3expr4(std::string_view countryXName) : countryXName_(countryXName) {}

  inline auto operator()(std::string_view name) const {
    return (true) && (name == countryXName_);
  }

 private:
  std::string_view countryXName_;
};

struct IC3expr5 {
 public:
  using result_t = bool;
  IC3expr5(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC3expr6 {
 public:
  using result_t = bool;
  IC3expr6(std::string_view countryXName, std::string_view countryYName)
      : countryXName_(countryXName), countryYName_(countryYName) {}

  inline auto operator()(std::string_view name, std::string_view name_0) const {
    return name != countryXName_ && name_0 != countryYName_;
  }

 private:
  std::string_view countryXName_;
  std::string_view countryYName_;
};

struct IC3expr7 {
 public:
  using result_t = int64_t;
  IC3expr7() {}

  inline auto operator()(int64_t var18, int64_t var19) const {
    return var18 + var19;
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
    Date startDate = input.get_date();
    Date endDate = input.get_date();
    auto expr0 = gs::make_filter(
        IC3expr0(countryYName), gs::PropertySelector<std::string_view>("name"));
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 0, std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 7,
        std::array<label_id_t, 2>{(label_id_t) 3, (label_id_t) 2});
    auto ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto expr2 = gs::make_filter(IC3expr1(startDate, endDate),
                                 gs::PropertySelector<Date>("creationDate"),
                                 gs::PropertySelector<Date>("creationDate"));
    auto get_v_opt1 =
        make_getv_opt(gs::VOpt::Itself,
                      std::array<label_id_t, 2>{(label_id_t) 2, (label_id_t) 3},
                      std::move(expr2));
    auto ctx2 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt1));
    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);
    auto ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(1)>(
            graph, std::move(ctx2), std::move(edge_expand_opt2));

    auto expr4 = gs::make_filter(IC3expr2());
    auto get_v_opt3 = make_getv_opt(gs::VOpt::Itself,
                                    std::array<label_id_t, 1>{(label_id_t) 1},
                                    std::move(expr4));
    auto ctx4 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx3), std::move(get_v_opt3));
    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0,
        std::array<label_id_t, 2>{(label_id_t) 3, (label_id_t) 2});
    auto ctx5 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(2)>(
            graph, std::move(ctx4), std::move(edge_expand_opt4));

    auto expr6 = gs::make_filter(IC3expr3(startDate, endDate),
                                 gs::PropertySelector<Date>("creationDate"),
                                 gs::PropertySelector<Date>("creationDate"));
    auto get_v_opt5 =
        make_getv_opt(gs::VOpt::Itself,
                      std::array<label_id_t, 2>{(label_id_t) 2, (label_id_t) 3},
                      std::move(expr6));
    auto ctx6 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx5), std::move(get_v_opt5));
    auto edge_expand_opt6 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);
    auto ctx7 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(3)>(
            graph, std::move(ctx6), std::move(edge_expand_opt6));

    auto expr8 = gs::make_filter(
        IC3expr4(countryXName), gs::PropertySelector<std::string_view>("name"));
    auto get_v_opt7 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr8));
    auto ctx8 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx7), std::move(get_v_opt7));
    auto edge_expand_opt9 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto get_v_opt8 = make_getv_opt(gs::VOpt::Itself,
                                    std::array<label_id_t, 1>{(label_id_t) 1});

    auto path_opt10 = gs::make_path_expandv_opt(
        std::move(edge_expand_opt9), std::move(get_v_opt8), gs::Range(1, 3));
    auto ctx9 = Engine::PathExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(2)>(
        graph, std::move(ctx8), std::move(path_opt10));
    auto expr10 = gs::make_filter(IC3expr5(personId),
                                  gs::PropertySelector<int64_t>("id"));
    auto get_v_opt11 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr10));
    auto ctx10 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx9), std::move(get_v_opt11));
    auto edge_expand_opt12 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);
    auto ctx11 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(2)>(
            graph, std::move(ctx10), std::move(edge_expand_opt12));

    auto edge_expand_opt13 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 11, (label_id_t) 0);
    auto ctx12 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(6)>(
            graph, std::move(ctx11), std::move(edge_expand_opt13));

    auto expr13 =
        gs::make_filter(IC3expr6(countryXName, countryYName),
                        gs::PropertySelector<std::string_view>("name"),
                        gs::PropertySelector<std::string_view>("name"));
    auto get_v_opt14 = make_getv_opt(gs::VOpt::Itself,
                                     std::array<label_id_t, 1>{(label_id_t) 0},
                                     std::move(expr13));
    auto ctx13 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx12), std::move(get_v_opt14));
    GroupKey<2, grape::EmptyType> group_key15(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto agg_func16 = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 3>{});

    auto agg_func17 = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 1>{});

    auto ctx14 =
        Engine::GroupBy(graph, std::move(ctx13), std::tuple{group_key15},
                        std::tuple{agg_func16, agg_func17});
    auto ctx15 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx14),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_expr<1, 2>(
                       IC3expr7(), gs::PropertySelector<int64_t>("None"),
                       gs::PropertySelector<int64_t>("None"))});
    auto ctx16 = Engine::Sort(
        graph, std::move(ctx15), gs::Range(0, 20),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::DESC, 5, int64_t>(""),
                   gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("")});
    for (auto iter : ctx16) {
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
    oid_t id = input.get<oid_t>("personIdQ3");

    int64_t start_date = input.get<int64_t>("startDate");
    int64_t duration_days = input.get<int32_t>("durationDays");
    std::string country_x = input.get<std::string>("countryXName");
    std::string country_y = input.get<std::string>("countryYName");
    int32_t limit = 20;
    int64_t end_date = start_date + duration_days * 24 * 3600 * 1000;

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(country_x);
    input_encoder.put_string(country_y);
    input_encoder.put_long(start_date);
    input_encoder.put_long(end_date);

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
