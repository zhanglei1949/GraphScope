#ifndef FLEX_TEST_HQPS_IC_IC10_H_
#define FLEX_TEST_HQPS_IC_IC10_H_

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {
struct IC10left_left_left_left_expr0 {
 public:
  using result_t = bool;
  IC10left_left_left_left_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC10left_left_left_right_expr0 {
 public:
  using result_t = bool;
  IC10left_left_left_right_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const { return id == personId_; }

 private:
  int64_t personId_;
};

struct IC10left_left_left_right_expr1 {
 public:
  using result_t = bool;
  IC10left_left_left_right_expr1() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC10left_left_left_left_expr1 {
 public:
  using result_t = bool;
  IC10left_left_left_left_expr1() {}
  template <typename vertex_id_t>
  inline auto operator()(vertex_id_t var3, vertex_id_t var4) const {
    return var3 != var4;
  }

 private:
};

struct IC10left_left_left_left_expr2 {
 public:
  using result_t = bool;
  IC10left_left_left_left_expr2(int64_t month) : month_(month) {}

  inline auto operator()(Date var5, Date var6, Date var7, Date var8) const {
    return (gs::DateTimeExtractor<Interval::MONTH>::extract(var5) == month_ &&
            gs::DateTimeExtractor<Interval::DAY>::extract(var6) >= 21) ||
           ((gs::DateTimeExtractor<Interval::MONTH>::extract(var7) ==
             (month_ % 12) + 1) &&
            gs::DateTimeExtractor<Interval::DAY>::extract(var8) < 22);
  }

 private:
  int64_t month_;
};

struct IC10left_left_right_expr0 {
 public:
  using result_t = bool;
  IC10left_left_right_expr0() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC10left_right_expr0 {
 public:
  using result_t = bool;
  IC10left_right_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const { return id == personId_; }

 private:
  int64_t personId_;
};

struct IC10left_right_expr1 {
 public:
  using result_t = bool;
  IC10left_right_expr1() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC10right_expr0 {
 public:
  using result_t = bool;
  IC10right_expr0() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC10left_left_left_left_expr3 {
 public:
  using result_t = int64_t;
  IC10left_left_left_left_expr3() {}

  inline auto operator()(int64_t var14, int64_t var15, int64_t var16) const {
    return var14 - (var15 - var16);
  }

 private:
};
/*
I1016 14:45:30.844205 234419 rt_bench.cc:150]  min: 26608247;
I1016 14:45:30.844209 234419 rt_bench.cc:151]  max: 51321487;
I1016 14:45:30.844213 234419 rt_bench.cc:152]  P50: 46339172;
I1016 14:45:30.844218 234419 rt_bench.cc:153]  P90: 48999466;
I1016 14:45:30.844221 234419 rt_bench.cc:154]  P95: 49530263;
I1016 14:45:30.844226 234419 rt_bench.cc:155]  P99: 51321487*/

// Auto generated query class definition
class IC10 {
 private:
  mutable double get_other_person_time = 0.0;
  mutable double get_friend_time = 0.0;
  mutable double person_anti_join_time = 0.0;
  mutable double filter_start_peron_time = 0.0;
  mutable double filter_with_month_time = 0.0;
  mutable double dedup_time = 0.0;
  mutable double get_all_post_time = 0.0;
  mutable double count_all_post_time = 0.0;
  mutable double get_active_post_time = 0.0;
  mutable double count_active_post_time = 0.0;

  mutable double all_post_left_join_time = 0.0;
  mutable double active_post_left_join_time = 0.0;
  mutable double person_place_join_time = 0.0;

  mutable double order_by_and_proj_time = 0.0;

 public:
  ~IC10() {
    LOG(INFO) << "get_other_person_time: " << get_other_person_time;
    LOG(INFO) << "get_friend_time: " << get_friend_time;
    LOG(INFO) << "person_anti_join_time: " << person_anti_join_time;
    LOG(INFO) << "filter_start_peron_time: " << filter_start_peron_time;
    LOG(INFO) << "filter_with_month_time: " << filter_with_month_time;
    LOG(INFO) << "dedup_time: " << dedup_time;
    LOG(INFO) << "get_all_post_time: " << get_all_post_time;
    LOG(INFO) << "count_all_post_time: " << count_all_post_time;
    LOG(INFO) << "get_active_post_time: " << get_active_post_time;
    LOG(INFO) << "count_active_post_time: " << count_active_post_time;
    LOG(INFO) << "all_post_left_join_time: " << all_post_left_join_time;
    LOG(INFO) << "active_post_left_join_time: " << active_post_left_join_time;
    LOG(INFO) << "person_place_join_time: " << person_place_join_time;
    LOG(INFO) << "order_by_and_proj_time: " << order_by_and_proj_time;
  }
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  // Query function for query class
  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) const {
    int64_t personId = input.get_long();
    int64_t month = input.get_int();

    // auto left_left_left_left_expr0 =
    //     gs::make_filter(IC10left_left_left_left_expr0(personId),
    //                     gs::PropertySelector<int64_t>("id"));
    // auto left_left_left_left_ctx0 =
    //     Engine::template ScanVertex<gs::AppendOpt::Persist>(
    //         graph, 1, std::move(left_left_left_left_expr0));
    double t0 = -grape::GetCurrentTime();
    auto left_left_left_left_ctx0 =
        Engine::template ScanVertexWithOid<gs::AppendOpt::Persist>(graph, 1,
                                                                   personId);

    auto left_left_left_left_edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto left_left_left_left_get_v_opt0 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{(label_id_t) 1});

    auto left_left_left_left_path_opt2 = gs::make_path_expandv_opt(
        std::move(left_left_left_left_edge_expand_opt1),
        std::move(left_left_left_left_get_v_opt0), gs::Range(2, 3));
    auto left_left_left_left_ctx1 =
        Engine::PathExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(left_left_left_left_ctx0),
            std::move(left_left_left_left_path_opt2));
    t0 += grape::GetCurrentTime();
    get_other_person_time += t0;

    // auto left_left_left_right_expr0 =
    //     gs::make_filter(IC10left_left_left_right_expr0(personId),
    //                     gs::PropertySelector<int64_t>("id"));
    // auto left_left_left_right_ctx0 =
    //     Engine::template ScanVertex<gs::AppendOpt::Persist>(
    //         graph, 1, std::move(left_left_left_right_expr0));
    double t1 = -grape::GetCurrentTime();
    auto left_left_left_right_ctx0 =
        Engine::template ScanVertexWithOid<gs::AppendOpt::Persist>(graph, 1,
                                                                   personId);

    auto left_left_left_right_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);
    auto left_left_left_right_ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(0)>(
            graph, std::move(left_left_left_right_ctx0),
            std::move(left_left_left_right_edge_expand_opt0));

    auto left_left_left_right_expr2 =
        gs::make_filter(IC10left_left_left_right_expr1());
    auto left_left_left_right_get_v_opt1 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 0>{},
                      std::move(left_left_left_right_expr2));
    auto left_left_left_right_ctx2 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_left_left_right_ctx1),
            std::move(left_left_left_right_get_v_opt1));
    t1 += grape::GetCurrentTime();
    get_friend_time += t1;

    double t2 = -grape::GetCurrentTime();
    auto left_left_left_left_ctx2 =
        Engine::template Join<0, 1, 0, 1, gs::JoinKind::AntiJoin>(
            std::move(left_left_left_left_ctx1),
            std::move(left_left_left_right_ctx2));
    t2 -= grape::GetCurrentTime();
    person_anti_join_time += t2;

    double t3 = -grape::GetCurrentTime();
    auto left_left_left_left_expr2 =
        gs::make_filter(IC10left_left_left_left_expr1(),
                        gs::PropertySelector<grape::EmptyType>("None"),
                        gs::PropertySelector<grape::EmptyType>("None"));
    auto left_left_left_left_ctx3 =
        Engine::template Select<INPUT_COL_ID(1), INPUT_COL_ID(0)>(
            graph, std::move(left_left_left_left_ctx2),
            std::move(left_left_left_left_expr2));
    t3 += grape::GetCurrentTime();
    filter_start_peron_time += t3;

    double t4 = -grape::GetCurrentTime();
    auto left_left_left_left_ctx4 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_left_left_ctx3),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<Date>("birthday"))});

    auto left_left_left_left_expr3 = gs::make_filter(
        IC10left_left_left_left_expr2(month),
        gs::PropertySelector<Date>("None"), gs::PropertySelector<Date>("None"),
        gs::PropertySelector<Date>("None"), gs::PropertySelector<Date>("None"));
    auto left_left_left_left_ctx5 =
        Engine::template Select<INPUT_COL_ID(1), INPUT_COL_ID(1),
                                INPUT_COL_ID(1), INPUT_COL_ID(1)>(
            graph, std::move(left_left_left_left_ctx4),
            std::move(left_left_left_left_expr3));
    t4 += grape::GetCurrentTime();
    filter_with_month_time += t4;

    double t5 = -grape::GetCurrentTime();
    auto left_left_left_left_ctx6 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_left_left_ctx5),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
            gs::PropertySelector<grape::EmptyType>(""))});
    auto left_left_left_left_ctx7 =
        Engine::template Dedup<0>(std::move(left_left_left_left_ctx6));
    t5 += grape::GetCurrentTime();
    dedup_time += t5;

    double t6 = -grape::GetCurrentTime();
    auto left_left_right_ctx0 =
        Engine::template ScanVertex<gs::AppendOpt::Persist>(
            graph, 3, Filter<TruePredicate>());

    auto left_left_right_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);
    auto left_left_right_ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(0)>(
            graph, std::move(left_left_right_ctx0),
            std::move(left_left_right_edge_expand_opt0));

    auto left_left_right_expr1 = gs::make_filter(IC10left_left_right_expr0());
    auto left_left_right_get_v_opt1 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 0>{},
                      std::move(left_left_right_expr1));
    auto left_left_right_ctx2 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_left_right_ctx1),
            std::move(left_left_right_get_v_opt1));
    t6 += grape::GetCurrentTime();
    get_all_post_time += t6;

    double t61 = -grape::GetCurrentTime();
    auto left_left_left_left_ctx8 =
        Engine::template Join<0, 1, gs::JoinKind::LeftOuterJoin>(
            std::move(left_left_left_left_ctx7),
            std::move(left_left_right_ctx2));
    t61 += grape::GetCurrentTime();
    all_post_left_join_time += t61;

    double t7 = -grape::GetCurrentTime();
    GroupKey<0, grape::EmptyType> left_left_left_left_group_key9(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto left_left_left_left_agg_func10 =
        gs::make_aggregate_prop<gs::AggFunc::COUNT>(
            std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
            std::integer_sequence<int32_t, 1>{});

    auto left_left_left_left_ctx9 =
        Engine::GroupBy(graph, std::move(left_left_left_left_ctx8),
                        std::tuple{left_left_left_left_group_key9},
                        std::tuple{left_left_left_left_agg_func10});
    t7 += grape::GetCurrentTime();
    count_all_post_time += t7;

    // auto left_right_expr0 = gs::make_filter(
    //     IC10left_right_expr0(personId), gs::PropertySelector<int64_t>("id"));
    // auto left_right_ctx0 = Engine::template
    // ScanVertex<gs::AppendOpt::Persist>(
    //     graph, 1, std::move(left_right_expr0));
    double t8 = -grape::GetCurrentTime();
    auto left_right_ctx0 =
        Engine::template ScanVertexWithOid<gs::AppendOpt::Persist>(graph, 1,
                                                                   personId);

    auto left_right_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 6, (label_id_t) 7);
    auto left_right_ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(left_right_ctx0),
            std::move(left_right_edge_expand_opt0));

    auto left_right_edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 1, (label_id_t) 3);
    auto left_right_ctx2 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(left_right_ctx1),
            std::move(left_right_edge_expand_opt1));

    auto left_right_edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);
    auto left_right_ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(2)>(
            graph, std::move(left_right_ctx2),
            std::move(left_right_edge_expand_opt2));

    auto left_right_expr4 = gs::make_filter(IC10left_right_expr1());
    auto left_right_get_v_opt3 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 0>{},
                      std::move(left_right_expr4));
    auto left_right_ctx4 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_right_ctx3),
            std::move(left_right_get_v_opt3));
    t8 += grape::GetCurrentTime();
    get_active_post_time += t8;

    double t9 = -grape::GetCurrentTime();
    auto left_left_left_left_ctx10 =
        Engine::template Join<0, 3, gs::JoinKind::LeftOuterJoin>(
            std::move(left_left_left_left_ctx9), std::move(left_right_ctx4));
    t9 += grape::GetCurrentTime();
    active_post_left_join_time += t9;

    GroupKey<0, grape::EmptyType> left_left_left_left_group_key11(
        gs::PropertySelector<grape::EmptyType>("None"));

    GroupKey<1, grape::EmptyType> left_left_left_left_group_key12(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto left_left_left_left_agg_func13 =
        gs::make_aggregate_prop<gs::AggFunc::COUNT_DISTINCT>(
            std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
            std::integer_sequence<int32_t, 4>{});

    double t10 = -grape::GetCurrentTime();
    auto left_left_left_left_ctx11 =
        Engine::GroupBy(graph, std::move(left_left_left_left_ctx10),
                        std::tuple{left_left_left_left_group_key11,
                                   left_left_left_left_group_key12},
                        std::tuple{left_left_left_left_agg_func13});
    t10 += grape::GetCurrentTime();
    count_active_post_time += t10;

    double t11 = -grape::GetCurrentTime();
    auto right_expr0 = gs::make_filter(IC10right_expr0());
    auto right_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, std::move(right_expr0));

    auto right_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);
    auto right_ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(right_ctx0), std::move(right_edge_expand_opt0));

    auto left_left_left_left_ctx12 =
        Engine::template Join<0, 0, gs::JoinKind::InnerJoin>(
            std::move(left_left_left_left_ctx11), std::move(right_ctx1));
    t11 += grape::GetCurrentTime();
    person_place_join_time += t11;

    double t12 = -grape::GetCurrentTime();
    auto left_left_left_left_ctx13 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_left_left_ctx12),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_expr<INPUT_COL_ID(2), INPUT_COL_ID(1),
                                             INPUT_COL_ID(2)>(
                       IC10left_left_left_left_expr3(),
                       gs::PropertySelector<int64_t>("None"),
                       gs::PropertySelector<int64_t>("None"),
                       gs::PropertySelector<int64_t>("None")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("gender")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(3)>(
                       gs::PropertySelector<std::string_view>("name"))});
    auto left_left_left_left_ctx14 = Engine::Sort(
        graph, std::move(left_left_left_left_ctx13), gs::Range(0, 10),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::DESC, 3, int64_t>(""),
                   gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("")});
    t12 += grape::GetCurrentTime();
    order_by_and_proj_time += t12;

    for (auto iter : left_left_left_left_ctx14) {
      auto eles = iter.GetAllElement();
      output.put_long(std::get<0>(eles));
      output.put_string_view(std::get<1>(eles));
      output.put_string_view(std::get<2>(eles));
      output.put_long(std::get<3>(eles));
      output.put_string_view(std::get<4>(eles));
      output.put_string_view(std::get<5>(eles));
    }
  }

  void Query(const MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ10");
    int32_t month = input.get<int64_t>("month");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_int(month);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("personId", output_decoder.get_long());           // id
      node.put("personFirstName", output_decoder.get_string());  // post cnt
      node.put("personLastName", output_decoder.get_string());
      node.put("commonInterestScore", output_decoder.get_long());
      node.put("personGender", output_decoder.get_string());
      node.put("personCityName", output_decoder.get_string());

      output.push_back(std::make_pair("", node));
    }
  }
};
}  // namespace gs

#endif
