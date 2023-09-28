#ifndef IC6_H
#define IC6_H

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

namespace gs {
// Auto generated expression class definition
struct IC6expr0 {
 public:
  using result_t = bool;
  IC6expr0(std::string_view tagName) : tagName_(tagName) {}

  inline auto operator()(std::string_view name) const {
    return name == tagName_;
  }

 private:
  std::string_view tagName_;
};

struct IC6expr1 {
 public:
  using result_t = bool;
  IC6expr1() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC6expr2 {
 public:
  using result_t = bool;
  IC6expr2(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == personId_);
  }

 private:
  int64_t personId_;
};

struct IC6expr3 {
 public:
  using result_t = bool;
  IC6expr3() {}
  template <typename vertex_id_t>
  inline auto operator()(vertex_id_t var8, vertex_id_t var9) const {
    return var8 != var9;
  }

 private:
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
    auto expr0 = gs::make_filter(
        IC6expr0(tagName), gs::PropertySelector<std::string_view>("name"));
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 7, std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 1, (label_id_t) 3);
    auto ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto expr2 = gs::make_filter(IC6expr1());
    auto get_v_opt1 = make_getv_opt(gs::VOpt::Itself,
                                    std::array<label_id_t, 1>{(label_id_t) 3},
                                    std::move(expr2));
    auto ctx2 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt1));
    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);
    auto ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(ctx2), std::move(edge_expand_opt2));

    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto get_v_opt3 = make_getv_opt(gs::VOpt::Itself,
                                    std::array<label_id_t, 1>{(label_id_t) 1});

    auto path_opt5 = gs::make_path_expandv_opt(
        std::move(edge_expand_opt4), std::move(get_v_opt3), gs::Range(1, 3));
    auto ctx4 = Engine::PathExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(2)>(
        graph, std::move(ctx3), std::move(path_opt5));
    auto expr5 = gs::make_filter(IC6expr2(personId),
                                 gs::PropertySelector<int64_t>("id"));
    auto get_v_opt6 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr5));
    auto ctx5 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx4), std::move(get_v_opt6));
    auto edge_expand_opt7 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 1, (label_id_t) 7);
    auto ctx6 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(ctx5), std::move(edge_expand_opt7));

    auto expr7 = gs::make_filter(
        IC6expr3(), gs::PropertySelector<grape::EmptyType>("None"),
        gs::PropertySelector<grape::EmptyType>("None"));
    auto ctx7 = Engine::template Select<INPUT_COL_ID(4), INPUT_COL_ID(0)>(
        graph, std::move(ctx6), std::move(expr7));

    GroupKey<4, std::string_view> group_key10(
        gs::PropertySelector<std::string_view>("name"));

    auto agg_func11 = gs::make_aggregate_prop<gs::AggFunc::COUNT_DISTINCT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 1>{});

    auto ctx8 = Engine::GroupBy(graph, std::move(ctx7), std::tuple{group_key10},
                                std::tuple{agg_func11});
    auto ctx9 = Engine::Sort(
        graph, std::move(ctx8), gs::Range(0, 10),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t>(""),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view>("")});
    for (auto iter : ctx9) {
      auto eles = iter.GetAllElement();
      output.put_string_view(std::get<0>(eles));
      output.put_long(std::get<1>(eles));
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
