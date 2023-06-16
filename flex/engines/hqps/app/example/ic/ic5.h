/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC5_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC5_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/engines/hqps/app/example/ldbc_snb_labels.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

class IC5Express0 {
 public:
  IC5Express0(int64_t oid) : oid_(oid) {}

  template <typename label_id_t>
  inline bool operator()(label_id_t label_id, int64_t oid) const {
    return label_id == 1 && oid == oid_;
  }

 private:
  int64_t oid_;
};

class IC5Expression1 {
 public:
  using result_t = bool;
  IC5Expression1(int64_t min_date) : min_join_date_(min_date) {}

  bool operator()(int64_t join_date) { return join_date > min_join_date_; }

 private:
  int64_t min_join_date_;
};

template <typename GRAPH_INTERFACE>
class QueryIC5 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ5");
    int64_t min_date = input.get<int64_t>("minDate");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_long(min_date);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("forumTitle", output_decoder.get_string());  // id
      node.put("postCount", output_decoder.get_int());      // post cnt

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    int64_t min_join_date = input.get_long();
    int32_t limit = 20;

    label_id_t person_label_id =
        graph.GetVertexLabelId(SNBLabels::person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(SNBLabels::knows_label);
    label_id_t forum_label_id = graph.GetVertexLabelId(SNBLabels::forum_label);
    label_id_t has_member_label_id =
        graph.GetEdgeLabelId(SNBLabels::hasMember_label);
    label_id_t container_of_label_id =
        graph.GetEdgeLabelId(SNBLabels::container_of_label);
    label_id_t post_label_id = graph.GetVertexLabelId(SNBLabels::post_label);
    label_id_t has_creator_label_id =
        graph.GetEdgeLabelId(SNBLabels::hasCreator_label);

    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, id);

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(1, 3));
    auto ctx1 =
        Engine::template PathExpandV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx0), std::move(path_expand_opt));

    // copy a right ctx.
    auto right_ctx_1(ctx1);

    auto filter = gs::make_filter(IC5Expression1(min_join_date),
                                  gs::PropertySelector<int64_t>("joinDate"));
    auto left_edge_expand_opt3 =
        gs::make_edge_expandv_opt(gs::Direction::In, has_member_label_id,
                                  forum_label_id, std::move(filter));
    auto left_ctx3 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx1), std::move(left_edge_expand_opt3));

    // person hasCreator -> post
    auto right_edge_expand_opt5 = gs::make_edge_expandv_opt(
        gs::Direction::In, has_creator_label_id, post_label_id);
    auto right_ctx_2 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(right_ctx_1), std::move(right_edge_expand_opt5));

    auto right_edge_expand_opt6 = gs::make_edge_expandv_opt(
        gs::Direction::In, container_of_label_id, forum_label_id);
    auto right_ctx_3 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(right_ctx_2), std::move(right_edge_expand_opt6));
    // group by forum

    auto ctx_joined =
        Engine::template Join<0, 1, 0, 2, JoinKind::LeftOuterJoin>(
            std::move(left_ctx3), std::move(right_ctx_3));
    // after join is 0,1

    auto right_ctx_4 =
        Engine::GroupBy(graph, std::move(ctx_joined),
                        std::tuple{GroupKey<1, grape::EmptyType>()},
                        std::tuple{gs::make_aggregate_prop<AggFunc::COUNT>(
                            std::tuple{PropertySelector<grape::EmptyType>()},
                            std::integer_sequence<int32_t, 2>{})});

    auto ctx8 = Engine::Sort(
        graph, std::move(right_ctx_4), gs::Range(0, 20),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 1, size_t>("None"),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0,
                                 gs::oid_t>("id")  // id
        });

    auto ctx9 = Engine::template Project<true>(
        graph, std::move(ctx8),
        std::tuple{gs::make_mapper_with_variable<0>(
            gs::PropertySelector<std::string_view>("title"))});

    for (auto iter : ctx9) {
      auto data_tuple = iter.GetAllElement();
      LOG(INFO) << gs::to_string(data_tuple);
      output.put_string_view(std::get<2>(data_tuple));
      output.put_int(std::get<1>(data_tuple));
    }
    LOG(INFO) << "Finish IC5";
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC5_H_
