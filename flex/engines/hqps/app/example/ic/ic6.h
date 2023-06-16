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
#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC6_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC6_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/app/example/ldbc_snb_labels.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

class IC6Expression2 {
 public:
  using result_t = bool;
  IC6Expression2(std::string_view param1) : param1_(param1) {}

  bool operator()(std::string_view name) const { return name == param1_; }

 private:
  std::string_view param1_;
};

class IC6Expression3 {
 public:
  using result_t = bool;
  IC6Expression3(std::string_view param1) : param1_(param1) {}

  bool operator()(std::string_view name) { return name != param1_; }

 private:
  std::string_view param1_;
};

template <typename GRAPH_INTERFACE>
class QueryIC6 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using Engine = SyncEngine<GRAPH_INTERFACE>;

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
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
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("tagName", output_decoder.get_string());  // id
      node.put("postCount", output_decoder.get_int());   // post cnt

      output.push_back(std::make_pair("", node));
    }
    LOG(INFO) << "Finsih put result into ptree";
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    std::string_view tag_name = input.get_string();
    int32_t limit = 10;

    label_id_t person_label_id =
        graph.GetVertexLabelId(SNBLabels::person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(SNBLabels::knows_label);
    label_id_t post_label_id = graph.GetVertexLabelId(SNBLabels::post_label);
    label_id_t comment_label_id =
        graph.GetVertexLabelId(SNBLabels::comment_label);
    label_id_t has_creator_label_id =
        graph.GetEdgeLabelId(SNBLabels::hasCreator_label);
    label_id_t forum_label_id = graph.GetVertexLabelId(SNBLabels::forum_label);
    label_id_t has_member_label_id =
        graph.GetEdgeLabelId(SNBLabels::hasMember_label);
    label_id_t container_of_label_id =
        graph.GetEdgeLabelId(SNBLabels::container_of_label);
    label_id_t tag_label_id = graph.GetVertexLabelId(SNBLabels::tag_label);
    label_id_t has_tag_label_id = graph.GetEdgeLabelId(SNBLabels::hasTag_label);

    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, id);

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(1, 3));
    auto ctx1 = Engine::template PathExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx0), std::move(path_expand_opt));

    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::In, has_creator_label_id, post_label_id);
    auto ctx4 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx1), std::move(edge_expand_opt4));

    auto edge_expand_opt5 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx5 = Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx4), std::move(edge_expand_opt5));

    auto filter =
        gs::make_filter(IC6Expression2(tag_name),
                        gs::PropertySelector<std::string_view>("name"));
    auto get_v_opt6 = gs::make_getv_opt(gs::VOpt::Itself,
                                        std::array<label_id_t, 1>{tag_label_id},
                                        std::move(filter));
    auto ctx6 = Engine::template GetV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx5), std::move(get_v_opt6));

    LOG(INFO) << "after get v, head size: " << ctx6.GetHead().Size();

    auto ctx7 = Engine::template Project<false>(
        graph, std::move(ctx6),
        std::tuple{gs::IdentityMapper<0, InternalIdSelector>()});

    auto edge_expand_opt8 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx8 = Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(0)>(
        graph, std::move(ctx7), std::move(edge_expand_opt8));

    auto filter2 =
        gs::make_filter(IC6Expression3(tag_name),
                        gs::PropertySelector<std::string_view>("name"));
    auto get_v_opt9 = gs::make_getv_opt(gs::VOpt::Itself,
                                        std::array<label_id_t, 1>{tag_label_id},
                                        std::move(filter2));
    auto ctx9 = Engine::template GetV<AppendOpt::Persist, LAST_COL>(
        graph, std::move(ctx8), std::move(get_v_opt9));
    LOG(INFO) << "after filter with name neq" << tag_name
              << ", head size: " << ctx9.GetHead().Size();

    GroupKey<INPUT_COL_ID(1), grape::EmptyType> group_key(
        gs::PropertySelector<grape::EmptyType>{});
    auto agg = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>{}},
        std::integer_sequence<int32_t, 0>{});
    auto ctx10 = Engine::GroupBy(graph, std::move(ctx9),
                                 std::tuple{std::move(group_key)},
                                 std::tuple{std::move(agg)});

    // // sort by
    // TODO: sort by none.none, means using inner id as sort key.
    gs::OrderingPropPair<gs::SortOrder::DESC, INPUT_COL_ID(1), int64_t> pair0(
        "None");  // indicate the set's element itself.
    gs::OrderingPropPair<gs::SortOrder::ASC, INPUT_COL_ID(0), std::string_view>
        pair1("name");  // id
    auto ctx11 = Engine::Sort(graph, std::move(ctx10), gs::Range(0, 10),
                              std::tuple{pair0, pair1});

    // gs::AliasTagProp<0, 2, std::string_view> prop_col0({"name"});
    // gs::AliasTagProp<4, 6, gs::oid_t> prop_col1({"id"});
    auto mapper1 = gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
        PropertySelector<std::string_view>("name"));
    auto mapper2 =
        gs::make_mapper_with_variable<1>(PropertySelector<grape::EmptyType>());
    auto ctx12 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx11), std::tuple{mapper1, mapper2});

    for (auto iter : ctx12) {
      auto ele = iter.GetAllElement();
      output.put_string_view(std::get<0>(ele));
      output.put_int(std::get<1>(ele));
      LOG(INFO) << gs::to_string(ele);
    }
    LOG(INFO) << "End";
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC6_H_
