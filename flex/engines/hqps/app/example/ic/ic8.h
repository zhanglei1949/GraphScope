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
#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC8_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC8_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/app/example/ldbc_snb_labels.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename GRAPH_INTERFACE>
class QueryIC8 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using Engine = SyncEngine<GRAPH_INTERFACE>;

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ8");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("personId", output_decoder.get_long());           // id
      node.put("personFirstName", output_decoder.get_string());  // post cnt
      node.put("personLastName", output_decoder.get_string());
      node.put("commentCreationDate", output_decoder.get_long());
      node.put("commentId", output_decoder.get_long());
      node.put("commentContent", output_decoder.get_string());

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    int32_t limit = 20;

    label_id_t person_label_id =
        graph.GetVertexLabelId(SNBLabels::person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(SNBLabels::knows_label);
    label_id_t post_label_id = graph.GetVertexLabelId(SNBLabels::post_label);
    label_id_t comment_label_id =
        graph.GetVertexLabelId(SNBLabels::comment_label);
    label_id_t has_creator_label_id =
        graph.GetEdgeLabelId(SNBLabels::hasCreator_label);
    label_id_t reply_of_label_id =
        graph.GetEdgeLabelId(SNBLabels::replyOf_label);
    label_id_t forum_label_id = graph.GetVertexLabelId(SNBLabels::forum_label);
    label_id_t likes_label_id = graph.GetEdgeLabelId(SNBLabels::likes_label);
    label_id_t has_member_label_id =
        graph.GetEdgeLabelId(SNBLabels::hasMember_label);
    label_id_t container_of_label_id =
        graph.GetEdgeLabelId(SNBLabels::container_of_label);
    label_id_t tag_label_id = graph.GetVertexLabelId(SNBLabels::tag_label);
    label_id_t has_tag_label_id = graph.GetEdgeLabelId(SNBLabels::hasTag_label);

    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, id);

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::In, has_creator_label_id,
        std::array<label_id_t, 2>{post_label_id, comment_label_id});
    // message
    auto ctx1 = Engine::template EdgeExpandVMultiLabel<AppendOpt::Temp,
                                                       INPUT_COL_ID(-1)>(
        graph, std::move(ctx0), std::move(edge_expand_opt));

    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::In, reply_of_label_id, comment_label_id);
    auto ctx2 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx1), std::move(edge_expand_opt2));
    // replied comment

    auto edge_expand_opt3 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_creator_label_id, person_label_id);
    auto ctx3 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx2), std::move(edge_expand_opt3));

    auto ctx4 = Engine::Sort(
        graph, std::move(ctx3), gs::Range(0, limit),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, INPUT_COL_ID(0), int64_t>(
                "creationDate"),  // indicate the set's element itself.
            gs::OrderingPropPair<gs::SortOrder::ASC, INPUT_COL_ID(0),
                                 gs::oid_t>("id")  // id
        });

    auto ctx5 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx4),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<oid_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("content")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("creationDate")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<oid_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<std::string_view>("lastName"))});

    for (auto iter : ctx5) {
      auto element = iter.GetAllElement();
      output.put_long(std::get<3>(element));         // person id
      output.put_string_view(std::get<4>(element));  // person first name
      output.put_string_view(std::get<5>(element));  // person last name
      output.put_long(std::get<2>(element));         // creaiontdate.
      output.put_long(std::get<0>(element));         // id
      output.put_string_view(std::get<1>(element));  // content
    }
  }
};
}  // namespace gs

#endif  // GRAPHSCOPE_APPS_IC8_H_