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
#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC12_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC12_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/engines/hqps/app/example/ldbc_snb_labels.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

class IC12Expression2 {
 public:
  using result_t = bool;
  IC12Expression2(std::string_view tag_class_name)
      : tag_class_name_(tag_class_name) {}

  bool operator()(const std::string_view& data) const {
    return data == tag_class_name_;
  }

 private:
  std::string_view tag_class_name_;
};

template <typename GRAPH_INTERFACE>
class QueryIC12 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using Engine = SyncEngine<GRAPH_INTERFACE>;

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ12");
    std::string tag_class_name = input.get<std::string>("tagClassName");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(tag_class_name);
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
      // new node for list.
      boost::property_tree::ptree tag_names;
      int32_t size = output_decoder.get_int();
      for (auto i = 0; i < size; ++i) {
        std::string str{output_decoder.get_string()};
        tag_names.push_back(
            std::make_pair("", boost::property_tree::ptree(str)));
      }
      node.add_child("tagNames", tag_names);
      node.put("replyCount", output_decoder.get_int());
      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    std::string_view tag_class_name = input.get_string();
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
    label_id_t work_at_label_id = graph.GetEdgeLabelId(SNBLabels::workAt_label);
    label_id_t tag_label_id = graph.GetVertexLabelId(SNBLabels::tag_label);
    label_id_t has_tag_label_id = graph.GetEdgeLabelId(SNBLabels::hasTag_label);
    label_id_t has_type_label_id =
        graph.GetEdgeLabelId(SNBLabels::hasType_label);
    label_id_t tag_class_label_id =
        graph.GetVertexLabelId(SNBLabels::tagClass_label);
    label_id_t is_sub_class_of_label_id =
        graph.GetEdgeLabelId(SNBLabels::is_subclass_of_label);

    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, id);
    // message

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto ctx1 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx0), std::move(edge_expand_opt1));

    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::In, has_creator_label_id, comment_label_id);
    // message
    auto ctx2 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx1), std::move(edge_expand_opt2));

    auto edge_expand_opt3 = gs::make_edge_expandv_opt(
        gs::Direction::Out, reply_of_label_id, post_label_id);
    // post
    auto ctx3 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(ctx2), std::move(edge_expand_opt3));

    // tags
    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx4 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(2)>(
            graph, std::move(ctx3), std::move(edge_expand_opt4));

    // tag classes
    auto edge_expand_opt5 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_type_label_id, tag_class_label_id);
    auto ctx5 = Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(3)>(
        graph, std::move(ctx4), std::move(edge_expand_opt5));

    // PathExpand to find all valid tag classes.

    auto edge_expand_opt6 = gs::make_edge_expandv_opt(
        gs::Direction::Out, is_sub_class_of_label_id, tag_class_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{tag_class_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt6), std::move(get_v_opt), gs::Range(0, 10));
    auto ctx6 =
        Engine::template PathExpandV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx5), std::move(path_expand_opt));
    // auto ctx7 = Engine::template Dedup<-1>(std::move(ctx6));

    auto filter =
        gs::make_filter(IC12Expression2(tag_class_name),
                        gs::PropertySelector<std::string_view>("name"));
    auto ctx7 = Engine::template Select<INPUT_COL_ID(-1)>(
        graph, std::move(ctx6), std::move(filter));

    auto ctx8 = Engine::GroupBy(
        graph, std::move(ctx7),
        std::tuple{
            GroupKey<0, grape::EmptyType>(),
        },
        std::tuple{gs::make_aggregate_prop<gs::AggFunc::TO_SET>(
                       std::tuple{PropertySelector<std::string_view>("name")},
                       std::integer_sequence<int32_t, INPUT_COL_ID(3)>{}),
                   gs::make_aggregate_prop<gs::AggFunc::COUNT>(
                       std::tuple{PropertySelector<grape::EmptyType>("None")},
                       std::integer_sequence<int32_t, INPUT_COL_ID(1)>{})});

    auto ctx9 = Engine::Sort(
        graph, std::move(ctx8), gs::Range(0, limit),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, INPUT_COL_ID(2), size_t>(
                "none"),
            gs::OrderingPropPair<gs::SortOrder::ASC, INPUT_COL_ID(0),
                                 gs::oid_t>("id")  // id
        });

    auto ctx10 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx9),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<oid_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>()),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>())});

    for (auto iter : ctx10) {
      auto element = iter.GetAllElement();
      output.put_long(std::get<0>(element));         // person id
      output.put_string_view(std::get<1>(element));  // person first name
      output.put_string_view(std::get<2>(element));  // person last name
      output.put_int(std::get<3>(element).size());
      for (auto tag_name : std::get<3>(element)) {
        output.put_string_view(tag_name);  // tag_name
      }
      output.put_int(std::get<4>(element));  // reply cnt
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC12_H_