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
#ifndef ENGINES_HPQS_APP_EXAMPLE_IS_IS3_H_
#define ENGINES_HPQS_APP_EXAMPLE_IS_IS3_H_

#include "flex/engines/hqps/app/example/ldbc_snb_labels.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename GRAPH_INTERFACE>
class IS3 {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ2");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("messageId", output_decoder.get_string());       // id
      node.put("messageContent", output_decoder.get_string());  // dist
      LOG(FATAL) << "impl";

      output.push_back(std::make_pair("", node));
    }
  }

  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
             Encoder& output) const {
    auto personId = input.get_long();

    auto knows_label_id = graph.GetEdgeLabelId(SNBLabels::knows_label);
    auto person_label_id = graph.GetVertexLabelId(SNBLabels::person_label);

    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Persist>(
        graph, person_label_id, personId);

    auto edge_expand_opt0 = gs::make_edge_expande_opt<int64_t>(
        {"creationDate"}, gs::Direction::Both, knows_label_id, person_label_id);
    auto ctx1 =
        Engine::template EdgeExpandE<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));
    auto get_v_opt1 = make_getv_opt(gs::VOpt::Other,
                                    std::array<label_id_t, 1>{person_label_id});

    auto ctx2 = Engine::template GetV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt1));

    auto ctx3 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx2),
        std::tuple{gs::make_mapper_with_variable<2>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<2>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<2>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_variable<1>(
                       gs::PropertySelector<int64_t>("creationDate"))});

    auto ctx4 = Engine::Sort(
        graph, std::move(ctx3), gs::Range(0, INT_MAX),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 3, int64_t>("None"),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("None")});

    for (auto iter : ctx4) {
      auto eles = iter.GetAllElement();
      output.put_long(std::get<0>(eles));
      output.put_string_view(std::get<1>(eles));
      output.put_string_view(std::get<2>(eles));
      output.put_long(std::get<3>(eles));
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IS_IS3_H_
