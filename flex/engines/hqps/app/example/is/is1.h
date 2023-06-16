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
#ifndef ENGINES_HPQS_APP_EXAMPLE_IS_IS1_H_
#define ENGINES_HPQS_APP_EXAMPLE_IS_IS1_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/engines/hqps/app/example/ldbc_snb_labels.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

struct IS1Expr0 {
  using result_t = bool;
  IS1Expr0(int64_t personId) : personId_(personId) {}
  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == personId_);
  }

 private:
  int64_t personId_;
};

template <typename GRAPH_INTERFACE>
class IS1 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ1");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("friendFirstName", output_decoder.get_string());   // id
      node.put("friendLastName", output_decoder.get_string());    // dist
      node.put("friendBirthday", output_decoder.get_long());      // lastName"
      node.put("friendLocationIP", output_decoder.get_string());  // birthday
      node.put("friendBrowserUsed",
               output_decoder.get_string());  // creationDate
      node.put("friendId",
               output_decoder.get_long());  // gender
      node.put("friendGender",
               output_decoder.get_string());  // browserUsed
      node.put("friendCreationDate",
               output_decoder.get_long());  // locationIP

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    int64_t personId = input.get_long();

    auto person_label_id = graph.GetVertexLabelId(SNBLabels::person_label);
    auto knows_label_id = graph.GetEdgeLabelId(SNBLabels::knows_label);
    auto isLocated_label_id =
        graph.GetEdgeLabelId(SNBLabels::isLocatedIn_label);
    auto place_label_id = graph.GetVertexLabelId(SNBLabels::place_label);

    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Persist>(
        graph, person_label_id, personId);
    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Out, isLocated_label_id, place_label_id);

    auto ctx1 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto ctx2 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx1),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("birthday")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("locationIP")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("browserUsed")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("gender")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("creationDate"))

        });
    // no limit
    for (auto iter : ctx2) {
      auto ele = iter.GetAllElement();
      output.put_string_view(std::get<0>(ele));
      output.put_string_view(std::get<1>(ele));
      output.put_long(std::get<2>(ele));
      output.put_string_view(std::get<3>(ele));
      output.put_string_view(std::get<4>(ele));
      output.put_long(std::get<5>(ele));
      output.put_string_view(std::get<6>(ele));
      output.put_long(std::get<7>(ele));
    }
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IS_IS1_H_
