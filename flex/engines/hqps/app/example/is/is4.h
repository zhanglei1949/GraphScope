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
#ifndef ENGINES_HPQS_APP_EXAMPLE_IS_IS4_H_
#define ENGINES_HPQS_APP_EXAMPLE_IS_IS4_H_

#include "flex/engines/hqps/app/example/ldbc_snb_labels.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

struct IS4expr0 {
  using result_t = bool;
  IS4expr0(int64_t messageId) : messageId_(messageId) {}
  inline auto operator()(int64_t var0) const { return var0 == messageId_; }

 private:
  int64_t messageId_;
};

template <typename GRAPH_INTERFACE>
class IS4 {
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

  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    auto messageId = input.get_long();

    auto post_label_id = graph.GetVertexLabelId(SNBLabels::post_label);
    auto comment_label_id = graph.GetVertexLabelId(SNBLabels::comment_label);

    auto filter = gs::make_filter(IS4expr0(messageId),
                                  gs::PropertySelector<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<AppendOpt::Persist>(
        time_stamp, graph,
        std::array<label_id_t, 2>{post_label_id, comment_label_id},
        std::move(filter));

    auto ctx1 = Engine::template Project<PROJ_TO_NEW>(
        time_stamp, graph, std::move(ctx0),
        std::tuple{gs::make_mapper_with_variable<0>(
                       gs::PropertySelector<int64_t>("creationDate")),
                   gs::make_mapper_with_variable<0>(
                       gs::PropertySelector<std::string_view>("content")),
                   gs::make_mapper_with_variable<0>(
                       gs::PropertySelector<std::string_view>("imageFile"))});

    for (auto iter : ctx1) {
      auto eles = iter.GetAllElement();
      output.put_long(std::get<0>(eles));
      if (std::get<1>(eles).empty()) {
        output.put_string_view(std::get<2>(eles));
      } else {
        output.put_string_view(std::get<1>(eles));
      }
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IS_IS4_H_
