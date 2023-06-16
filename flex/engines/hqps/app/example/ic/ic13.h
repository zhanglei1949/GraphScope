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
#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC13_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC13_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/engines/hqps/app/example/ldbc_snb_labels.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

class IC13Expression0 {
 public:
  IC13Expression0(oid_t oid) : oid_(oid) {}

  bool operator()(const oid_t& data) const { return oid_ == data; }

 private:
  oid_t oid_;
};

template <typename GRAPH_INTERFACE>
class QueryIC13 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t src_id = input.get<oid_t>("person1IdQ13StartNode");
    oid_t dst_id = input.get<oid_t>("person2IdQ13EndNode");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(src_id);
    input_encoder.put_long(dst_id);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      output.put("shortestPathLength", output_decoder.get_int());  // id
      // output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    int64_t src_id = input.get_long();
    int64_t dst_id = input.get_long();

    label_id_t person_label_id =
        graph.GetVertexLabelId(SNBLabels::person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(SNBLabels::knows_label);

    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, src_id);
    // message

    auto edge_expand_opt6 = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});

    auto filter = gs::make_filter(IC13Expression0(dst_id),
                                  gs::PropertySelector<oid_t>("id"));

    auto shortest_path_opt = gs::make_shortest_path_opt(
        std::move(edge_expand_opt6), std::move(get_v_opt),
        gs::Range(0, INT_MAX), std::move(filter), PathOpt::Simple,
        ResultOpt::AllV);

    auto ctx1 =
        Engine::template ShortestPath<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx0), std::move(shortest_path_opt));

    size_t len = 0;
    for (auto iter : ctx1) {
      auto ele = std::get<0>(iter.GetAllElement());
      if (len != 0) {
        auto l = ele.length();
        CHECK(len == l);
      } else {
        len = ele.length();
      }
    }
    output.put_int(len);
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC13_H_
