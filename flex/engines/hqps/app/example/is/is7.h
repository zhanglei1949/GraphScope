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
#ifndef ENGINES_HPQS_APP_EXAMPLE_IS_IS7_H_
#define ENGINES_HPQS_APP_EXAMPLE_IS_IS7_H_

#include "flex/engines/hqps/app/example/ldbc_snb_labels.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

struct IS7Expr0 {
  using result_t = bool;
  IS7Expr0(int64_t messageId) : messageId_(messageId) {}
  inline auto operator()(int64_t var0) const { return var0 == messageId_; }

 private:
  int64_t messageId_;
};

struct IS7Expr1 {
  using result_t = bool;
  IS7Expr1() {}

  template <typename ELE_t>
  inline auto operator()(const ELE_t& edge_ele_tuple) const {
    if (edge_ele_tuple == NONE) {
      return false;
    } else {
      return true;
    }
  }

 private:
};

template <typename GRAPH_INTERFACE>
class IS7 {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    LOG(INFO) << "start";
    oid_t id = input.get<oid_t>("messageRepliesId");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("commentId", output_decoder.get_long());         // id
      node.put("commentContent", output_decoder.get_string());  // dist

      output.push_back(std::make_pair("", node));
    }
  }

  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
             Encoder& output) const {
    auto messageId = input.get_long();

    auto post_label_id = graph.GetVertexLabelId(SNBLabels::post_label);
    auto comment_label_id = graph.GetVertexLabelId(SNBLabels::comment_label);
    auto reply_of_label_id = graph.GetEdgeLabelId(SNBLabels::replyOf_label);
    auto hasCreator_label_id =
        graph.GetEdgeLabelId(SNBLabels::hasCreator_label);
    auto person_label_id = graph.GetVertexLabelId(SNBLabels::person_label);
    auto knows_label_id = graph.GetEdgeLabelId(SNBLabels::knows_label);

    auto filter = gs::make_filter(IS7Expr0(messageId),
                                  gs::PropertySelector<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<AppendOpt::Persist>(
        graph, std::array<label_id_t, 2>{post_label_id, comment_label_id},
        std::move(filter));
    auto right_ctx(ctx0);

    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::In, reply_of_label_id, comment_label_id);

    auto ctx1 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Out, hasCreator_label_id, person_label_id);

    auto ctx2 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx1), std::move(edge_expand_opt1));
    for (auto iter : ctx2) {
      auto ele = iter.GetAllElement();
      LOG(INFO) << "ctx2:" << gs::to_string(ele);
    }

    // right side

    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, hasCreator_label_id, person_label_id);
    auto right_ctx1 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(right_ctx), std::move(edge_expand_opt2));

    auto edge_expand_opt3 = gs::make_edge_expande_opt<int64_t>(
        {"creationDate"}, gs::Direction::Both, knows_label_id, person_label_id);
    auto right_ctx2 =
        Engine::template EdgeExpandE<AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(right_ctx1), std::move(edge_expand_opt3));

    auto get_v_opt0 = make_getv_opt(gs::VOpt::Other,
                                    std::array<label_id_t, 1>{person_label_id});
    auto right_ctx3 =
        Engine::template GetV<AppendOpt::Persist, INPUT_COL_ID(2)>(
            graph, std::move(right_ctx2), std::move(get_v_opt0));

    for (auto iter : right_ctx3) {
      auto ele = iter.GetAllElement();
      LOG(INFO) << "ctx2:" << gs::to_string(ele);
    }

    auto ctx3 =
        Engine::template Join<INPUT_COL_ID(0), INPUT_COL_ID(2), INPUT_COL_ID(0),
                              INPUT_COL_ID(3), JoinKind::LeftOuterJoin>(
            std::move(ctx2), std::move(right_ctx3));
    // message, cmt, replyAuthor, msgAuthor, knows.

    auto ctx4 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx3),
        std::tuple{gs::make_mapper_with_variable<1>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<1>(
                       gs::PropertySelector<std::string_view>("content")),
                   gs::make_mapper_with_variable<1>(
                       gs::PropertySelector<int64_t>("creationDate")),
                   gs::make_mapper_with_variable<2>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<2>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<2>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_expr<4>(
                       IS7Expr1(), gs::PropertySelector<grape::EmptyType>())});

    for (auto iter : ctx4) {
      auto ele = iter.GetAllElement();
      LOG(INFO) << gs::to_string(ele);
      output.put_long(std::get<0>(ele));
      output.put_string_view(std::get<1>(ele));
      output.put_long(std::get<2>(ele));
      output.put_long(std::get<3>(ele));
      output.put_string_view(std::get<4>(ele));
      output.put_string_view(std::get<5>(ele));
      output.put_byte(std::get<6>(ele));
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IS_IS7_H_
