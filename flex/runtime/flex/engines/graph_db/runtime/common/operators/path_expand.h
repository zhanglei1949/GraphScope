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

#ifndef RUNTIME_COMMON_OPERATORS_PATH_EXPAND_H_
#define RUNTIME_COMMON_OPERATORS_PATH_EXPAND_H_

#include <vector>

#include "flex/engines/graph_db/runtime/adhoc/operators/special_predicates.h"
#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/operators/path_expand_impl.h"
#include "flex/engines/graph_db/runtime/common/types.h"

namespace gs {

namespace runtime {

struct PathExpandParams {
  int start_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
  int hop_lower;
  int hop_upper;
  std::set<int> keep_cols;
};

struct ShortestPathParams {
  int start_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  int v_alias;
  Direction dir;
  int hop_lower;
  int hop_upper;
};

class PathExpand {
 public:
  // PathExpand(expandOpt == Vertex && alias == -1 && resultOpt == END_V) +
  // GetV(opt == END)
  static Context edge_expand_v(const GraphReadInterface& graph, Context&& ctx,
                               const PathExpandParams& params);
  static Context edge_expand_p(const GraphReadInterface& graph, Context&& ctx,
                               const PathExpandParams& params);

  static Context all_shortest_paths_with_given_source_and_dest(
      const GraphReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, const std::pair<label_t, vid_t>& dst);
  // single dst
  static Context single_source_single_dest_shortest_path(
      const GraphReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, std::pair<label_t, vid_t>& dest);

  template <typename PRED_T>
  static Context single_source_shortest_path_with_order_by_length_limit(
      const GraphReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, const PRED_T& pred, int limit_upper) {
    std::vector<size_t> shuffle_offset;
    auto input_vertex_col =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    if (params.labels.size() == 1 &&
        params.labels[0].src_label == params.labels[0].dst_label &&
        params.dir == Direction::kBoth &&
        input_vertex_col->get_labels_set().size() == 1) {
      const auto& properties = graph.schema().get_edge_properties(
          params.labels[0].src_label, params.labels[0].dst_label,
          params.labels[0].edge_label);
      if (properties.empty()) {
        auto tup = single_source_shortest_path_with_order_by_length_limit_impl<
            grape::EmptyType, PRED_T>(
            graph, *input_vertex_col, params.labels[0].edge_label, params.dir,
            params.hop_lower, params.hop_upper, pred, limit_upper);
        ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                               std::get<2>(tup));
        ctx.set(params.alias, std::get<1>(tup));
        return ctx;
      } else if (properties.size() == 1) {
        if (properties[0] == PropertyType::Int32()) {
          auto tup =
              single_source_shortest_path_with_order_by_length_limit_impl<
                  int, PRED_T>(graph, *input_vertex_col,
                               params.labels[0].edge_label, params.dir,
                               params.hop_lower, params.hop_upper, pred,
                               limit_upper);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Int64()) {
          auto tup =
              single_source_shortest_path_with_order_by_length_limit_impl<
                  int64_t, PRED_T>(graph, *input_vertex_col,
                                   params.labels[0].edge_label, params.dir,
                                   params.hop_lower, params.hop_upper, pred,
                                   limit_upper);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Date()) {
          auto tup =
              single_source_shortest_path_with_order_by_length_limit_impl<
                  Date, PRED_T>(graph, *input_vertex_col,
                                params.labels[0].edge_label, params.dir,
                                params.hop_lower, params.hop_upper, pred,
                                limit_upper);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        }
      }
    }
    LOG(FATAL) << "not support edge property type ";
    return ctx;
  }

  template <typename PRED_T>
  static Context single_source_shortest_path(const GraphReadInterface& graph,
                                             Context&& ctx,
                                             const ShortestPathParams& params,
                                             const PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    auto input_vertex_col =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    if (params.labels.size() == 1 &&
        params.labels[0].src_label == params.labels[0].dst_label &&
        params.dir == Direction::kBoth &&
        input_vertex_col->get_labels_set().size() == 1) {
      const auto& properties = graph.schema().get_edge_properties(
          params.labels[0].src_label, params.labels[0].dst_label,
          params.labels[0].edge_label);
      if (properties.empty()) {
        auto tup = single_source_shortest_path_impl<grape::EmptyType, PRED_T>(
            graph, *input_vertex_col, params.labels[0].edge_label, params.dir,
            params.hop_lower, params.hop_upper, pred);
        ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                               std::get<2>(tup));
        ctx.set(params.alias, std::get<1>(tup));
        return ctx;
      } else if (properties.size() == 1) {
        if (properties[0] == PropertyType::Int32()) {
          auto tup = single_source_shortest_path_impl<int, PRED_T>(
              graph, *input_vertex_col, params.labels[0].edge_label, params.dir,
              params.hop_lower, params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Int64()) {
          auto tup = single_source_shortest_path_impl<int64_t, PRED_T>(
              graph, *input_vertex_col, params.labels[0].edge_label, params.dir,
              params.hop_lower, params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Date()) {
          auto tup = single_source_shortest_path_impl<Date, PRED_T>(
              graph, *input_vertex_col, params.labels[0].edge_label, params.dir,
              params.hop_lower, params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        }
      }
    }
    auto tup = default_single_source_shortest_path_impl<PRED_T>(
        graph, *input_vertex_col, params.labels, params.dir, params.hop_lower,
        params.hop_upper, pred);
    ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup), std::get<2>(tup));
    ctx.set(params.alias, std::get<1>(tup));
    return ctx;
  }

  static Context single_source_shortest_path_with_special_vertex_predicate(
      const GraphReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, const SPVertexPredicate& pred) {
    if (pred.type() == SPVertexPredicateType::kIdEQ) {
      return single_source_shortest_path<VertexIdEQPredicateBeta>(
          graph, std::move(ctx), params,
          dynamic_cast<const VertexIdEQPredicateBeta&>(pred));
    } else {
      if (pred.data_type() == RTAnyType::kI64Value) {
        if (pred.type() == SPVertexPredicateType::kPropertyLT) {
          return single_source_shortest_path<
              VertexPropertyLTPredicateBeta<int64_t>>(
              graph, std::move(ctx), params,
              dynamic_cast<const VertexPropertyLTPredicateBeta<int64_t>&>(
                  pred));
        } else if (pred.type() == SPVertexPredicateType::kPropertyGT) {
          return single_source_shortest_path<
              VertexPropertyGTPredicateBeta<int64_t>>(
              graph, std::move(ctx), params,
              dynamic_cast<const VertexPropertyGTPredicateBeta<int64_t>&>(
                  pred));
        } else if (pred.type() == SPVertexPredicateType::kPropertyLE) {
          return single_source_shortest_path<
              VertexPropertyLEPredicateBeta<int64_t>>(
              graph, std::move(ctx), params,
              dynamic_cast<const VertexPropertyLEPredicateBeta<int64_t>&>(
                  pred));
        } else if (pred.type() == SPVertexPredicateType::kPropertyBetween) {
          return single_source_shortest_path<
              VertexPropertyBetweenPredicateBeta<int64_t>>(
              graph, std::move(ctx), params,
              dynamic_cast<const VertexPropertyBetweenPredicateBeta<int64_t>&>(
                  pred));
        } else {
          CHECK(pred.type() == SPVertexPredicateType::kPropertyEQ);
          return single_source_shortest_path<
              VertexPropertyEQPredicateBeta<int64_t>>(
              graph, std::move(ctx), params,
              dynamic_cast<const VertexPropertyEQPredicateBeta<int64_t>&>(
                  pred));
        }
      } else if (pred.data_type() == RTAnyType::kStringValue) {
        if (pred.type() == SPVertexPredicateType::kPropertyEQ) {
          return single_source_shortest_path<
              VertexPropertyEQPredicateBeta<std::string_view>>(
              graph, std::move(ctx), params,
              dynamic_cast<
                  const VertexPropertyEQPredicateBeta<std::string_view>&>(
                  pred));
        }
      }
    }
    LOG(FATAL) << "not impl";
    return ctx;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_PATH_EXPAND_H_