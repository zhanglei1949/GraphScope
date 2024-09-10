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

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/special_predicates.h"
#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
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
  static Context edge_expand_v(const ReadTransaction& txn, Context&& ctx,
                               const PathExpandParams& params);
  static Context edge_expand_p(const ReadTransaction& txn, Context&& ctx,
                               const PathExpandParams& params);

  // single dst
  static Context single_source_single_dest_shortest_path(
      const ReadTransaction& txn, Context&& ctx,
      const ShortestPathParams& params, std::pair<label_t, vid_t>& dest);

  template <typename PRED_T>
  static Context single_source_shortest_path(const ReadTransaction& txn,
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
      const auto& properties = txn.schema().get_edge_properties(
          params.labels[0].src_label, params.labels[0].dst_label,
          params.labels[0].edge_label);
      if (properties.empty()) {
        auto tup = single_source_shortest_path_impl<grape::EmptyType, PRED_T>(
            txn, *input_vertex_col, params.labels[0].edge_label, params.dir,
            params.hop_lower, params.hop_upper, pred);
        ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                               std::get<2>(tup));
        ctx.set(params.alias, std::get<1>(tup));
        return ctx;
      } else if (properties.size() == 1) {
        if (properties[0] == PropertyType::Int32()) {
          auto tup = single_source_shortest_path_impl<int, PRED_T>(
              txn, *input_vertex_col, params.labels[0].edge_label, params.dir,
              params.hop_lower, params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Int64()) {
          auto tup = single_source_shortest_path_impl<int64_t, PRED_T>(
              txn, *input_vertex_col, params.labels[0].edge_label, params.dir,
              params.hop_lower, params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Date()) {
          auto tup = single_source_shortest_path_impl<Date, PRED_T>(
              txn, *input_vertex_col, params.labels[0].edge_label, params.dir,
              params.hop_lower, params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        }
      }
    }
    auto tup = default_single_source_shortest_path_impl<PRED_T>(
        txn, *input_vertex_col, params.labels, params.dir, params.hop_lower,
        params.hop_upper, pred);
    ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup), std::get<2>(tup));
    ctx.set(params.alias, std::get<1>(tup));
    return ctx;
  }

  static Context single_source_shortest_path_with_special_vertex_predicate(
      const ReadTransaction& txn, Context&& ctx,
      const ShortestPathParams& params, const SPVertexPredicate& pred) {
    if (pred.type() == SPVertexPredicateType::kIdEQ) {
      return single_source_shortest_path<VertexIdEQPredicateBeta>(
          txn, std::move(ctx), params,
          dynamic_cast<const VertexIdEQPredicateBeta&>(pred));
    } else {
      if (pred.data_type() == RTAnyType::kI64Value) {
        if (pred.type() == SPVertexPredicateType::kPropertyLT) {
          return single_source_shortest_path<
              VertexPropertyLTPredicateBeta<int64_t>>(
              txn, std::move(ctx), params,
              dynamic_cast<const VertexPropertyLTPredicateBeta<int64_t>&>(
                  pred));
        } else if (pred.type() == SPVertexPredicateType::kPropertyGT) {
          return single_source_shortest_path<
              VertexPropertyGTPredicateBeta<int64_t>>(
              txn, std::move(ctx), params,
              dynamic_cast<const VertexPropertyGTPredicateBeta<int64_t>&>(
                  pred));
        } else if (pred.type() == SPVertexPredicateType::kPropertyLE) {
          return single_source_shortest_path<
              VertexPropertyLEPredicateBeta<int64_t>>(
              txn, std::move(ctx), params,
              dynamic_cast<const VertexPropertyLEPredicateBeta<int64_t>&>(
                  pred));
        } else if (pred.type() == SPVertexPredicateType::kPropertyBetween) {
          return single_source_shortest_path<
              VertexPropertyBetweenPredicateBeta<int64_t>>(
              txn, std::move(ctx), params,
              dynamic_cast<const VertexPropertyBetweenPredicateBeta<int64_t>&>(
                  pred));
        } else {
          CHECK(pred.type() == SPVertexPredicateType::kPropertyEQ);
          return single_source_shortest_path<
              VertexPropertyEQPredicateBeta<int64_t>>(
              txn, std::move(ctx), params,
              dynamic_cast<const VertexPropertyEQPredicateBeta<int64_t>&>(
                  pred));
        }
      } else if (pred.data_type() == RTAnyType::kStringValue) {
        if (pred.type() == SPVertexPredicateType::kPropertyEQ) {
          return single_source_shortest_path<
              VertexPropertyEQPredicateBeta<std::string_view>>(
              txn, std::move(ctx), params,
              dynamic_cast<
                  const VertexPropertyEQPredicateBeta<std::string_view>&>(
                  pred));
        }
      }
    }
    LOG(FATAL) << "not impl";
    return ctx;
  }

  template <typename PRED_T>
  static Context edge_expand_v_pred(const ReadTransaction& txn, Context&& ctx,
                                    const PathExpandParams& params,
                                    const PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    if (params.labels.size() == 1 &&
        params.labels[0].src_label == params.labels[0].dst_label) {
      if (params.dir == Direction::kOut) {
        auto& input_vertex_list = *std::dynamic_pointer_cast<SLVertexColumn>(
            ctx.get(params.start_tag));
        label_t output_vertex_label = params.labels[0].dst_label;
        label_t edge_label = params.labels[0].edge_label;
        label_t vertex_label = params.labels[0].src_label;
        SLVertexColumnBuilder builder(output_vertex_label);

#if 0
        std::vector<vid_t> input;
        std::vector<vid_t> output;
        input_vertex_list.foreach_vertex(
            [&](size_t index, label_t label, vid_t v) {
              int depth = 0;
              input.clear();
              output.clear();
              input.push_back(v);
              while (depth < params.hop_upper && !input.empty()) {
                if (depth >= params.hop_lower) {
                  for (auto u : input) {
                    if (pred(label, u)) {
                      builder.push_back_opt(u);
                      shuffle_offset.push_back(index);
                    }

                    auto oe_iter = txn.GetOutEdgeIterator(
                        label, u, output_vertex_label, edge_label);
                    while (oe_iter.IsValid()) {
                      output.push_back(oe_iter.GetNeighbor());
                      oe_iter.Next();
                    }
                  }
                } else {
                  for (auto u : input) {
                    auto oe_iter = txn.GetOutEdgeIterator(
                        label, u, output_vertex_label, edge_label);
                    while (oe_iter.IsValid()) {
                      output.push_back(oe_iter.GetNeighbor());
                      oe_iter.Next();
                    }
                  }
                }
                ++depth;
                input.clear();
                std::swap(input, output);
              }
            });
#else
        std::vector<std::pair<size_t, vid_t>> input;
        std::vector<std::pair<size_t, vid_t>> output;
        input_vertex_list.foreach_vertex(
            [&](size_t index, label_t label, vid_t v) {
              output.emplace_back(index, v);
            });
        int depth = 0;
        auto oe_csr = txn.GetOutgoingSingleImmutableGraphView<grape::EmptyType>(
            vertex_label, vertex_label, edge_label);
        while (depth < params.hop_upper && !output.empty()) {
          input.clear();
          std::swap(input, output);

          for (auto& pair : input) {
            if (pred(vertex_label, pair.second, pair.first)) {
              builder.push_back_opt(pair.second);
              shuffle_offset.push_back(pair.first);
            }
          }
          if (depth + 1 >= params.hop_upper) {
            break;
          }
          for (auto& pair : input) {
            auto index = pair.first;
            auto v = pair.second;
            if (oe_csr.exist(v)) {
              output.emplace_back(index, oe_csr.get_edge(v).neighbor);
            }
            // auto oe_iter = txn.GetOutEdgeIterator(
            //     vertex_label, v, output_vertex_label, edge_label);
            // while (oe_iter.IsValid()) {
            //   auto nbr = oe_iter.GetNeighbor();
            //   output.emplace_back(index, nbr);
            //   oe_iter.Next();
            // }
          }

          ++depth;
        }
#endif

        ctx.set_with_reshuffle_beta(params.alias, builder.finish(),
                                    shuffle_offset, params.keep_cols);
        return ctx;
      }
    }
    LOG(FATAL) << "not support...";
    return ctx;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_PATH_EXPAND_H_