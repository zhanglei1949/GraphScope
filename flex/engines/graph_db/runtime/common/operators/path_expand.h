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
#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
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
  static Context single_source_shortest_path(const ReadTransaction& txn,
                                             Context&& ctx,
                                             const ShortestPathParams& params,
                                             std::pair<label_t, vid_t>& dest);

  template <typename PRED_T>
  static void single_source_shortest_path_with_pred_impl(
      const ReadTransaction& txn, const ShortestPathParams& params, int v,
      std::vector<std::vector<vid_t>>& path, PRED_T& pred) {
    std::unordered_map<vid_t, int> parent;
    std::vector<vid_t> cur;
    std::vector<vid_t> next;
    cur.push_back(v);
    parent[v] = -1;
    int depth = 0;
    while (depth < params.hop_upper && !cur.empty()) {
      for (auto u : cur) {
        if (depth >= params.hop_lower && pred(params.labels[0].src_label, u)) {
          std::vector<vid_t> p;
          int x = u;
          while (x != -1) {
            p.push_back(x);
            x = parent[x];
          }
          std::reverse(p.begin(), p.end());
          path.push_back(p);
        }
        auto oe_iter = txn.GetOutEdgeIterator(params.labels[0].src_label, u,
                                              params.labels[0].dst_label,
                                              params.labels[0].edge_label);
        while (oe_iter.IsValid()) {
          auto nbr = oe_iter.GetNeighbor();
          if (parent.find(nbr) == parent.end()) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
          oe_iter.Next();
        }

        auto ie_iter = txn.GetInEdgeIterator(params.labels[0].src_label, u,
                                             params.labels[0].dst_label,
                                             params.labels[0].edge_label);
        while (ie_iter.IsValid()) {
          auto nbr = ie_iter.GetNeighbor();
          if (parent.find(nbr) == parent.end()) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
          ie_iter.Next();
        }
      }
      ++depth;
      cur.clear();
      std::swap(cur, next);
    }
  }

  template <typename PRED_T>
  static Context single_source_shortest_path_with_predicate(
      const ReadTransaction& txn, Context&& ctx,
      const ShortestPathParams& params, PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    if (params.labels.size() == 1 &&
        params.labels[0].src_label == params.labels[0].dst_label &&
        params.dir == Direction::kBoth) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
      SLVertexColumnBuilder builder(params.labels[0].dst_label);
      GeneralPathColumnBuilder path_builder;
      std::vector<std::shared_ptr<PathImpl>> path_impls;
      foreach_vertex(input_vertex_list, [&](size_t index, label_t label,
                                            vid_t v) {
        std::vector<std::vector<vid_t>> paths;
        single_source_shortest_path_with_pred_impl(txn, params, v, paths, pred);
        {
          for (auto path : paths) {
            // skip empty path and single vertex path
            if (path.empty() || path.size() == 1) {
              continue;
            }
            builder.push_back_opt(path.back());
            shuffle_offset.push_back(index);
            auto impl =
                PathImpl::make_path_impl(params.labels[0].src_label, path);
            path_builder.push_back_opt(Path::make_path(impl));
            path_impls.emplace_back(impl);
          }
        }
      });

      path_builder.set_path_impls(path_impls);
      LOG(INFO) << "path_builder size: " << params.v_alias << " "
                << params.alias;
      ctx.set_with_reshuffle(params.v_alias, builder.finish(), shuffle_offset);
      ctx.set(params.alias, path_builder.finish());
      return ctx;
    }
    LOG(FATAL) << "not support...";
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