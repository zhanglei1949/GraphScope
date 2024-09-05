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

#include "flex/engines/graph_db/runtime/common/operators/path_expand.h"
#include "flex/engines/graph_db/runtime/common/operators/path_expand_impl.h"

namespace gs {

namespace runtime {

Context PathExpand::edge_expand_v(const ReadTransaction& txn, Context&& ctx,
                                  const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  if (params.labels.size() == 1 &&
      ctx.get(params.start_tag)->column_type() == ContextColumnType::kVertex &&
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag))
              ->vertex_column_type() == VertexColumnType::kSingle) {
    auto& input_vertex_list =
        *std::dynamic_pointer_cast<SLVertexColumn>(ctx.get(params.start_tag));
    auto pair = path_expand_vertex_without_predicate_impl(
        txn, input_vertex_list, params.labels, params.dir, params.hop_lower,
        params.hop_upper);
    ctx.set_with_reshuffle_beta(params.alias, pair.first, pair.second,
                                params.keep_cols);
    return ctx;
  } else {
    if (params.dir == Direction::kOut) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
      std::set<label_t> labels;
      for (auto& label : params.labels) {
        labels.emplace(label.dst_label);
      }

      MLVertexColumnBuilder builder(labels);
      std::vector<std::tuple<label_t, vid_t, size_t>> input;
      std::vector<std::tuple<label_t, vid_t, size_t>> output;
      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       output.emplace_back(label, v, index);
                     });
      int depth = 0;
      while (depth < params.hop_upper) {
        input.clear();
        std::swap(input, output);
        if (depth >= params.hop_lower) {
          for (auto& tuple : input) {
            builder.push_back_vertex(
                std::make_pair(std::get<0>(tuple), std::get<1>(tuple)));
            shuffle_offset.push_back(std::get<2>(tuple));
          }
        }

        if (depth + 1 >= params.hop_upper) {
          break;
        }

        for (auto& tuple : input) {
          auto label = std::get<0>(tuple);
          auto v = std::get<1>(tuple);
          auto index = std::get<2>(tuple);
          for (auto& label_triplet : params.labels) {
            if (label_triplet.src_label == label) {
              auto oe_iter = txn.GetOutEdgeIterator(label_triplet.src_label, v,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);

              while (oe_iter.IsValid()) {
                auto nbr = oe_iter.GetNeighbor();
                output.emplace_back(label_triplet.dst_label, nbr, index);
                oe_iter.Next();
              }
            }
          }
        }
        ++depth;
      }
      ctx.set_with_reshuffle_beta(params.alias, builder.finish(),
                                  shuffle_offset, params.keep_cols);
      return ctx;
    } else if (params.dir == Direction::kBoth) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<MLVertexColumn>(ctx.get(params.start_tag));
      std::set<label_t> labels;
      for (auto& label : params.labels) {
        labels.emplace(label.dst_label);
      }

      MLVertexColumnBuilder builder(labels);
      std::vector<std::tuple<label_t, vid_t, size_t>> input;
      std::vector<std::tuple<label_t, vid_t, size_t>> output;
      input_vertex_list.foreach_vertex(
          [&](size_t index, label_t label, vid_t v) {
            output.emplace_back(label, v, index);
          });
      int depth = 0;
      while (depth < params.hop_upper) {
        input.clear();
        std::swap(input, output);
        if (depth >= params.hop_lower) {
          for (auto& tuple : input) {
            builder.push_back_vertex(
                std::make_pair(std::get<0>(tuple), std::get<1>(tuple)));
            shuffle_offset.push_back(std::get<2>(tuple));
          }
        }

        if (depth + 1 >= params.hop_upper) {
          break;
        }

        for (auto& tuple : input) {
          auto label = std::get<0>(tuple);
          auto v = std::get<1>(tuple);
          auto index = std::get<2>(tuple);
          for (auto& label_triplet : params.labels) {
            if (label_triplet.src_label == label) {
              auto oe_iter = txn.GetOutEdgeIterator(label_triplet.src_label, v,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);

              while (oe_iter.IsValid()) {
                auto nbr = oe_iter.GetNeighbor();
                output.emplace_back(label_triplet.dst_label, nbr, index);
                oe_iter.Next();
              }
            }
            if (label_triplet.dst_label == label) {
              auto ie_iter = txn.GetInEdgeIterator(label_triplet.dst_label, v,
                                                   label_triplet.src_label,
                                                   label_triplet.edge_label);
              while (ie_iter.IsValid()) {
                auto nbr = ie_iter.GetNeighbor();
                output.emplace_back(label_triplet.src_label, nbr, index);
                ie_iter.Next();
              }
            }
          }
        }
        depth++;
      }
      ctx.set_with_reshuffle_beta(params.alias, builder.finish(),
                                  shuffle_offset, params.keep_cols);
      return ctx;
    }
  }
  LOG(FATAL) << "not support...";
  return ctx;
}

Context PathExpand::edge_expand_p(const ReadTransaction& txn, Context&& ctx,
                                  const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  auto dir = params.dir;
  std::vector<std::pair<std::shared_ptr<PathImpl>, size_t>> input;
  std::vector<std::pair<std::shared_ptr<PathImpl>, size_t>> output;
  std::vector<std::shared_ptr<PathImpl>> path_impls;

  GeneralPathColumnBuilder builder;
  if (dir == Direction::kOut) {
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     auto p = PathImpl::make_path_impl(label, v);
                     input.emplace_back(p, index);
                   });
    int depth = 0;
    while (depth < params.hop_upper) {
      output.clear();
      if (depth >= params.hop_lower) {
        for (auto& [path, index] : input) {
          builder.push_back_opt(Path::make_path(path));
          path_impls.emplace_back(path);
          shuffle_offset.push_back(index);
        }
      }
      if (depth + 1 >= params.hop_upper) {
        break;
      }

      for (auto& [path, index] : input) {
        auto end = path->get_end();
        for (auto& label_triplet : labels) {
          if (label_triplet.src_label == end.first) {
            auto oe_iter = txn.GetOutEdgeIterator(end.first, end.second,
                                                  label_triplet.dst_label,
                                                  label_triplet.edge_label);
            while (oe_iter.IsValid()) {
              std::shared_ptr<PathImpl> new_path =
                  path->expand(label_triplet.dst_label, oe_iter.GetNeighbor());
              output.emplace_back(new_path, index);
              oe_iter.Next();
            }
          }
        }
      }

      input.clear();
      std::swap(input, output);
      ++depth;
    }
    builder.set_path_impls(path_impls);
    ctx.set_with_reshuffle_beta(params.alias, builder.finish(), shuffle_offset,
                                params.keep_cols);

    return ctx;
  } else if (dir == Direction::kBoth) {
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     auto p = PathImpl::make_path_impl(label, v);
                     input.emplace_back(p, index);
                   });
    int depth = 0;
    while (depth < params.hop_upper) {
      output.clear();
      if (depth >= params.hop_lower) {
        for (auto& [path, index] : input) {
          builder.push_back_opt(Path::make_path(path));
          path_impls.emplace_back(path);
          shuffle_offset.push_back(index);
        }
      }
      if (depth + 1 >= params.hop_upper) {
        break;
      }

      for (auto& [path, index] : input) {
        auto end = path->get_end();
        for (auto& label_triplet : labels) {
          if (label_triplet.src_label == end.first) {
            auto oe_iter = txn.GetOutEdgeIterator(end.first, end.second,
                                                  label_triplet.dst_label,
                                                  label_triplet.edge_label);
            while (oe_iter.IsValid()) {
              auto new_path =
                  path->expand(label_triplet.dst_label, oe_iter.GetNeighbor());
              output.emplace_back(new_path, index);
              oe_iter.Next();
            }
          }
          if (label_triplet.dst_label == end.first) {
            auto ie_iter = txn.GetInEdgeIterator(end.first, end.second,
                                                 label_triplet.src_label,
                                                 label_triplet.edge_label);
            while (ie_iter.IsValid()) {
              auto new_path =
                  path->expand(label_triplet.src_label, ie_iter.GetNeighbor());
              output.emplace_back(new_path, index);
              ie_iter.Next();
            }
          }
        }
      }

      input.clear();
      std::swap(input, output);
      ++depth;
    }
    builder.set_path_impls(path_impls);
    ctx.set_with_reshuffle_beta(params.alias, builder.finish(), shuffle_offset,
                                params.keep_cols);
    return ctx;
  }
  LOG(FATAL) << "not support...";
  return ctx;
}

static bool single_source_single_dest_shortest_path_impl(
    const ReadTransaction& txn, const ShortestPathParams& params, vid_t src,
    vid_t dst, std::vector<vid_t>& path) {
  std::queue<vid_t> q1;
  std::queue<vid_t> q2;
  std::queue<vid_t> tmp;

  label_t v_label = params.labels[0].src_label;
  label_t e_label = params.labels[0].edge_label;
  std::vector<int> pre(txn.GetVertexNum(v_label), -1);
  std::vector<int> dis(txn.GetVertexNum(v_label), 0);
  q1.push(src);
  dis[src] = 1;
  q2.push(dst);
  dis[dst] = -1;

  while (true) {
    if (q1.size() <= q2.size()) {
      if (q1.empty()) {
        break;
      }
      while (!q1.empty()) {
        int x = q1.front();
        if (dis[x] >= params.hop_upper + 1) {
          return false;
        }
        q1.pop();
        auto oe_iter = txn.GetOutEdgeIterator(v_label, x, v_label, e_label);
        while (oe_iter.IsValid()) {
          int y = oe_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] + 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] < 0) {
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            std::reverse(path.begin(), path.end());
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          oe_iter.Next();
        }
        auto ie_iter = txn.GetInEdgeIterator(v_label, x, v_label, e_label);
        while (ie_iter.IsValid()) {
          int y = ie_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] + 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] < 0) {
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            std::reverse(path.begin(), path.end());
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          ie_iter.Next();
        }
      }
      std::swap(q1, tmp);
    } else {
      if (q2.empty()) {
        break;
      }
      while (!q2.empty()) {
        int x = q2.front();
        if (dis[x] <= -params.hop_upper - 1) {
          return false;
        }
        q2.pop();
        auto oe_iter = txn.GetOutEdgeIterator(v_label, x, v_label, e_label);
        while (oe_iter.IsValid()) {
          int y = oe_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] - 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] > 0) {
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            std::reverse(path.begin(), path.end());
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          oe_iter.Next();
        }
        auto ie_iter = txn.GetInEdgeIterator(v_label, x, v_label, e_label);
        while (ie_iter.IsValid()) {
          int y = ie_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] - 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] > 0) {
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            std::reverse(path.begin(), path.end());
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          ie_iter.Next();
        }
      }
      std::swap(q2, tmp);
    }
  }
  return false;
}

Context PathExpand::single_source_single_dest_shortest_path(
    const ReadTransaction& txn, Context&& ctx, const ShortestPathParams& params,
    std::pair<label_t, vid_t>& dest) {
  std::vector<size_t> shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  CHECK(labels.size() == 1) << "only support one label triplet";
  CHECK(label_sets.size() == 1) << "only support one label set";
  auto label_triplet = labels[0];
  CHECK(label_triplet.src_label == label_triplet.dst_label)
      << "only support same src and dst label";
  auto dir = params.dir;
  CHECK(dir == Direction::kBoth) << "only support both direction";
  SLVertexColumnBuilder builder(label_triplet.dst_label);
  GeneralPathColumnBuilder path_builder;
  std::vector<std::shared_ptr<PathImpl>> path_impls;
  foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
    std::vector<vid_t> path;
    if (single_source_single_dest_shortest_path_impl(txn, params, v,
                                                     dest.second, path)) {
      builder.push_back_opt(dest.second);
      shuffle_offset.push_back(index);
      auto impl = PathImpl::make_path_impl(label_triplet.src_label, path);
      path_builder.push_back_opt(Path::make_path(impl));
      path_impls.emplace_back(impl);
    }
  });

  path_builder.set_path_impls(path_impls);

  ctx.set_with_reshuffle(params.v_alias, builder.finish(), shuffle_offset);
  ctx.set(params.alias, path_builder.finish());
  return ctx;
}

}  // namespace runtime

}  // namespace gs
