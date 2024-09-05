
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

#ifndef RUNTIME_COMMON_OPERATORS_PATH_EXPAND_IMPL_H_
#define RUNTIME_COMMON_OPERATORS_PATH_EXPAND_IMPL_H_

#include <memory>
#include <utility>
#include <vector>

#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

namespace gs {
namespace runtime {

template <typename EDATA_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
iterative_expand_vertex_on_graph_view(const GraphView<EDATA_T>& view,
                                      const SLVertexColumn& input, int lower,
                                      int upper) {
  int input_label = input.label();
  SLVertexColumnBuilder builder(input_label);
  std::vector<size_t> offsets;
  if (upper == lower) {
    return std::make_pair(builder.finish(), std::move(offsets));
  }
  if (upper == 1) {
    CHECK_EQ(lower, 0);
    size_t idx = 0;
    for (auto v : input.vertices()) {
      builder.push_back_opt(v);
      offsets.push_back(idx++);
    }
    return std::make_pair(builder.finish(), std::move(offsets));
  }
  // upper >= 2
  std::vector<std::pair<vid_t, vid_t>> input_list;
  std::vector<std::pair<vid_t, vid_t>> output_list;

  {
    vid_t idx = 0;
    for (auto v : input.vertices()) {
      output_list.emplace_back(v, idx++);
    }
  }
  int depth = 0;
  while (!output_list.empty()) {
    input_list.clear();
    std::swap(input_list, output_list);
    if (depth >= lower && depth < upper) {
      if (depth == (upper - 1)) {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.first);
          offsets.push_back(pair.second);
        }
      } else {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.first);
          offsets.push_back(pair.second);

          auto es = view.get_edges(pair.first);
          for (auto& e : es) {
            output_list.emplace_back(e.neighbor, pair.second);
          }
        }
      }
    } else if (depth < lower) {
      for (auto& pair : input_list) {
        auto es = view.get_edges(pair.first);
        for (auto& e : es) {
          output_list.emplace_back(e.neighbor, pair.second);
        }
      }
    }
    ++depth;
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename EDATA_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
iterative_expand_vertex_on_dual_graph_view(const GraphView<EDATA_T>& iview,
                                           const GraphView<EDATA_T>& oview,
                                           const SLVertexColumn& input,
                                           int lower, int upper) {
  int input_label = input.label();
  SLVertexColumnBuilder builder(input_label);
  std::vector<size_t> offsets;
  if (upper == lower) {
    return std::make_pair(builder.finish(), std::move(offsets));
  }
  if (upper == 1) {
    CHECK_EQ(lower, 0);
    size_t idx = 0;
    for (auto v : input.vertices()) {
      builder.push_back_opt(v);
      offsets.push_back(idx++);
    }
    return std::make_pair(builder.finish(), std::move(offsets));
  }
  // upper >= 2
  std::vector<std::pair<vid_t, vid_t>> input_list;
  std::vector<std::pair<vid_t, vid_t>> output_list;

  {
    vid_t idx = 0;
    for (auto v : input.vertices()) {
      output_list.emplace_back(v, idx++);
    }
  }
  int depth = 0;
  while (!output_list.empty()) {
    input_list.clear();
    std::swap(input_list, output_list);
    if (depth >= lower && depth < upper) {
      if (depth == (upper - 1)) {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.first);
          offsets.push_back(pair.second);
        }
      } else {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.first);
          offsets.push_back(pair.second);

          auto ies = iview.get_edges(pair.first);
          for (auto& e : ies) {
            output_list.emplace_back(e.neighbor, pair.second);
          }
          auto oes = oview.get_edges(pair.first);
          for (auto& e : oes) {
            output_list.emplace_back(e.neighbor, pair.second);
          }
        }
      }
    } else if (depth < lower) {
      for (auto& pair : input_list) {
        auto ies = iview.get_edges(pair.first);
        for (auto& e : ies) {
          output_list.emplace_back(e.neighbor, pair.second);
        }
        auto oes = oview.get_edges(pair.first);
        for (auto& e : oes) {
          output_list.emplace_back(e.neighbor, pair.second);
        }
      }
    }
    ++depth;
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
path_expand_vertex_without_predicate_impl(
    const ReadTransaction& txn, const SLVertexColumn& input,
    const std::vector<LabelTriplet>& labels, Direction dir, int lower,
    int upper);

template <typename EDATA_T, typename PRED_T>
void sssp_dir(const GraphView<EDATA_T>& view, label_t v_label, vid_t v,
              vid_t vnum, size_t idx, int lower, int upper,
              SLVertexColumnBuilder& dest_col_builder,
              GeneralPathColumnBuilder& path_col_builder,
              std::vector<std::shared_ptr<PathImpl>>& path_impls,
              std::vector<size_t>& offsets, const PRED_T& pred) {
  std::vector<vid_t> cur;
  std::vector<vid_t> next;
  cur.push_back(v);
  int depth = 0;
  static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
  std::vector<vid_t> parent(vnum, invalid_vid);

  while (depth < upper && !cur.empty()) {
    if (depth >= lower) {
      if (depth == upper - 1) {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, path);
            path_col_builder.push_back_opt(Path::make_path(impl));
            path_impls.emplace_back(impl);
            offsets.push_back(idx);
          }
        }
      } else {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, path);
            path_col_builder.push_back_opt(Path::make_path(impl));
            path_impls.emplace_back(impl);
            offsets.push_back(idx);
          }
          for (auto& e : view.get_edges(u)) {
            auto nbr = e.neighbor;
            if (parent[nbr] == invalid_vid) {
              parent[nbr] = u;
              next.push_back(nbr);
            }
          }
        }
      }
    } else {
      for (auto u : cur) {
        for (auto& e : view.get_edges(u)) {
          auto nbr = e.neighbor;
          if (parent[nbr] == invalid_vid) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
        }
      }
    }
    ++depth;
    cur.clear();
    std::swap(cur, next);
  }
}

template <typename EDATA_T, typename PRED_T>
void sssp_both_dir(const GraphView<EDATA_T>& view0,
                   const GraphView<EDATA_T>& view1, label_t v_label, vid_t v,
                   vid_t vnum, size_t idx, int lower, int upper,
                   SLVertexColumnBuilder& dest_col_builder,
                   GeneralPathColumnBuilder& path_col_builder,
                   std::vector<std::shared_ptr<PathImpl>>& path_impls,
                   std::vector<size_t>& offsets, const PRED_T& pred) {
  std::vector<vid_t> cur;
  std::vector<vid_t> next;
  cur.push_back(v);
  int depth = 0;
  static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
  std::vector<vid_t> parent(vnum, invalid_vid);

  while (depth < upper && !cur.empty()) {
    if (depth >= lower) {
      if (depth == upper - 1) {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, path);
            path_col_builder.push_back_opt(Path::make_path(impl));
            path_impls.emplace_back(impl);
            offsets.push_back(idx);
          }
        }
      } else {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, path);
            path_col_builder.push_back_opt(Path::make_path(impl));
            path_impls.emplace_back(impl);
            offsets.push_back(idx);
          }
          for (auto& e : view0.get_edges(u)) {
            auto nbr = e.neighbor;
            if (parent[nbr] == invalid_vid) {
              parent[nbr] = u;
              next.push_back(nbr);
            }
          }
          for (auto& e : view1.get_edges(u)) {
            auto nbr = e.neighbor;
            if (parent[nbr] == invalid_vid) {
              parent[nbr] = u;
              next.push_back(nbr);
            }
          }
        }
      }
    } else {
      for (auto u : cur) {
        for (auto& e : view0.get_edges(u)) {
          auto nbr = e.neighbor;
          if (parent[nbr] == invalid_vid) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
        }
        for (auto& e : view1.get_edges(u)) {
          auto nbr = e.neighbor;
          if (parent[nbr] == invalid_vid) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
        }
      }
    }
    ++depth;
    cur.clear();
    std::swap(cur, next);
  }
}

template <typename EDATA_T, typename PRED_T>
std::tuple<std::shared_ptr<IContextColumn>, std::shared_ptr<IContextColumn>,
           std::vector<size_t>>
single_source_shortest_path_impl(const ReadTransaction& txn,
                                 const IVertexColumn& input, label_t e_label,
                                 Direction dir, int lower, int upper,
                                 const PRED_T& pred) {
  label_t v_label = *input.get_labels_set().begin();
  vid_t vnum = txn.GetVertexNum(v_label);
  SLVertexColumnBuilder dest_col_builder(v_label);
  GeneralPathColumnBuilder path_col_builder;
  std::vector<std::shared_ptr<PathImpl>> path_impls;
  std::vector<size_t> offsets;
  if (dir == Direction::kIn || dir == Direction::kOut) {
    GraphView<EDATA_T> view =
        (dir == Direction::kIn)
            ? txn.GetIncomingGraphView<EDATA_T>(v_label, v_label, e_label)
            : txn.GetOutgoingGraphView<EDATA_T>(v_label, v_label, e_label);
    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      sssp_dir(view, label, v, vnum, idx, lower, upper, dest_col_builder,
               path_col_builder, path_impls, offsets, pred);
    });
  } else {
    CHECK(dir == Direction::kBoth);
    GraphView<EDATA_T> oe_view =
        txn.GetOutgoingGraphView<EDATA_T>(v_label, v_label, e_label);
    GraphView<EDATA_T> ie_view =
        txn.GetIncomingGraphView<EDATA_T>(v_label, v_label, e_label);
    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      sssp_both_dir(oe_view, ie_view, v_label, v, vnum, idx, lower, upper,
                    dest_col_builder, path_col_builder, path_impls, offsets,
                    pred);
    });
  }
  path_col_builder.set_path_impls(path_impls);
  return std::make_tuple(dest_col_builder.finish(), path_col_builder.finish(),
                         std::move(offsets));
}

template <typename PRED_T>
std::tuple<std::shared_ptr<IContextColumn>, std::shared_ptr<IContextColumn>,
           std::vector<size_t>>
default_single_source_shortest_path_impl(
    const ReadTransaction& txn, const IVertexColumn& input,
    const std::vector<LabelTriplet>& labels, Direction dir, int lower,
    int upper, const PRED_T& pred) {
  label_t label_num = txn.schema().vertex_label_num();
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>> labels_map(
      label_num);
  const auto& input_labels_set = input.get_labels_set();
  std::set<label_t> dest_labels;
  for (auto& triplet : labels) {
    if (!txn.schema().exist(triplet.src_label, triplet.dst_label,
                            triplet.edge_label)) {
      continue;
    }
    if (dir == Direction::kOut || dir == Direction::kBoth) {
      if (input_labels_set.find(triplet.src_label) != input_labels_set.end()) {
        labels_map[triplet.src_label].emplace_back(
            triplet.dst_label, triplet.edge_label, Direction::kOut);
        dest_labels.insert(triplet.dst_label);
      }
    }
    if (dir == Direction::kIn || dir == Direction::kBoth) {
      if (input_labels_set.find(triplet.dst_label) != input_labels_set.end()) {
        labels_map[triplet.dst_label].emplace_back(
            triplet.src_label, triplet.edge_label, Direction::kIn);
        dest_labels.insert(triplet.src_label);
      }
    }
  }
  GeneralPathColumnBuilder path_col_builder;
  std::vector<std::shared_ptr<PathImpl>> path_impls;
  std::vector<size_t> offsets;

  std::shared_ptr<IContextColumn> dest_col(nullptr);
  if (dest_labels.size() == 1) {
    SLVertexColumnBuilder dest_col_builder(*dest_labels.begin());

    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      std::vector<std::pair<label_t, vid_t>> cur;
      std::vector<std::pair<label_t, vid_t>> next;
      cur.emplace_back(label, v);
      std::map<std::pair<label_t, vid_t>, std::pair<label_t, vid_t>> parent;
      int depth = 0;
      while (depth < upper && !cur.empty()) {
        for (auto u : cur) {
          if (depth >= lower && pred(u.first, u.second)) {
            std::vector<std::pair<label_t, vid_t>> path;
            auto x = u;
            while (!(x.first == label && x.second == v)) {
              path.push_back(x);
              x = parent[x];
            }
            path.emplace_back(label, v);
            std::reverse(path.begin(), path.end());

            if (path.size() > 1) {
              auto impl =
                  PathImpl::make_path_impl(path[0].first, path[0].second);
              for (size_t k = 1; k < path.size(); ++k) {
                impl->expand(path[k].first, path[k].second);
              }
              path_col_builder.push_back_opt(Path::make_path(impl));
              path_impls.emplace_back(impl);

              dest_col_builder.push_back_opt(u.second);
              offsets.push_back(idx);
            }
          }

          for (auto& l : labels_map[u.first]) {
            label_t nbr_label = std::get<0>(l);
            auto iter = (std::get<2>(l) == Direction::kOut)
                            ? txn.GetOutEdgeIterator(u.first, u.second,
                                                     nbr_label, std::get<1>(l))
                            : txn.GetInEdgeIterator(u.first, u.second,
                                                    nbr_label, std::get<1>(l));
            while (iter.IsValid()) {
              auto nbr = std::make_pair(nbr_label, iter.GetNeighbor());
              if (parent.find(nbr) == parent.end()) {
                parent[nbr] = u;
                next.push_back(nbr);
              }
              iter.Next();
            }
          }

          ++depth;
          cur.clear();
          std::swap(cur, next);
        }
      }
    });

    dest_col = dest_col_builder.finish();
  } else {
    MLVertexColumnBuilder dest_col_builder;

    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      std::vector<std::pair<label_t, vid_t>> cur;
      std::vector<std::pair<label_t, vid_t>> next;
      cur.emplace_back(label, v);
      std::map<std::pair<label_t, vid_t>, std::pair<label_t, vid_t>> parent;
      int depth = 0;
      while (depth < upper && !cur.empty()) {
        for (auto u : cur) {
          if (depth >= lower && pred(u.first, u.second)) {
            std::vector<std::pair<label_t, vid_t>> path;
            auto x = u;
            while (!(x.first == label && x.second == v)) {
              path.push_back(x);
              x = parent[x];
            }
            path.emplace_back(label, v);
            std::reverse(path.begin(), path.end());

            if (path.size() > 1) {
              auto impl =
                  PathImpl::make_path_impl(path[0].first, path[0].second);
              for (size_t k = 1; k < path.size(); ++k) {
                impl->expand(path[k].first, path[k].second);
              }
              path_col_builder.push_back_opt(Path::make_path(impl));
              path_impls.emplace_back(impl);

              dest_col_builder.push_back_vertex(
                  std::make_pair(u.first, u.second));
              offsets.push_back(idx);
            }
          }

          for (auto& l : labels_map[u.first]) {
            label_t nbr_label = std::get<0>(l);
            auto iter = (std::get<2>(l) == Direction::kOut)
                            ? txn.GetOutEdgeIterator(u.first, u.second,
                                                     nbr_label, std::get<1>(l))
                            : txn.GetInEdgeIterator(u.first, u.second,
                                                    nbr_label, std::get<1>(l));
            while (iter.IsValid()) {
              auto nbr = std::make_pair(nbr_label, iter.GetNeighbor());
              if (parent.find(nbr) == parent.end()) {
                parent[nbr] = u;
                next.push_back(nbr);
              }
              iter.Next();
            }
          }

          ++depth;
          cur.clear();
          std::swap(cur, next);
        }
      }
    });

    dest_col = dest_col_builder.finish();
  }
  path_col_builder.set_path_impls(path_impls);
  return std::make_tuple(dest_col, path_col_builder.finish(),
                         std::move(offsets));
}

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_PATH_EXPAND_H_