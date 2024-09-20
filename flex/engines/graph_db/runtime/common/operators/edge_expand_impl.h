
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

#ifndef RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_IMPL_H_
#define RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_IMPL_H_

#include <memory>
#include <utility>
#include <vector>

#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

namespace gs {
namespace runtime {

template <typename EDATA_T, typename PRED_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_on_graph_view(const GraphView<EDATA_T>& view,
                            const SLVertexColumn& input, label_t nbr_label,
                            label_t e_label, Direction dir,
                            const PRED_T& pred) {
  label_t input_label = input.label();

  SLVertexColumnBuilder builder(nbr_label);
  std::vector<size_t> offsets;
  size_t idx = 0;
  for (auto v : input.vertices()) {
    auto es = view.get_edges(v);
    for (auto& e : es) {
      if (pred(input_label, v, nbr_label, e.neighbor, e_label, dir, e.data)) {
        builder.push_back_opt(e.neighbor);
        offsets.push_back(idx);
      }
    }
    ++idx;
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename EDATA_T, typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_se(const ReadTransaction& txn, const SLVertexColumn& input,
                    label_t nbr_label, label_t edge_label, Direction dir,
                    const PRED_T& pred) {
  // LOG(INFO) << "!!!!!!!!!!!!! hit A";
  label_t input_label = input.label();
  CHECK((dir == Direction::kIn) || (dir == Direction::kOut));
  GraphView<EDATA_T> view = (dir == Direction::kIn)
                                ? txn.GetIncomingGraphView<EDATA_T>(
                                      input_label, nbr_label, edge_label)
                                : txn.GetOutgoingGraphView<EDATA_T>(
                                      input_label, nbr_label, edge_label);
  return expand_vertex_on_graph_view(view, input, nbr_label, edge_label, dir,
                                     pred);
}

template <typename EDATA_T, typename PRED_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_on_graph_view_optional(const GraphView<EDATA_T>& view,
                                     const IVertexColumn& input,
                                     label_t nbr_label, label_t e_label,
                                     Direction dir, const PRED_T& pred) {
  label_t input_label = *input.get_labels_set().begin();
  // LOG(INFO) << "before expand size: " << input.size();
  OptionalSLVertexColumnBuilder builder(nbr_label);
  std::vector<size_t> offsets;
  if (input.is_optional()) {
    const auto& col = dynamic_cast<const OptionalSLVertexColumn&>(input);
    col.foreach_vertex([&](size_t idx, label_t l, vid_t v) {
      if (!input.has_value(idx)) {
        builder.push_back_null();
        offsets.push_back(idx);
        return;
      }
      auto es = view.get_edges(v);
      bool found = false;
      for (auto& e : es) {
        if (pred(input_label, v, nbr_label, e.neighbor, e_label, dir, e.data)) {
          builder.push_back_opt(e.neighbor);
          offsets.push_back(idx);
          found = true;
        }
      }
      if (!found) {
        builder.push_back_null();
        offsets.push_back(idx);
      }
    });
  } else {
    // LOG(INFO) << "input_label: " << (int) input_label << " is not optional";
    const auto& col = dynamic_cast<const SLVertexColumn&>(input);
    col.foreach_vertex([&](size_t idx, label_t l, vid_t v) {
      auto es = view.get_edges(v);
      bool found = false;
      for (auto& e : es) {
        if (pred(input_label, v, nbr_label, e.neighbor, e_label, dir, e.data)) {
          builder.push_back_opt(e.neighbor);
          offsets.push_back(idx);
          found = true;
        }
      }
      if (!found) {
        builder.push_back_null();
        offsets.push_back(idx);
      }
    });
  }

  //  LOG(INFO) << "after expand size: " << offsets.size();

  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename EDATA_T, typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_se_optional(const ReadTransaction& txn,
                             const IVertexColumn& input, label_t nbr_label,
                             label_t edge_label, Direction dir,
                             const PRED_T& pred) {
  // LOG(INFO) << "!!!!!!!!!!!!! hit A";
  label_t input_label = *input.get_labels_set().begin();
  CHECK((dir == Direction::kIn) || (dir == Direction::kOut));
  GraphView<EDATA_T> view = (dir == Direction::kIn)
                                ? txn.GetIncomingGraphView<EDATA_T>(
                                      input_label, nbr_label, edge_label)
                                : txn.GetOutgoingGraphView<EDATA_T>(
                                      input_label, nbr_label, edge_label);
  return expand_vertex_on_graph_view_optional(view, input, nbr_label,
                                              edge_label, dir, pred);
}

template <typename EDATA_T, typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_sp(
    const ReadTransaction& txn, const SLVertexColumn& input,
    const std::vector<std::tuple<label_t, label_t, Direction>>& label_dirs,
    const PRED_T& pred) {
  // LOG(INFO) << "!!!!!!!!!!!!! hit B";
  std::vector<GraphView<EDATA_T>*> views;
  label_t input_label = input.label();
  std::vector<label_t> nbr_labels;
  for (auto& t : label_dirs) {
    label_t nbr_label = std::get<0>(t);
    label_t edge_label = std::get<1>(t);
    Direction dir = std::get<2>(t);
    nbr_labels.push_back(nbr_label);
    if (dir == Direction::kOut) {
      views.emplace_back(new GraphView(txn.GetOutgoingGraphView<EDATA_T>(
          input_label, nbr_label, edge_label)));
    } else {
      CHECK(dir == Direction::kIn);
      views.emplace_back(new GraphView(txn.GetIncomingGraphView<EDATA_T>(
          input_label, nbr_label, edge_label)));
    }
  }

  std::vector<size_t> offsets;
  std::shared_ptr<IContextColumn> col(nullptr);
  bool single_nbr_label = true;
  for (size_t k = 1; k < nbr_labels.size(); ++k) {
    if (nbr_labels[k] != nbr_labels[0]) {
      single_nbr_label = false;
      break;
    }
  }
  if (single_nbr_label) {
    size_t idx = 0;
    SLVertexColumnBuilder builder(nbr_labels[0]);
#if 1
    for (auto v : input.vertices()) {
      size_t csr_idx = 0;
      for (auto csr : views) {
        label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
        label_t edge_label = std::get<1>(label_dirs[csr_idx]);
        Direction dir = std::get<2>(label_dirs[csr_idx]);
        auto es = csr->get_edges(v);
        for (auto& e : es) {
          if (pred(input_label, v, nbr_label, e.neighbor, edge_label, dir,
                   e.data)) {
            builder.push_back_opt(e.neighbor);
            offsets.push_back(idx);
          }
        }
        ++csr_idx;
      }
      ++idx;
    }
#else
#endif
    col = builder.finish();
  } else {
    size_t idx = 0;
#if 0
    MLVertexColumnBuilder builder;
    for (auto v : input.vertices()) {
      size_t csr_idx = 0;
      for (auto csr : views) {
        label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
        label_t edge_label = std::get<1>(label_dirs[csr_idx]);
        Direction dir = std::get<2>(label_dirs[csr_idx]);
        auto es = csr->get_edges(v);
        for (auto& e : es) {
          if (pred(input_label, v, nbr_label, e.neighbor, edge_label, dir,
                   e.data)) {
            builder.push_back_vertex(std::make_pair(nbr_label, e.neighbor));
            offsets.push_back(idx);
          }
        }
        ++csr_idx;
      }
      ++idx;
    }
#else
    // LOG(INFO) << "!!!!!!!!hit";
    MSVertexColumnBuilder builder;
    size_t csr_idx = 0;
    for (auto csr : views) {
      label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
      label_t edge_label = std::get<1>(label_dirs[csr_idx]);
      Direction dir = std::get<2>(label_dirs[csr_idx]);
      idx = 0;
      builder.start_label(nbr_label);
      for (auto v : input.vertices()) {
        auto es = csr->get_edges(v);
        for (auto& e : es) {
          if (pred(input_label, v, nbr_label, e.neighbor, edge_label, dir,
                   e.data)) {
            // builder.push_back_vertex(std::make_pair(nbr_label, e.neighbor));
            builder.push_back_opt(e.neighbor);
            offsets.push_back(idx);
          }
        }
        ++idx;
      }
      ++csr_idx;
    }
#endif
    col = builder.finish();
  }

  for (auto ptr : views) {
    delete ptr;
  }

  return std::make_pair(col, std::move(offsets));
}

template <typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_mp(
    const ReadTransaction& txn, const SLVertexColumn& input,
    const std::vector<std::tuple<label_t, label_t, Direction>>& labels,
    const PRED_T& pred) {
  // LOG(INFO) << "!!!!!!!!!!!!! hit C";
  MLVertexColumnBuilder builder;
  label_t input_label = input.label();
  size_t idx = 0;
  std::vector<size_t> offsets;
  for (auto v : input.vertices()) {
    for (auto& t : labels) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);
      auto it =
          (dir == Direction::kOut)
              ? (txn.GetOutEdgeIterator(input_label, v, nbr_label, edge_label))
              : (txn.GetInEdgeIterator(input_label, v, nbr_label, edge_label));
      while (it.IsValid()) {
        auto nbr = it.GetNeighbor();
        if (pred(input_label, v, nbr_label, nbr, edge_label, dir,
                 it.GetData())) {
          builder.push_back_vertex(std::make_pair(nbr_label, nbr));
          offsets.push_back(idx);
        }
        it.Next();
      }
    }
    ++idx;
  }
  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename EDATA_T, typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_se(
    const ReadTransaction& txn, const MLVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred) {
  // LOG(INFO) << "!!!!!!!!!!!!! hit D";
  int label_num = label_dirs.size();
  std::vector<GraphView<EDATA_T>*> views(label_num, nullptr);
  std::vector<label_t> nbr_labels(label_num,
                                  std::numeric_limits<label_t>::max());
  std::vector<label_t> edge_labels(label_num,
                                   std::numeric_limits<label_t>::max());
  std::vector<Direction> dirs(label_num);
  std::set<label_t> nbr_labels_set;
  bool all_exist = true;
  for (auto i : input.get_labels_set()) {
    if (label_dirs[i].empty()) {
      all_exist = false;
      continue;
    }
    auto& t = label_dirs[i][0];
    label_t nbr_label = std::get<0>(t);
    label_t edge_label = std::get<1>(t);
    Direction dir = std::get<2>(t);
    nbr_labels[i] = nbr_label;
    edge_labels[i] = edge_label;
    dirs[i] = dir;
    nbr_labels_set.insert(nbr_label);
    if (dir == Direction::kOut) {
      views[i] = new GraphView(txn.GetOutgoingGraphView<EDATA_T>(
          static_cast<label_t>(i), nbr_label, edge_label));
    } else {
      CHECK(dir == Direction::kIn);
      views[i] = new GraphView(txn.GetIncomingGraphView<EDATA_T>(
          static_cast<label_t>(i), nbr_label, edge_label));
    }
  }

  std::vector<size_t> offsets;
  std::shared_ptr<IContextColumn> col(nullptr);

  if (nbr_labels_set.size() == 1) {
    SLVertexColumnBuilder builder(*nbr_labels_set.begin());
    if (all_exist) {
      input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
        auto es = views[l]->get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_labels[l], e.neighbor, edge_labels[l], dirs[l],
                   e.data)) {
            builder.push_back_opt(e.neighbor);
            offsets.push_back(idx);
          }
        }
      });
    } else {
      input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
        if (views[l]) {
          auto es = views[l]->get_edges(vid);
          for (auto& e : es) {
            if (pred(l, vid, nbr_labels[l], e.neighbor, edge_labels[l], dirs[l],
                     e.data)) {
              builder.push_back_opt(e.neighbor);
              offsets.push_back(idx);
            }
          }
        }
      });
    }
    col = builder.finish();
  } else {
    MLVertexColumnBuilder builder;
    if (all_exist) {
      input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
        auto es = views[l]->get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_labels[l], e.neighbor, edge_labels[l], dirs[l],
                   e.data)) {
            builder.push_back_vertex(std::make_pair(nbr_labels[l], e.neighbor));
            offsets.push_back(idx);
          }
        }
      });
    } else {
      input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
        if (views[l]) {
          auto es = views[l]->get_edges(vid);
          for (auto& e : es) {
            if (pred(l, vid, nbr_labels[l], e.neighbor, edge_labels[l], dirs[l],
                     e.data)) {
              builder.push_back_vertex(
                  std::make_pair(nbr_labels[l], e.neighbor));
              offsets.push_back(idx);
            }
          }
        }
      });
    }
    col = builder.finish();
  }
  for (auto ptr : views) {
    if (ptr != nullptr) {
      delete ptr;
    }
  }
  return std::make_pair(col, std::move(offsets));
}

template <typename EDATA_T, typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_se(
    const ReadTransaction& txn, const MSVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred) {
  // LOG(INFO) << "!!!!!!!!!!!!! hit D";
  int label_num = label_dirs.size();
  std::vector<GraphView<EDATA_T>*> views(label_num, nullptr);
  std::vector<label_t> nbr_labels(label_num,
                                  std::numeric_limits<label_t>::max());
  std::vector<label_t> edge_labels(label_num,
                                   std::numeric_limits<label_t>::max());
  std::vector<Direction> dirs(label_num);
  std::set<label_t> nbr_labels_set;
  // std::vector<std::pair<label_t, label_t>> nbr_src_label_pair;
  for (auto i : input.get_labels_set()) {
    if (label_dirs[i].empty()) {
      continue;
    }
    auto& t = label_dirs[i][0];
    label_t nbr_label = std::get<0>(t);
    label_t edge_label = std::get<1>(t);
    // nbr_src_label_pair.emplace_back(nbr_label, i);
    Direction dir = std::get<2>(t);
    nbr_labels[i] = nbr_label;
    edge_labels[i] = edge_label;
    dirs[i] = dir;
    nbr_labels_set.insert(nbr_label);
    if (dir == Direction::kOut) {
      views[i] = new GraphView(txn.GetOutgoingGraphView<EDATA_T>(
          static_cast<label_t>(i), nbr_label, edge_label));
    } else {
      CHECK(dir == Direction::kIn);
      views[i] = new GraphView(txn.GetIncomingGraphView<EDATA_T>(
          static_cast<label_t>(i), nbr_label, edge_label));
    }
  }

  std::vector<size_t> offsets;
  std::shared_ptr<IContextColumn> col(nullptr);

  if (nbr_labels_set.size() == 1) {
    SLVertexColumnBuilder builder(*nbr_labels_set.begin());
    size_t input_seg_num = input.seg_num();
    size_t idx = 0;
    for (size_t k = 0; k < input_seg_num; ++k) {
      label_t l = input.seg_label(k);
      auto view = views[l];
      if (view) {
        for (auto vid : input.seg_vertices(k)) {
          auto es = view->get_edges(vid);
          for (auto& e : es) {
            if (pred(l, vid, nbr_labels[l], e.neighbor, edge_labels[l], dirs[l],
                     e.data)) {
              builder.push_back_opt(e.neighbor);
              offsets.push_back(idx);
            }
          }
          ++idx;
        }
      } else {
        idx += input.seg_vertices(k).size();
      }
    }
    col = builder.finish();
  } else {
    size_t idx = 0;
    // std::sort(nbr_src_label_pair.begin(), nbr_src_label_pair.end());
    MSVertexColumnBuilder builder;
    size_t input_seg_num = input.seg_num();
    for (size_t k = 0; k < input_seg_num; ++k) {
      label_t l = input.seg_label(k);
      auto view = views[l];
      if (view) {
        label_t nbr_label = nbr_labels[l];
        builder.start_label(nbr_label);
        for (auto vid : input.seg_vertices(k)) {
          auto es = view->get_edges(vid);
          for (auto& e : es) {
            if (pred(l, vid, nbr_labels[l], e.neighbor, edge_labels[l], dirs[l],
                     e.data)) {
              builder.push_back_opt(e.neighbor);
              offsets.push_back(idx);
            }
          }
          ++idx;
        }
      } else {
        idx += input.seg_vertices(k).size();
      }
    }
    col = builder.finish();
  }
  for (auto ptr : views) {
    if (ptr != nullptr) {
      delete ptr;
    }
  }
  return std::make_pair(col, std::move(offsets));
}

template <typename EDATA_T, typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_sp(
    const ReadTransaction& txn, const MLVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred) {
  // LOG(INFO) << "!!!!!!!!!!!!! hit E";
  int label_num = label_dirs.size();
  std::vector<std::vector<GraphView<EDATA_T>*>> views(label_num);
  std::set<label_t> nbr_labels_set;
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>
      label_dirs_map(label_num);

  for (int i = 0; i < label_num; ++i) {
    for (auto& t : label_dirs[i]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);

      nbr_labels_set.insert(nbr_label);
      if (dir == Direction::kOut) {
        views[i].emplace_back(new GraphView(txn.GetOutgoingGraphView<EDATA_T>(
            static_cast<label_t>(i), nbr_label, edge_label)));
      } else {
        CHECK(dir == Direction::kIn);
        views[i].emplace_back(new GraphView(txn.GetIncomingGraphView<EDATA_T>(
            static_cast<label_t>(i), nbr_label, edge_label)));
      }
      label_dirs_map[i].emplace_back(nbr_label, edge_label, dir);
    }
  }

  std::vector<size_t> offsets;
  std::shared_ptr<IContextColumn> col(nullptr);

  if (nbr_labels_set.size() == 1) {
    SLVertexColumnBuilder builder(*nbr_labels_set.begin());
    input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
      size_t csr_idx = 0;
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(label_dirs_map[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs_map[l][csr_idx]);
        Direction dir = std::get<2>(label_dirs_map[l][csr_idx]);
        auto es = view->get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_label, e.neighbor, edge_label, dir, e.data)) {
            builder.push_back_opt(e.neighbor);
            offsets.push_back(idx);
          }
        }
        ++csr_idx;
      }
    });
    col = builder.finish();
  } else {
    MLVertexColumnBuilder builder;
    input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
      size_t csr_idx = 0;
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(label_dirs_map[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs_map[l][csr_idx]);
        Direction dir = std::get<2>(label_dirs_map[l][csr_idx]);
        auto es = view->get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_label, e.neighbor, edge_label, dir, e.data)) {
            builder.push_back_vertex(std::make_pair(nbr_label, e.neighbor));
            offsets.push_back(idx);
          }
        }
        ++csr_idx;
      }
    });
    col = builder.finish();
  }
  for (auto& vec : views) {
    for (auto ptr : vec) {
      delete ptr;
    }
  }
  return std::make_pair(col, std::move(offsets));
}

template <typename EDATA_T, typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_sp(
    const ReadTransaction& txn, const MSVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred) {
  int label_num = label_dirs.size();
  std::vector<std::vector<GraphView<EDATA_T>*>> views(label_num);
  std::set<label_t> nbr_labels_set;
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>
      label_dirs_map(label_num);

  for (int i = 0; i < label_num; ++i) {
    for (auto& t : label_dirs[i]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);

      nbr_labels_set.insert(nbr_label);
      if (dir == Direction::kOut) {
        views[i].emplace_back(new GraphView(txn.GetOutgoingGraphView<EDATA_T>(
            static_cast<label_t>(i), nbr_label, edge_label)));
      } else {
        CHECK(dir == Direction::kIn);
        views[i].emplace_back(new GraphView(txn.GetIncomingGraphView<EDATA_T>(
            static_cast<label_t>(i), nbr_label, edge_label)));
      }
      label_dirs_map[i].emplace_back(nbr_label, edge_label, dir);
    }
  }

  std::vector<size_t> offsets;
  std::shared_ptr<IContextColumn> col(nullptr);

  if (nbr_labels_set.size() == 1) {
    SLVertexColumnBuilder builder(*nbr_labels_set.begin());
    // not optimized for ms vertex column access
    LOG(INFO) << "not optimized for ms vertex column access";
    input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
      size_t csr_idx = 0;
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(label_dirs_map[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs_map[l][csr_idx]);
        Direction dir = std::get<2>(label_dirs_map[l][csr_idx]);
        auto es = view->get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_label, e.neighbor, edge_label, dir, e.data)) {
            builder.push_back_opt(e.neighbor);
            offsets.push_back(idx);
          }
        }
        ++csr_idx;
      }
    });
    col = builder.finish();
  } else {
    MLVertexColumnBuilder builder;
    // not optimized for ms vertex column access
    LOG(INFO) << "not optimized for ms vertex column access";
    input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
      size_t csr_idx = 0;
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(label_dirs_map[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs_map[l][csr_idx]);
        Direction dir = std::get<2>(label_dirs_map[l][csr_idx]);
        auto es = view->get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_label, e.neighbor, edge_label, dir, e.data)) {
            builder.push_back_vertex(std::make_pair(nbr_label, e.neighbor));
            offsets.push_back(idx);
          }
        }
        ++csr_idx;
      }
    });
    col = builder.finish();
  }
  for (auto& vec : views) {
    for (auto ptr : vec) {
      delete ptr;
    }
  }
  return std::make_pair(col, std::move(offsets));
}

template <typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_mp(
    const ReadTransaction& txn, const MLVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred) {
  // LOG(INFO) << "!!!!!!!!!!!!! hit F";
  MLVertexColumnBuilder builder;
  std::vector<size_t> offsets;
  input.foreach_vertex([&](size_t idx, label_t label, vid_t v) {
    for (auto& t : label_dirs[label]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);
      auto it = (dir == Direction::kOut)
                    ? (txn.GetOutEdgeIterator(label, v, nbr_label, edge_label))
                    : (txn.GetInEdgeIterator(label, v, nbr_label, edge_label));
      while (it.IsValid()) {
        auto nbr = it.GetNeighbor();
        if (pred(label, v, nbr_label, nbr, edge_label, dir, it.GetData())) {
          builder.push_back_vertex(std::make_pair(nbr_label, nbr));
          offsets.push_back(idx);
        }
        it.Next();
      }
    }
  });
  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_mp(
    const ReadTransaction& txn, const MSVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred) {
  // LOG(INFO) << "!!!!!!!!!!!!! hit F";
  MLVertexColumnBuilder builder;
  std::vector<size_t> offsets;
  // not optimized for ms vertex access
  LOG(INFO) << "not optimized for ms vertex column access";
  input.foreach_vertex([&](size_t idx, label_t label, vid_t v) {
    for (auto& t : label_dirs[label]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);
      auto it = (dir == Direction::kOut)
                    ? (txn.GetOutEdgeIterator(label, v, nbr_label, edge_label))
                    : (txn.GetInEdgeIterator(label, v, nbr_label, edge_label));
      while (it.IsValid()) {
        auto nbr = it.GetNeighbor();
        if (pred(label, v, nbr_label, nbr, edge_label, dir, it.GetData())) {
          builder.push_back_vertex(std::make_pair(nbr_label, nbr));
          offsets.push_back(idx);
        }
        it.Next();
      }
    }
  });
  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename GPRED_T, typename EDATA_T>
struct GPredWrapper {
  GPredWrapper(const GPRED_T& gpred) : gpred_(gpred) {}

  bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr_vid,
                  label_t edge_label, Direction dir, const EDATA_T& ed) const {
    Any edata = AnyConverter<EDATA_T>::to_any(ed);
    if (dir == Direction::kOut) {
      return gpred_(LabelTriplet(v_label, nbr_label, edge_label), v, nbr_vid,
                    edata, Direction::kOut, 0);
    } else {
      return gpred_(LabelTriplet(nbr_label, v_label, edge_label), nbr_vid, v,
                    edata, Direction::kIn, 0);
    }
  }

  const GPRED_T& gpred_;
};

template <typename GPRED_T>
struct GPredWrapper<GPRED_T, Any> {
  GPredWrapper(const GPRED_T& gpred) : gpred_(gpred) {}

  bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr_vid,
                  label_t edge_label, Direction dir, const Any& edata) const {
    if (dir == Direction::kOut) {
      return gpred_(LabelTriplet(v_label, nbr_label, edge_label), v, nbr_vid,
                    edata, Direction::kOut, 0);
    } else {
      return gpred_(LabelTriplet(nbr_label, v_label, edge_label), nbr_vid, v,
                    edata, Direction::kIn, 0);
    }
  }

  const GPRED_T& gpred_;
};

template <typename GPRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_impl(const ReadTransaction& txn, const SLVertexColumn& input,
                   const std::vector<LabelTriplet>& labels, Direction dir,
                   const GPRED_T& gpred) {
  label_t input_label = input.label();
  std::vector<std::tuple<label_t, label_t, Direction>> label_dirs;
  std::vector<PropertyType> ed_types;
  for (auto& triplet : labels) {
    if (!txn.schema().exist(triplet.src_label, triplet.dst_label,
                            triplet.edge_label)) {
      continue;
    }
    if (triplet.src_label == input_label &&
        ((dir == Direction::kOut) || (dir == Direction::kBoth))) {
      label_dirs.emplace_back(triplet.dst_label, triplet.edge_label,
                              Direction::kOut);
      const auto& properties = txn.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      if (properties.empty()) {
        ed_types.push_back(PropertyType::Empty());
      } else {
        CHECK_EQ(properties.size(), 1);
        ed_types.push_back(properties[0]);
      }
    }
    if (triplet.dst_label == input_label &&
        ((dir == Direction::kIn) || (dir == Direction::kBoth))) {
      label_dirs.emplace_back(triplet.src_label, triplet.edge_label,
                              Direction::kIn);
      const auto& properties = txn.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      if (properties.empty()) {
        ed_types.push_back(PropertyType::Empty());
      } else {
        CHECK_EQ(properties.size(), 1);
        ed_types.push_back(properties[0]);
      }
    }
  }
  grape::DistinctSort(label_dirs);
  bool se = (label_dirs.size() == 1);
  bool sp = true;
  if (!se) {
    for (size_t k = 1; k < ed_types.size(); ++k) {
      if (ed_types[k] != ed_types[0]) {
        sp = false;
        break;
      }
    }
  }
  if (sp) {
    const PropertyType& ed_type = ed_types[0];
    if (ed_type == PropertyType::Empty()) {
      if (se) {
        return expand_vertex_np_se<grape::EmptyType,
                                   GPredWrapper<GPRED_T, grape::EmptyType>>(
            txn, input, std::get<0>(label_dirs[0]), std::get<1>(label_dirs[0]),
            std::get<2>(label_dirs[0]),
            GPredWrapper<GPRED_T, grape::EmptyType>(gpred));
      } else {
        return expand_vertex_np_me_sp<grape::EmptyType,
                                      GPredWrapper<GPRED_T, grape::EmptyType>>(
            txn, input, label_dirs,
            GPredWrapper<GPRED_T, grape::EmptyType>(gpred));
      }
    } else if (ed_type == PropertyType::Int32()) {
      if (se) {
        return expand_vertex_np_se<int, GPredWrapper<GPRED_T, int>>(
            txn, input, std::get<0>(label_dirs[0]), std::get<1>(label_dirs[0]),
            std::get<2>(label_dirs[0]), GPredWrapper<GPRED_T, int>(gpred));
      } else {
        return expand_vertex_np_me_sp<int, GPredWrapper<GPRED_T, int>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, int>(gpred));
      }
    } else if (ed_type == PropertyType::Int64()) {
      if (se) {
        return expand_vertex_np_se<int64_t, GPredWrapper<GPRED_T, int64_t>>(
            txn, input, std::get<0>(label_dirs[0]), std::get<1>(label_dirs[0]),
            std::get<2>(label_dirs[0]), GPredWrapper<GPRED_T, int64_t>(gpred));
      } else {
        return expand_vertex_np_me_sp<int64_t, GPredWrapper<GPRED_T, int64_t>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, int64_t>(gpred));
      }
    } else if (ed_type == PropertyType::Date()) {
      if (se) {
        return expand_vertex_np_se<Date, GPredWrapper<GPRED_T, Date>>(
            txn, input, std::get<0>(label_dirs[0]), std::get<1>(label_dirs[0]),
            std::get<2>(label_dirs[0]), GPredWrapper<GPRED_T, Date>(gpred));
      } else {
        return expand_vertex_np_me_sp<Date, GPredWrapper<GPRED_T, Date>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, Date>(gpred));
      }
    } else {
      LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
    }
  } else {
    LOG(INFO)
        << "different edge property type in an edge(vertex) expand, fallback";
  }
  return expand_vertex_np_me_mp<GPredWrapper<GPRED_T, Any>>(
      txn, input, label_dirs, GPredWrapper<GPRED_T, Any>(gpred));
}

template <typename GPRED_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_impl(const ReadTransaction& txn, const MLVertexColumn& input,
                   const std::vector<LabelTriplet>& labels, Direction dir,
                   const GPRED_T& gpred) {
  const std::set<label_t>& input_labels = input.get_labels_set();
  int label_num = txn.schema().vertex_label_num();
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>> label_dirs(
      label_num);
  std::vector<PropertyType> ed_types;
  for (auto& triplet : labels) {
    if (!txn.schema().exist(triplet.src_label, triplet.dst_label,
                            triplet.edge_label)) {
      continue;
    }
    if ((input_labels.find(triplet.src_label) != input_labels.end()) &&
        ((dir == Direction::kOut) || (dir == Direction::kBoth))) {
      label_dirs[triplet.src_label].emplace_back(
          triplet.dst_label, triplet.edge_label, Direction::kOut);
      const auto& properties = txn.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      if (properties.empty()) {
        ed_types.push_back(PropertyType::Empty());
      } else {
        CHECK_EQ(properties.size(), 1);
        ed_types.push_back(properties[0]);
      }
    }
    if ((input_labels.find(triplet.dst_label) != input_labels.end()) &&
        ((dir == Direction::kIn) || (dir == Direction::kBoth))) {
      label_dirs[triplet.dst_label].emplace_back(
          triplet.src_label, triplet.edge_label, Direction::kIn);
      const auto& properties = txn.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      if (properties.empty()) {
        ed_types.push_back(PropertyType::Empty());
      } else {
        CHECK_EQ(properties.size(), 1);
        ed_types.push_back(properties[0]);
      }
    }
  }
  bool se = true;
  for (auto& vec : label_dirs) {
    grape::DistinctSort(vec);
    if (vec.size() > 1) {
      se = false;
    }
  }
  bool sp = true;
  for (size_t k = 1; k < ed_types.size(); ++k) {
    if (ed_types[k] != ed_types[0]) {
      sp = false;
      break;
    }
  }
  if (sp) {
    const PropertyType& ed_type = ed_types[0];
    if (ed_type == PropertyType::Empty()) {
      if (se) {
        return expand_vertex_np_se<grape::EmptyType,
                                   GPredWrapper<GPRED_T, grape::EmptyType>>(
            txn, input, label_dirs,
            GPredWrapper<GPRED_T, grape::EmptyType>(gpred));
      } else {
        return expand_vertex_np_me_sp<grape::EmptyType,
                                      GPredWrapper<GPRED_T, grape::EmptyType>>(
            txn, input, label_dirs,
            GPredWrapper<GPRED_T, grape::EmptyType>(gpred));
      }
    } else if (ed_type == PropertyType::Int32()) {
      if (se) {
        return expand_vertex_np_se<int, GPredWrapper<GPRED_T, int>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, int>(gpred));
      } else {
        return expand_vertex_np_me_sp<int, GPredWrapper<GPRED_T, int>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, int>(gpred));
      }
    } else if (ed_type == PropertyType::Int64()) {
      if (se) {
        return expand_vertex_np_se<int64_t, GPredWrapper<GPRED_T, int64_t>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, int64_t>(gpred));
      } else {
        return expand_vertex_np_me_sp<int64_t, GPredWrapper<GPRED_T, int64_t>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, int64_t>(gpred));
      }
    } else if (ed_type == PropertyType::Date()) {
      if (se) {
        return expand_vertex_np_se<Date, GPredWrapper<GPRED_T, Date>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, Date>(gpred));
      } else {
        return expand_vertex_np_me_sp<Date, GPredWrapper<GPRED_T, Date>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, Date>(gpred));
      }
    } else {
      LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
    }
  } else {
    LOG(INFO)
        << "different edge property type in an edge(vertex) expand, fallback";
  }
  return expand_vertex_np_me_mp<GPredWrapper<GPRED_T, Any>>(
      txn, input, label_dirs, GPredWrapper<GPRED_T, Any>(gpred));
}

template <typename GPRED_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_impl(const ReadTransaction& txn, const MSVertexColumn& input,
                   const std::vector<LabelTriplet>& labels, Direction dir,
                   const GPRED_T& gpred) {
  const std::set<label_t>& input_labels = input.get_labels_set();
  int label_num = txn.schema().vertex_label_num();
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>> label_dirs(
      label_num);
  std::vector<PropertyType> ed_types;
  for (auto& triplet : labels) {
    if (!txn.schema().exist(triplet.src_label, triplet.dst_label,
                            triplet.edge_label)) {
      continue;
    }
    if ((input_labels.find(triplet.src_label) != input_labels.end()) &&
        ((dir == Direction::kOut) || (dir == Direction::kBoth))) {
      label_dirs[triplet.src_label].emplace_back(
          triplet.dst_label, triplet.edge_label, Direction::kOut);
      const auto& properties = txn.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      if (properties.empty()) {
        ed_types.push_back(PropertyType::Empty());
      } else {
        CHECK_EQ(properties.size(), 1);
        ed_types.push_back(properties[0]);
      }
    }
    if ((input_labels.find(triplet.dst_label) != input_labels.end()) &&
        ((dir == Direction::kIn) || (dir == Direction::kBoth))) {
      label_dirs[triplet.dst_label].emplace_back(
          triplet.src_label, triplet.edge_label, Direction::kIn);
      const auto& properties = txn.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      if (properties.empty()) {
        ed_types.push_back(PropertyType::Empty());
      } else {
        CHECK_EQ(properties.size(), 1);
        ed_types.push_back(properties[0]);
      }
    }
  }
  bool se = true;
  for (auto& vec : label_dirs) {
    grape::DistinctSort(vec);
    if (vec.size() > 1) {
      se = false;
    }
  }
  bool sp = true;
  for (size_t k = 1; k < ed_types.size(); ++k) {
    if (ed_types[k] != ed_types[0]) {
      sp = false;
      break;
    }
  }
  if (sp) {
    const PropertyType& ed_type = ed_types[0];
    if (ed_type == PropertyType::Empty()) {
      if (se) {
        return expand_vertex_np_se<grape::EmptyType,
                                   GPredWrapper<GPRED_T, grape::EmptyType>>(
            txn, input, label_dirs,
            GPredWrapper<GPRED_T, grape::EmptyType>(gpred));
      } else {
        return expand_vertex_np_me_sp<grape::EmptyType,
                                      GPredWrapper<GPRED_T, grape::EmptyType>>(
            txn, input, label_dirs,
            GPredWrapper<GPRED_T, grape::EmptyType>(gpred));
      }
    } else if (ed_type == PropertyType::Int32()) {
      if (se) {
        return expand_vertex_np_se<int, GPredWrapper<GPRED_T, int>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, int>(gpred));
      } else {
        return expand_vertex_np_me_sp<int, GPredWrapper<GPRED_T, int>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, int>(gpred));
      }
    } else if (ed_type == PropertyType::Int64()) {
      if (se) {
        return expand_vertex_np_se<int64_t, GPredWrapper<GPRED_T, int64_t>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, int64_t>(gpred));
      } else {
        return expand_vertex_np_me_sp<int64_t, GPredWrapper<GPRED_T, int64_t>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, int64_t>(gpred));
      }
    } else if (ed_type == PropertyType::Date()) {
      if (se) {
        return expand_vertex_np_se<Date, GPredWrapper<GPRED_T, Date>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, Date>(gpred));
      } else {
        return expand_vertex_np_me_sp<Date, GPredWrapper<GPRED_T, Date>>(
            txn, input, label_dirs, GPredWrapper<GPRED_T, Date>(gpred));
      }
    } else {
      LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
    }
  } else {
    LOG(INFO)
        << "different edge property type in an edge(vertex) expand, fallback";
  }
  return expand_vertex_np_me_mp<GPredWrapper<GPRED_T, Any>>(
      txn, input, label_dirs, GPredWrapper<GPRED_T, Any>(gpred));
}

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_without_predicate_impl(const ReadTransaction& txn,
                                     const SLVertexColumn& input,
                                     const std::vector<LabelTriplet>& labels,
                                     Direction dir);

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_without_predicate_optional_impl(
    const ReadTransaction& txn, const IVertexColumn& input,
    const std::vector<LabelTriplet>& labels, Direction dir);

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_without_predicate_impl(const ReadTransaction& txn,
                                     const MLVertexColumn& input,
                                     const std::vector<LabelTriplet>& labels,
                                     Direction dir);

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_without_predicate_impl(const ReadTransaction& txn,
                                     const MSVertexColumn& input,
                                     const std::vector<LabelTriplet>& labels,
                                     Direction dir);

template <typename EDATA_T, typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_edge_ep_se(const ReadTransaction& txn, const SLVertexColumn& input,
                  label_t nbr_label, label_t edge_label, Direction dir,
                  const PropertyType& prop_type, const PRED_T& pred) {
  label_t input_label = input.label();
  CHECK((dir == Direction::kIn) || (dir == Direction::kOut));
  LabelTriplet triplet(dir == Direction::kIn ? nbr_label : input_label,
                       dir == Direction::kIn ? input_label : nbr_label,
                       edge_label);
  SDSLEdgeColumnBuilderBeta<EDATA_T> builder(dir, triplet, prop_type);
  std::vector<size_t> offsets;
  if (dir == Direction::kIn) {
    GraphView<EDATA_T> view =
        txn.GetIncomingGraphView<EDATA_T>(input_label, nbr_label, edge_label);
    size_t idx = 0;
    for (auto v : input.vertices()) {
      auto es = view.get_edges(v);
      for (auto& e : es) {
        Any edata = AnyConverter<EDATA_T>::to_any(e.data);
        if (pred(triplet, e.neighbor, v, edata, dir, idx)) {
          builder.push_back_opt(e.neighbor, v, e.data);
          offsets.push_back(idx);
        }
      }
      ++idx;
    }
  } else {
    CHECK(dir == Direction::kOut);
    GraphView<EDATA_T> view =
        txn.GetOutgoingGraphView<EDATA_T>(input_label, nbr_label, edge_label);
    size_t idx = 0;
    for (auto v : input.vertices()) {
      auto es = view.get_edges(v);
      for (auto& e : es) {
        Any edata = AnyConverter<EDATA_T>::to_any(e.data);
        if (pred(triplet, v, e.neighbor, edata, dir, idx)) {
          builder.push_back_opt(v, e.neighbor, e.data);
          offsets.push_back(idx);
        }
      }
      ++idx;
    }
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename PRED_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_edge_impl(const ReadTransaction& txn, const SLVertexColumn& input,
                 const LabelTriplet& triplet, const PRED_T& pred,
                 Direction dir) {
  label_t input_label = input.label();
  std::tuple<label_t, label_t, Direction> label_dir;
  CHECK(txn.schema().exist(triplet.src_label, triplet.dst_label,
                           triplet.edge_label));
  if (dir == Direction::kOut) {
    CHECK(triplet.src_label == input_label);
    std::get<0>(label_dir) = triplet.dst_label;
  } else {
    CHECK(dir == Direction::kIn);
    CHECK(triplet.dst_label == input_label);
    std::get<0>(label_dir) = triplet.src_label;
  }
  std::get<1>(label_dir) = triplet.edge_label;
  std::get<2>(label_dir) = dir;

  const auto& properties = txn.schema().get_edge_properties(
      triplet.src_label, triplet.dst_label, triplet.edge_label);
  if (properties.empty()) {
    return expand_edge_ep_se<grape::EmptyType, PRED_T>(
        txn, input, std::get<0>(label_dir), std::get<1>(label_dir),
        std::get<2>(label_dir), PropertyType::Empty(), pred);
  } else if (properties.size() == 1) {
    const PropertyType& ed_type = properties[0];
    if (ed_type == PropertyType::Int32()) {
      return expand_edge_ep_se<int, PRED_T>(
          txn, input, std::get<0>(label_dir), std::get<1>(label_dir),
          std::get<2>(label_dir), ed_type, pred);
    } else if (ed_type == PropertyType::Int64()) {
      return expand_edge_ep_se<int64_t, PRED_T>(
          txn, input, std::get<0>(label_dir), std::get<1>(label_dir),
          std::get<2>(label_dir), ed_type, pred);
    } else if (ed_type == PropertyType::Date()) {
      return expand_edge_ep_se<Date, PRED_T>(
          txn, input, std::get<0>(label_dir), std::get<1>(label_dir),
          std::get<2>(label_dir), ed_type, pred);
    } else {
      LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
    }
  } else {
    LOG(INFO) << "multiple properties...";
  }
  std::shared_ptr<IContextColumn> col(nullptr);
  std::vector<size_t> offsets;
  return std::make_pair(col, offsets);
}

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_