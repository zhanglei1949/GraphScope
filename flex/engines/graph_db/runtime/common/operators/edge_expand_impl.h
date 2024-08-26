
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

#include "flex/engines/graph_db/runtime/common/operators/edge_expand.h"

namespace gs {
namespace runtime {

template <typename EDATA_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_on_graph_view(const GraphView<EDATA_T>& view,
                            const SLVertexColumn& input, label_t nbr_label) {
  SLVertexColumnBuilder builder(nbr_label);
  std::vector<size_t> offsets;
  size_t idx = 0;
  for (auto v : input.vertices()) {
    auto es = view.get_edges(v);
    for (auto& e : es) {
      builder.push_back_opt(e.neighbor);
      offsets.push_back(idx);
    }
    ++idx;
  }
  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename EDATA_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_on_dual_graph_view(const GraphView<EDATA_T>& view0,
                                 const GraphView<EDATA_T>& view1,
                                 const SLVertexColumn& input,
                                 label_t nbr_label) {
  SLVertexColumnBuilder builder(nbr_label);
  std::vector<size_t> offsets;
  size_t idx = 0;
  for (auto v : input.vertices()) {
    auto es0 = view0.get_edges(v);
    for (auto& e : es0) {
      builder.push_back_opt(e.neighbor);
      offsets.push_back(idx);
    }
    auto es1 = view1.get_edges(v);
    for (auto& e : es1) {
      builder.push_back_opt(e.neighbor);
      offsets.push_back(idx);
    }
    ++idx;
  }
  return std::make_pair(builder.finish(), std::move(offsets));
}

inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_si_so_se(const ReadTransaction& txn,
                          const SLVertexColumn& input,
                          const LabelTriplet& label, Direction dir) {
  const auto& properties = txn.schema().get_edge_properties(
      label.src_label, label.dst_label, label.edge_label);
  CHECK_LE(properties.size(), 1);
  if (dir == Direction::kOut) {
    label_t v_label = input.label();
    CHECK(v_label == label.src_label);
    label_t nbr_label = label.dst_label;
    label_t e_label = label.edge_label;

    if (properties.empty() || properties[0] == PropertyType::Empty()) {
      GraphView<grape::EmptyType> view =
          txn.GetOutgoingGraphView<grape::EmptyType>(v_label, nbr_label,
                                                     e_label);
      return expand_vertex_on_graph_view(view, input, nbr_label);
    } else if (properties[0] == PropertyType::Int32()) {
      GraphView<int> view =
          txn.GetOutgoingGraphView<int>(v_label, nbr_label, e_label);
      return expand_vertex_on_graph_view(view, input, nbr_label);
    } else if (properties[0] == PropertyType::Int64()) {
      GraphView<int64_t> view =
          txn.GetOutgoingGraphView<int64_t>(v_label, nbr_label, e_label);
      return expand_vertex_on_graph_view(view, input, nbr_label);
    } else if (properties[0] == PropertyType::Date()) {
      GraphView<Date> view =
          txn.GetOutgoingGraphView<Date>(v_label, nbr_label, e_label);
      return expand_vertex_on_graph_view(view, input, nbr_label);
    } else {
      LOG(INFO) << "type - " << properties[0] << " not implemented, fallback";
      SLVertexColumnBuilder builder(nbr_label);
      std::vector<size_t> offsets;
      size_t idx = 0;
      for (auto v : input.vertices()) {
        auto it = txn.GetOutEdgeIterator(v_label, v, nbr_label, e_label);
        while (it.IsValid()) {
          auto nbr = it.GetNeighbor();
          builder.push_back_opt(nbr);
          offsets.push_back(idx);
          it.Next();
        }
        ++idx;
      }
      return std::make_pair(builder.finish(), std::move(offsets));
    }
  } else if (dir == Direction::kIn) {
    label_t v_label = input.label();
    CHECK(v_label == label.dst_label);
    label_t nbr_label = label.src_label;
    label_t e_label = label.edge_label;

    if (properties.empty() || properties[0] == PropertyType::Empty()) {
      GraphView<grape::EmptyType> view =
          txn.GetIncomingGraphView<grape::EmptyType>(v_label, nbr_label,
                                                     e_label);
      return expand_vertex_on_graph_view(view, input, nbr_label);
    } else if (properties[0] == PropertyType::Int32()) {
      GraphView<int> view =
          txn.GetIncomingGraphView<int>(v_label, nbr_label, e_label);
      return expand_vertex_on_graph_view(view, input, nbr_label);
    } else if (properties[0] == PropertyType::Int64()) {
      GraphView<int64_t> view =
          txn.GetIncomingGraphView<int64_t>(v_label, nbr_label, e_label);
      return expand_vertex_on_graph_view(view, input, nbr_label);
    } else if (properties[0] == PropertyType::Date()) {
      GraphView<Date> view =
          txn.GetIncomingGraphView<Date>(v_label, nbr_label, e_label);
      return expand_vertex_on_graph_view(view, input, nbr_label);
    } else {
      LOG(INFO) << "type - " << properties[0] << " not implemented, fallback";
      SLVertexColumnBuilder builder(nbr_label);
      std::vector<size_t> offsets;
      size_t idx = 0;
      for (auto v : input.vertices()) {
        auto it = txn.GetInEdgeIterator(v_label, v, nbr_label, e_label);
        while (it.IsValid()) {
          auto nbr = it.GetNeighbor();
          builder.push_back_opt(nbr);
          offsets.push_back(idx);
          it.Next();
        }
        ++idx;
      }
      return std::make_pair(builder.finish(), std::move(offsets));
    }
  } else if (dir == Direction::kBoth) {
    label_t v_label = input.label();
    CHECK(v_label == label.src_label);
    CHECK(v_label == label.dst_label);
    label_t e_label = label.edge_label;

    if (properties.empty() || properties[0] == PropertyType::Empty()) {
      GraphView<grape::EmptyType> view0 =
          txn.GetIncomingGraphView<grape::EmptyType>(v_label, v_label, e_label);
      GraphView<grape::EmptyType> view1 =
          txn.GetOutgoingGraphView<grape::EmptyType>(v_label, v_label, e_label);
      return expand_vertex_on_dual_graph_view(view0, view1, input, v_label);
    } else if (properties[0] == PropertyType::Int32()) {
      GraphView<int> view0 =
          txn.GetIncomingGraphView<int>(v_label, v_label, e_label);
      GraphView<int> view1 =
          txn.GetOutgoingGraphView<int>(v_label, v_label, e_label);
      return expand_vertex_on_dual_graph_view(view0, view1, input, v_label);
    } else if (properties[0] == PropertyType::Int64()) {
      GraphView<int64_t> view0 =
          txn.GetIncomingGraphView<int64_t>(v_label, v_label, e_label);
      GraphView<int64_t> view1 =
          txn.GetOutgoingGraphView<int64_t>(v_label, v_label, e_label);
      return expand_vertex_on_dual_graph_view(view0, view1, input, v_label);
    } else if (properties[0] == PropertyType::Date()) {
      GraphView<Date> view0 =
          txn.GetIncomingGraphView<Date>(v_label, v_label, e_label);
      GraphView<Date> view1 =
          txn.GetOutgoingGraphView<Date>(v_label, v_label, e_label);
      return expand_vertex_on_dual_graph_view(view0, view1, input, v_label);
    } else {
      LOG(INFO) << "type - " << properties[0] << " not implemented, fallback";
      SLVertexColumnBuilder builder(v_label);
      std::vector<size_t> offsets;
      size_t idx = 0;
      for (auto v : input.vertices()) {
        auto it0 = txn.GetOutEdgeIterator(v_label, v, v_label, e_label);
        while (it0.IsValid()) {
          auto nbr = it0.GetNeighbor();
          builder.push_back_opt(nbr);
          offsets.push_back(idx);
          it0.Next();
        }
        auto it1 = txn.GetInEdgeIterator(v_label, v, v_label, e_label);
        while (it1.IsValid()) {
          auto nbr = it1.GetNeighbor();
          builder.push_back_opt(nbr);
          offsets.push_back(idx);
          it1.Next();
        }
        ++idx;
      }
      return std::make_pair(builder.finish(), std::move(offsets));
    }
  } else {
    LOG(FATAL) << "invalid direction - " << static_cast<int>(dir);
    std::shared_ptr<IContextColumn> ret(nullptr);
    std::vector<size_t> offsets;
    return std::make_pair(ret, offsets);
  }
}

// so: output vertex column is a SLVertexColumn
// se: for each label of input column, only single label edge is iterated
template <typename EDATA_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_mli_so_se_impl(const ReadTransaction& txn,
                                const MLVertexColumn& input,
                                const std::vector<LabelTriplet>& labels,
                                label_t nbr_label, Direction dir) {
  int label_num = txn.schema().vertex_label_num();
  std::vector<size_t> offsets;
  std::vector<GraphView<EDATA_T>*> views(label_num, nullptr);

  if (dir == Direction::kOut) {
    for (auto src_label : input.get_labels_set()) {
      for (auto& triplet : labels) {
        if (triplet.src_label == src_label) {
          views[src_label] = new GraphView(txn.GetOutgoingGraphView<EDATA_T>(
              src_label, triplet.dst_label, triplet.edge_label));
          break;
        }
      }
    }
  } else {
    CHECK(dir == Direction::kIn)
        << "both direction should not enter this function";
    for (auto dst_label : input.get_labels_set()) {
      for (auto& triplet : labels) {
        if (triplet.dst_label == dst_label) {
          views[dst_label] = new GraphView(txn.GetIncomingGraphView<EDATA_T>(
              dst_label, triplet.src_label, triplet.edge_label));
          break;
        }
      }
    }
  }

  SLVertexColumnBuilder builder(nbr_label);

  input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
    if (views[l]) {
      auto es = views[l]->get_edges(vid);
      for (auto& e : es) {
        builder.push_back_opt(e.neighbor);
        offsets.push_back(idx);
      }
    }
  });

  for (auto ptr : views) {
    if (ptr != nullptr) {
      delete ptr;
    }
  }
  return std::make_pair(builder.finish(), std::move(offsets));
}

inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_mli(const ReadTransaction& txn, const MLVertexColumn& input,
                     const std::vector<LabelTriplet>& labels, Direction dir) {
  int label_num = txn.schema().vertex_label_num();
  const auto& input_labels_set = input.get_labels_set();
  if (dir == Direction::kOut || dir == Direction::kIn) {
    std::vector<bool> accessed(label_num, false);
    bool single_csr = true;
    std::vector<PropertyType> ed_types;
    std::set<label_t> nbr_labels;
    if (dir == Direction::kOut) {
      for (auto& triplet : labels) {
        if (txn.schema().exist(triplet.src_label, triplet.dst_label,
                               triplet.edge_label)) {
          if (input_labels_set.count(triplet.src_label)) {
            if (!accessed[triplet.src_label]) {
              accessed[triplet.src_label] = true;
            } else {
              single_csr = false;
            }
            nbr_labels.insert(triplet.dst_label);
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
      }
    } else {
      for (auto& triplet : labels) {
        if (txn.schema().exist(triplet.src_label, triplet.dst_label,
                               triplet.edge_label)) {
          if (input_labels_set.count(triplet.dst_label)) {
            if (!accessed[triplet.dst_label]) {
              accessed[triplet.dst_label] = true;
            } else {
              single_csr = false;
            }
            nbr_labels.insert(triplet.src_label);
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
      }
    }
    bool single_ed_type = true;
    for (size_t k = 1; k < ed_types.size(); ++k) {
      if (ed_types[k] != ed_types[0]) {
        single_ed_type = false;
        break;
      }
    }
    if (single_csr && single_ed_type && nbr_labels.size() == 1) {
      const PropertyType& ed_type = *ed_types.begin();
      label_t nbr_label = *nbr_labels.begin();
      if (ed_type == PropertyType::Empty()) {
        return expand_vertex_np_mli_so_se_impl<grape::EmptyType>(
            txn, input, labels, nbr_label, dir);
      } else if (ed_type == PropertyType::Int32()) {
        return expand_vertex_np_mli_so_se_impl<int>(txn, input, labels,
                                                    nbr_label, dir);
      } else if (ed_type == PropertyType::Int64()) {
        return expand_vertex_np_mli_so_se_impl<int64_t>(txn, input, labels,
                                                        nbr_label, dir);

      } else if (ed_type == PropertyType::Date()) {
        return expand_vertex_np_mli_so_se_impl<Date>(txn, input, labels,
                                                     nbr_label, dir);
      } else {
        LOG(INFO) << "type - " << ed_type << " not implemented, fallback";
      }
    } else {
      LOG(INFO) << "conditions for optimization not match, fallback";
    }
    std::set<label_t> nbr_labels_set;
    std::vector<size_t> offsets;
    if (dir == Direction::kOut) {
      std::vector<std::vector<std::pair<label_t, label_t>>> oe_labels(
          label_num);
      for (auto& triplet : labels) {
        if (txn.schema().exist(triplet.src_label, triplet.dst_label,
                               triplet.edge_label)) {
          if (input_labels_set.count(triplet.src_label)) {
            nbr_labels_set.insert(triplet.dst_label);
            oe_labels[triplet.src_label].emplace_back(triplet.dst_label,
                                                      triplet.edge_label);
          }
        }
      }
      if (nbr_labels_set.size() == 1) {
        SLVertexColumnBuilder builder(*nbr_labels_set.begin());
        input.foreach_vertex([&](size_t idx, label_t l, vid_t v) {
          for (auto& pair : oe_labels[l]) {
            auto it = txn.GetOutEdgeIterator(l, v, pair.first, pair.second);
            while (it.IsValid()) {
              builder.push_back_opt(it.GetNeighbor());
              offsets.push_back(idx);
              it.Next();
            }
          }
        });
        return std::make_pair(builder.finish(), std::move(offsets));
      } else {
        MLVertexColumnBuilder builder;
        input.foreach_vertex([&](size_t idx, label_t l, vid_t v) {
          for (auto& pair : oe_labels[l]) {
            auto it = txn.GetOutEdgeIterator(l, v, pair.first, pair.second);
            while (it.IsValid()) {
              builder.push_back_vertex(
                  std::make_pair(pair.first, it.GetNeighbor()));
              offsets.push_back(idx);
              it.Next();
            }
          }
        });
        return std::make_pair(builder.finish(), std::move(offsets));
      }
    } else {
      std::vector<std::vector<std::pair<label_t, label_t>>> ie_labels(
          label_num);
      for (auto& triplet : labels) {
        if (txn.schema().exist(triplet.src_label, triplet.dst_label,
                               triplet.edge_label)) {
          if (input_labels_set.count(triplet.dst_label)) {
            nbr_labels_set.insert(triplet.src_label);
            ie_labels[triplet.dst_label].emplace_back(triplet.src_label,
                                                      triplet.edge_label);
          }
        }
      }
      if (nbr_labels_set.size() == 1) {
        SLVertexColumnBuilder builder(*nbr_labels_set.begin());
        input.foreach_vertex([&](size_t idx, label_t l, vid_t v) {
          for (auto& pair : ie_labels[l]) {
            auto it = txn.GetInEdgeIterator(l, v, pair.first, pair.second);
            while (it.IsValid()) {
              builder.push_back_opt(it.GetNeighbor());
              offsets.push_back(idx);
              it.Next();
            }
          }
        });
        return std::make_pair(builder.finish(), std::move(offsets));
      } else {
        MLVertexColumnBuilder builder;
        input.foreach_vertex([&](size_t idx, label_t l, vid_t v) {
          for (auto& pair : ie_labels[l]) {
            auto it = txn.GetInEdgeIterator(l, v, pair.first, pair.second);
            while (it.IsValid()) {
              builder.push_back_vertex(
                  std::make_pair(pair.first, it.GetNeighbor()));
              offsets.push_back(idx);
              it.Next();
            }
          }
        });
        return std::make_pair(builder.finish(), std::move(offsets));
      }
    }
  } else {
    LOG(INFO) << "Both direction is not implemented, fallback";
    std::set<label_t> nbr_labels_set;
    std::vector<size_t> offsets;
    std::vector<std::vector<std::pair<label_t, label_t>>> oe_labels(label_num),
        ie_labels(label_num);
    for (auto& triplet : labels) {
      if (txn.schema().exist(triplet.src_label, triplet.dst_label,
                             triplet.edge_label)) {
        if (input_labels_set.count(triplet.src_label)) {
          nbr_labels_set.insert(triplet.dst_label);
          oe_labels[triplet.src_label].emplace_back(triplet.dst_label,
                                                    triplet.edge_label);
        }
        if (input_labels_set.count(triplet.dst_label)) {
          nbr_labels_set.insert(triplet.src_label);
          ie_labels[triplet.dst_label].emplace_back(triplet.src_label,
                                                    triplet.edge_label);
        }
      }
    }
    if (nbr_labels_set.size() == 1) {
      SLVertexColumnBuilder builder(*nbr_labels_set.begin());
      input.foreach_vertex([&](size_t idx, label_t l, vid_t v) {
        for (auto& pair : oe_labels[l]) {
          auto it = txn.GetOutEdgeIterator(l, v, pair.first, pair.second);
          while (it.IsValid()) {
            builder.push_back_opt(it.GetNeighbor());
            offsets.push_back(idx);
            it.Next();
          }
        }
        for (auto& pair : ie_labels[l]) {
          auto it = txn.GetInEdgeIterator(l, v, pair.first, pair.second);
          while (it.IsValid()) {
            builder.push_back_opt(it.GetNeighbor());
            offsets.push_back(idx);
            it.Next();
          }
        }
      });
      return std::make_pair(builder.finish(), std::move(offsets));
    } else {
      MLVertexColumnBuilder builder;
      input.foreach_vertex([&](size_t idx, label_t l, vid_t v) {
        for (auto& pair : oe_labels[l]) {
          auto it = txn.GetOutEdgeIterator(l, v, pair.first, pair.second);
          while (it.IsValid()) {
            builder.push_back_vertex(
                std::make_pair(pair.first, it.GetNeighbor()));
            offsets.push_back(idx);
            it.Next();
          }
        }
        for (auto& pair : ie_labels[l]) {
          auto it = txn.GetInEdgeIterator(l, v, pair.first, pair.second);
          while (it.IsValid()) {
            builder.push_back_vertex(
                std::make_pair(pair.first, it.GetNeighbor()));
            offsets.push_back(idx);
            it.Next();
          }
        }
      });
      return std::make_pair(builder.finish(), std::move(offsets));
    }
  }
}

template <typename EDATA_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_se(const ReadTransaction& txn, const SLVertexColumn& input,
                    label_t nbr_label, label_t edge_label, Direction dir) {
  label_t input_label = input.label();
  std::vector<size_t> offsets;
  SLVertexColumnBuilder builder(nbr_label);
  CHECK((dir == Direction::kIn) || (dir == Direction::kOut));
  GraphView<EDATA_T> view = (dir == Direction::kIn)
                                ? txn.GetIncomingGraphView<EDATA_T>(
                                      input_label, nbr_label, edge_label)
                                : txn.GetOutgoingGraphView<EDATA_T>(
                                      input_label, nbr_label, edge_label);
  size_t idx = 0;
  for (auto v : input.vertices()) {
    auto es = view.get_edges(v);
    for (auto& e : es) {
      builder.push_back_opt(e.neighbor);
      offsets.push_back(idx);
    }
    ++idx;
  }
  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename EDATA_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_sp(
    const ReadTransaction& txn, const SLVertexColumn& input,
    const std::vector<std::tuple<label_t, label_t, Direction>>& label_dirs) {
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
    for (auto v : input.vertices()) {
      for (auto csr : views) {
        auto es = csr->get_edges(v);
        for (auto& e : es) {
          builder.push_back_opt(e.neighbor);
          offsets.push_back(idx);
        }
      }
      ++idx;
    }
    col = builder.finish();
  } else {
    size_t idx = 0;
    MLVertexColumnBuilder builder;
    for (auto v : input.vertices()) {
      const label_t* label_ptr = nbr_labels.data();
      for (auto csr : views) {
        auto es = csr->get_edges(v);
        for (auto& e : es) {
          builder.push_back_vertex(std::make_pair(*label_ptr, e.neighbor));
          offsets.push_back(idx);
        }
        ++label_ptr;
      }
      ++idx;
    }
    col = builder.finish();
  }

  for (auto ptr : views) {
    delete ptr;
  }

  return std::make_pair(col, std::move(offsets));
}

inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_mp(
    const ReadTransaction& txn, const SLVertexColumn& input,
    const std::vector<std::tuple<label_t, label_t, Direction>>& labels) {
  MLVertexColumnBuilder builder;
  label_t input_label = input.label();
  size_t idx = 0;
  std::vector<size_t> offsets;
  for (auto v : input.vertices()) {
    for (auto& t : labels) {
      label_t nbr_label = std::get<0>(t);
      auto it = (std::get<2>(t) == Direction::kOut)
                    ? (txn.GetOutEdgeIterator(input_label, v, nbr_label,
                                              std::get<1>(t)))
                    : (txn.GetInEdgeIterator(input_label, v, nbr_label,
                                             std::get<1>(t)));
      while (it.IsValid()) {
        auto nbr = it.GetNeighbor();
        builder.push_back_vertex(std::make_pair(nbr_label, nbr));
        offsets.push_back(idx);
        it.Next();
      }
    }
    ++idx;
  }
  return std::make_pair(builder.finish(), std::move(offsets));
}

inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_without_predicate_impl(const ReadTransaction& txn,
                                     const SLVertexColumn& input,
                                     const std::vector<LabelTriplet>& labels,
                                     Direction dir) {
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
        return expand_vertex_np_se<grape::EmptyType>(
            txn, input, std::get<0>(label_dirs[0]), std::get<1>(label_dirs[0]),
            std::get<2>(label_dirs[0]));
      } else {
        return expand_vertex_np_me_sp<grape::EmptyType>(txn, input, label_dirs);
      }
    } else if (ed_type == PropertyType::Int32()) {
      if (se) {
        return expand_vertex_np_se<int>(txn, input, std::get<0>(label_dirs[0]),
                                        std::get<1>(label_dirs[0]),
                                        std::get<2>(label_dirs[0]));
      } else {
        return expand_vertex_np_me_sp<int>(txn, input, label_dirs);
      }
    } else if (ed_type == PropertyType::Int64()) {
      if (se) {
        return expand_vertex_np_se<int64_t>(
            txn, input, std::get<0>(label_dirs[0]), std::get<1>(label_dirs[0]),
            std::get<2>(label_dirs[0]));
      } else {
        return expand_vertex_np_me_sp<int64_t>(txn, input, label_dirs);
      }
    } else if (ed_type == PropertyType::Date()) {
      if (se) {
        return expand_vertex_np_se<Date>(txn, input, std::get<0>(label_dirs[0]),
                                         std::get<1>(label_dirs[0]),
                                         std::get<2>(label_dirs[0]));
      } else {
        return expand_vertex_np_me_sp<Date>(txn, input, label_dirs);
      }
    } else {
      LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
    }
  } else {
    LOG(INFO)
        << "different edge property type in an edge(vertex) expand, fallback";
  }
  return expand_vertex_np_me_mp(txn, input, label_dirs);
}

template <typename EDATA_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_se(
    const ReadTransaction& txn, const MLVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs) {
  // double t = -grape::GetCurrentTime();
  int label_num = label_dirs.size();
  std::vector<GraphView<EDATA_T>*> views(label_num, nullptr);
  std::vector<label_t> nbr_labels(label_num,
                                  std::numeric_limits<label_t>::max());
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

  // t += grape::GetCurrentTime();
  // LOG(INFO) << "preprocess2: " << t;

  if (nbr_labels_set.size() == 1) {
    SLVertexColumnBuilder builder(*nbr_labels_set.begin());
    if (all_exist) {
      input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
        auto es = views[l]->get_edges(vid);
        for (auto& e : es) {
          builder.push_back_opt(e.neighbor);
          offsets.push_back(idx);
        }
      });
    } else {
      input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
        if (views[l]) {
          auto es = views[l]->get_edges(vid);
          for (auto& e : es) {
            builder.push_back_opt(e.neighbor);
            offsets.push_back(idx);
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
          builder.push_back_vertex(std::make_pair(nbr_labels[l], e.neighbor));
          offsets.push_back(idx);
        }
      });
    } else {
      input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
        if (views[l]) {
          auto es = views[l]->get_edges(vid);
          for (auto& e : es) {
            builder.push_back_vertex(std::make_pair(nbr_labels[l], e.neighbor));
            offsets.push_back(idx);
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

template <typename EDATA_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_sp(
    const ReadTransaction& txn, const MLVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs) {
  int label_num = label_dirs.size();
  std::vector<std::vector<GraphView<EDATA_T>*>> views(label_num);
  std::set<label_t> nbr_labels_set;

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
    }
  }

  std::vector<size_t> offsets;
  std::shared_ptr<IContextColumn> col(nullptr);

  if (nbr_labels_set.size() == 1) {
    SLVertexColumnBuilder builder(*nbr_labels_set.begin());
    input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
      for (auto& view : views[l]) {
        auto es = view->get_edges(vid);
        for (auto& e : es) {
          builder.push_back_opt(e.neighbor);
          offsets.push_back(idx);
        }
      }
    });
    col = builder.finish();
  } else {
    MLVertexColumnBuilder builder;
    input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
      const auto* label_ptr = label_dirs[l].data();
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(*label_ptr);
        ++label_ptr;
        auto es = view->get_edges(vid);
        for (auto& e : es) {
          builder.push_back_vertex(std::make_pair(nbr_label, e.neighbor));
          offsets.push_back(idx);
        }
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

inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_mp(
    const ReadTransaction& txn, const MLVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs) {
  MLVertexColumnBuilder builder;
  std::vector<size_t> offsets;
  input.foreach_vertex([&](size_t idx, label_t label, vid_t v) {
    for (auto& t : label_dirs[label]) {
      label_t nbr_label = std::get<0>(t);
      auto it =
          (std::get<2>(t) == Direction::kOut)
              ? (txn.GetOutEdgeIterator(label, v, nbr_label, std::get<1>(t)))
              : (txn.GetInEdgeIterator(label, v, nbr_label, std::get<1>(t)));
      while (it.IsValid()) {
        auto nbr = it.GetNeighbor();
        builder.push_back_vertex(std::make_pair(nbr_label, nbr));
        offsets.push_back(idx);
        it.Next();
      }
    }
  });
  return std::make_pair(builder.finish(), std::move(offsets));
}

inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_without_predicate_impl(const ReadTransaction& txn,
                                     const MLVertexColumn& input,
                                     const std::vector<LabelTriplet>& labels,
                                     Direction dir) {
  // double t = -grape::GetCurrentTime();
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
  // t += grape::GetCurrentTime();
  // LOG(INFO) << "preprocess: " << t;
  if (sp) {
    const PropertyType& ed_type = ed_types[0];
    if (ed_type == PropertyType::Empty()) {
      if (se) {
        return expand_vertex_np_se<grape::EmptyType>(txn, input, label_dirs);
      } else {
        return expand_vertex_np_me_sp<grape::EmptyType>(txn, input, label_dirs);
      }
    } else if (ed_type == PropertyType::Int32()) {
      if (se) {
        return expand_vertex_np_se<int>(txn, input, label_dirs);
      } else {
        return expand_vertex_np_me_sp<int>(txn, input, label_dirs);
      }
    } else if (ed_type == PropertyType::Int64()) {
      if (se) {
        return expand_vertex_np_se<int64_t>(txn, input, label_dirs);
      } else {
        return expand_vertex_np_me_sp<int64_t>(txn, input, label_dirs);
      }
    } else if (ed_type == PropertyType::Date()) {
      if (se) {
        return expand_vertex_np_se<Date>(txn, input, label_dirs);
      } else {
        return expand_vertex_np_me_sp<Date>(txn, input, label_dirs);
      }
    } else {
      LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
    }
  } else {
    LOG(INFO)
        << "different edge property type in an edge(vertex) expand, fallback";
  }
  return expand_vertex_np_me_mp(txn, input, label_dirs);
}

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_