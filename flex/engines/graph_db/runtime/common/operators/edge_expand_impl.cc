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

#include "flex/engines/graph_db/runtime/common/operators/edge_expand_impl.h"

namespace gs {

namespace runtime {

template <typename EDATA_T>
struct DummyPredicate {
  bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr_vid,
                  label_t edge_label, Direction dir, const EDATA_T& ed) const {
    return true;
  }
};

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
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
        return expand_vertex_np_se<grape::EmptyType,
                                   DummyPredicate<grape::EmptyType>>(
            txn, input, std::get<0>(label_dirs[0]), std::get<1>(label_dirs[0]),
            std::get<2>(label_dirs[0]), DummyPredicate<grape::EmptyType>());
      } else {
        return expand_vertex_np_me_sp<grape::EmptyType,
                                      DummyPredicate<grape::EmptyType>>(
            txn, input, label_dirs, DummyPredicate<grape::EmptyType>());
      }
    } else if (ed_type == PropertyType::Int32()) {
      if (se) {
        return expand_vertex_np_se<int, DummyPredicate<int>>(
            txn, input, std::get<0>(label_dirs[0]), std::get<1>(label_dirs[0]),
            std::get<2>(label_dirs[0]), DummyPredicate<int>());
      } else {
        return expand_vertex_np_me_sp<int, DummyPredicate<int>>(
            txn, input, label_dirs, DummyPredicate<int>());
      }
    } else if (ed_type == PropertyType::Int64()) {
      if (se) {
        return expand_vertex_np_se<int64_t, DummyPredicate<int64_t>>(
            txn, input, std::get<0>(label_dirs[0]), std::get<1>(label_dirs[0]),
            std::get<2>(label_dirs[0]), DummyPredicate<int64_t>());
      } else {
        return expand_vertex_np_me_sp<int64_t, DummyPredicate<int64_t>>(
            txn, input, label_dirs, DummyPredicate<int64_t>());
      }
    } else if (ed_type == PropertyType::Date()) {
      if (se) {
        return expand_vertex_np_se<Date, DummyPredicate<Date>>(
            txn, input, std::get<0>(label_dirs[0]), std::get<1>(label_dirs[0]),
            std::get<2>(label_dirs[0]), DummyPredicate<Date>());
      } else {
        return expand_vertex_np_me_sp<Date, DummyPredicate<Date>>(
            txn, input, label_dirs, DummyPredicate<Date>());
      }
    } else {
      LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
    }
  } else {
    LOG(INFO)
        << "different edge property type in an edge(vertex) expand, fallback";
  }
  return expand_vertex_np_me_mp<DummyPredicate<Any>>(txn, input, label_dirs,
                                                     DummyPredicate<Any>());
}

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_without_predicate_impl(const ReadTransaction& txn,
                                     const MLVertexColumn& input,
                                     const std::vector<LabelTriplet>& labels,
                                     Direction dir) {
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
                                   DummyPredicate<grape::EmptyType>>(
            txn, input, label_dirs, DummyPredicate<grape::EmptyType>());
      } else {
        return expand_vertex_np_me_sp<grape::EmptyType,
                                      DummyPredicate<grape::EmptyType>>(
            txn, input, label_dirs, DummyPredicate<grape::EmptyType>());
      }
    } else if (ed_type == PropertyType::Int32()) {
      if (se) {
        return expand_vertex_np_se<int, DummyPredicate<int>>(
            txn, input, label_dirs, DummyPredicate<int>());
      } else {
        return expand_vertex_np_me_sp<int, DummyPredicate<int>>(
            txn, input, label_dirs, DummyPredicate<int>());
      }
    } else if (ed_type == PropertyType::Int64()) {
      if (se) {
        return expand_vertex_np_se<int64_t, DummyPredicate<int64_t>>(
            txn, input, label_dirs, DummyPredicate<int64_t>());
      } else {
        return expand_vertex_np_me_sp<int64_t, DummyPredicate<int64_t>>(
            txn, input, label_dirs, DummyPredicate<int64_t>());
      }
    } else if (ed_type == PropertyType::Date()) {
      if (se) {
        return expand_vertex_np_se<Date, DummyPredicate<Date>>(
            txn, input, label_dirs, DummyPredicate<Date>());
      } else {
        return expand_vertex_np_me_sp<Date, DummyPredicate<Date>>(
            txn, input, label_dirs, DummyPredicate<Date>());
      }
    } else {
      LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
    }
  } else {
    LOG(INFO)
        << "different edge property type in an edge(vertex) expand, fallback";
  }
  return expand_vertex_np_me_mp<DummyPredicate<Any>>(txn, input, label_dirs,
                                                     DummyPredicate<Any>());
}

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_without_predicate_impl(const ReadTransaction& txn,
                                     const MSVertexColumn& input,
                                     const std::vector<LabelTriplet>& labels,
                                     Direction dir) {
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
                                   DummyPredicate<grape::EmptyType>>(
            txn, input, label_dirs, DummyPredicate<grape::EmptyType>());
      } else {
        return expand_vertex_np_me_sp<grape::EmptyType,
                                      DummyPredicate<grape::EmptyType>>(
            txn, input, label_dirs, DummyPredicate<grape::EmptyType>());
      }
    } else if (ed_type == PropertyType::Int32()) {
      if (se) {
        return expand_vertex_np_se<int, DummyPredicate<int>>(
            txn, input, label_dirs, DummyPredicate<int>());
      } else {
        return expand_vertex_np_me_sp<int, DummyPredicate<int>>(
            txn, input, label_dirs, DummyPredicate<int>());
      }
    } else if (ed_type == PropertyType::Int64()) {
      if (se) {
        return expand_vertex_np_se<int64_t, DummyPredicate<int64_t>>(
            txn, input, label_dirs, DummyPredicate<int64_t>());
      } else {
        return expand_vertex_np_me_sp<int64_t, DummyPredicate<int64_t>>(
            txn, input, label_dirs, DummyPredicate<int64_t>());
      }
    } else if (ed_type == PropertyType::Date()) {
      if (se) {
        return expand_vertex_np_se<Date, DummyPredicate<Date>>(
            txn, input, label_dirs, DummyPredicate<Date>());
      } else {
        return expand_vertex_np_me_sp<Date, DummyPredicate<Date>>(
            txn, input, label_dirs, DummyPredicate<Date>());
      }
    } else {
      LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
    }
  } else {
    LOG(INFO)
        << "different edge property type in an edge(vertex) expand, fallback";
  }
  return expand_vertex_np_me_mp<DummyPredicate<Any>>(txn, input, label_dirs,
                                                     DummyPredicate<Any>());
}

}  // namespace runtime

}  // namespace gs