
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

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_PATH_EXPAND_H_