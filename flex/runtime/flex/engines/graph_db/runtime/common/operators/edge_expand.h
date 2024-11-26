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

#ifndef RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_
#define RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_

#include <set>

#include "flex/engines/graph_db/runtime/adhoc/operators/special_predicates.h"
#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/operators/edge_expand_impl.h"

#include "glog/logging.h"

namespace gs {
namespace runtime {

struct EdgeExpandParams {
  int v_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
  bool is_optional;
};

class OprTimer;

class EdgeExpand {
 public:
  template <typename PRED_T>
  static Context expand_edge(const GraphReadInterface& graph, Context&& ctx,
                             const EdgeExpandParams& params,
                             const PRED_T& pred) {
    if (params.is_optional) {
      LOG(FATAL) << "not support optional edge expand";
    }
    std::vector<size_t> shuffle_offset;
    std::shared_ptr<IVertexColumn> input_vertex_list_ptr =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    VertexColumnType input_vertex_list_type =
        input_vertex_list_ptr->vertex_column_type();
    if (params.labels.size() == 1) {
      if (input_vertex_list_type == VertexColumnType::kSingle) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list_ptr);
        auto pair =
            expand_edge_impl<PRED_T>(graph, *casted_input_vertex_list,
                                     params.labels[0], pred, params.dir);
        if (pair.first != nullptr) {
          ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
          return ctx;
        }
      }
      LOG(INFO) << "not hit, fallback";
      if (params.dir == Direction::kIn) {
        auto& input_vertex_list = *input_vertex_list_ptr;
        label_t output_vertex_label = params.labels[0].src_label;
        label_t edge_label = params.labels[0].edge_label;

        auto& props = graph.schema().get_edge_properties(
            params.labels[0].src_label, params.labels[0].dst_label,
            params.labels[0].edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }
        if (props.size() > 1) {
          pt = PropertyType::kRecordView;
        }

        SDSLEdgeColumnBuilder builder(Direction::kIn, params.labels[0], pt,
                                      props);

        foreach_vertex(input_vertex_list,
                       [&](size_t index, label_t label, vid_t v) {
                         auto ie_iter = graph.GetInEdgeIterator(
                             label, v, output_vertex_label, edge_label);
                         while (ie_iter.IsValid()) {
                           auto nbr = ie_iter.GetNeighbor();
                           if (pred(params.labels[0], nbr, v, ie_iter.GetData(),
                                    Direction::kIn, index)) {
                             assert(ie_iter.GetData().type == pt);
                             builder.push_back_opt(nbr, v, ie_iter.GetData());
                             shuffle_offset.push_back(index);
                           }
                           ie_iter.Next();
                         }
                       });

        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      } else if (params.dir == Direction::kOut) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        label_t output_vertex_label = params.labels[0].dst_label;
        label_t edge_label = params.labels[0].edge_label;
        label_t src_label = params.labels[0].src_label;

        auto& props = graph.schema().get_edge_properties(
            params.labels[0].src_label, params.labels[0].dst_label,
            params.labels[0].edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }
        if (props.size() > 1) {
          pt = PropertyType::kRecordView;
        }

        SDSLEdgeColumnBuilder builder(Direction::kOut, params.labels[0], pt,
                                      props);

        foreach_vertex(input_vertex_list,
                       [&](size_t index, label_t label, vid_t v) {
                         if (label != src_label) {
                           return;
                         }
                         auto oe_iter = graph.GetOutEdgeIterator(
                             label, v, output_vertex_label, edge_label);
                         while (oe_iter.IsValid()) {
                           auto nbr = oe_iter.GetNeighbor();
                           if (pred(params.labels[0], v, nbr, oe_iter.GetData(),
                                    Direction::kOut, index)) {
                             assert(oe_iter.GetData().type == pt);
                             builder.push_back_opt(v, nbr, oe_iter.GetData());
                             shuffle_offset.push_back(index);
                           }
                           oe_iter.Next();
                         }
                       });

        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      } else {
        LOG(FATAL) << "expand edge both";
      }
    } else {
      LOG(INFO) << "not hit, fallback";
      if (params.dir == Direction::kBoth) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
        for (auto& triplet : params.labels) {
          auto& props = graph.schema().get_edge_properties(
              triplet.src_label, triplet.dst_label, triplet.edge_label);
          PropertyType pt = PropertyType::kEmpty;
          if (!props.empty()) {
            pt = props[0];
          }
          label_props.emplace_back(triplet, pt);
        }
        BDMLEdgeColumnBuilder builder(label_props);

        foreach_vertex(
            input_vertex_list, [&](size_t index, label_t label, vid_t v) {
              for (auto& label_prop : label_props) {
                auto& triplet = label_prop.first;
                if (label == triplet.src_label) {
                  auto oe_iter = graph.GetOutEdgeIterator(
                      label, v, triplet.dst_label, triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    if (pred(triplet, v, nbr, oe_iter.GetData(),
                             Direction::kOut, index)) {
                      assert(oe_iter.GetData().type == label_prop.second);
                      builder.push_back_opt(triplet, v, nbr, oe_iter.GetData(),
                                            Direction::kOut);
                      shuffle_offset.push_back(index);
                    }
                    oe_iter.Next();
                  }
                }
                if (label == triplet.dst_label) {
                  auto ie_iter = graph.GetInEdgeIterator(
                      label, v, triplet.src_label, triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    if (pred(triplet, nbr, v, ie_iter.GetData(), Direction::kIn,
                             index)) {
                      assert(ie_iter.GetData().type == label_prop.second);
                      builder.push_back_opt(triplet, nbr, v, ie_iter.GetData(),
                                            Direction::kIn);
                      shuffle_offset.push_back(index);
                    }
                    ie_iter.Next();
                  }
                }
              }
            });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      } else if (params.dir == Direction::kOut) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
        for (auto& triplet : params.labels) {
          auto& props = graph.schema().get_edge_properties(
              triplet.src_label, triplet.dst_label, triplet.edge_label);
          PropertyType pt = PropertyType::kEmpty;
          if (!props.empty()) {
            pt = props[0];
          }
          label_props.emplace_back(triplet, pt);
        }
        SDMLEdgeColumnBuilder builder(Direction::kOut, label_props);

        foreach_vertex(
            input_vertex_list, [&](size_t index, label_t label, vid_t v) {
              for (auto& label_prop : label_props) {
                auto& triplet = label_prop.first;
                if (label != triplet.src_label)
                  continue;
                auto oe_iter = graph.GetOutEdgeIterator(
                    label, v, triplet.dst_label, triplet.edge_label);
                while (oe_iter.IsValid()) {
                  auto nbr = oe_iter.GetNeighbor();
                  if (pred(triplet, v, nbr, oe_iter.GetData(), Direction::kOut,
                           index)) {
                    assert(oe_iter.GetData().type == label_prop.second);
                    builder.push_back_opt(triplet, v, nbr, oe_iter.GetData());
                    shuffle_offset.push_back(index);
                  }
                  oe_iter.Next();
                }
              }
            });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      }
    }
    LOG(FATAL) << "not support";
  }

  static Context expand_edge_with_special_edge_predicate(
      const GraphReadInterface& graph, Context&& ctx,
      const EdgeExpandParams& params, const SPEdgePredicate& pred) {
    if (params.is_optional) {
      LOG(FATAL) << "not support optional edge expand";
    }
    if (pred.data_type() == RTAnyType::kI64Value) {
      if (pred.type() == SPEdgePredicateType::kPropertyGT) {
        return expand_edge<EdgePropertyGTPredicate<int64_t>>(
            graph, std::move(ctx), params,
            dynamic_cast<const EdgePropertyGTPredicate<int64_t>&>(pred));
      } else if (pred.type() == SPEdgePredicateType::kPropertyLT) {
        return expand_edge<EdgePropertyLTPredicate<int64_t>>(
            graph, std::move(ctx), params,
            dynamic_cast<const EdgePropertyLTPredicate<int64_t>&>(pred));
      }
    } else if (pred.data_type() == RTAnyType::kI32Value) {
      if (pred.type() == SPEdgePredicateType::kPropertyGT) {
        return expand_edge<EdgePropertyGTPredicate<int>>(
            graph, std::move(ctx), params,
            dynamic_cast<const EdgePropertyGTPredicate<int>&>(pred));
      } else if (pred.type() == SPEdgePredicateType::kPropertyLT) {
        return expand_edge<EdgePropertyLTPredicate<int>>(
            graph, std::move(ctx), params,
            dynamic_cast<const EdgePropertyLTPredicate<int>&>(pred));
      }
    }
    LOG(FATAL) << "not impl";
    return Context();
  }

  static Context expand_edge_without_predicate(const GraphReadInterface& graph,
                                               Context&& ctx,
                                               const EdgeExpandParams& params,
                                               OprTimer& timer);

  template <typename PRED_T>
  static Context expand_vertex(const GraphReadInterface& graph, Context&& ctx,
                               const EdgeExpandParams& params,
                               const PRED_T& pred) {
    if (params.is_optional) {
      LOG(FATAL) << "not support optional edge expand";
    }
    std::shared_ptr<IVertexColumn> input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    VertexColumnType input_vertex_list_type =
        input_vertex_list->vertex_column_type();

    if (input_vertex_list_type == VertexColumnType::kSingle) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
      auto pair = expand_vertex_impl<PRED_T>(graph, *casted_input_vertex_list,
                                             params.labels, params.dir, pred);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    } else if (input_vertex_list_type == VertexColumnType::kMultiple) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<MLVertexColumn>(input_vertex_list);
      auto pair = expand_vertex_impl<PRED_T>(graph, *casted_input_vertex_list,
                                             params.labels, params.dir, pred);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    } else if (input_vertex_list_type == VertexColumnType::kMultiSegment) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<MSVertexColumn>(input_vertex_list);
      auto pair = expand_vertex_impl<PRED_T>(graph, *casted_input_vertex_list,
                                             params.labels, params.dir, pred);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    } else {
      LOG(FATAL) << "unexpected to reach here...";
      return ctx;
    }
  }

  static Context expand_vertex_ep_lt(const GraphReadInterface& graph,
                                     Context&& ctx,
                                     const EdgeExpandParams& params,
                                     const std::string& ep_val) {
    if (params.is_optional) {
      LOG(FATAL) << "not support optional edge expand";
    }
    std::shared_ptr<IVertexColumn> input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    VertexColumnType input_vertex_list_type =
        input_vertex_list->vertex_column_type();

    if (input_vertex_list_type == VertexColumnType::kSingle) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
      label_t input_label = casted_input_vertex_list->label();
      std::vector<std::tuple<label_t, label_t, Direction>> label_dirs;
      std::vector<PropertyType> ed_types;
      for (auto& triplet : params.labels) {
        if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                                  triplet.edge_label)) {
          continue;
        }
        if (triplet.src_label == input_label &&
            ((params.dir == Direction::kOut) ||
             (params.dir == Direction::kBoth))) {
          label_dirs.emplace_back(triplet.dst_label, triplet.edge_label,
                                  Direction::kOut);
          const auto& properties = graph.schema().get_edge_properties(
              triplet.src_label, triplet.dst_label, triplet.edge_label);
          if (properties.empty()) {
            ed_types.push_back(PropertyType::Empty());
          } else {
            CHECK_EQ(properties.size(), 1);
            ed_types.push_back(properties[0]);
          }
        }
        if (triplet.dst_label == input_label &&
            ((params.dir == Direction::kIn) ||
             (params.dir == Direction::kBoth))) {
          label_dirs.emplace_back(triplet.src_label, triplet.edge_label,
                                  Direction::kIn);
          const auto& properties = graph.schema().get_edge_properties(
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
      if (!sp) {
        LOG(FATAL) << "not support multiple edge types";
      }
      const PropertyType& ed_type = ed_types[0];
      if (ed_type == PropertyType::Date()) {
        Date max_value(std::stoll(ep_val));
        std::vector<GraphReadInterface::graph_view_t<Date>> views;
        for (auto& t : label_dirs) {
          label_t nbr_label = std::get<0>(t);
          label_t edge_label = std::get<1>(t);
          Direction dir = std::get<2>(t);
          if (dir == Direction::kOut) {
            views.emplace_back(graph.GetOutgoingGraphView<Date>(
                input_label, nbr_label, edge_label));
          } else {
            CHECK(dir == Direction::kIn);
            views.emplace_back(graph.GetIncomingGraphView<Date>(
                input_label, nbr_label, edge_label));
          }
        }
        MSVertexColumnBuilder builder;
        size_t csr_idx = 0;
        std::vector<size_t> offsets;
        for (auto& csr : views) {
          label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
          // label_t edge_label = std::get<1>(label_dirs[csr_idx]);
          // Direction dir = std::get<2>(label_dirs[csr_idx]);
          size_t idx = 0;
          builder.start_label(nbr_label);
          for (auto v : casted_input_vertex_list->vertices()) {
            csr.foreach_edges_lt(v, max_value, [&](vid_t nbr, const Date& e) {
              builder.push_back_opt(nbr);
              offsets.push_back(idx);
            });
            ++idx;
          }
          ++csr_idx;
        }
        std::shared_ptr<IContextColumn> col = builder.finish();
        ctx.set_with_reshuffle(params.alias, col, offsets);
        return ctx;
      } else {
        LOG(FATAL) << "not support edge type";
      }
    } else {
      LOG(FATAL) << "unexpected to reach here...";
      return ctx;
    }
  }
  static Context expand_vertex_ep_gt(const GraphReadInterface& graph,
                                     Context&& ctx,
                                     const EdgeExpandParams& params,
                                     const std::string& ep_val) {
    if (params.is_optional) {
      LOG(FATAL) << "not support optional edge expand";
    }
    std::shared_ptr<IVertexColumn> input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    VertexColumnType input_vertex_list_type =
        input_vertex_list->vertex_column_type();

    if (input_vertex_list_type == VertexColumnType::kSingle) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
      label_t input_label = casted_input_vertex_list->label();
      std::vector<std::tuple<label_t, label_t, Direction>> label_dirs;
      std::vector<PropertyType> ed_types;
      for (auto& triplet : params.labels) {
        if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                                  triplet.edge_label)) {
          continue;
        }
        if (triplet.src_label == input_label &&
            ((params.dir == Direction::kOut) ||
             (params.dir == Direction::kBoth))) {
          label_dirs.emplace_back(triplet.dst_label, triplet.edge_label,
                                  Direction::kOut);
          const auto& properties = graph.schema().get_edge_properties(
              triplet.src_label, triplet.dst_label, triplet.edge_label);
          if (properties.empty()) {
            ed_types.push_back(PropertyType::Empty());
          } else {
            CHECK_EQ(properties.size(), 1);
            ed_types.push_back(properties[0]);
          }
        }
        if (triplet.dst_label == input_label &&
            ((params.dir == Direction::kIn) ||
             (params.dir == Direction::kBoth))) {
          label_dirs.emplace_back(triplet.src_label, triplet.edge_label,
                                  Direction::kIn);
          const auto& properties = graph.schema().get_edge_properties(
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
      if (!sp) {
        LOG(FATAL) << "not support multiple edge types";
      }
      const PropertyType& ed_type = ed_types[0];
      if (se) {
        if (ed_type == PropertyType::Date()) {
          Date max_value(std::stoll(ep_val));
          std::vector<GraphReadInterface::graph_view_t<Date>> views;
          for (auto& t : label_dirs) {
            label_t nbr_label = std::get<0>(t);
            label_t edge_label = std::get<1>(t);
            Direction dir = std::get<2>(t);
            if (dir == Direction::kOut) {
              views.emplace_back(graph.GetOutgoingGraphView<Date>(
                  input_label, nbr_label, edge_label));
            } else {
              CHECK(dir == Direction::kIn);
              views.emplace_back(graph.GetIncomingGraphView<Date>(
                  input_label, nbr_label, edge_label));
            }
          }
          SLVertexColumnBuilder builder(std::get<0>(label_dirs[0]));
          size_t csr_idx = 0;
          std::vector<size_t> offsets;
          for (auto& csr : views) {
            size_t idx = 0;
            for (auto v : casted_input_vertex_list->vertices()) {
              csr.foreach_edges_gt(v, max_value,
                                   [&](vid_t nbr, const Date& val) {
                                     builder.push_back_opt(nbr);
                                     offsets.push_back(idx);
                                   });
              ++idx;
            }
            ++csr_idx;
          }
          std::shared_ptr<IContextColumn> col = builder.finish();
          ctx.set_with_reshuffle(params.alias, col, offsets);
          return ctx;
        } else {
          LOG(FATAL) << "not support edge type";
        }
      } else {
        if (ed_type == PropertyType::Date()) {
          Date max_value(std::stoll(ep_val));
          std::vector<GraphReadInterface::graph_view_t<Date>> views;
          for (auto& t : label_dirs) {
            label_t nbr_label = std::get<0>(t);
            label_t edge_label = std::get<1>(t);
            Direction dir = std::get<2>(t);
            if (dir == Direction::kOut) {
              views.emplace_back(graph.GetOutgoingGraphView<Date>(
                  input_label, nbr_label, edge_label));
            } else {
              CHECK(dir == Direction::kIn);
              views.emplace_back(graph.GetIncomingGraphView<Date>(
                  input_label, nbr_label, edge_label));
            }
          }
          MSVertexColumnBuilder builder;
          size_t csr_idx = 0;
          std::vector<size_t> offsets;
          for (auto& csr : views) {
            label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
            size_t idx = 0;
            builder.start_label(nbr_label);
            LOG(INFO) << "start label: " << static_cast<int>(nbr_label);
            for (auto v : casted_input_vertex_list->vertices()) {
              csr.foreach_edges_gt(v, max_value,
                                   [&](vid_t nbr, const Date& val) {
                                     builder.push_back_opt(nbr);
                                     offsets.push_back(idx);
                                   });
              ++idx;
            }
            ++csr_idx;
          }
          std::shared_ptr<IContextColumn> col = builder.finish();
          ctx.set_with_reshuffle(params.alias, col, offsets);
          return ctx;
        } else {
          LOG(FATAL) << "not support edge type";
        }
      }
    } else {
      LOG(FATAL) << "unexpected to reach here...";
      return ctx;
    }
  }
  template <typename PRED_T>
  struct SPVPWrapper {
    SPVPWrapper(const PRED_T& pred) : pred_(pred) {}

    bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                    const Any& edata, Direction dir, size_t path_idx) const {
      if (dir == Direction::kOut) {
        return pred_(label.dst_label, dst);
      } else {
        return pred_(label.src_label, src);
      }
    }

    const PRED_T& pred_;
  };

  static Context expand_vertex_with_special_vertex_predicate(
      const GraphReadInterface& graph, Context&& ctx,
      const EdgeExpandParams& params, const SPVertexPredicate& pred) {
    if (params.is_optional) {
      LOG(FATAL) << "not support optional edge expand";
    }
    if (pred.type() == SPVertexPredicateType::kIdEQ) {
      return expand_vertex<SPVPWrapper<VertexIdEQPredicateBeta>>(
          graph, std::move(ctx), params,
          SPVPWrapper(dynamic_cast<const VertexIdEQPredicateBeta&>(pred)));
    } else {
      if (pred.data_type() == RTAnyType::kI64Value) {
        if (pred.type() == SPVertexPredicateType::kPropertyLT) {
          return expand_vertex<
              SPVPWrapper<VertexPropertyLTPredicateBeta<int64_t>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(
                  dynamic_cast<const VertexPropertyLTPredicateBeta<int64_t>&>(
                      pred)));
        } else if (pred.type() == SPVertexPredicateType::kPropertyGT) {
          return expand_vertex<
              SPVPWrapper<VertexPropertyGTPredicateBeta<int64_t>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(
                  dynamic_cast<const VertexPropertyGTPredicateBeta<int64_t>&>(
                      pred)));
        } else if (pred.type() == SPVertexPredicateType::kPropertyLE) {
          return expand_vertex<
              SPVPWrapper<VertexPropertyLEPredicateBeta<int64_t>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(
                  dynamic_cast<const VertexPropertyLEPredicateBeta<int64_t>&>(
                      pred)));
        } else if (pred.type() == SPVertexPredicateType::kPropertyBetween) {
          return expand_vertex<
              SPVPWrapper<VertexPropertyBetweenPredicateBeta<int64_t>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(dynamic_cast<
                          const VertexPropertyBetweenPredicateBeta<int64_t>&>(
                  pred)));
        } else {
          CHECK(pred.type() == SPVertexPredicateType::kPropertyEQ);
          return expand_vertex<
              SPVPWrapper<VertexPropertyEQPredicateBeta<int64_t>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(
                  dynamic_cast<const VertexPropertyEQPredicateBeta<int64_t>&>(
                      pred)));
        }
      } else if (pred.data_type() == RTAnyType::kTimestamp) {
        if (pred.type() == SPVertexPredicateType::kPropertyLT) {
          return expand_vertex<
              SPVPWrapper<VertexPropertyLTPredicateBeta<Date>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(
                  dynamic_cast<const VertexPropertyLTPredicateBeta<Date>&>(
                      pred)));
        } else if (pred.type() == SPVertexPredicateType::kPropertyGT) {
          return expand_vertex<
              SPVPWrapper<VertexPropertyGTPredicateBeta<Date>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(
                  dynamic_cast<const VertexPropertyGTPredicateBeta<Date>&>(
                      pred)));
        } else if (pred.type() == SPVertexPredicateType::kPropertyLE) {
          return expand_vertex<
              SPVPWrapper<VertexPropertyLEPredicateBeta<Date>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(
                  dynamic_cast<const VertexPropertyLEPredicateBeta<Date>&>(
                      pred)));
        } else if (pred.type() == SPVertexPredicateType::kPropertyBetween) {
          return expand_vertex<
              SPVPWrapper<VertexPropertyBetweenPredicateBeta<Date>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(
                  dynamic_cast<const VertexPropertyBetweenPredicateBeta<Date>&>(
                      pred)));
        } else {
          CHECK(pred.type() == SPVertexPredicateType::kPropertyEQ);
          return expand_vertex<
              SPVPWrapper<VertexPropertyEQPredicateBeta<Date>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(
                  dynamic_cast<const VertexPropertyEQPredicateBeta<Date>&>(
                      pred)));
        }
      } else if (pred.data_type() == RTAnyType::kStringValue) {
        if (pred.type() == SPVertexPredicateType::kPropertyEQ) {
          return expand_vertex<
              SPVPWrapper<VertexPropertyEQPredicateBeta<std::string_view>>>(
              graph, std::move(ctx), params,
              SPVPWrapper(
                  dynamic_cast<
                      const VertexPropertyEQPredicateBeta<std::string_view>&>(
                      pred)));
        }
      }
    }
    LOG(FATAL) << static_cast<int>(pred.type()) << "not impl";
    return ctx;
  }

  static Context expand_vertex_without_predicate(
      const GraphReadInterface& graph, Context&& ctx,
      const EdgeExpandParams& params);
};

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_