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

#include "flex/engines/graph_db/runtime/common/operators/edge_expand.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/special_predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

struct VertexPredicateWrapper {
  VertexPredicateWrapper(const GeneralVertexPredicate& pred) : pred_(pred) {}
  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t path_idx) const {
    if (dir == Direction::kOut) {
      return pred_(label.dst_label, dst, path_idx);
    } else {
      return pred_(label.src_label, src, path_idx);
    }
  }
  const GeneralVertexPredicate& pred_;
};

struct VertexEdgePredicateWrapper {
  VertexEdgePredicateWrapper(const GeneralVertexPredicate& v_pred,
                             const GeneralEdgePredicate& e_pred)
      : v_pred_(v_pred), e_pred_(e_pred) {}

  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t path_idx) const {
    if (dir == Direction::kOut) {
      return v_pred_(label.dst_label, dst, path_idx) &&
             e_pred_(label, src, dst, edata, dir, path_idx);
    } else {
      return v_pred_(label.src_label, src, path_idx) &&
             e_pred_(label, src, dst, edata, dir, path_idx);
    }
  }

  const GeneralVertexPredicate& v_pred_;
  const GeneralEdgePredicate& e_pred_;
};

struct ExactVertexEdgePredicateWrapper {
  ExactVertexEdgePredicateWrapper(const ExactVertexPredicate& v_pred,
                                  const GeneralEdgePredicate& e_pred)
      : v_pred_(v_pred), e_pred_(e_pred) {}

  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t path_idx) const {
    if (dir == Direction::kOut) {
      return v_pred_(label.dst_label, dst, path_idx) &&
             e_pred_(label, src, dst, edata, dir, path_idx);
    } else {
      return v_pred_(label.src_label, src, path_idx) &&
             e_pred_(label, src, dst, edata, dir, path_idx);
    }
  }

  const ExactVertexPredicate& v_pred_;
  const GeneralEdgePredicate& e_pred_;
};

struct ExactVertexPredicateWrapper {
  ExactVertexPredicateWrapper(const ExactVertexPredicate& pred) : pred_(pred) {}

  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t path_idx) const {
    if (dir == Direction::kOut) {
      return pred_(label.dst_label, dst, path_idx);
    } else {
      return pred_(label.src_label, src, path_idx);
    }
  }

  const ExactVertexPredicate& pred_;
};

bool is_ep_gt(const common::Expression& expr) {
  if (expr.operators_size() != 3) {
    return false;
  }
  if (!(expr.operators(0).has_var() &&
        expr.operators(0).var().has_property())) {
    return false;
  }
  if (!(expr.operators(1).item_case() == common::ExprOpr::ItemCase::kLogical &&
        expr.operators(1).logical() == common::Logical::GT)) {
    return false;
  }
  if (!expr.operators(2).has_param()) {
    return false;
  }
  return true;
}

bool is_ep_lt(const common::Expression& expr) {
  if (expr.operators_size() != 3) {
    return false;
  }
  if (!(expr.operators(0).has_var() &&
        expr.operators(0).var().has_property())) {
    return false;
  }
  if (!(expr.operators(1).item_case() == common::ExprOpr::ItemCase::kLogical &&
        expr.operators(1).logical() == common::Logical::LT)) {
    return false;
  }
  if (!expr.operators(2).has_param()) {
    return false;
  }
  return true;
}

Context eval_edge_expand(const physical::EdgeExpand& opr,
                         const GraphReadInterface& graph, Context&& ctx,
                         const std::map<std::string, std::string>& params,
                         OprTimer& timer,
                         const physical::PhysicalOpr_MetaData& meta) {
  TimerUnit tx;
  tx.start();
  int v_tag;
  if (!opr.has_v_tag()) {
    v_tag = -1;
  } else {
    v_tag = opr.v_tag().value();
  }

  Direction dir = parse_direction(opr.direction());
  bool is_optional = opr.is_optional();
  //  LOG(INFO) << opr.DebugString() << " \n row num:" << ctx.row_num();
  // CHECK(!is_optional);

  CHECK(opr.has_params());
  const algebra::QueryParams& query_params = opr.params();

  int alias = -1;
  if (opr.has_alias()) {
    alias = opr.alias().value();
  }

  EdgeExpandParams eep;
  eep.v_tag = v_tag;
  eep.labels = parse_label_triplets(meta);
  eep.dir = dir;
  eep.alias = alias;
  eep.is_optional = is_optional;

  if (opr.expand_opt() ==
      physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    if (query_params.has_predicate()) {
      if (is_ep_gt(query_params.predicate())) {
        std::string param_name =
            query_params.predicate().operators(2).param().name();
        std::string param_value = params.at(param_name);
        auto ret = EdgeExpand::expand_vertex_ep_gt(graph, std::move(ctx), eep,
                                                   param_value);
        timer.record_routine("edge_expand::expand_vertex_ep_gt", tx);
        return ret;
      } else if (is_ep_lt(query_params.predicate())) {
        std::string param_name =
            query_params.predicate().operators(2).param().name();
        std::string param_value = params.at(param_name);
        auto ret = EdgeExpand::expand_vertex_ep_lt(graph, std::move(ctx), eep,
                                                   param_value);
        timer.record_routine("edge_expand::expand_vertex_ep_lt", tx);
        return ret;
      } else {
        GeneralEdgePredicate pred(graph, ctx, params, query_params.predicate());
        auto ret = EdgeExpand::expand_vertex<GeneralEdgePredicate>(
            graph, std::move(ctx), eep, pred);
        timer.record_routine("edge_expand::expand_vertex", tx);
        return ret;
      }
    } else {
      auto ret = EdgeExpand::expand_vertex_without_predicate(
          graph, std::move(ctx), eep);
      timer.record_routine("edge_expand::expand_vertex_without_predicate", tx);
      return ret;
    }
  } else if (opr.expand_opt() ==
             physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
    if (query_params.has_predicate()) {
      auto sp_edge_pred =
          parse_special_edge_predicate(query_params.predicate(), graph, params);
      if (sp_edge_pred == nullptr) {
        GeneralEdgePredicate pred(graph, ctx, params, query_params.predicate());
        auto ret = EdgeExpand::expand_edge(graph, std::move(ctx), eep, pred);
        timer.record_routine("edge_expand::expand_edge", tx);
        return ret;
      } else {
        auto ret = EdgeExpand::expand_edge_with_special_edge_predicate(
            graph, std::move(ctx), eep, *sp_edge_pred);
        timer.record_routine(
            "edge_expand::expand_edge_with_special_edge_predicate", tx);
        return ret;
      }
    } else {
      auto ret = EdgeExpand::expand_edge_without_predicate(
          graph, std::move(ctx), eep, timer);
      timer.record_routine("#### expand_edge_without_predicate", tx);
      return ret;
    }
  } else {
    LOG(FATAL) << "not support";
  }
  return ctx;
}

bool edge_expand_get_v_fusable(const physical::EdgeExpand& ee_opr,
                               const physical::GetV& v_opr, const Context& ctx,
                               const physical::PhysicalOpr_MetaData& meta) {
  if (ee_opr.expand_opt() !=
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE &&
      ee_opr.expand_opt() !=
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    // VLOG(1) << "not edge expand, fallback";
    return false;
  }
  if (ee_opr.params().has_predicate()) {
    // VLOG(1) << "edge expand has predicate, fallback";
    return false;
  }
  int alias = -1;
  if (ee_opr.has_alias()) {
    alias = ee_opr.alias().value();
  }
  if (alias != -1) {
    // VLOG(1) << "alias of edge expand is not -1, fallback";
    return false;
  }

  int tag = -1;
  if (v_opr.has_tag()) {
    tag = v_opr.tag().value();
  }
  if (tag != -1) {
    // VLOG(1) << "the input of get_v is -1, fallback";
    return false;
  }

  Direction dir = parse_direction(ee_opr.direction());
  if (ee_opr.expand_opt() ==
      physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    if (v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_ITSELF) {
      return true;
    }
  } else if (ee_opr.expand_opt() ==
             physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
    if (dir == Direction::kOut &&
        v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_END) {
      return true;
    }
    if (dir == Direction::kIn &&
        v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_START) {
      return true;
    }
  }
  return false;

  // VLOG(1) << "direction of edge_expand is not consistent with vopt of get_v";
  return false;
}

bool tc_fusable(const physical::EdgeExpand& ee_opr0,
                const physical::GroupBy& group_by_opr,
                const physical::EdgeExpand& ee_opr1,
                const physical::GetV& v_opr1,
                const physical::EdgeExpand& ee_opr2,
                const algebra::Select& select_opr, const Context& ctx) {
  return true;
}

Context eval_edge_expand_get_v(const physical::EdgeExpand& ee_opr,
                               const physical::GetV& v_opr,
                               const GraphReadInterface& graph, Context&& ctx,
                               const std::map<std::string, std::string>& params,
                               OprTimer& timer,
                               const physical::PhysicalOpr_MetaData& meta) {
  TimerUnit tx;
  tx.start();
  int v_tag;
  if (!ee_opr.has_v_tag()) {
    v_tag = -1;
  } else {
    v_tag = ee_opr.v_tag().value();
  }

  Direction dir = parse_direction(ee_opr.direction());
  bool is_optional = ee_opr.is_optional();
  // CHECK(!is_optional);

  CHECK(ee_opr.has_params());
  const algebra::QueryParams& query_params = ee_opr.params();

  int alias = -1;
  if (v_opr.has_alias()) {
    alias = v_opr.alias().value();
  }

  CHECK(ee_opr.expand_opt() ==
            physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE ||
        ee_opr.expand_opt() ==
            physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
  CHECK(!query_params.has_predicate());

  EdgeExpandParams eep;
  eep.v_tag = v_tag;
  eep.labels = parse_label_triplets(meta);
  eep.dir = dir;
  eep.alias = alias;
  eep.is_optional = is_optional;
  // LOG(INFO) << is_optional << " " << "is optional";

  if (!v_opr.params().has_predicate()) {
    auto ret =
        EdgeExpand::expand_vertex_without_predicate(graph, std::move(ctx), eep);
    timer.record_routine("eegv::expand_vertex_without_predicate0", tx);
    return ret;
  } else {
    std::set<label_t> labels_set;
    label_t exact_pk_label;
    Any exact_pk;
    if (is_label_within_predicate(v_opr.params().predicate(), labels_set)) {
      bool within = true;
      if (dir == Direction::kOut) {
        for (auto& triplet : eep.labels) {
          if (labels_set.find(triplet.dst_label) == labels_set.end()) {
            within = false;
            break;
          }
        }
      } else if (dir == Direction::kIn) {
        for (auto& triplet : eep.labels) {
          if (labels_set.find(triplet.src_label) == labels_set.end()) {
            within = false;
            break;
          }
        }
      } else {
        for (auto& triplet : eep.labels) {
          if (labels_set.find(triplet.dst_label) == labels_set.end()) {
            within = false;
            break;
          }
          if (labels_set.find(triplet.src_label) == labels_set.end()) {
            within = false;
            break;
          }
        }
      }

      if (within) {
        if (query_params.has_predicate()) {
          GeneralEdgePredicate pred(graph, ctx, params,
                                    query_params.predicate());
          auto ret = EdgeExpand::expand_vertex<GeneralEdgePredicate>(
              graph, std::move(ctx), eep, pred);
          timer.record_routine("eegv::expand_vertex0", tx);
          return ret;
        } else {
          auto ret = EdgeExpand::expand_vertex_without_predicate(
              graph, std::move(ctx), eep);
          timer.record_routine("eegv::expand_vertex_without_predicate1", tx);
          return ret;
        }
      } else {
        GeneralVertexPredicate v_pred(graph, ctx, params,
                                      v_opr.params().predicate());
        if (query_params.has_predicate()) {
          GeneralEdgePredicate e_pred(graph, ctx, params,
                                      query_params.predicate());
          VertexEdgePredicateWrapper ve_pred(v_pred, e_pred);
          auto ret = EdgeExpand::expand_vertex<VertexEdgePredicateWrapper>(
              graph, std::move(ctx), eep, ve_pred);
          timer.record_routine("eegv::expand_vertex1", tx);
          return ret;
        } else {
          VertexPredicateWrapper vpred(v_pred);
          auto ret = EdgeExpand::expand_vertex<VertexPredicateWrapper>(
              graph, std::move(ctx), eep, vpred);
          timer.record_routine("eegv::expand_vertex2", tx);
          return ret;
        }
      }
    } else if (is_pk_exact_check(v_opr.params().predicate(), params,
                                 exact_pk_label, exact_pk)) {
      vid_t index = std::numeric_limits<vid_t>::max();
      graph.GetVertexIndex(exact_pk_label, exact_pk, index);
      ExactVertexPredicate v_pred(exact_pk_label, index);
      if (query_params.has_predicate()) {
        GeneralEdgePredicate e_pred(graph, ctx, params,
                                    query_params.predicate());
        ExactVertexEdgePredicateWrapper ve_pred(v_pred, e_pred);

        auto ret = EdgeExpand::expand_vertex<ExactVertexEdgePredicateWrapper>(
            graph, std::move(ctx), eep, ve_pred);
        timer.record_routine("eegv::expand_vertex3", tx);
        return ret;
      } else {
        auto ret = EdgeExpand::expand_vertex<ExactVertexPredicateWrapper>(
            graph, std::move(ctx), eep, v_pred);
        timer.record_routine("eegv::expand_vertex4", tx);
        return ret;
      }
    } else {
      if (query_params.has_predicate()) {
        GeneralVertexPredicate v_pred(graph, ctx, params,
                                      v_opr.params().predicate());
        GeneralEdgePredicate e_pred(graph, ctx, params,
                                    query_params.predicate());
        VertexEdgePredicateWrapper ve_pred(v_pred, e_pred);
        auto ret = EdgeExpand::expand_vertex<VertexEdgePredicateWrapper>(
            graph, std::move(ctx), eep, ve_pred);
        timer.record_routine("eegv::expand_vertex5", tx);
        return ret;
      } else {
        auto vertex_col =
            std::dynamic_pointer_cast<IVertexColumn>(ctx.get(eep.v_tag));
        if (vertex_col->vertex_column_type() == VertexColumnType::kMultiple) {
          auto ee_ret = eval_edge_expand(ee_opr, graph, std::move(ctx), params,
                                         timer, meta);
          auto v_ret = eval_get_v(v_opr, graph, std::move(ctx), params, timer);
          timer.record_routine("eegv::edge_expand+get_v", tx);
          return v_ret;
        } else {
          auto sp_vertex_pred = parse_special_vertex_predicate(
              v_opr.params().predicate(), graph, params);
          if (sp_vertex_pred == nullptr) {
            GeneralVertexPredicate v_pred(graph, ctx, params,
                                          v_opr.params().predicate());
            VertexPredicateWrapper vpred(v_pred);
            auto ret = EdgeExpand::expand_vertex<VertexPredicateWrapper>(
                graph, std::move(ctx), eep, vpred);
            timer.record_routine("eegv::expand_vertex6", tx);
            return ret;
          } else {
            auto ret = EdgeExpand::expand_vertex_with_special_vertex_predicate(
                graph, std::move(ctx), eep, *sp_vertex_pred);
            timer.record_routine(
                "eegv::expand_vertex_with_special_vertex_predicate", tx);
            return ret;
          }
        }
      }
    }
  }
}

Context eval_tc(const physical::EdgeExpand& ee_opr0,
                const physical::GroupBy& group_by_opr,
                const physical::EdgeExpand& ee_opr1,
                const physical::GetV& v_opr1,
                const physical::EdgeExpand& ee_opr2,
                const algebra::Select& select_opr,
                const GraphReadInterface& graph, Context&& ctx,
                const std::map<std::string, std::string>& params,
                const physical::PhysicalOpr_MetaData& meta0,
                const physical::PhysicalOpr_MetaData& meta1,
                const physical::PhysicalOpr_MetaData& meta2) {
  CHECK(!ee_opr0.is_optional());
  CHECK(!ee_opr1.is_optional());
  CHECK(!ee_opr2.is_optional());

  int input_tag = -1;
  if (ee_opr0.has_v_tag()) {
    input_tag = ee_opr0.v_tag().value();
  }

  Direction dir0 = parse_direction(ee_opr0.direction());
  Direction dir1 = parse_direction(ee_opr1.direction());
  Direction dir2 = parse_direction(ee_opr2.direction());

  std::shared_ptr<IVertexColumn> input_vertex_list =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(input_tag));
  CHECK(input_vertex_list->vertex_column_type() == VertexColumnType::kSingle);
  auto casted_input_vertex_list =
      std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
  label_t input_label = casted_input_vertex_list->label();

  label_t d0_nbr_label, d0_e_label, d1_nbr_label, d1_e_label, d2_nbr_label,
      d2_e_label;
  PropertyType d0_ep, d1_ep, d2_ep;
  {
    auto labels0 = parse_label_triplets(meta0);
    CHECK_EQ(labels0.size(), 1);
    d0_e_label = labels0[0].edge_label;
    if (dir0 == Direction::kOut) {
      CHECK_EQ(labels0[0].src_label, input_label);
      d0_nbr_label = labels0[0].dst_label;
    } else if (dir0 == Direction::kIn) {
      CHECK_EQ(labels0[0].dst_label, input_label);
      d0_nbr_label = labels0[0].src_label;
    } else {
      LOG(FATAL) << "both direction not supported";
    }

    const auto& properties0 = graph.schema().get_edge_properties(
        labels0[0].src_label, labels0[0].dst_label, labels0[0].edge_label);
    if (properties0.empty()) {
      d0_ep = PropertyType::Empty();
    } else {
      CHECK_EQ(1, properties0.size());
      d0_ep = properties0[0];
    }

    auto labels1 = parse_label_triplets(meta1);
    CHECK_EQ(labels1.size(), 1);
    d1_e_label = labels1[0].edge_label;
    if (dir1 == Direction::kOut) {
      CHECK_EQ(labels1[0].src_label, input_label);
      d1_nbr_label = labels1[0].dst_label;
    } else if (dir1 == Direction::kIn) {
      CHECK_EQ(labels1[0].dst_label, input_label);
      d1_nbr_label = labels1[0].src_label;
    } else {
      LOG(FATAL) << "both direction not supported";
    }

    const auto& properties1 = graph.schema().get_edge_properties(
        labels1[0].src_label, labels1[0].dst_label, labels1[0].edge_label);
    if (properties1.empty()) {
      d1_ep = PropertyType::Empty();
    } else {
      CHECK_EQ(1, properties1.size());
      d1_ep = properties1[0];
    }

    auto labels2 = parse_label_triplets(meta2);
    CHECK_EQ(labels2.size(), 1);
    d2_e_label = labels2[0].edge_label;
    if (dir2 == Direction::kOut) {
      CHECK_EQ(labels2[0].src_label, d1_nbr_label);
      d2_nbr_label = labels2[0].dst_label;
    } else if (dir1 == Direction::kIn) {
      CHECK_EQ(labels2[0].dst_label, d1_nbr_label);
      d2_nbr_label = labels2[0].src_label;
    } else {
      LOG(FATAL) << "both direction not supported";
    }

    const auto& properties2 = graph.schema().get_edge_properties(
        labels2[0].src_label, labels2[0].dst_label, labels2[0].edge_label);
    if (properties2.empty()) {
      d2_ep = PropertyType::Empty();
    } else {
      CHECK_EQ(1, properties2.size());
      d2_ep = properties2[0];
    }
  }
  CHECK(d0_ep == PropertyType::Date());
  CHECK(d1_ep == PropertyType::Date());
  CHECK(d2_ep == PropertyType::Empty());
  auto csr0 = (dir0 == Direction::kOut)
                  ? graph.GetOutgoingGraphView<Date>(input_label, d0_nbr_label,
                                                     d0_e_label)
                  : graph.GetIncomingGraphView<Date>(input_label, d0_nbr_label,
                                                     d0_e_label);
  auto csr1 = (dir1 == Direction::kOut)
                  ? graph.GetOutgoingGraphView<Date>(input_label, d1_nbr_label,
                                                     d1_e_label)
                  : graph.GetIncomingGraphView<Date>(input_label, d1_nbr_label,
                                                     d1_e_label);
  auto csr2 = (dir2 == Direction::kOut)
                  ? graph.GetOutgoingGraphView<grape::EmptyType>(
                        d1_nbr_label, d2_nbr_label, d2_e_label)
                  : graph.GetIncomingGraphView<grape::EmptyType>(
                        d1_nbr_label, d2_nbr_label, d2_e_label);

  const algebra::QueryParams& ee_opr0_qp = ee_opr0.params();
  std::string param_name = ee_opr0_qp.predicate().operators(2).param().name();
  std::string param_value = params.at(param_name);

  Date min_date(std::stoll(param_value));

  SLVertexColumnBuilder builder1(d1_nbr_label);
  SLVertexColumnBuilder builder2(d2_nbr_label);
  std::vector<size_t> offsets;

  size_t idx = 0;
  static thread_local GraphReadInterface::vertex_array_t<bool> d0_set;
  static thread_local std::vector<vid_t> d0_vec;

  d0_set.Init(graph.GetVertexSet(d0_nbr_label), false);
  for (auto v : casted_input_vertex_list->vertices()) {
    csr0.foreach_edges_gt(v, min_date, [&](vid_t u, const Date& date) {
      d0_set[u] = true;
      d0_vec.push_back(u);
    });
    for (auto& e1 : csr1.get_edges(v)) {
      auto nbr1 = e1.get_neighbor();
      for (auto& e2 : csr2.get_edges(nbr1)) {
        auto nbr2 = e2.get_neighbor();
        if (d0_set[nbr2]) {
          builder1.push_back_opt(nbr1);
          builder2.push_back_opt(nbr2);
          offsets.push_back(idx);
        }
      }
    }
    for (auto u : d0_vec) {
      d0_set[u] = false;
    }
    d0_vec.clear();
    ++idx;
  }

  int alias1 = -1;
  if (ee_opr1.has_alias()) {
    alias1 = ee_opr1.alias().value();
  }
  if (v_opr1.has_alias()) {
    alias1 = v_opr1.alias().value();
  }
  int alias2 = -1;
  if (ee_opr2.has_alias()) {
    alias2 = ee_opr2.alias().value();
  }

  std::shared_ptr<IContextColumn> col1 = builder1.finish();
  std::shared_ptr<IContextColumn> col2 = builder2.finish();
  ctx.set_with_reshuffle(alias1, col1, offsets);
  ctx.set(alias2, col2);
  return ctx;
}

}  // namespace runtime

}  // namespace gs