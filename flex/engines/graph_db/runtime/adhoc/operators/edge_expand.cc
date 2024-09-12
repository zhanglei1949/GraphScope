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

Context eval_edge_expand(const physical::EdgeExpand& opr,
                         const ReadTransaction& txn, Context&& ctx,
                         const std::map<std::string, std::string>& params,
                         const physical::PhysicalOpr_MetaData& meta,
                         int op_id) {
  int v_tag;
  if (!opr.has_v_tag()) {
    v_tag = -1;
  } else {
    v_tag = opr.v_tag().value();
  }

  Direction dir = parse_direction(opr.direction());
  bool is_optional = opr.is_optional();
  // CHECK(!is_optional);

  CHECK(opr.has_params());
  const algebra::QueryParams& query_params = opr.params();

  int alias = -1;
  if (opr.has_alias()) {
    alias = opr.alias().value();
  }

  auto& op_cost = OpCost::get();

  EdgeExpandParams eep;
  eep.v_tag = v_tag;
  eep.labels = parse_label_triplets(meta);
  eep.dir = dir;
  eep.alias = alias;
  eep.is_optional = is_optional;

  if (opr.expand_opt() ==
      physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    if (query_params.has_predicate()) {
      // LOG(INFO) << "##### 11 " << op_id;
      double t = -grape::GetCurrentTime();

      GeneralEdgePredicate pred(txn, ctx, params, query_params.predicate());
      auto ret = EdgeExpand::expand_vertex<GeneralEdgePredicate>(
          txn, std::move(ctx), eep, pred);

      t += grape::GetCurrentTime();
      op_cost.table["expand_vertex_with_predicate"] += t;
      return ret;
    } else {
      double t = -grape::GetCurrentTime();
      auto ret =
          EdgeExpand::expand_vertex_without_predicate(txn, std::move(ctx), eep);
      t += grape::GetCurrentTime();

      op_cost.table["expand_vertex_without_predicate"] += t;
      return ret;
    }
  } else if (opr.expand_opt() ==
             physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
    if (query_params.has_predicate()) {
      auto sp_edge_pred =
          parse_special_edge_predicate(query_params.predicate(), txn, params);
      if (sp_edge_pred == nullptr) {
        GeneralEdgePredicate pred(txn, ctx, params, query_params.predicate());

        double t = -grape::GetCurrentTime();
        auto ret = EdgeExpand::expand_edge(txn, std::move(ctx), eep, pred);
        t += grape::GetCurrentTime();

        op_cost.table["expand_edge_with_predicate"] += t;

        return ret;
      } else {
        double t = -grape::GetCurrentTime();
        auto ret = EdgeExpand::expand_edge_with_special_edge_predicate(
            txn, std::move(ctx), eep, *sp_edge_pred);
        t += grape::GetCurrentTime();

        op_cost.table["expand_edge_with_sp_predicate"] += t;
      }
    } else {
      double t = -grape::GetCurrentTime();
      auto ret =
          EdgeExpand::expand_edge_without_predicate(txn, std::move(ctx), eep);
      t += grape::GetCurrentTime();

      op_cost.table["expand_edge_without_predicate"] += t;
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
    // LOG(INFO) << "not edge expand, fallback";
    return false;
  }
  if (ee_opr.params().has_predicate()) {
    // LOG(INFO) << "edge expand has predicate, fallback";
    return false;
  }
  int alias = -1;
  if (ee_opr.has_alias()) {
    alias = ee_opr.alias().value();
  }
  if (alias != -1) {
    // LOG(INFO) << "alias of edge expand is not -1, fallback";
    return false;
  }

  int tag = -1;
  if (v_opr.has_tag()) {
    tag = v_opr.tag().value();
  }
  if (tag != -1) {
    LOG(INFO) << "the input of get_v is -1, fallback";
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

  LOG(INFO) << "direction of edge_expand is not consistent with vopt of get_v";
  return false;
}

Context eval_edge_expand_get_v(const physical::EdgeExpand& ee_opr,
                               const physical::GetV& v_opr,
                               const ReadTransaction& txn, Context&& ctx,
                               const std::map<std::string, std::string>& params,
                               const physical::PhysicalOpr_MetaData& meta,
                               int op_id) {
  auto& op_cost = OpCost::get();
  int v_tag;
  if (!ee_opr.has_v_tag()) {
    v_tag = -1;
  } else {
    v_tag = ee_opr.v_tag().value();
  }

  Direction dir = parse_direction(ee_opr.direction());
  bool is_optional = ee_opr.is_optional();
  CHECK(!is_optional);

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

  if (!v_opr.params().has_predicate()) {
    if (query_params.has_predicate()) {
      // LOG(INFO) << "##### 0 " << op_id;
      GeneralEdgePredicate pred(txn, ctx, params, query_params.predicate());
      double t = -grape::GetCurrentTime();
      auto ret = EdgeExpand::expand_vertex<GeneralEdgePredicate>(
          txn, std::move(ctx), eep, pred);
      t += grape::GetCurrentTime();
      op_cost.table["#### 0-" + std::to_string(op_id)] += t;
      return ret;
    } else {
      // LOG(INFO) << "##### 1 " << op_id;
      double t = -grape::GetCurrentTime();
      auto ret =
          EdgeExpand::expand_vertex_without_predicate(txn, std::move(ctx), eep);
      t += grape::GetCurrentTime();
      op_cost.table["#### 1-" + std::to_string(op_id)] += t;
      return ret;
    }
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
          GeneralEdgePredicate pred(txn, ctx, params, query_params.predicate());
          // LOG(INFO) << "##### 2 " << op_id;
          return EdgeExpand::expand_vertex<GeneralEdgePredicate>(
              txn, std::move(ctx), eep, pred);
        } else {
          // LOG(INFO) << "##### 3 " << op_id;
          double t = -grape::GetCurrentTime();
          auto ret = EdgeExpand::expand_vertex_without_predicate(
              txn, std::move(ctx), eep);
          t += grape::GetCurrentTime();
          op_cost.table["#### 3-" + std::to_string(op_id)] += t;

          return ret;
        }
      } else {
        // LOG(INFO) << "within label predicate, but not within...";
        GeneralVertexPredicate v_pred(txn, ctx, params,
                                      v_opr.params().predicate());
        if (query_params.has_predicate()) {
          GeneralEdgePredicate e_pred(txn, ctx, params,
                                      query_params.predicate());
          VertexEdgePredicateWrapper ve_pred(v_pred, e_pred);
          // LOG(INFO) << "##### 4 " << op_id;
          return EdgeExpand::expand_vertex<VertexEdgePredicateWrapper>(
              txn, std::move(ctx), eep, ve_pred);
        } else {
          VertexPredicateWrapper vpred(v_pred);
          // LOG(INFO) << "##### 5 " << op_id;
          return EdgeExpand::expand_vertex<VertexPredicateWrapper>(
              txn, std::move(ctx), eep, vpred);
        }
      }
    } else if (is_pk_exact_check(v_opr.params().predicate(), params,
                                 exact_pk_label, exact_pk)) {
      vid_t index = std::numeric_limits<vid_t>::max();
      txn.GetVertexIndex(exact_pk_label, exact_pk, index);
      ExactVertexPredicate v_pred(exact_pk_label, index);
      if (query_params.has_predicate()) {
        GeneralEdgePredicate e_pred(txn, ctx, params, query_params.predicate());
        ExactVertexEdgePredicateWrapper ve_pred(v_pred, e_pred);

        // LOG(INFO) << "##### 6 " << op_id;
        return EdgeExpand::expand_vertex<ExactVertexEdgePredicateWrapper>(
            txn, std::move(ctx), eep, ve_pred);
      } else {
        // LOG(INFO) << "##### 7 " << op_id;
        return EdgeExpand::expand_vertex<ExactVertexPredicateWrapper>(
            txn, std::move(ctx), eep, v_pred);
      }
    } else {
      // LOG(INFO) << "not special vertex predicate";
      if (query_params.has_predicate()) {
        GeneralVertexPredicate v_pred(txn, ctx, params,
                                      v_opr.params().predicate());
        GeneralEdgePredicate e_pred(txn, ctx, params, query_params.predicate());
        VertexEdgePredicateWrapper ve_pred(v_pred, e_pred);
        // LOG(INFO) << "##### 8 " << op_id;
        return EdgeExpand::expand_vertex<VertexEdgePredicateWrapper>(
            txn, std::move(ctx), eep, ve_pred);
      } else {
        auto vertex_col =
            std::dynamic_pointer_cast<IVertexColumn>(ctx.get(eep.v_tag));
        if (vertex_col->vertex_column_type() == VertexColumnType::kMultiple) {
          // if (true) {
          double t = -grape::GetCurrentTime();
          auto ee_ret = eval_edge_expand(ee_opr, txn, std::move(ctx), params,
                                         meta, op_id);
          auto v_ret = eval_get_v(v_opr, txn, std::move(ctx), params);
          t += grape::GetCurrentTime();
          op_cost.table["#### 9Split-" + std::to_string(op_id)] += t;
          return v_ret;
        } else {
          auto sp_vertex_pred = parse_special_vertex_predicate(
              v_opr.params().predicate(), txn, params);
          if (sp_vertex_pred == nullptr) {
            GeneralVertexPredicate v_pred(txn, ctx, params,
                                          v_opr.params().predicate());
            VertexPredicateWrapper vpred(v_pred);
            // LOG(INFO) << "##### 9 " << op_id;
            double t = -grape::GetCurrentTime();
            auto ret = EdgeExpand::expand_vertex<VertexPredicateWrapper>(
                txn, std::move(ctx), eep, vpred);
            t += grape::GetCurrentTime();
            op_cost.table["#### 9Fuse-" + std::to_string(op_id)] += t;
            return ret;
          } else {
            double t = -grape::GetCurrentTime();
            auto ret = EdgeExpand::expand_vertex_with_special_vertex_predicate(
                txn, std::move(ctx), eep, *sp_vertex_pred);
            t += grape::GetCurrentTime();
            op_cost.table["#### 9FuseBeta-" + std::to_string(op_id)] += t;
            return ret;
          }
        }
      }
    }
  }
}

}  // namespace runtime

}  // namespace gs