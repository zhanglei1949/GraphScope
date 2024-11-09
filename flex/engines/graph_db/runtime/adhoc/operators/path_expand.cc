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
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/special_predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"

namespace gs {

namespace runtime {

Context eval_path_expand_v(const physical::PathExpand& opr,
                           const GraphReadInterface& graph, Context&& ctx,
                           const std::map<std::string, std::string>& params,
                           OprTimer& timer,
                           const physical::PhysicalOpr_MetaData& meta,
                           int alias) {
  int start_tag = opr.has_start_tag() ? opr.start_tag().value() : -1;
  CHECK(opr.path_opt() ==
        physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY);
  if (opr.result_opt() !=
      physical::PathExpand_ResultOpt::PathExpand_ResultOpt_END_V) {
    //    LOG(FATAL) << "not support";
  }
  CHECK(!opr.is_optional());

  Direction dir = parse_direction(opr.base().edge_expand().direction());
  CHECK(!opr.base().edge_expand().is_optional());
  const algebra::QueryParams& query_params = opr.base().edge_expand().params();
  PathExpandParams pep;
  pep.alias = alias;
  pep.dir = dir;
  pep.hop_lower = opr.hop_range().lower();
  pep.hop_upper = opr.hop_range().upper();
  for (size_t ci = 0; ci < ctx.col_num(); ++ci) {
    if (ctx.get(ci) != nullptr) {
      pep.keep_cols.insert(ci);
    }
  }
  pep.start_tag = start_tag;
  pep.labels = parse_label_triplets(meta);
  if (opr.base().edge_expand().expand_opt() ==
      physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    if (query_params.has_predicate()) {
      LOG(FATAL) << "not support";
    } else {
      TimerUnit tx;
      tx.start();
      auto ret = PathExpand::edge_expand_v(graph, std::move(ctx), pep);
      timer.record_routine("#### edge_expand_v", tx);
      return ret;
    }
  } else {
    LOG(FATAL) << "not support";
  }

  return ctx;
}

Context eval_path_expand_p(const physical::PathExpand& opr,
                           const GraphReadInterface& graph, Context&& ctx,
                           const std::map<std::string, std::string>& params,
                           const physical::PhysicalOpr_MetaData& meta,
                           int alias) {
  LOG(INFO) << opr.DebugString();
  CHECK(opr.has_start_tag());
  int start_tag = opr.start_tag().value();
  CHECK(opr.path_opt() ==
        physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY);

  CHECK(!opr.is_optional());

  Direction dir = parse_direction(opr.base().edge_expand().direction());
  CHECK(!opr.base().edge_expand().is_optional());
  const algebra::QueryParams& query_params = opr.base().edge_expand().params();
  PathExpandParams pep;
  pep.alias = alias;
  pep.dir = dir;
  pep.hop_lower = opr.hop_range().lower();
  pep.hop_upper = opr.hop_range().upper();
  for (size_t ci = 0; ci < ctx.col_num(); ++ci) {
    if (ctx.get(ci) != nullptr) {
      pep.keep_cols.insert(ci);
    }
  }
  pep.start_tag = start_tag;
  pep.labels = parse_label_triplets(meta);

  if (query_params.has_predicate()) {
    LOG(FATAL) << "not support";
  } else {
    return PathExpand::edge_expand_p(graph, std::move(ctx), pep);
  }

  return ctx;
}

Context eval_shortest_path_with_order_by_length_limit(
    const physical::PathExpand& opr, const GraphReadInterface& graph,
    Context&& ctx, const std::map<std::string, std::string>& params,
    const physical::PhysicalOpr_MetaData& meta, const physical::GetV& get_v_opr,
    int v_alias, int path_len_alias, int limit) {
  CHECK(opr.has_start_tag());
  int start_tag = opr.start_tag().value();
  CHECK(!opr.is_optional());
  ShortestPathParams spp;
  spp.start_tag = start_tag;
  spp.dir = parse_direction(opr.base().edge_expand().direction());
  spp.v_alias = v_alias;
  spp.alias = path_len_alias;
  spp.hop_lower = opr.hop_range().lower();
  spp.hop_upper = opr.hop_range().upper();
  spp.labels = parse_label_triplets(meta);
  CHECK(spp.labels.size() == 1) << "only support one label triplet";
  if (get_v_opr.has_params() && get_v_opr.params().has_predicate()) {
    auto sp_vertex_pred = parse_special_vertex_predicate(
        get_v_opr.params().predicate(), graph, params);
    if (sp_vertex_pred == nullptr) {
      LOG(FATAL) << "not support"
                 << get_v_opr.params().predicate().DebugString();
    } else {
      auto pred = get_v_opr.params().predicate();
      if (sp_vertex_pred->data_type() == RTAnyType::kStringValue) {
        if (sp_vertex_pred->type() == SPVertexPredicateType::kPropertyEQ) {
          auto casted_pred = dynamic_cast<
              const VertexPropertyEQPredicateBeta<std::string_view>&>(
              *sp_vertex_pred);
          return PathExpand::
              single_source_shortest_path_with_order_by_length_limit(
                  graph, std::move(ctx), spp, casted_pred, limit);
        }
      }
    }
  }
  LOG(FATAL) << "not support";
  return Context();
}

Context eval_shortest_path(const physical::PathExpand& opr,
                           const GraphReadInterface& graph, Context&& ctx,
                           const std::map<std::string, std::string>& params,
                           OprTimer& timer,
                           const physical::PhysicalOpr_MetaData& meta,
                           const physical::GetV& v_opr, int v_alias) {
  CHECK(opr.has_start_tag());
  int start_tag = opr.start_tag().value();
  CHECK(!opr.is_optional());

  ShortestPathParams spp;
  spp.start_tag = start_tag;
  spp.dir = parse_direction(opr.base().edge_expand().direction());
  spp.v_alias = v_alias;
  spp.alias = opr.has_alias() ? opr.alias().value() : -1;
  spp.hop_lower = opr.hop_range().lower();
  spp.hop_upper = opr.hop_range().upper();

  spp.labels = parse_label_triplets(meta);
  CHECK(spp.labels.size() == 1) << "only support one label triplet";
  CHECK(spp.labels[0].src_label == spp.labels[0].dst_label)
      << "only support same src and dst label";

  gs::Any vertex;
  if (v_opr.has_params() && v_opr.params().has_predicate() &&
      is_pk_oid_exact_check(v_opr.params().predicate(), params, vertex)) {
    vid_t vid;
    CHECK(graph.GetVertexIndex(spp.labels[0].dst_label, vertex, vid))
        << "vertex not found";
    auto v = std::make_pair(spp.labels[0].dst_label, vid);
    TimerUnit tx;
    tx.start();
    auto ret = PathExpand::single_source_single_dest_shortest_path(
        graph, std::move(ctx), spp, v);
    timer.record_routine("#### single_source_single_dest_shortest_path", tx);
    return ret;
  } else {
    if (v_opr.has_params() && v_opr.params().has_predicate()) {
      auto sp_vertex_pred = parse_special_vertex_predicate(
          v_opr.params().predicate(), graph, params);
      if (sp_vertex_pred == nullptr) {
        Context tmp_ctx;
        auto predicate =
            parse_expression(graph, tmp_ctx, params, v_opr.params().predicate(),
                             VarType::kVertexVar);
        auto pred = [&predicate](label_t label, vid_t v) {
          return predicate->eval_vertex(label, v, 0).as_bool();
        };
        return PathExpand::single_source_shortest_path(graph, std::move(ctx),
                                                       spp, pred);
      } else {
        return PathExpand::
            single_source_shortest_path_with_special_vertex_predicate(
                graph, std::move(ctx), spp, *sp_vertex_pred);
      }
    } else {
      auto pred = [](label_t label, vid_t v) { return true; };
      return PathExpand::single_source_shortest_path(graph, std::move(ctx), spp,
                                                     pred);
    }
  }
}

Context eval_all_shortest_paths(
    const physical::PathExpand& opr, const GraphReadInterface& graph,
    Context&& ctx, const std::map<std::string, std::string>& params,
    const physical::PhysicalOpr_MetaData& meta, const physical::GetV& v_opr,
    int v_alias) {
  CHECK(opr.has_start_tag());
  int start_tag = opr.start_tag().value();
  CHECK(!opr.is_optional());

  ShortestPathParams aspp;
  aspp.start_tag = start_tag;
  aspp.dir = parse_direction(opr.base().edge_expand().direction());
  aspp.v_alias = v_alias;
  aspp.alias = opr.has_alias() ? opr.alias().value() : -1;
  aspp.hop_lower = opr.hop_range().lower();
  aspp.hop_upper = opr.hop_range().upper();

  aspp.labels = parse_label_triplets(meta);
  CHECK(aspp.labels.size() == 1) << "only support one label triplet";
  CHECK(aspp.labels[0].src_label == aspp.labels[0].dst_label)
      << "only support same src and dst label";

  gs::Any vertex;
  if (v_opr.has_params() && v_opr.params().has_predicate() &&
      is_pk_oid_exact_check(v_opr.params().predicate(), params, vertex)) {
    vid_t vid;
    CHECK(graph.GetVertexIndex(aspp.labels[0].dst_label, vertex, vid))
        << "vertex not found";
    auto v = std::make_pair(aspp.labels[0].dst_label, vid);
    return PathExpand::all_shortest_paths_with_given_source_and_dest(
        graph, std::move(ctx), aspp, v);

  } else {
    LOG(FATAL) << "only support all shortest paths from a single source to a "
                  "single destination";
  }
  return Context();
}

}  // namespace runtime

}  // namespace gs