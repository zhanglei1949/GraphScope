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
#include "flex/engines/graph_db/runtime/adhoc/utils.h"

namespace gs {

namespace runtime {

Context eval_path_expand_v(const physical::PathExpand& opr,
                           const ReadTransaction& txn, Context&& ctx,
                           const std::map<std::string, std::string>& params,
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
      return PathExpand::edge_expand_v(txn, std::move(ctx), pep);
    }
  } else {
    LOG(FATAL) << "not support";
  }

  return ctx;
}

Context eval_path_expand_p(const physical::PathExpand& opr,
                           const ReadTransaction& txn, Context&& ctx,
                           const std::map<std::string, std::string>& params,
                           const physical::PhysicalOpr_MetaData& meta,
                           int alias) {
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
    return PathExpand::edge_expand_p(txn, std::move(ctx), pep);
  }

  return ctx;
}

Context eval_shortest_path(const physical::PathExpand& opr,
                           const ReadTransaction& txn, Context&& ctx,
                           const std::map<std::string, std::string>& params,
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
    CHECK(txn.GetVertexIndex(spp.labels[0].dst_label, vertex, vid))
        << "vertex not found";
    auto v = std::make_pair(spp.labels[0].dst_label, vid);
    return PathExpand::single_source_shortest_path(txn, std::move(ctx), spp, v);
  } else {
    if (v_opr.has_params() && v_opr.params().has_predicate()) {
      Context tmp_ctx;
      auto predicate =
          parse_expression(txn, tmp_ctx, params, v_opr.params().predicate(),
                           VarType::kVertexVar);
      auto pred = [&predicate](label_t label, vid_t v) {
        return predicate->eval_vertex(label, v, 0).as_bool();
      };
      return PathExpand::single_source_shortest_path_with_predicate(
          txn, std::move(ctx), spp, pred);

    } else {
      LOG(FATAL) << "shortest path not support" << v_opr.DebugString();
    }
  }
}

}  // namespace runtime

}  // namespace gs