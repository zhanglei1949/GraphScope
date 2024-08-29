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
#include "flex/engines/graph_db/runtime/adhoc/predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

Context eval_edge_expand(const physical::EdgeExpand& opr,
                         const ReadTransaction& txn, Context&& ctx,
                         const std::map<std::string, std::string>& params,
                         const physical::PhysicalOpr_MetaData& meta) {
  int v_tag;
  if (!opr.has_v_tag()) {
    v_tag = -1;
  } else {
    v_tag = opr.v_tag().value();
  }

  Direction dir = parse_direction(opr.direction());
  bool is_optional = opr.is_optional();
  CHECK(!is_optional);

  CHECK(opr.has_params());
  const algebra::QueryParams& query_params = opr.params();

  int alias = -1;
  if (opr.has_alias()) {
    alias = opr.alias().value();
  }

  auto& op_cost = OpCost::get();

  if (opr.expand_opt() ==
      physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    if (query_params.has_predicate()) {
      LOG(FATAL) << "not support";
    } else {
      EdgeExpandParams eep;
      eep.v_tag = v_tag;
      eep.labels = parse_label_triplets(meta);
      eep.dir = dir;
      eep.alias = alias;

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
      EdgeExpandParams eep;
      eep.v_tag = v_tag;
      eep.labels = parse_label_triplets(meta);
      eep.dir = dir;
      eep.alias = alias;

      GeneralEdgePredicate pred(txn, ctx, params, query_params.predicate());

      double t = -grape::GetCurrentTime();
      auto ret = EdgeExpand::expand_edge(txn, std::move(ctx), eep, pred);
      t += grape::GetCurrentTime();

      op_cost.table["expand_edge_with_predicate"] += t;

      return ret;
    } else {
      EdgeExpandParams eep;
      eep.v_tag = v_tag;
      eep.labels = parse_label_triplets(meta);
      eep.dir = dir;
      eep.alias = alias;

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
      physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
    // LOG(INFO) << "not edge expand, fallback";
    return false;
  }
  // if (ee_opr.params().has_predicate()) {
  //   LOG(INFO) << "edge expand has predicate, fallback";
  //   return false;
  // }
  int alias = -1;
  if (ee_opr.has_alias()) {
    alias = ee_opr.alias().value();
  }
  if (alias != -1) {
    LOG(INFO) << "alias of edge expand is not -1, fallback";
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
  if (v_opr.has_params()) {
    if (v_opr.params().has_predicate()) {
      LOG(INFO) << "get_v has predicate, fallback";
      return false;
    }
  }

  int input_tag = -1;
  if (ee_opr.has_v_tag()) {
    input_tag = ee_opr.v_tag().value();
  }
  auto upstream = std::dynamic_pointer_cast<IVertexColumn>(ctx.get(input_tag));
  if (upstream == nullptr) {
    LOG(INFO) << "upstream is not a vertex column, fallback";
    return false;
  }
  const std::set<label_t>& input_vertex_labels = upstream->get_labels_set();
  std::vector<label_t> v_tables = parse_tables(v_opr.params());

  std::vector<LabelTriplet> triplets = parse_label_triplets(meta);
  Direction dir = parse_direction(ee_opr.direction());

  std::vector<label_t> output_vertex_labels;
  if (dir == Direction::kOut &&
      v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_END) {
    for (auto& triplet : triplets) {
      output_vertex_labels.push_back(triplet.dst_label);
    }
  } else if (dir == Direction::kIn &&
             v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_START) {
    for (auto& triplet : triplets) {
      output_vertex_labels.push_back(triplet.src_label);
    }
  } else {
    LOG(INFO)
        << "direction of edge_expand is not consistent with vopt of get_v";
    return false;
  }

  grape::DistinctSort(v_tables);
  grape::DistinctSort(output_vertex_labels);
  if (!v_tables.empty() && v_tables != output_vertex_labels) {
    return false;
  }
  return true;
}

Context eval_edge_expand_get_v(const physical::EdgeExpand& ee_opr,
                               const physical::GetV& v_opr,
                               const ReadTransaction& txn, Context&& ctx,
                               const std::map<std::string, std::string>& params,
                               const physical::PhysicalOpr_MetaData& meta) {
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
        physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE);

  if (!query_params.has_predicate()) {
    EdgeExpandParams eep;
    eep.v_tag = v_tag;
    eep.labels = parse_label_triplets(meta);
    eep.dir = dir;
    eep.alias = alias;

    return EdgeExpand::expand_vertex_without_predicate(txn, std::move(ctx),
                                                       eep);
  } else {
    EdgeExpandParams eep;
    eep.v_tag = v_tag;
    eep.labels = parse_label_triplets(meta);
    eep.dir = dir;
    eep.alias = alias;

    GeneralEdgePredicate pred(txn, ctx, params, query_params.predicate());
    LOG(INFO) << "enter expand vertex with predicate";
    return EdgeExpand::expand_vertex<GeneralEdgePredicate>(txn, std::move(ctx),
                                                           eep, pred);
  }
}

}  // namespace runtime

}  // namespace gs