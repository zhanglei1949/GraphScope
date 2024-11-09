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

#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

namespace gs {

namespace runtime {

void OprTimer::output(const std::string& path) const {
#ifdef RT_PROFILE
  std::ofstream fout(path);
  fout << "Total time: " << total_time_ << std::endl;
  fout << "============= operators =============" << std::endl;
  double opr_total = 0;
  for (const auto& pair : opr_timers_) {
    opr_total += pair.second;
    fout << pair.first << ": " << pair.second << " ("
         << pair.second / total_time_ * 100.0 << "%)" << std::endl;
  }
  fout << "remaining: " << total_time_ - opr_total << " ("
       << (total_time_ - opr_total) / total_time_ * 100.0 << "%)" << std::endl;
  fout << "============= routines  =============" << std::endl;
  for (const auto& pair : routine_timers_) {
    fout << pair.first << ": " << pair.second << " ("
         << pair.second / total_time_ * 100.0 << "%)" << std::endl;
  }
  fout << "=====================================" << std::endl;
#endif
}

void OprTimer::clear() {
#ifdef RT_PROFILE
  opr_timers_.clear();
  routine_timers_.clear();
  total_time_ = 0;
#endif
}

OprTimer& OprTimer::operator+=(const OprTimer& other) {
#ifdef RT_PROFILE
  total_time_ += other.total_time_;
  for (const auto& pair : other.opr_timers_) {
    opr_timers_[pair.first] += pair.second;
  }
  for (const auto& pair : other.routine_timers_) {
    routine_timers_[pair.first] += pair.second;
  }
#endif
  return *this;
}

static std::string get_opr_name(const physical::PhysicalOpr& opr) {
  switch (opr.opr().op_kind_case()) {
  case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
    return "scan";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
    return "edge_expand";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
    return "get_v";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kOrderBy: {
    return "order_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kProject: {
    return "project";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSink: {
    return "sink";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
    return "dedup";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kGroupBy: {
    return "group_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
    return "select";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kPath: {
    return "path";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kJoin: {
    return "join";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kRoot: {
    return "root";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kIntersect: {
    return "intersect";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kUnion: {
    return "union";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kUnfold: {
    return "unfold";
  }
  default:
    return "unknown - " +
           std::to_string(static_cast<int>(opr.opr().op_kind_case()));
  }
}

static bool is_shortest_path_with_order_by_limit(
    const physical::PhysicalPlan& plan, int i, int& path_len_alias,
    int& vertex_alias, int& limit_upper) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  int start_tag = opr.path().start_tag().value();
  // must be any shortest path
  if (opr.path().path_opt() !=
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ANY_SHORTEST ||
      opr.path().result_opt() !=
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V_E) {
    return false;
  }
  if (i + 4 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    const auto& get_v_filter_opr = plan.plan(i + 2).opr();
    const auto& select_opr = plan.plan(i + 3).opr();
    const auto& project_opr = plan.plan(i + 4).opr();
    const auto& order_by_opr = plan.plan(i + 5).opr();
    if (!get_v_opr.has_vertex() || !get_v_filter_opr.has_vertex() ||
        !project_opr.has_project() || !order_by_opr.has_order_by()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::END) {
      return false;
    }

    if (get_v_filter_opr.vertex().opt() != physical::GetV::ITSELF) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    int get_v_alias = get_v_opr.vertex().has_alias()
                          ? get_v_opr.vertex().alias().value()
                          : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }
    int get_v_filter_tag = get_v_filter_opr.vertex().has_tag()
                               ? get_v_filter_opr.vertex().tag().value()
                               : -1;
    if (get_v_filter_tag != get_v_alias && get_v_filter_tag != -1) {
      return false;
    }
    if (!select_opr.has_select()) {
      return false;
    }
    if (!select_opr.select().has_predicate()) {
      return false;
    }
    auto pred = select_opr.select().predicate();
    if (pred.operators_size() != 3) {
      return false;
    }
    if (!pred.operators(0).has_var() ||
        !(pred.operators(1).item_case() == common::ExprOpr::kLogical) ||
        pred.operators(1).logical() != common::Logical::NE ||
        !pred.operators(2).has_var()) {
      return false;
    }

    if (!pred.operators(0).var().has_tag() ||
        !pred.operators(2).var().has_tag()) {
      return false;
    }
    if (pred.operators(0).var().tag().id() != get_v_alias &&
        pred.operators(2).var().tag().id() != get_v_alias) {
      return false;
    }

    if (pred.operators(0).var().tag().id() != start_tag &&
        pred.operators(2).var().tag().id() != start_tag) {
      return false;
    }

    // only vertex and length(path)
    if (project_opr.project().mappings_size() != 2 ||
        project_opr.project().is_append()) {
      return false;
    }

    auto mapping = project_opr.project().mappings();
    if (!mapping[0].has_expr() || !mapping[1].has_expr()) {
      return false;
    }
    if (mapping[0].expr().operators_size() != 1 ||
        mapping[1].expr().operators_size() != 1) {
      return false;
    }
    if (!mapping[0].expr().operators(0).has_var() ||
        !mapping[1].expr().operators(0).has_var()) {
      return false;
    }
    if (!mapping[0].expr().operators(0).var().has_tag() ||
        !mapping[1].expr().operators(0).var().has_tag()) {
      return false;
    }
    common::Variable path_len_var0;
    common::Variable vertex_var;
    if (mapping[0].expr().operators(0).var().tag().id() == path_alias) {
      path_len_var0 = mapping[0].expr().operators(0).var();
      vertex_var = mapping[1].expr().operators(0).var();
      path_len_alias = mapping[0].alias().value();
      vertex_alias = mapping[1].alias().value();

    } else if (mapping[1].expr().operators(0).var().tag().id() == path_alias) {
      path_len_var0 = mapping[1].expr().operators(0).var();
      vertex_var = mapping[0].expr().operators(0).var();
      path_len_alias = mapping[1].alias().value();
      vertex_alias = mapping[0].alias().value();
    } else {
      return false;
    }
    if (!path_len_var0.has_property() || !path_len_var0.property().has_len()) {
      return false;
    }

    if (vertex_var.has_property()) {
      return false;
    }

    // must has order by limit
    if (!order_by_opr.order_by().has_limit()) {
      return false;
    }
    limit_upper = order_by_opr.order_by().limit().upper();
    if (order_by_opr.order_by().pairs_size() < 0) {
      return false;
    }
    if (!order_by_opr.order_by().pairs()[0].has_key()) {
      return false;
    }
    if (!order_by_opr.order_by().pairs()[0].key().has_tag()) {
      return false;
    }
    if (order_by_opr.order_by().pairs()[0].key().tag().id() != path_len_alias) {
      return false;
    }
    if (order_by_opr.order_by().pairs()[0].order() !=
        algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC) {
      return false;
    }
    return true;
  }
  return false;
}

static bool is_shortest_path(const physical::PhysicalPlan& plan, int i) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  // must be any shortest path
  if (opr.path().path_opt() !=
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ANY_SHORTEST ||
      opr.path().result_opt() !=
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V_E) {
    return false;
  }
  if (i + 2 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    const auto& get_v_filter_opr = plan.plan(i + 2).opr();
    if (!get_v_filter_opr.has_vertex() || !get_v_opr.has_vertex()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::END) {
      return false;
    }
    if (get_v_filter_opr.vertex().opt() != physical::GetV::ITSELF) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    int get_v_alias = get_v_opr.vertex().has_alias()
                          ? get_v_opr.vertex().alias().value()
                          : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }
    int get_v_filter_tag = get_v_filter_opr.vertex().has_tag()
                               ? get_v_filter_opr.vertex().tag().value()
                               : -1;
    if (get_v_filter_tag != get_v_alias && get_v_filter_tag != -1) {
      return false;
    }

    return true;
  }
  return false;
}

bool is_all_shortest_path(const physical::PhysicalPlan& plan, int i) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  if (opr.path().path_opt() !=
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ALL_SHORTEST ||
      opr.path().result_opt() !=
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V_E) {
    return false;
  }

  if (i + 2 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    const auto& get_v_filter_opr = plan.plan(i + 2).opr();
    if (!get_v_filter_opr.has_vertex() || !get_v_opr.has_vertex()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::END) {
      return false;
    }
    if (get_v_filter_opr.vertex().opt() != physical::GetV::ITSELF) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    int get_v_alias = get_v_opr.vertex().has_alias()
                          ? get_v_opr.vertex().alias().value()
                          : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }
    int get_v_filter_tag = get_v_filter_opr.vertex().has_tag()
                               ? get_v_filter_opr.vertex().tag().value()
                               : -1;
    if (get_v_filter_tag != get_v_alias && get_v_filter_tag != -1) {
      return false;
    }

    return true;
  }
  return false;
}

bool try_reuse_left_plan_column(const physical::Join& op, int& Tag,
                                int& Alias) {
  int left_key0 = op.left_keys(0).tag().id();
  int left_key1 = op.left_keys(1).tag().id();
  int right_key0 = op.right_keys(0).tag().id();
  int right_key1 = op.right_keys(1).tag().id();
  if (!op.right_keys(0).has_node_type() || !op.right_keys(1).has_node_type()) {
    return false;
  }
  if (op.right_keys(0).node_type().type_case() !=
          common::IrDataType::kGraphType ||
      op.right_keys(1).node_type().type_case() !=
          common::IrDataType::kGraphType) {
    return false;
  }

  if ((op.right_keys(0).node_type().graph_type().element_opt() !=
       common::GraphDataType::GraphElementOpt::
           GraphDataType_GraphElementOpt_VERTEX) ||
      (op.right_keys(1).node_type().graph_type().element_opt() !=
       common::GraphDataType::GraphElementOpt::
           GraphDataType_GraphElementOpt_VERTEX)) {
    return false;
  }
  if (op.right_keys(1).node_type().graph_type().graph_data_type_size() != 1 ||
      op.right_keys(0).node_type().graph_type().graph_data_type_size() != 1) {
    return false;
  }
  if (op.right_keys(0)
          .node_type()
          .graph_type()
          .graph_data_type(0)
          .label()
          .label() != op.right_keys(1)
                          .node_type()
                          .graph_type()
                          .graph_data_type(0)
                          .label()
                          .label()) {
    return false;
  }
  auto right_plan = op.right_plan();

  if (right_plan.plan(0).opr().has_scan()) {
    auto scan = right_plan.plan(0).opr().scan();
    int alias = -1;
    if (scan.has_alias()) {
      alias = scan.alias().value();
    }
    if (!(alias == right_key0 || alias == right_key1)) {
      return false;
    }
    if (alias == right_key0) {
      Tag = left_key0;
      Alias = alias;
    } else {
      Tag = left_key1;
      Alias = alias;
    }

    if (scan.has_idx_predicate()) {
      return false;
    }
    auto params = scan.params();
    if (params.has_predicate()) {
      return false;
    }
    if (params.tables_size() != 1) {
      return false;
    }
    int num = right_plan.plan().size();
    auto last_op = right_plan.plan(num - 1);

    if (last_op.opr().has_edge()) {
      auto edge = last_op.opr().edge();
      int alias = -1;
      if (edge.has_alias()) {
        alias = edge.alias().value();
      }
      if (alias != right_key0 && alias != right_key1) {
        return false;
      }
      if (edge.expand_opt() !=
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
        return false;
      }
      if (!edge.has_params()) {
        return false;
      }
      if (edge.params().tables_size() != 1) {
        return false;
      }

      if (edge.params().has_predicate()) {
        return false;
      }
      return true;
    }
  }
  return false;
}

Context runtime_eval_impl(const physical::PhysicalPlan& plan, Context&& ctx,
                          const GraphReadInterface& graph,
                          const std::map<std::string, std::string>& params,
                          OprTimer& timer, bool skip_scan = false) {
  Context ret = ctx;
  TimerUnit t;

  // LOG(INFO) << plan.DebugString();

  int opr_num = plan.plan_size();
  bool terminate = false;
  // in this case, we skip the first scan opr as it is already extracted from
  // the left plan
  int start_idx = skip_scan ? 1 : 0;
  for (int i = start_idx; i < opr_num; ++i) {
    const physical::PhysicalOpr& opr = plan.plan(i);
    assert(opr.has_opr());
    switch (opr.opr().op_kind_case()) {
    case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
      t.start();
      ret = eval_scan(opr.opr().scan(), graph, params, timer);
      timer.record_opr("scan", t);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
      CHECK_EQ(opr.meta_data_size(), 1);
      if ((i + 5) < opr_num && plan.plan(i + 1).opr().has_group_by() &&
          plan.plan(i + 2).opr().has_edge() &&
          plan.plan(i + 3).opr().has_vertex() &&
          plan.plan(i + 4).opr().has_edge() &&
          plan.plan(i + 5).opr().has_select() &&
          tc_fusable(opr.opr().edge(), plan.plan(i + 1).opr().group_by(),
                     plan.plan(i + 2).opr().edge(),
                     plan.plan(i + 3).opr().vertex(),
                     plan.plan(i + 4).opr().edge(),
                     plan.plan(i + 5).opr().select(), ret)) {
        t.start();
        ret = eval_tc(
            opr.opr().edge(), plan.plan(i + 1).opr().group_by(),
            plan.plan(i + 2).opr().edge(), plan.plan(i + 3).opr().vertex(),
            plan.plan(i + 4).opr().edge(), plan.plan(i + 5).opr().select(),
            graph, std::move(ret), params, opr.meta_data(0),
            plan.plan(i + 2).meta_data(0), plan.plan(i + 4).meta_data(0));
        timer.record_opr("edge_expand_tc", t);
        i += 5;
      } else if ((i + 1) < opr_num) {
        const physical::PhysicalOpr& next_opr = plan.plan(i + 1);
        if (next_opr.opr().has_vertex() &&
            edge_expand_get_v_fusable(opr.opr().edge(), next_opr.opr().vertex(),
                                      ret, opr.meta_data(0))) {
          t.start();
          ret = eval_edge_expand_get_v(
              opr.opr().edge(), next_opr.opr().vertex(), graph, std::move(ret),
              params, timer, opr.meta_data(0));
          timer.record_opr("edge_expand_get_v", t);
          ++i;
        } else {
          t.start();
          ret = eval_edge_expand(opr.opr().edge(), graph, std::move(ret),
                                 params, timer, opr.meta_data(0));
          timer.record_opr("edge_expand", t);
        }
      } else {
        t.start();
        ret = eval_edge_expand(opr.opr().edge(), graph, std::move(ret), params,
                               timer, opr.meta_data(0));
        timer.record_opr("edge_expand", t);
      }
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
      t.start();
      ret =
          eval_get_v(opr.opr().vertex(), graph, std::move(ret), params, timer);
      timer.record_opr("get_v", t);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kProject: {
      std::vector<common::IrDataType> data_types;
      if (opr.meta_data_size() == opr.opr().project().mappings_size()) {
        for (int i = 0; i < opr.meta_data_size(); ++i) {
          if (opr.meta_data(i).type().type_case() ==
              common::IrDataType::TypeCase::TYPE_NOT_SET) {
            // LOG(INFO) << "type not set";
          }
          data_types.push_back(opr.meta_data(i).type());
        }
      }
      if ((i + 1) < opr_num) {
        const physical::PhysicalOpr& next_opr = plan.plan(i + 1);
        if (next_opr.opr().has_order_by() &&
            project_order_by_fusable(opr.opr().project(),
                                     next_opr.opr().order_by(), ret,
                                     data_types)) {
          t.start();
          ret = eval_project_order_by(
              opr.opr().project(), next_opr.opr().order_by(), graph,
              std::move(ret), timer, params, data_types);
          timer.record_opr("project_order_by", t);
          ++i;
        } else {
          t.start();
          ret = eval_project(opr.opr().project(), graph, std::move(ret), params,
                             data_types);
          timer.record_opr("project", t);
        }
      } else {
        t.start();
        ret = eval_project(opr.opr().project(), graph, std::move(ret), params,
                           data_types);
        timer.record_opr("project", t);
      }
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kOrderBy: {
      t.start();
      ret = eval_order_by(opr.opr().order_by(), graph, std::move(ret), timer);
      timer.record_opr("order_by", t);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kGroupBy: {
      t.start();
      ret = eval_group_by(opr.opr().group_by(), graph, std::move(ret));
      timer.record_opr("group_by", t);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
      t.start();
      ret = eval_dedup(opr.opr().dedup(), graph, std::move(ret));
      timer.record_opr("dedup", t);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
      t.start();
      ret =
          eval_select(opr.opr().select(), graph, std::move(ret), params, timer);
      timer.record_opr("select", t);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kPath: {
      if ((i + 2) < opr_num) {
        int path_len_alias = -1;
        int vertex_alias = -1;
        int limit_upper = -1;
        if (is_shortest_path_with_order_by_limit(plan, i, path_len_alias,
                                                 vertex_alias, limit_upper)) {
          t.start();
          ret = eval_shortest_path_with_order_by_length_limit(
              opr.opr().path(), graph, std::move(ret), params, opr.meta_data(0),
              plan.plan(i + 2).opr().vertex(), vertex_alias, path_len_alias,
              limit_upper);
          timer.record_opr("shortest_path_with_order_by_length_limit", t);
          i += 4;
          break;
        }
        if (is_shortest_path(plan, i)) {
          auto vertex = plan.plan(i + 2).opr().vertex();
          int v_alias = -1;
          if (!vertex.has_alias()) {
            v_alias = plan.plan(i + 1).opr().vertex().has_alias()
                          ? plan.plan(i + 1).opr().vertex().alias().value()
                          : -1;
          } else {
            v_alias = vertex.alias().value();
          }
          t.start();
          ret = eval_shortest_path(opr.opr().path(), graph, std::move(ret),
                                   params, timer, opr.meta_data(0), vertex,
                                   v_alias);
          timer.record_opr("shortest_path", t);
          i += 2;
          break;
        } else if (is_all_shortest_path(plan, i)) {
          auto vertex = plan.plan(i + 2).opr().vertex();
          int v_alias = -1;
          if (!vertex.has_alias()) {
            v_alias = plan.plan(i + 1).opr().vertex().has_alias()
                          ? plan.plan(i + 1).opr().vertex().alias().value()
                          : -1;
          } else {
            v_alias = vertex.alias().value();
          }
          t.start();
          ret = eval_all_shortest_paths(opr.opr().path(), graph, std::move(ret),
                                        params, opr.meta_data(0), vertex,
                                        v_alias);
          timer.record_opr("#### all_shortest_paths", t);
          i += 2;
          break;
        }
      }
      if ((i + 1) < opr_num) {
        const physical::PhysicalOpr& next_opr = plan.plan(i + 1);
        if (next_opr.opr().has_vertex() &&
            opr.opr().path().result_opt() ==
                physical::PathExpand_ResultOpt::PathExpand_ResultOpt_END_V &&
            opr.opr().path().base().edge_expand().expand_opt() ==
                physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
          int alias = -1;
          if (next_opr.opr().vertex().has_alias()) {
            alias = next_opr.opr().vertex().alias().value();
          }
          t.start();
          ret = eval_path_expand_v(opr.opr().path(), graph, std::move(ret),
                                   params, timer, opr.meta_data(0), alias);
          timer.record_opr("path_expand_v", t);
          ++i;
        } else {
          int alias = -1;
          if (opr.opr().path().has_alias()) {
            alias = opr.opr().path().alias().value();
          }
          t.start();
          ret = eval_path_expand_p(opr.opr().path(), graph, std::move(ret),
                                   params, opr.meta_data(0), alias);
          timer.record_opr("path_expand_p", t);
        }
      } else {
        LOG(FATAL) << "not support";
      }
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kSink: {
      terminate = true;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kRoot: {
      // do nothing
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kJoin: {
      auto op = opr.opr().join();
      if (op.left_keys_size() == 2 && op.right_keys_size() == 2) {
        int tag = -1;
        int alias = -1;
        if (try_reuse_left_plan_column(op, tag, alias)) {
          auto ctx = runtime_eval_impl(op.left_plan(), std::move(ret), graph,
                                       params, timer);
          Context ctx2;
          std::vector<size_t> offset;
          ctx.get(tag)->generate_dedup_offset(offset);
          ctx2.set(alias, ctx.get(tag));
          ctx2.reshuffle(offset);
          ctx2 = runtime_eval_impl(op.right_plan(), std::move(ctx2), graph,
                                   params, timer, true);
          t.start();
          ret = eval_join(graph, params, op, std::move(ctx), std::move(ctx2));
          timer.record_opr("join_reuse", t);
          break;
        }
      }
      Context ret_dup(ret);
      auto ctx = runtime_eval_impl(op.left_plan(), std::move(ret), graph,
                                   params, timer);
      auto ctx2 = runtime_eval_impl(op.right_plan(), std::move(ret_dup), graph,
                                    params, timer);
      t.start();
      ret = eval_join(graph, params, op, std::move(ctx), std::move(ctx2));
      timer.record_opr("join", t);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kIntersect: {
      auto op = opr.opr().intersect();

      size_t num = op.sub_plans_size();
      std::vector<Context> ctxs;
      for (size_t i = 0; i < num; ++i) {
        Context n_ctx;
        n_ctx.set_prev_context(&ret);
        ctxs.push_back(runtime_eval_impl(op.sub_plans(i), std::move(n_ctx),
                                         graph, params, timer));
      }
      t.start();
      ret = eval_intersect(graph, op, std::move(ret), std::move(ctxs));
      timer.record_opr("intersect", t);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kLimit: {
      t.start();
      ret = eval_limit(opr.opr().limit(), std::move(ret));
      timer.record_opr("limit", t);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kUnfold: {
      t.start();
      ret = eval_unfold(opr.opr().unfold(), std::move(ret));
      timer.record_opr("unfold", t);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kUnion: {
      auto op = opr.opr().union_();
      size_t num = op.sub_plans_size();
      std::vector<Context> ctxs;
      for (size_t i = 0; i < num; ++i) {
        Context n_ctx = ret;

        ctxs.emplace_back(runtime_eval_impl(op.sub_plans(i), std::move(n_ctx),
                                            graph, params, timer));
      }
      t.start();
      ret = eval_union(std::move(ctxs));
      timer.record_opr("union", t);
    } break;
    default:
      LOG(FATAL) << "opr not support..." << get_opr_name(opr)
                 << opr.DebugString();
      break;
    }
    // LOG(INFO) << "after op - " << get_opr_name(opr);
    // ret.desc();
    if (terminate) {
      break;
    }
  }
  return ret;
}

Context runtime_eval(const physical::PhysicalPlan& plan,
                     const GraphReadInterface& graph,
                     const std::map<std::string, std::string>& params,
                     OprTimer& timer) {
  TimerUnit t;
  t.start();
  auto ret = runtime_eval_impl(plan, Context(), graph, params, timer);
  timer.add_total(t);
  return ret;
}

WriteContext runtime_eval_impl(const physical::PhysicalPlan& plan,
                               WriteContext&& ctx, GraphInsertInterface& graph,
                               const std::map<std::string, std::string>& params,
                               OprTimer& timer) {
  TimerUnit t;
  int opr_num = plan.plan_size();
  WriteContext ret = ctx;
  for (int i = 0; i < opr_num; ++i) {
    const physical::PhysicalOpr& opr = plan.plan(i);
    assert(opr.has_opr());
    switch (opr.opr().op_kind_case()) {
    case physical::PhysicalOpr_Operator::OpKindCase::kProject: {
      t.start();
      ret = eval_project(opr.opr().project(), graph, std::move(ret), params);
      timer.record_opr("project", t);
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kLoad: {
      t.start();
      ret = eval_load(opr.opr().load(), graph, std::move(ret), params);
      timer.record_opr("load", t);
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kSink: {
      t.start();
      graph.Commit();
      timer.record_opr("commit", t);
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kUnfold: {
      t.start();
      ret = eval_unfold(opr.opr().unfold(), std::move(ret));
      timer.record_opr("unfold", t);
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
      t.start();
      ret = eval_dedup(opr.opr().dedup(), graph, std::move(ret));
      timer.record_opr("dedup", t);
      break;
    }
    default: {
      LOG(FATAL) << "opr not support..." << opr.DebugString();
      break;
    }
    }
  }

  return ctx;
}
// for insert transaction
WriteContext runtime_eval(const physical::PhysicalPlan& plan,
                          GraphInsertInterface& graph,
                          const std::map<std::string, std::string>& params,
                          OprTimer& timer) {
  TimerUnit t;
  t.start();
  auto ret = runtime_eval_impl(plan, WriteContext(), graph, params, timer);
  timer.add_total(t);
  return ret;
}

}  // namespace runtime

}  // namespace gs