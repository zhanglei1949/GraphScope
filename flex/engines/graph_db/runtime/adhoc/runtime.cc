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
  default:
    return "unknown - " +
           std::to_string(static_cast<int>(opr.opr().op_kind_case()));
  }
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

Context runtime_eval_impl(const physical::PhysicalPlan& plan, Context&& ctx,
                          const ReadTransaction& txn,
                          const std::map<std::string, std::string>& params,
                          int op_id_offset = 0,
                          const std::string& prefix = "") {
  Context ret = ctx;

  auto& op_cost = OpCost::get().table;

  int opr_num = plan.plan_size();
  bool terminate = false;
  for (int i = 0; i < opr_num; ++i) {
    const physical::PhysicalOpr& opr = plan.plan(i);
    double t = -grape::GetCurrentTime();
    assert(opr.has_opr());
    std::string op_name =
        prefix + "|" + std::to_string(i) + ":" + get_opr_name(opr);
    switch (opr.opr().op_kind_case()) {
    case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
      ret = eval_scan(opr.opr().scan(), txn, params);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
      CHECK_EQ(opr.meta_data_size(), 1);
      if ((i + 1) < opr_num) {
        const physical::PhysicalOpr& next_opr = plan.plan(i + 1);
        if (next_opr.opr().has_vertex() &&
            edge_expand_get_v_fusable(opr.opr().edge(), next_opr.opr().vertex(),
                                      ret, opr.meta_data(0))) {
          ret = eval_edge_expand_get_v(
              opr.opr().edge(), next_opr.opr().vertex(), txn, std::move(ret),
              params, opr.meta_data(0), i + op_id_offset);
          op_name += "_get_v";
          ++i;
        } else {
          ret = eval_edge_expand(opr.opr().edge(), txn, std::move(ret), params,
                                 opr.meta_data(0), op_id_offset + i);
        }
      } else {
        ret = eval_edge_expand(opr.opr().edge(), txn, std::move(ret), params,
                               opr.meta_data(0), op_id_offset + i);
      }
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
      ret = eval_get_v(opr.opr().vertex(), txn, std::move(ret), params);
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
          ret = eval_project_order_by(opr.opr().project(),
                                      next_opr.opr().order_by(), txn,
                                      std::move(ret), params, data_types);
          op_name += "_order_by";
          ++i;
        } else {
          ret = eval_project(opr.opr().project(), txn, std::move(ret), params,
                             data_types);
        }
      } else {
        ret = eval_project(opr.opr().project(), txn, std::move(ret), params,
                           data_types);
      }
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kOrderBy: {
      ret = eval_order_by(opr.opr().order_by(), txn, std::move(ret));
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kGroupBy: {
      ret = eval_group_by(opr.opr().group_by(), txn, std::move(ret));
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
      ret = eval_dedup(opr.opr().dedup(), txn, std::move(ret));
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
      ret = eval_select(opr.opr().select(), txn, std::move(ret), params);
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kPath: {
      if ((i + 2) < opr_num) {
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
          ret = eval_shortest_path(opr.opr().path(), txn, std::move(ret),
                                   params, opr.meta_data(0), vertex, v_alias);
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
          ret = eval_all_shortest_paths(opr.opr().path(), txn, std::move(ret),
                                        params, opr.meta_data(0), vertex,
                                        v_alias);
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
          ret = eval_path_expand_v(opr.opr().path(), txn, std::move(ret),
                                   params, opr.meta_data(0), alias);
          ++i;
        } else {
          int alias = -1;
          if (opr.opr().path().has_alias()) {
            alias = opr.opr().path().alias().value();
          }
          ret = eval_path_expand_p(opr.opr().path(), txn, std::move(ret),
                                   params, opr.meta_data(0), alias);
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
      Context ret_dup;
      auto ctx = runtime_eval_impl(op.left_plan(), std::move(ret), txn, params,
                                   op_id_offset + 100, op_name + "-left");
      auto ctx2 =
          runtime_eval_impl(op.right_plan(), std::move(ret_dup), txn, params,
                            op_id_offset + 200, op_name + "-right");
      double tj = -grape::GetCurrentTime();
      ret = eval_join(op, std::move(ctx), std::move(ctx2));
      tj += grape::GetCurrentTime();
      op_cost[op_name + "-impl"] += tj;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kIntersect: {
      auto op = opr.opr().intersect();
      size_t num = op.sub_plans_size();
      std::vector<Context> ctxs;
      for (size_t i = 0; i < num; ++i) {
        Context n_ctx;
        n_ctx.set_prev_context(&ret);
        ctxs.push_back(runtime_eval_impl(op.sub_plans(i), std::move(n_ctx), txn,
                                         params, op_id_offset + i * 200,
                                         op_name + "-sub[" + std::to_string(i) +
                                             "/" + std ::to_string(num) + "]"));
      }
      ret = eval_intersect(txn, op, std::move(ret), std::move(ctxs));
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kLimit: {
      ret = eval_limit(opr.opr().limit(), std::move(ret));
    } break;

    default:
      LOG(FATAL) << "opr not support..." << get_opr_name(opr)
                 << opr.DebugString();
      break;
    }
    t += grape::GetCurrentTime();
    op_cost[op_name] += t;
    // LOG(INFO) << "after op - " << op_name;
    // ret.desc();
    if (terminate) {
      break;
    }
  }
  return ret;
}

Context runtime_eval(const physical::PhysicalPlan& plan,
                     const ReadTransaction& txn,
                     const std::map<std::string, std::string>& params) {
  double t = -grape::GetCurrentTime();
  auto ret = runtime_eval_impl(plan, Context(), txn, params);
  t += grape::GetCurrentTime();
  OpCost::get().add_total(t);
  return ret;
}

}  // namespace runtime

}  // namespace gs