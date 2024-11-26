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

#include "flex/engines/graph_db/runtime/adhoc/expr.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_property_columns.h"

namespace gs {

namespace runtime {
bool is_vertex_within_set(const common::Expression& expr, const Context& ctx,
                          int& vertex_tag, int& set_tag) {
  if (expr.operators_size() != 3) {
    return false;
  }
  if (expr.operators(1).item_case() != common::ExprOpr::kLogical) {
    return false;
  }
  if (expr.operators(1).logical() != common::WITHIN) {
    return false;
  }
  if ((!expr.operators(0).has_var()) || (!expr.operators(2).has_var())) {
    return false;
  }
  if (!expr.operators(0).var().has_tag() ||
      !expr.operators(2).var().has_tag()) {
    return false;
  }
  vertex_tag = expr.operators(0).var().tag().id();
  set_tag = expr.operators(2).var().tag().id();
  if (ctx.get(vertex_tag)->column_type() != ContextColumnType::kVertex ||
      ctx.get(set_tag)->column_type() != ContextColumnType::kValue) {
    return false;
  }
  if (!(ctx.get(set_tag)->elem_type() == RTAnyType::kSet)) {
    return false;
  }
  return true;
}

Context eval_select_vertex_within_set(
    const algebra::Select& opr, const GraphReadInterface& graph, Context&& ctx,
    const std::map<std::string, std::string>& params, int vertex_tag,
    int set_tag) {
  std::vector<size_t> offsets;
  auto& vertex_col =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(vertex_tag));
  auto& set_col = *std::dynamic_pointer_cast<SetValueColumn<VertexRecord>>(
      ctx.get(set_tag));
  size_t row_num = ctx.row_num();
  for (size_t i = 0; i < row_num; ++i) {
    auto vertex = vertex_col.get_vertex(i);
    auto set = set_col.get_value(i);
    auto ptr = dynamic_cast<SetImpl<VertexRecord>*>(set.impl_);
    if (ptr->exists(vertex)) {
      offsets.push_back(i);
    }
  }
  ctx.reshuffle(offsets);
  return ctx;
}

bool is_date_within(const algebra::Select& opr, const GraphReadInterface& graph,
                    const Context& ctx,
                    const std::map<std::string, std::string>& params,
                    int vertex_tag, int& month) {
  if (opr.predicate().operators_size() != 27) {
    return false;
  }
  if (!((opr.predicate().operators(0).item_case() ==
         common::ExprOpr::ItemCase::kExtract) &&
        (opr.predicate().operators(0).extract().interval() ==
         common::Extract::MONTH))) {
    return false;
  }
  if (!((opr.predicate().operators(5).item_case() ==
         common::ExprOpr::ItemCase::kExtract) &&
        (opr.predicate().operators(5).extract().interval() ==
         common::Extract::DAY))) {
    return false;
  }
  if (!((opr.predicate().operators(10).item_case() ==
         common::ExprOpr::ItemCase::kExtract) &&
        (opr.predicate().operators(10).extract().interval() ==
         common::Extract::MONTH))) {
    return false;
  }
  if (!((opr.predicate().operators(23).item_case() ==
         common::ExprOpr::ItemCase::kExtract) &&
        (opr.predicate().operators(23).extract().interval() ==
         common::Extract::DAY))) {
    return false;
  }

  if (!opr.predicate().operators(1).has_var() ||
      !opr.predicate().operators(6).has_var() ||
      !opr.predicate().operators(11).has_var() ||
      !opr.predicate().operators(24).has_var()) {
    return false;
  }
  vertex_tag = opr.predicate().operators(1).var().tag().id();
  if (opr.predicate().operators(6).var().tag().id() != vertex_tag ||
      opr.predicate().operators(11).var().tag().id() != vertex_tag ||
      opr.predicate().operators(24).var().tag().id() != vertex_tag) {
    return false;
  }

  if (!opr.predicate().operators(3).has_param() ||
      !opr.predicate().operators(14).has_param()) {
    return false;
  }
  month = std::stoi(params.at(opr.predicate().operators(3).param().name()));
  // TODO: other conditions
  return true;
}

bool is_vertex_ne_id(const GraphReadInterface& graph,
                     const common::Expression& expr, const Context& ctx,
                     const std::map<std::string, std::string>& params,
                     int& vertex_tag, vid_t& vid) {
  if (expr.operators_size() != 3) {
    return false;
  }
  if (expr.operators(1).item_case() != common::ExprOpr::kLogical) {
    return false;
  }
  if (expr.operators(1).logical() != common::NE) {
    return false;
  }
  if ((!expr.operators(0).has_var())) {
    return false;
  }
  if (!expr.operators(0).var().has_tag()) {
    return false;
  }
  vertex_tag = expr.operators(0).var().tag().id();
  if (expr.operators(2).item_case() != common::ExprOpr::kParam) {
    return false;
  }
  if (ctx.get(vertex_tag)->column_type() != ContextColumnType::kVertex) {
    return false;
  }
  int64_t oid = std::stoll(params.at(expr.operators(2).param().name()));
  auto& vertex_col =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(vertex_tag));
  if (vertex_col.get_labels_set().size() != 1) {
    return false;
  }
  auto label = *vertex_col.get_labels_set().begin();
  if (!graph.GetVertexIndex(label, oid, vid)) {
    return false;
  }

  return true;
}

Context eval_select_vertex_ne_id(
    const algebra::Select& opr, const GraphReadInterface& graph, Context&& ctx,
    const std::map<std::string, std::string>& params, int vertex_tag,
    vid_t vid) {
  std::vector<size_t> offsets;
  auto& vertex_col =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(vertex_tag));
  size_t row_num = ctx.row_num();
  for (size_t i = 0; i < row_num; ++i) {
    auto vertex = vertex_col.get_vertex(i);
    if (vertex.vid_ != vid) {
      offsets.push_back(i);
    }
  }
  ctx.reshuffle(offsets);
  return ctx;
}

bool date_within(Day ts, int month, int next_month) {
  // struct tm tm;
  // auto micro_second = ts / 1000;
  // gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
  int m = ts.month();
  int d = ts.day();
  return (m == month && d >= 21) || (m == next_month && d < 22);
}

Context eval_select_date_within(
    const algebra::Select& opr, const GraphReadInterface& graph, Context&& ctx,
    const std::map<std::string, std::string>& params, int date_tag, int month) {
  std::vector<size_t> offsets;
  // auto& date_col =
  //     *std::dynamic_pointer_cast<ValueColumn<Day>>(ctx.get(date_tag));
  auto& date_col = *std::dynamic_pointer_cast<SLVertexPropertyColumn<Day>>(
      ctx.get(date_tag));

  size_t row_num = ctx.row_num();
  int next_month = (month % 12) + 1;
  for (size_t i = 0; i < row_num; ++i) {
    Day ts = date_col.get_value(i);
    if (date_within(ts, month, next_month)) {
      offsets.push_back(i);
    }
  }
  ctx.reshuffle(offsets);
  return ctx;
}

Context eval_select(const algebra::Select& opr, const GraphReadInterface& graph,
                    Context&& ctx,
                    const std::map<std::string, std::string>& params,
                    OprTimer& timer) {
  int vertex_tag = -1;
  int set_tag = -1;
  vid_t vid{};
  TimerUnit t;
  t.start();
  if (is_vertex_ne_id(graph, opr.predicate(), ctx, params, vertex_tag, vid)) {
    auto ret = eval_select_vertex_ne_id(opr, graph, std::move(ctx), params,
                                        vertex_tag, vid);
    timer.record_routine("select::vertex_ne_id", t);
    return ret;
  }
  if (is_vertex_within_set(opr.predicate(), ctx, vertex_tag, set_tag)) {
    auto ret = eval_select_vertex_within_set(opr, graph, std::move(ctx), params,
                                             vertex_tag, set_tag);
    timer.record_routine("select::vertex_within_set", t);
    return ret;
  }
  int date_tag = -1;
  int month = -1;
  if (is_date_within(opr, graph, ctx, params, date_tag, month)) {
    auto ret = eval_select_date_within(opr, graph, std::move(ctx), params,
                                       date_tag, month);
    timer.record_routine("select::date_within", t);
    return ret;
  }

  Expr expr(graph, ctx, params, opr.predicate(), VarType::kPathVar);

  std::vector<size_t> offsets;
  size_t row_num = ctx.row_num();
  if (expr.is_optional()) {
    for (size_t i = 0; i < row_num; ++i) {
      if (expr.eval_path(i, 0).is_null()) {
        continue;
      } else if (expr.eval_path(i, 0).as_bool()) {
        offsets.push_back(i);
      }
    }
  } else {
    for (size_t i = 0; i < row_num; ++i) {
      if (expr.eval_path(i).as_bool()) {
        offsets.push_back(i);
      }
    }
  }

  ctx.reshuffle(offsets);
  timer.record_routine("select::default", t);
  return ctx;
}

}  // namespace runtime

}  // namespace gs
