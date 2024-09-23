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
    const algebra::Select& opr, const ReadTransaction& txn, Context&& ctx,
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

bool is_vertex_ne_id(const ReadTransaction& txn, const common::Expression& expr,
                     const Context& ctx,
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
  if (!txn.GetVertexIndex(label, oid, vid)) {
    return false;
  }

  return true;
}

Context eval_select_vertex_ne_id(
    const algebra::Select& opr, const ReadTransaction& txn, Context&& ctx,
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

Context eval_select(const algebra::Select& opr, const ReadTransaction& txn,
                    Context&& ctx,
                    const std::map<std::string, std::string>& params) {
  int vertex_tag = -1;
  int set_tag = -1;
  vid_t vid{};
  if (is_vertex_ne_id(txn, opr.predicate(), ctx, params, vertex_tag, vid)) {
    // LOG(INFO) << "Select vertex ne id";
    return eval_select_vertex_ne_id(opr, txn, std::move(ctx), params,
                                    vertex_tag, vid);
  }
  if (is_vertex_within_set(opr.predicate(), ctx, vertex_tag, set_tag)) {
    // LOG(INFO) << "Select vertex within set";
    return eval_select_vertex_within_set(opr, txn, std::move(ctx), params,
                                         vertex_tag, set_tag);
  }

  Expr expr(txn, ctx, params, opr.predicate(), VarType::kPathVar);

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
  return ctx;
}

}  // namespace runtime

}  // namespace gs