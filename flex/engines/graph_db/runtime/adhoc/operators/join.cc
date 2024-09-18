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

#include "flex/engines/graph_db/runtime/common/operators/join.h"
#include "flex/engines/graph_db/runtime/adhoc/expr.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {
namespace runtime {

struct condition_expr_base {
  virtual std::pair<label_t, vid_t> operator()(int i, int j) = 0;
  virtual ~condition_expr_base() {}
};

struct condition_left_vertex_expr : public condition_expr_base {
  std::pair<label_t, vid_t> operator()(int i, int j) {
    return col->get_vertex(i);
  }
  condition_left_vertex_expr(std::shared_ptr<IVertexColumn> col) : col(col) {}
  std::shared_ptr<IVertexColumn> col;
};

struct condition_right_vertex_expr : public condition_expr_base {
  std::pair<label_t, vid_t> operator()(int i, int j) {
    return col->get_vertex(j);
  }

  ~condition_right_vertex_expr() {}
  condition_right_vertex_expr(std::shared_ptr<IVertexColumn> col) : col(col) {}
  std::shared_ptr<IVertexColumn> col;
};

struct condition_left_relation_src_expr : public condition_expr_base {
  std::pair<label_t, vid_t> operator()(int i, int j) {
    auto val = col->get_value(i);
    return std::make_pair(val.label, val.src);
  }

  condition_left_relation_src_expr(std::shared_ptr<ValueColumn<Relation>> col)
      : col(col) {}
  std::shared_ptr<ValueColumn<Relation>> col;
};

struct condition_right_relation_src_expr : public condition_expr_base {
  std::pair<label_t, vid_t> operator()(int i, int j) {
    auto val = col->get_value(j);
    return std::make_pair(val.label, val.src);
  }

  condition_right_relation_src_expr(std::shared_ptr<ValueColumn<Relation>> col)
      : col(col) {}
  std::shared_ptr<ValueColumn<Relation>> col;
};

struct condition_left_relation_dst_expr : public condition_expr_base {
  std::pair<label_t, vid_t> operator()(int i, int j) {
    auto val = col->get_value(i);
    return std::make_pair(val.label, val.dst);
  }

  condition_left_relation_dst_expr(std::shared_ptr<ValueColumn<Relation>> col)
      : col(col) {}
  std::shared_ptr<ValueColumn<Relation>> col;
};

struct condition_right_relation_dst_expr : public condition_expr_base {
  std::pair<label_t, vid_t> operator()(int i, int j) {
    auto val = col->get_value(j);
    return std::make_pair(val.label, val.dst);
  }

  condition_right_relation_dst_expr(std::shared_ptr<ValueColumn<Relation>> col)
      : col(col) {}
  std::shared_ptr<ValueColumn<Relation>> col;
};

struct condition_expr {
  condition_expr() {}
  bool operator()(int i, int j) const {
    auto v0 = exprs[0]->operator()(i, j);
    auto v1 = exprs[1]->operator()(i, j);
    auto v2 = exprs[2]->operator()(i, j);
    auto v3 = exprs[3]->operator()(i, j);
    auto v4 = exprs[4]->operator()(i, j);
    auto v5 = exprs[5]->operator()(i, j);
    auto v6 = exprs[6]->operator()(i, j);
    auto v7 = exprs[7]->operator()(i, j);
    return (v0 == v1 && v2 == v3) || (v4 == v5 && v6 == v7);
  }
  std::array<std::unique_ptr<condition_expr_base>, 8> exprs;
};

auto parse_join_condition(const ReadTransaction& txn, const Context& ctx,
                          const Context& ctx2,
                          const std::map<std::string, std::string>& params,
                          const common::Expression& expr) {
  // int opr_num = expr.operators().size();
  auto var0 = expr.operators(0);
  CHECK(var0.has_var());
  CHECK(var0.var().has_tag());
  int tag0 = var0.var().tag().id();

  auto var1 = expr.operators(1);
  CHECK(var1.logical() == common::Logical::EQ);

  auto var2 = expr.operators(2);
  CHECK(var2.has_udf_func());
  auto func2 = var2.udf_func();
  CHECK(func2.name() == "gs.function.startNode");
  int tag2 = func2.parameters(0).operators(0).var().tag().id();

  auto var3 = expr.operators(3);
  CHECK(var3.logical() == common::Logical::AND);

  auto var4 = expr.operators(4);
  CHECK(var4.has_var());
  CHECK(var4.var().has_tag());
  int tag4 = var4.var().tag().id();

  auto var5 = expr.operators(5);
  CHECK(var5.logical() == common::Logical::EQ);

  auto var6 = expr.operators(6);
  CHECK(var6.has_udf_func());
  auto func6 = var6.udf_func();
  CHECK(func6.name() == "gs.function.endNode");
  int tag6 = func6.parameters(0).operators(0).var().tag().id();

  auto var7 = expr.operators(7);
  CHECK(var7.logical() == common::Logical::OR);

  auto var8 = expr.operators(8);
  CHECK(var8.has_var());
  CHECK(var8.var().has_tag());
  int tag8 = var8.var().tag().id();

  auto var9 = expr.operators(9);
  CHECK(var9.logical() == common::Logical::EQ);

  auto var10 = expr.operators(10);
  CHECK(var10.has_udf_func());
  auto func10 = var10.udf_func();
  CHECK(func10.name() == "gs.function.endNode");
  int tag10 = func10.parameters(0).operators(0).var().tag().id();

  auto var11 = expr.operators(11);
  CHECK(var11.logical() == common::Logical::AND);

  auto var12 = expr.operators(12);
  CHECK(var12.has_var());
  CHECK(var12.var().has_tag());
  int tag12 = var12.var().tag().id();

  auto var13 = expr.operators(13);
  CHECK(var13.logical() == common::Logical::EQ);

  auto var14 = expr.operators(14);
  CHECK(var14.has_udf_func());
  auto func14 = var14.udf_func();
  CHECK(func14.name() == "gs.function.startNode");
  int tag14 = func14.parameters(0).operators(0).var().tag().id();

  std::unique_ptr<condition_expr_base> expr0 = nullptr;
  if (tag0 < (int) ctx.col_num() && ctx.get(tag0)) {
    auto col = std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag0));
    expr0 = std::make_unique<condition_left_vertex_expr>(col);

  } else {
    LOG(INFO) << ctx2.get(tag0)->column_info();
    auto col = std::dynamic_pointer_cast<IVertexColumn>(ctx2.get(tag0));

    expr0 = std::make_unique<condition_right_vertex_expr>(col);
  }

  std::unique_ptr<condition_expr_base> expr2 = nullptr;
  if (tag2 < (int) ctx.col_num() && ctx.get(tag2)) {
    auto col = std::dynamic_pointer_cast<ValueColumn<Relation>>(ctx.get(tag2));
    expr2 = std::make_unique<condition_left_relation_src_expr>(col);
  } else {
    auto col = std::dynamic_pointer_cast<ValueColumn<Relation>>(ctx2.get(tag2));
    expr2 = std::make_unique<condition_right_relation_src_expr>(col);
  }

  std::unique_ptr<condition_expr_base> expr4 = nullptr;
  if (tag4 < (int) ctx.col_num() && ctx.get(tag4)) {
    auto col = std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag4));
    expr4 = std::make_unique<condition_left_vertex_expr>(col);
  } else {
    auto col = std::dynamic_pointer_cast<IVertexColumn>(ctx2.get(tag4));
    expr4 = std::make_unique<condition_right_vertex_expr>(col);
  }

  std::unique_ptr<condition_expr_base> expr6 = nullptr;
  if (tag6 < (int) ctx.col_num() && ctx.get(tag6)) {
    auto col = std::dynamic_pointer_cast<ValueColumn<Relation>>(ctx.get(tag6));
    expr6 = std::make_unique<condition_left_relation_dst_expr>(col);
  } else {
    auto col = std::dynamic_pointer_cast<ValueColumn<Relation>>(ctx2.get(tag6));
    expr6 = std::make_unique<condition_right_relation_dst_expr>(col);
  }

  std::unique_ptr<condition_expr_base> expr8 = nullptr;
  if (tag8 < (int) ctx.col_num() && ctx.get(tag8)) {
    auto col = std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag8));
    expr8 = std::make_unique<condition_left_vertex_expr>(col);
  } else {
    auto col = std::dynamic_pointer_cast<IVertexColumn>(ctx2.get(tag8));
    expr8 = std::make_unique<condition_right_vertex_expr>(col);
  }

  std::unique_ptr<condition_expr_base> expr10 = nullptr;
  if (tag10 < (int) ctx.col_num() && ctx.get(tag10)) {
    auto col = std::dynamic_pointer_cast<ValueColumn<Relation>>(ctx.get(tag10));
    expr10 = std::make_unique<condition_left_relation_dst_expr>(col);
  } else {
    auto col =
        std::dynamic_pointer_cast<ValueColumn<Relation>>(ctx2.get(tag10));
    expr10 = std::make_unique<condition_right_relation_dst_expr>(col);
  }

  std::unique_ptr<condition_expr_base> expr12 = nullptr;
  if (tag12 < (int) ctx.col_num() && ctx.get(tag12)) {
    auto col = std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag12));
    expr12 = std::make_unique<condition_left_vertex_expr>(col);
  } else {
    auto col = std::dynamic_pointer_cast<IVertexColumn>(ctx2.get(tag12));
    expr12 = std::make_unique<condition_right_vertex_expr>(col);
  }

  std::unique_ptr<condition_expr_base> expr14 = nullptr;
  if (tag14 < (int) ctx.col_num() && ctx.get(tag14)) {
    auto col = std::dynamic_pointer_cast<ValueColumn<Relation>>(ctx.get(tag14));
    expr14 = std::make_unique<condition_left_relation_src_expr>(col);
  } else {
    auto col =
        std::dynamic_pointer_cast<ValueColumn<Relation>>(ctx2.get(tag14));
    expr14 = std::make_unique<condition_right_relation_src_expr>(col);
  }

  condition_expr pred;
  pred.exprs[0] = std::move(expr0);
  pred.exprs[1] = std::move(expr2);
  pred.exprs[2] = std::move(expr4);
  pred.exprs[3] = std::move(expr6);
  pred.exprs[4] = std::move(expr8);
  pred.exprs[5] = std::move(expr10);
  pred.exprs[6] = std::move(expr12);
  pred.exprs[7] = std::move(expr14);
  return pred;
}

Context eval_join(const ReadTransaction& txn,
                  const std::map<std::string, std::string>& params,
                  const physical::Join& opr, Context&& ctx, Context&& ctx2) {
  JoinParams p;

  auto left_keys = opr.left_keys();
  for (int i = 0; i < left_keys.size(); i++) {
    if (!left_keys.Get(i).has_tag()) {
      LOG(FATAL) << "left_keys should have tag";
    }
    p.left_columns.push_back(left_keys.Get(i).tag().id());
  }
  auto right_keys = opr.right_keys();
  for (int i = 0; i < right_keys.size(); i++) {
    if (!right_keys.Get(i).has_tag()) {
      LOG(FATAL) << "right_keys should have tag";
    }
    p.right_columns.push_back(right_keys.Get(i).tag().id());
  }
  if (opr.has_condition()) {
    // ctx2.set_prev_context(&ctx);
    LOG(INFO) << "join with condition" << opr.condition().DebugString();
    auto pred = parse_join_condition(txn, ctx, ctx2, params, opr.condition());
    if (opr.join_kind() == physical::Join_JoinKind::Join_JoinKind_INNER) {
      p.join_type = JoinKind::kInnerJoin;
    }
    return Join::join(std::move(ctx), std::move(ctx2), p, pred);
  }
  switch (opr.join_kind()) {
  case physical::Join_JoinKind::Join_JoinKind_INNER:
    p.join_type = JoinKind::kInnerJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_SEMI:
    p.join_type = JoinKind::kSemiJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_ANTI:
    p.join_type = JoinKind::kAntiJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER:
    p.join_type = JoinKind::kLeftOuterJoin;
    break;
  default:
    LOG(FATAL) << "unsupported join kind" << opr.join_kind();
  }
  return Join::join(std::move(ctx), std::move(ctx2), p);
}
}  // namespace runtime
}  // namespace gs