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
#include "flex/engines/graph_db/runtime/adhoc/utils.h"

namespace gs {

namespace runtime {

bool exchange_tag_alias(const physical::Project_ExprAlias& m, int& tag,
                        int& alias) {
  auto expr = m.expr();
  if (expr.operators().size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kVar) {
    auto var = expr.operators(0).var();
    tag = -1;
    if (var.has_tag() && !var.has_property()) {
      tag = var.tag().id();
    }
    alias = -1;
    if (m.has_alias()) {
      alias = m.alias().value();
    }
    if (tag == alias) {
      return true;
    }
  }
  return false;
}

Context eval_project(const physical::Project& opr, const ReadTransaction& txn,
                     Context&& ctx,
                     const std::map<std::string, std::string>& params,
                     const std::vector<common::IrDataType>& data_types) {
  bool is_append = opr.is_append();
  Context ret;
  if (is_append) {
    ret = std::move(ctx);
  }
  int mappings_size = opr.mappings_size();
  size_t row_num = ctx.row_num();
  std::vector<size_t> alias_ids;
  if (static_cast<size_t>(mappings_size) == data_types.size()) {
    for (int i = 0; i < mappings_size; ++i) {
      const physical::Project_ExprAlias& m = opr.mappings(i);
      {
        int tag, alias;
        if (exchange_tag_alias(m, tag, alias)) {
          alias_ids.push_back(alias);
          ret.set(alias, ctx.get(tag));
          continue;
        }
      }
      Expr expr(txn, ctx, params, m.expr(), VarType::kPathVar);
      int alias = -1;
      if (m.has_alias()) {
        alias = m.alias().value();
      }
      alias_ids.push_back(alias);
      auto col = build_column(data_types[i], expr, row_num);
      ret.set(alias, col);
    }
  } else {
    for (int i = 0; i < mappings_size; ++i) {
      const physical::Project_ExprAlias& m = opr.mappings(i);
      {
        int tag, alias;
        if (exchange_tag_alias(m, tag, alias)) {
          ret.set(alias, ctx.get(tag));
          alias_ids.push_back(alias);
          continue;
        }
      }

      Expr expr(txn, ctx, params, m.expr(), VarType::kPathVar);
      int alias = -1;
      if (m.has_alias()) {
        alias = m.alias().value();
      }
      alias_ids.push_back(alias);
      auto col = build_column_beta(expr, row_num);
      ret.set(alias, col);
    }
  }
  ret.update_tag_ids(alias_ids);

  return ret;
}

bool project_order_by_fusable(
    const physical::Project& project_opr, const algebra::OrderBy& order_by_opr,
    const Context& ctx, const std::vector<common::IrDataType>& data_types) {
  if (project_opr.is_append()) {
    // LOG(INFO) << "is append, fallback";
    return false;
  }

  int mappings_size = project_opr.mappings_size();
  if (static_cast<size_t>(mappings_size) != data_types.size()) {
    // LOG(INFO) << "mappings size not consistent with data types, fallback";
    return false;
  }

  std::set<int> new_generate_columns;
  for (int i = 0; i < mappings_size; ++i) {
    const physical::Project_ExprAlias& m = project_opr.mappings(i);
    if (m.has_alias()) {
      int alias = m.alias().value();
      if (ctx.exist(alias)) {
        // LOG(INFO) << "overwrite column, fallback";
        return false;
      }
      if (new_generate_columns.find(alias) != new_generate_columns.end()) {
        // LOG(INFO) << "multiple mappings with same alias, fallback";
        return false;
      }
      new_generate_columns.insert(alias);
    }
  }

  int order_by_keys_num = order_by_opr.pairs_size();
  std::set<int> order_by_keys;
  for (int k_i = 0; k_i < order_by_keys_num; ++k_i) {
    if (!order_by_opr.pairs(k_i).has_key()) {
      // LOG(INFO) << "order by - " << k_i << " -th pair has no key, fallback";
      return false;
    }
    if (!order_by_opr.pairs(k_i).key().has_tag()) {
      // LOG(INFO) << "order by - " << k_i << " -th pair has no tag, fallback";
      return false;
    }
    if (!order_by_opr.pairs(k_i).key().tag().has_id()) {
      // LOG(INFO) << "order by - " << k_i << " -th pair has no id, fallback";
      return false;
    }
    order_by_keys.insert(order_by_opr.pairs(k_i).key().tag().id());
  }
  if (data_types.size() == order_by_keys.size()) {
    // LOG(INFO)
    //     << "all column is required, partial project is not needed, fallback";
    return false;
  }
  for (auto key : order_by_keys) {
    if (new_generate_columns.find(key) == new_generate_columns.end() &&
        !ctx.exist(key)) {
      // LOG(INFO) << "missing key column for order by, fallback";
      return false;
    }
  }

  return true;
}

Context eval_project_order_by(
    const physical::Project& project_opr, const algebra::OrderBy& order_by_opr,
    const ReadTransaction& txn, Context&& ctx,
    const std::map<std::string, std::string>& params,
    const std::vector<common::IrDataType>& data_types) {
  double t0 = -grape::GetCurrentTime();
  int mappings_size = project_opr.mappings_size();

  int order_by_keys_num = order_by_opr.pairs_size();
  std::set<int> order_by_keys;
  for (int k_i = 0; k_i < order_by_keys_num; ++k_i) {
    order_by_keys.insert(order_by_opr.pairs(k_i).key().tag().id());
  }

  std::vector<int> added_alias_in_preproject;
  size_t row_num = ctx.row_num();
  for (int i = 0; i < mappings_size; ++i) {
    const physical::Project_ExprAlias& m = project_opr.mappings(i);
    if (m.has_alias() &&
        order_by_keys.find(m.alias().value()) != order_by_keys.end()) {
      {
        int alias = m.alias().value();
        CHECK(!ctx.exist(alias));
        Expr expr(txn, ctx, params, m.expr(), VarType::kPathVar);
        auto col = build_column(data_types[i], expr, row_num);
        ctx.set(alias, col);
        added_alias_in_preproject.push_back(alias);
      }
    }
  }

  t0 += grape::GetCurrentTime();

  double t1 = -grape::GetCurrentTime();
  ctx = eval_order_by(order_by_opr, txn, std::move(ctx));
  t1 += grape::GetCurrentTime();

  double t2 = -grape::GetCurrentTime();
  row_num = ctx.row_num();
  Context ret;

  std::vector<size_t> tags;
  for (int i = 0; i < mappings_size; ++i) {
    const physical::Project_ExprAlias& m = project_opr.mappings(i);
    if (!(m.has_alias() &&
          order_by_keys.find(m.alias().value()) != order_by_keys.end())) {
      int alias = m.alias().value();
      Expr expr(txn, ctx, params, m.expr(), VarType::kPathVar);
      auto col = build_column(data_types[i], expr, row_num);
      ret.set(alias, col);
      tags.push_back(alias);
    } else if (m.has_alias()) {
      int alias = m.alias().value();
      tags.push_back(alias);
    }
  }
  for (auto alias : added_alias_in_preproject) {
    ret.set(alias, ctx.get(alias));
  }
  ret.update_tag_ids(tags);
  t2 += grape::GetCurrentTime();

  auto& op_cost = OpCost::get().table;
  op_cost["project_order_by:partial_project"] += t0;
  op_cost["project_order_by:order_by"] += t1;
  op_cost["project_order_by:project"] += t2;

  return ret;
}

}  // namespace runtime

}  // namespace gs