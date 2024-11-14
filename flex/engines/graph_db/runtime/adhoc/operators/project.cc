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

bool is_extract_property(const common::Expression& expr, int& tag,
                         std::string& name) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kVar) {
    auto var = expr.operators(0).var();
    if (var.has_tag() && var.has_property()) {
      tag = var.tag().id();
      name = var.property().key().name();
      if (name == "id" || name == "label") {
        return false;
      }
      return true;
    }
  }
  return false;
}

bool is_check_property_in_range(const common::Expression& expr,
                                const std::map<std::string, std::string>& param,
                                int& tag, std::string& name, std::string& lower,
                                std::string& upper, common::Value& then_value,
                                common::Value& else_value) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kCase) {
    auto opr = expr.operators(0).case_();
    if (opr.when_then_expressions_size() != 1) {
      return false;
    }
    auto when = opr.when_then_expressions(0).when_expression();
    if (when.operators_size() != 7) {
      return false;
    }
    {
      if (!when.operators(0).has_var()) {
        return false;
      }
      auto var = when.operators(0).var();
      if (!var.has_tag()) {
        return false;
      }
      tag = var.tag().id();
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key()) {
        return false;
      }
      name = var.property().key().name();
    }
    {
      auto op = when.operators(1);
      if (op.item_case() != common::ExprOpr::kLogical ||
          op.logical() != common::GE) {
        return false;
      }
    }
    auto lower_param = when.operators(2);
    if (lower_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    lower = param.at(lower_param.param().name());
    {
      auto op = when.operators(3);
      if (op.item_case() != common::ExprOpr::kLogical ||
          op.logical() != common::AND) {
        return false;
      }
    }
    {
      if (!when.operators(4).has_var()) {
        return false;
      }
      auto var = when.operators(4).var();
      if (!var.has_tag()) {
        return false;
      }
      if (var.tag().id() != tag) {
        return false;
      }
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key() && name != var.property().key().name()) {
        return false;
      }
    }

    auto op = when.operators(5);
    if (op.item_case() != common::ExprOpr::kLogical ||
        op.logical() != common::LT) {
      return false;
    }
    auto upper_param = when.operators(6);
    if (upper_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    upper = param.at(upper_param.param().name());
    auto then = opr.when_then_expressions(0).then_result_expression();
    if (then.operators_size() != 1) {
      return false;
    }
    if (!then.operators(0).has_const_()) {
      return false;
    }
    then_value = then.operators(0).const_();
    auto else_expr = opr.else_result_expression();
    if (else_expr.operators_size() != 1) {
      return false;
    }
    if (!else_expr.operators(0).has_const_()) {
      return false;
    }
    else_value = else_expr.operators(0).const_();

    return true;
  }
  return false;
}

bool is_check_property_lt(const common::Expression& expr,
                          const std::map<std::string, std::string>& param,
                          int& tag, std::string& name, std::string& upper,
                          common::Value& then_value,
                          common::Value& else_value) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kCase) {
    auto opr = expr.operators(0).case_();
    if (opr.when_then_expressions_size() != 1) {
      return false;
    }
    auto when = opr.when_then_expressions(0).when_expression();
    if (when.operators_size() != 3) {
      return false;
    }
    {
      if (!when.operators(0).has_var()) {
        return false;
      }
      auto var = when.operators(0).var();
      if (!var.has_tag()) {
        return false;
      }
      tag = var.tag().id();
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key()) {
        return false;
      }
      name = var.property().key().name();
    }
    {
      auto op = when.operators(1);
      if (op.item_case() != common::ExprOpr::kLogical ||
          op.logical() != common::LT) {
        return false;
      }
    }
    auto upper_param = when.operators(2);
    if (upper_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    upper = param.at(upper_param.param().name());
    auto then = opr.when_then_expressions(0).then_result_expression();
    if (then.operators_size() != 1) {
      return false;
    }
    if (!then.operators(0).has_const_()) {
      return false;
    }
    then_value = then.operators(0).const_();
    auto else_expr = opr.else_result_expression();
    if (else_expr.operators_size() != 1) {
      return false;
    }
    if (!else_expr.operators(0).has_const_()) {
      return false;
    }
    else_value = else_expr.operators(0).const_();

    return true;
  }
  return false;
}

Context eval_project(const physical::Project& opr,
                     const GraphReadInterface& graph, Context&& ctx,
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

      // has no effect here
      /**
      {
        int tag;
        std::string name;
        bool flag = false;
        if (is_extract_property(m.expr(), tag, name)) {
          if (ctx.get(tag)->column_type() == ContextColumnType::kVertex) {
            auto vertex_col =
                std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag));

            if (vertex_col->get_labels_set().size() == 1) {
              CHECK(!vertex_col->is_optional());
              auto label = *vertex_col->get_labels_set().begin();
              if (data_types[i].type_case() == common::IrDataType::kDataType) {
                switch (data_types[i].data_type()) {
                case common::DataType::STRING: {
                  auto col = graph.get_vertex_property_column(label, name);
                  if (col == nullptr) {
                    OptionalValueColumnBuilder<std::string_view> builder;
                    builder.reserve(row_num);
                    for (size_t k = 0; k < row_num; ++k) {
                      builder.push_back_null();
                    }
                    ret.set(m.alias().value(), builder.finish());
                    alias_ids.push_back(m.alias().value());
                    flag = true;
                    break;
                  }
                  ValueColumnBuilder<std::string_view> builder;
                  builder.reserve(row_num);
                  if (col->type() == PropertyType::kStringView) {
                    auto typed_col =
                        std::dynamic_pointer_cast<StringColumn>(col);
                    for (size_t k = 0; k < row_num; ++k) {
                      auto vertex = vertex_col->get_vertex(k);
                      //                      LOG(INFO) << vertex.second;
                      auto prop = typed_col->get_view(vertex.second);
                      builder.push_back_opt(prop);
                    }
                    ret.set(m.alias().value(), builder.finish());
                    alias_ids.push_back(m.alias().value());
                    flag = true;
                  }
                } break;

                case common::DataType::INT32: {
                  ValueColumnBuilder<int32_t> builder;
                  builder.reserve(row_num);
                  auto col = graph.get_vertex_property_column(label, name);
                  if (col->type() == PropertyType::kInt32) {
                    auto typed_col = std::dynamic_pointer_cast<IntColumn>(col);
                    for (size_t k = 0; k < row_num; ++k) {
                      auto vertex = vertex_col->get_vertex(k);
                      auto prop = typed_col->get_view(vertex.second);
                      builder.push_back_opt(prop);
                    }
                    ret.set(m.alias().value(), builder.finish());
                    alias_ids.push_back(m.alias().value());
                    flag = true;
                  }
                } break;

                case common::DataType::INT64: {
                  ValueColumnBuilder<int64_t> builder;
                  builder.reserve(row_num);
                  // LOG(INFO) << (int) label << " " << name;
                  auto col = graph.get_vertex_property_column(label, name);
                  if (col->type() == PropertyType::kInt64) {
                    auto typed_col = std::dynamic_pointer_cast<LongColumn>(col);
                    for (size_t k = 0; k < row_num; ++k) {
                      auto vertex = vertex_col->get_vertex(k);
                      auto prop = typed_col->get_view(vertex.second);
                      builder.push_back_opt(prop);
                    }
                    ret.set(m.alias().value(), builder.finish());
                    alias_ids.push_back(m.alias().value());
                    flag = true;
                  } else if (col->type() == PropertyType::kDate) {
                    auto typed_col =
                        std::dynamic_pointer_cast<TypedColumn<Date>>(col);
                    for (size_t k = 0; k < row_num; ++k) {
                      auto vertex = vertex_col->get_vertex(k);
                      auto prop =
                          typed_col->get_view(vertex.second).milli_second;
                      builder.push_back_opt(prop);
                    }
                    ret.set(m.alias().value(), builder.finish());
                    alias_ids.push_back(m.alias().value());
                    flag = true;
                  }
                } break;

                case common::DataType::DOUBLE: {
                  ValueColumnBuilder<double> builder;
                  builder.reserve(row_num);
                  auto col = graph.get_vertex_property_column(label, name);
                  if (col->type() == PropertyType::kDouble) {
                    auto typed_col =
                        std::dynamic_pointer_cast<DoubleColumn>(col);
                    for (size_t k = 0; k < row_num; ++k) {
                      auto vertex = vertex_col->get_vertex(k);
                      auto prop = typed_col->get_view(vertex.second);
                      builder.push_back_opt(prop);
                    }
                    ret.set(m.alias().value(), builder.finish());
                    alias_ids.push_back(m.alias().value());
                    flag = true;
                  }
                } break;
                case common::DataType::TIMESTAMP: {
                  ValueColumnBuilder<int64_t> builder;
                  builder.reserve(row_num);
                  auto col = graph.get_vertex_property_column(label, name);
                  //                  LOG(INFO) << (int) col->type().type_enum;
                  if (col->type() == PropertyType::kDate) {
                    auto typed_col =
                        std::dynamic_pointer_cast<TypedColumn<Date>>(col);
                    for (size_t k = 0; k < row_num; ++k) {
                      auto vertex = vertex_col->get_vertex(k);
                      auto prop =
                          typed_col->get_view(vertex.second).milli_second;
                      builder.push_back_opt(prop);
                    }
                    ret.set(m.alias().value(), builder.finish());
                    alias_ids.push_back(m.alias().value());
                    flag = true;
                  } else if (col->type() == PropertyType::kDay) {
                    auto typed_col =
                        std::dynamic_pointer_cast<TypedColumn<Day>>(col);
                    for (size_t k = 0; k < row_num; ++k) {
                      auto vertex = vertex_col->get_vertex(k);
                      auto prop =
                          typed_col->get_view(vertex.second).to_timestamp();
                      builder.push_back_opt(prop);
                    }
                    ret.set(m.alias().value(), builder.finish());
                    alias_ids.push_back(m.alias().value());
                    flag = true;
                  }
                } break;

                default:
                  LOG(FATAL) << "not support for "
                             << static_cast<int>(data_types[i].data_type());
                }
              }
            }
          }
        }
        if (flag) {
          continue;
        }
      }*/

      {
        // value with type int and check property in range
        int tag;
        std::string name, lower, upper;
        common::Value then_value, else_value;
        if (is_check_property_in_range(m.expr(), params, tag, name, lower,
                                       upper, then_value, else_value)) {
          if (ctx.get(tag)->column_type() == ContextColumnType::kVertex) {
            auto vertex_col =
                std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag));
            if (vertex_col->get_labels_set().size() == 1) {
              CHECK(!vertex_col->is_optional());
              auto label = *vertex_col->get_labels_set().begin();
              if (data_types[i].type_case() == common::IrDataType::kDataType &&
                  data_types[i].data_type() == common::DataType::INT32) {
                ValueColumnBuilder<int32_t> builder;
                builder.reserve(row_num);

                auto col = graph.GetVertexColumn<Date>(label, name);
                if (!col.is_null()) {
                  auto lower_value = std::stoll(lower);
                  auto upper_value = std::stoll(upper);

                  int then_int = then_value.i32();
                  int else_int = else_value.i32();

                  for (size_t k = 0; k < row_num; ++k) {
                    auto vertex = vertex_col->get_vertex(k);
                    auto prop = col.get_view(vertex.vid_).milli_second;
                    if (prop >= lower_value && prop < upper_value) {
                      builder.push_back_opt(then_int);
                    } else {
                      builder.push_back_opt(else_int);
                    }
                  }

                  ret.set(m.alias().value(), builder.finish());
                  alias_ids.push_back(m.alias().value());
                  continue;
                }
              }
            }
          }
        }
      }

      {
        int tag;
        std::string name, upper;
        common::Value then_value, else_value;

        if (is_check_property_lt(m.expr(), params, tag, name, upper, then_value,
                                 else_value)) {
          if (ctx.get(tag)->column_type() == ContextColumnType::kVertex) {
            auto vertex_col =
                std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag));
            if (vertex_col->get_labels_set().size() == 1) {
              CHECK(!vertex_col->is_optional());
              auto label = *vertex_col->get_labels_set().begin();
              if (data_types[i].type_case() == common::IrDataType::kDataType &&
                  data_types[i].data_type() == common::DataType::INT32) {
                ValueColumnBuilder<int32_t> builder;
                builder.reserve(row_num);

                auto col = graph.GetVertexColumn<Date>(label, name);
                if (!col.is_null()) {
                  auto upper_value = std::stoll(upper);

                  int then_int = then_value.i32();
                  int else_int = else_value.i32();
                  for (size_t k = 0; k < row_num; ++k) {
                    auto vertex = vertex_col->get_vertex(k);
                    auto prop = col.get_view(vertex.vid_).milli_second;
                    if (prop < upper_value) {
                      builder.push_back_opt(then_int);
                    } else {
                      builder.push_back_opt(else_int);
                    }
                  }

                  ret.set(m.alias().value(), builder.finish());
                  alias_ids.push_back(m.alias().value());
                  continue;
                }
              }
            }
          }
        }
      }
      Expr expr(graph, ctx, params, m.expr(), VarType::kPathVar);

      int alias = -1;
      if (m.has_alias()) {
        alias = m.alias().value();
      }
      alias_ids.push_back(alias);
      // compiler bug here, data_types[i] is none
      if (data_types[i].type_case() == common::IrDataType::kDataType &&
          data_types[i].data_type() == common::DataType::NONE) {
        //        LOG(INFO) << "data type is none";
        if (expr.type() == RTAnyType::kF64Value) {
          common::IrDataType data_type;
          data_type.set_data_type(common::DataType::DOUBLE);
          auto col = build_column(data_type, expr, row_num);
          ret.set(alias, col);
        } else {
          LOG(FATAL) << "not support for "
                     << static_cast<int>(expr.type().type_enum_);
        }
      }

      else {
        auto col = build_column(data_types[i], expr, row_num);
        ret.set(alias, col);
      }
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

      Expr expr(graph, ctx, params, m.expr(), VarType::kPathVar);
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
    if (!(order_by_opr.pairs(k_i).key().tag().item_case() ==
          common::NameOrId::ItemCase::kId)) {
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

bool is_property_expr(const common::Expression& expr, int& tag_id,
                      std::string& property_name) {
  if (expr.operators_size() != 1) {
    LOG(INFO) << "AAAA";
    return false;
  }
  if (!expr.operators(0).has_var()) {
    LOG(INFO) << "AAAA";
    return false;
  }
  if (!expr.operators(0).var().has_property() ||
      !expr.operators(0).var().has_tag()) {
    LOG(INFO) << "AAAA";
    return false;
  }
  tag_id = expr.operators(0).var().tag().id();
  property_name = expr.operators(0).var().property().key().name();
  return true;
}

Context eval_project_order_by(
    const physical::Project& project_opr, const algebra::OrderBy& order_by_opr,
    const GraphReadInterface& graph, Context&& ctx, OprTimer& timer,
    const std::map<std::string, std::string>& params,
    const std::vector<common::IrDataType>& data_types) {
  TimerUnit tx;
  int mappings_size = project_opr.mappings_size();

  int order_by_keys_num = order_by_opr.pairs_size();
  std::set<int> order_by_keys;
  for (int k_i = 0; k_i < order_by_keys_num; ++k_i) {
    order_by_keys.insert(order_by_opr.pairs(k_i).key().tag().id());
  }

  std::vector<int> added_alias_in_preproject;
  size_t row_num = ctx.row_num();
  bool success = false;
  if (order_by_opr.has_limit() &&
      static_cast<size_t>(order_by_opr.limit().upper()) < ctx.row_num()) {
    int limit = order_by_opr.limit().upper();
    if (order_by_opr.pairs(0).has_key() &&
        order_by_opr.pairs(0).key().has_tag()) {
      int first_key_tag = order_by_opr.pairs(0).key().tag().id();
      if ((order_by_opr.pairs(0).order() ==
           algebra::OrderBy_OrderingPair_Order::
               OrderBy_OrderingPair_Order_ASC) ||
          (order_by_opr.pairs(0).order() ==
           algebra::OrderBy_OrderingPair_Order::
               OrderBy_OrderingPair_Order_DESC)) {
        bool asc =
            order_by_opr.pairs(0).order() ==
            algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC;
        std::vector<size_t> offsets;
        for (int i = 0; i < mappings_size; ++i) {
          const physical::Project_ExprAlias& m = project_opr.mappings(i);
          if (m.has_alias() && m.alias().value() == first_key_tag) {
            CHECK(!ctx.exist(first_key_tag));
            int tag_id;
            std::string property_name;
            if (is_property_expr(m.expr(), tag_id, property_name)) {
              if (ctx.get(tag_id)->column_type() ==
                  ContextColumnType::kVertex) {
                tx.start();
                auto col = build_topN_property_column(
                    graph, ctx.get(tag_id), property_name, limit, asc, offsets);
                if (col != nullptr) {
                  success = true;
                  ctx.reshuffle(offsets);
                  ctx.set(first_key_tag, col);
                  timer.record_routine(
                      "project_order_by:topN_by_vertex_property", tx);
                  added_alias_in_preproject.push_back(first_key_tag);
                  row_num = ctx.row_num();
                  break;
                } else {
                  LOG(INFO) << "build topN property column returns nullptr";
                }
              }
            }
            tx.start();
            Expr expr(graph, ctx, params, m.expr(), VarType::kPathVar);
            auto col = build_topN_column(data_types[i], expr, row_num, limit,
                                         asc, offsets);
            if (col != nullptr) {
              success = true;
              ctx.reshuffle(offsets);
              ctx.set(first_key_tag, col);
              timer.record_routine("project_order_by:topN_by_expr", tx);
              added_alias_in_preproject.push_back(first_key_tag);
              row_num = ctx.row_num();
            } else {
              LOG(INFO) << "build topN column returns nullptr";
            }
            break;
          }
        }
        if (success) {
          for (int i = 0; i < mappings_size; ++i) {
            const physical::Project_ExprAlias& m = project_opr.mappings(i);
            if (m.has_alias() && m.alias().value() != first_key_tag &&
                order_by_keys.find(m.alias().value()) != order_by_keys.end()) {
              int alias = m.alias().value();
              CHECK(!ctx.exist(alias));
              Expr expr(graph, ctx, params, m.expr(), VarType::kPathVar);
              auto col = build_column(data_types[i], expr, row_num);
              ctx.set(alias, col);
              added_alias_in_preproject.push_back(alias);
            }
          }
        }
      }
    }
  }

  if (!success) {
    for (int i = 0; i < mappings_size; ++i) {
      const physical::Project_ExprAlias& m = project_opr.mappings(i);
      if (m.has_alias() &&
          order_by_keys.find(m.alias().value()) != order_by_keys.end()) {
        {
          int alias = m.alias().value();
          CHECK(!ctx.exist(alias));
          Expr expr(graph, ctx, params, m.expr(), VarType::kPathVar);
          auto col = build_column(data_types[i], expr, row_num);
          ctx.set(alias, col);
          added_alias_in_preproject.push_back(alias);
        }
      }
    }
  }

  tx.start();
  ctx = eval_order_by(order_by_opr, graph, std::move(ctx), timer, !success);
  timer.record_routine("project_order_by:order_by", tx);

  row_num = ctx.row_num();
  Context ret;

  std::vector<size_t> tags;
  for (int i = 0; i < mappings_size; ++i) {
    const physical::Project_ExprAlias& m = project_opr.mappings(i);
    if (!(m.has_alias() &&
          order_by_keys.find(m.alias().value()) != order_by_keys.end())) {
      int alias = m.alias().value();
      Expr expr(graph, ctx, params, m.expr(), VarType::kPathVar);
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

  return ret;
}

// for insert transaction, lazy evaluation as we don't have the type information
WriteContext eval_project(const physical::Project& opr,
                          const GraphInsertInterface& graph, WriteContext&& ctx,
                          const std::map<std::string, std::string>& params) {
  int mappings_size = opr.mappings_size();
  WriteContext ret;
  std::unordered_map<int, std::pair<WriteContext::WriteParamsColumn,
                                    WriteContext::WriteParamsColumn>>
      pairs_cache;
  for (int i = 0; i < mappings_size; ++i) {
    const physical::Project_ExprAlias& m = opr.mappings(i);
    // LOG(INFO) << "eval project: " << m.expr().DebugString();
    CHECK(m.has_alias());
    CHECK(m.has_expr());
    CHECK(m.expr().operators_size() == 1);

    if (m.expr().operators(0).item_case() == common::ExprOpr::kParam) {
      auto param = m.expr().operators(0).param();
      CHECK(params.find(param.name()) != params.end());
      const auto& value = params.at(param.name());
      WriteContext::WriteParams p(value);
      ret.set(m.alias().value(), WriteContext::WriteParamsColumn({p}));
    } else if (m.expr().operators(0).item_case() == common::ExprOpr::kVar) {
      auto var = m.expr().operators(0).var();
      CHECK(var.has_tag());
      CHECK(!var.has_property());
      int tag = var.tag().id();
      ret.set(m.alias().value(), std::move(ctx.get(tag)));
    } else if (m.expr().operators(0).has_udf_func()) {
      auto udf_func = m.expr().operators(0).udf_func();
      if (udf_func.name() == "gs.function.first") {
        CHECK(udf_func.parameters_size() == 1 &&
              udf_func.parameters(0).operators_size() == 1);
        auto param = udf_func.parameters(0).operators(0);

        CHECK(param.item_case() == common::ExprOpr::kVar);
        auto var = param.var();
        CHECK(var.has_tag());
        CHECK(!var.has_property());
        int tag = var.tag().id();
        if (pairs_cache.count(tag)) {
          auto& pair = pairs_cache[tag];
          ret.set(m.alias().value(), std::move(pair.first));
        } else {
          auto col = ctx.get(tag);
          auto [first, second] = col.pairs();
          pairs_cache[tag] = {first, second};
          ret.set(m.alias().value(), std::move(first));
        }
      } else if (udf_func.name() == "gs.function.second") {
        CHECK(udf_func.parameters_size() == 1 &&
              udf_func.parameters(0).operators_size() == 1);
        auto param = udf_func.parameters(0).operators(0);

        CHECK(param.item_case() == common::ExprOpr::kVar);
        auto var = param.var();
        CHECK(var.has_tag());
        CHECK(!var.has_property());
        int tag = var.tag().id();
        if (pairs_cache.count(tag)) {
          auto& pair = pairs_cache[tag];
          ret.set(m.alias().value(), std::move(pair.second));
        } else {
          auto col = ctx.get(tag);
          auto [first, second] = col.pairs();
          pairs_cache[tag] = {first, second};
          ret.set(m.alias().value(), std::move(second));
        }
      } else {
        LOG(FATAL) << "not support for " << m.expr().DebugString();
      }
    } else {
      LOG(FATAL) << "not support for " << m.expr().DebugString();
    }
  }

  return ret;
}

}  // namespace runtime

}  // namespace gs