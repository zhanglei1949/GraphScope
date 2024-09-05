
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

#ifndef RUNTIME_ADHOC_OPERATORS_SPECIAL_PREDICATES_H_
#define RUNTIME_ADHOC_OPERATORS_SPECIAL_PREDICATES_H_

#include "flex/proto_generated_gie/expr.pb.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

namespace runtime {

inline bool is_label_within_predicate(const common::Expression& expr,
                                      std::set<label_t>& label_set) {
  if (expr.operators_size() == 3) {
    auto& var_op = expr.operators(0);

    if (var_op.has_var() && var_op.var().has_property() &&
        var_op.var().property().has_label()) {
      auto& within_op = expr.operators(1);
      if (within_op.item_case() == common::ExprOpr::kLogical &&
          within_op.logical() == common::Logical::WITHIN) {
        auto& labels_op = expr.operators(2);
        if (labels_op.has_const_() && labels_op.const_().has_i64_array()) {
          auto& array = labels_op.const_().i64_array();
          size_t num = array.item_size();
          for (size_t k = 0; k < num; ++k) {
            label_set.insert(static_cast<label_t>(array.item(k)));
          }
          return true;
        }
      }
    }
  }
  return false;
}

// TODO(lexiao): merge with is_pk_exact_check
inline bool is_pk_oid_exact_check(
    const common::Expression& expr,
    const std::map<std::string, std::string>& params, Any& pk) {
  if (expr.operators_size() != 3) {
    return false;
  }
  if (!(expr.operators(0).has_var() && expr.operators(0).var().has_property() &&
        expr.operators(0).var().property().has_key())) {
    auto& key = expr.operators(7).var().property().key();
    if (!(key.item_case() == common::NameOrId::ItemCase::kName &&
          key.name() == "id")) {
      return false;
    }
    return false;
  }
  if (!(expr.operators(1).item_case() == common::ExprOpr::kLogical &&
        expr.operators(1).logical() == common::Logical::EQ)) {
    return false;
  }

  if (expr.operators(2).has_param()) {
    auto& p = expr.operators(2).param();
    const std::string& p_name = p.name();
    auto p_iter = params.find(p_name);
    if (p_iter == params.end()) {
      return false;
    }
    if (!(p.has_data_type() &&
          p.data_type().type_case() ==
              common::IrDataType::TypeCase::kDataType &&
          p.data_type().data_type() == common::DataType::INT64)) {
      return false;
    }
    pk.set_i64(std::stoll(p_iter->second));
    return true;
  } else {
    return false;
  }
  return false;
}

inline bool is_pk_exact_check(const common::Expression& expr,
                              const std::map<std::string, std::string>& params,
                              label_t& label, Any& pk) {
  label_t local_label;
  int64_t local_pk;
  if (expr.operators_size() != 11) {
    return false;
  }
  if (!(expr.operators(0).item_case() == common::ExprOpr::kBrace &&
        expr.operators(0).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_LEFT_BRACE)) {
    return false;
  }
  if (!(expr.operators(1).has_var() && expr.operators(1).var().has_property() &&
        expr.operators(1).var().property().has_label())) {
    return false;
  }
  if (!(expr.operators(2).item_case() == common::ExprOpr::kLogical &&
        expr.operators(2).logical() == common::Logical::WITHIN)) {
    return false;
  }
  if (expr.operators(3).has_const_() &&
      expr.operators(3).const_().has_i64_array()) {
    auto& array = expr.operators(3).const_().i64_array();
    if (array.item_size() != 1) {
      return false;
    }
    local_label = static_cast<label_t>(array.item(0));
  } else {
    return false;
  }
  if (!(expr.operators(4).item_case() == common::ExprOpr::kBrace &&
        expr.operators(4).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_RIGHT_BRACE)) {
    return false;
  }
  if (!(expr.operators(5).item_case() == common::ExprOpr::kLogical &&
        expr.operators(5).logical() == common::Logical::AND)) {
    return false;
  }
  if (!(expr.operators(6).item_case() == common::ExprOpr::kBrace &&
        expr.operators(6).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_LEFT_BRACE)) {
    return false;
  }
  if (!(expr.operators(7).has_var() && expr.operators(7).var().has_property() &&
        expr.operators(7).var().property().has_key())) {
    auto& key = expr.operators(7).var().property().key();
    if (!(key.item_case() == common::NameOrId::ItemCase::kName &&
          key.name() == "id")) {
      return false;
    }
  }
  if (!(expr.operators(8).item_case() == common::ExprOpr::kLogical &&
        expr.operators(8).logical() == common::Logical::EQ)) {
    return false;
  }
  if (expr.operators(9).has_param()) {
    auto& p = expr.operators(9).param();
    const std::string& p_name = p.name();
    auto p_iter = params.find(p_name);
    if (p_iter == params.end()) {
      return false;
    }
    if (!(p.has_data_type() &&
          p.data_type().type_case() ==
              common::IrDataType::TypeCase::kDataType &&
          p.data_type().data_type() == common::DataType::INT64)) {
      return false;
    }
    local_pk = std::stoll(p_iter->second);
  } else {
    return false;
  }
  if (!(expr.operators(10).item_case() == common::ExprOpr::kBrace &&
        expr.operators(10).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_RIGHT_BRACE)) {
    return false;
  }

  label = local_label;
  pk.set_i64(local_pk);
  return true;
}

inline bool is_property_lt(const common::Expression& expr,
                           const std::map<std::string, std::string>& params,
                           std::string& property_name, RTAny& threshold) {
  if (expr.operators_size() != 3) {
    return false;
  }
  const common::ExprOpr& op0 = expr.operators(0);
  if (!op0.has_var()) {
    return false;
  }
  if (!op0.var().has_property()) {
    return false;
  }
  if (!op0.var().property().has_key()) {
    return false;
  }
  if (!(op0.var().property().key().item_case() ==
        common::NameOrId::ItemCase::kName)) {
    return false;
  }
  property_name = op0.var().property().key().name();
  const common::ExprOpr& op1 = expr.operators(1);
  if (!(op1.item_case() == common::ExprOpr::kLogical)) {
    return false;
  }
  if (op1.logical() != common::Logical::LT) {
    return false;
  }
  const common::ExprOpr& op2 = expr.operators(2);
  if (!op2.has_param()) {
    return false;
  }
  if (!op2.param().has_data_type()) {
    return false;
  }
  if (!(op2.param().data_type().type_case() ==
        common::IrDataType::TypeCase::kDataType)) {
    return false;
  }
  if (op2.param().data_type().data_type() != common::DataType::INT64) {
    return false;
  }
  threshold = TypedConverter<int64_t>::from_typed(
      TypedConverter<int64_t>::typed_from_string(
          params.at(op2.param().name())));
  return true;
}

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_OPERATORS_SPECIAL_PREDICATES_H_