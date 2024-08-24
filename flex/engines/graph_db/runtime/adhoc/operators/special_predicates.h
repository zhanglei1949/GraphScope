
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
      if (within_op.has_logical() &&
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

inline bool is_pk_exact_check(const common::Expression& expr,
                              const std::map<std::string, std::string>& params,
                              label_t& label, Any& pk) {
  label_t local_label;
  int64_t local_pk;
  if (expr.operators_size() != 11) {
    return false;
  }
  if (!(expr.operators(0).has_brace() &&
        expr.operators(0).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_LEFT_BRACE)) {
    return false;
  }
  if (!(expr.operators(1).has_var() && expr.operators(1).var().has_property() &&
        expr.operators(1).var().property().has_label())) {
    return false;
  }
  if (!(expr.operators(2).has_logical() &&
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
  if (!(expr.operators(4).has_brace() &&
        expr.operators(4).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_RIGHT_BRACE)) {
    return false;
  }
  if (!(expr.operators(5).has_logical() &&
        expr.operators(5).logical() == common::Logical::AND)) {
    return false;
  }
  if (!(expr.operators(6).has_brace() &&
        expr.operators(6).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_LEFT_BRACE)) {
    return false;
  }
  if (!(expr.operators(7).has_var() && expr.operators(7).var().has_property() &&
        expr.operators(7).var().property().has_key())) {
    auto& key = expr.operators(7).var().property().key();
    if (!(key.has_name() && key.name() == "id")) {
      return false;
    }
  }
  if (!(expr.operators(8).has_logical() &&
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
    if (!(p.has_data_type() && p.data_type().has_data_type() &&
          p.data_type().data_type() == common::DataType::INT64)) {
      return false;
    }
    local_pk = std::stoll(p_iter->second);
  } else {
    return false;
  }
  if (!(expr.operators(10).has_brace() &&
        expr.operators(10).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_RIGHT_BRACE)) {
    return false;
  }

  label = local_label;
  pk.set_i64(local_pk);
  return true;
}

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_OPERATORS_SPECIAL_PREDICATES_H_