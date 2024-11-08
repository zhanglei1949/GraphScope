
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

#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"
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

enum class SPVertexPredicateType {
  kPropertyGT,
  kPropertyLT,
  kPropertyLE,
  kPropertyEQ,
  kPropertyBetween,
  kIdEQ,
};

class SPVertexPredicate {
 public:
  virtual ~SPVertexPredicate() {}
  virtual SPVertexPredicateType type() const = 0;
  virtual RTAnyType data_type() const = 0;
};

template <typename T>
class VertexPropertyLTPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyLTPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    target_ = TypedConverter<T>::typed_from_string(target_str);
  }

  ~VertexPropertyLTPredicateBeta() = default;

  SPVertexPredicateType type() const override {
    return SPVertexPredicateType::kPropertyLT;
  }

  RTAnyType data_type() const override { return TypedConverter<T>::type(); }

  bool operator()(label_t label, vid_t v) const {
    return columns_[label].get_view(v) < target_;
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  T target_;
};

template <typename T>
class VertexPropertyLEPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyLEPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    target_ = TypedConverter<T>::typed_from_string(target_str);
  }

  ~VertexPropertyLEPredicateBeta() = default;

  SPVertexPredicateType type() const override {
    return SPVertexPredicateType::kPropertyLE;
  }

  RTAnyType data_type() const override { return TypedConverter<T>::type(); }

  bool operator()(label_t label, vid_t v) const {
    return !(target_ < columns_[label].get_view(v));
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  T target_;
};

template <typename T>
class VertexPropertyGTPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyGTPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    target_ = TypedConverter<T>::typed_from_string(target_str);
  }

  ~VertexPropertyGTPredicateBeta() = default;

  SPVertexPredicateType type() const override {
    return SPVertexPredicateType::kPropertyGT;
  }

  RTAnyType data_type() const override { return TypedConverter<T>::type(); }

  bool operator()(label_t label, vid_t v) const {
    return target_ < columns_[label].get_view(v);
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  T target_;
};

template <typename T>
class VertexPropertyEQPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyEQPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    target_ = TypedConverter<T>::typed_from_string(target_str);
  }

  ~VertexPropertyEQPredicateBeta() = default;

  SPVertexPredicateType type() const override {
    return SPVertexPredicateType::kPropertyEQ;
  }

  RTAnyType data_type() const override { return TypedConverter<T>::type(); }

  bool operator()(label_t label, vid_t v) const {
    return target_ == columns_[label].get_view(v);
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  T target_;
};

template <>
class VertexPropertyEQPredicateBeta<std::string_view>
    : public SPVertexPredicate {
  using T = std::string_view;

 public:
  VertexPropertyEQPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(
          graph.GetVertexColumn<std::string_view>(i, property_name));
    }
    target_ = target_str;
  }

  ~VertexPropertyEQPredicateBeta() = default;

  SPVertexPredicateType type() const override {
    return SPVertexPredicateType::kPropertyEQ;
  }

  RTAnyType data_type() const override { return TypedConverter<T>::type(); }

  bool operator()(label_t label, vid_t v) const {
    return target_ == columns_[label].get_view(v);
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  std::string target_;
};

template <typename T>
class VertexPropertyBetweenPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyBetweenPredicateBeta(const GraphReadInterface& graph,
                                     const std::string& property_name,
                                     const std::string& from_str,
                                     const std::string& to_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    from_ = TypedConverter<T>::typed_from_string(from_str);
    to_ = TypedConverter<T>::typed_from_string(to_str);
  }

  ~VertexPropertyBetweenPredicateBeta() = default;

  SPVertexPredicateType type() const override {
    return SPVertexPredicateType::kPropertyBetween;
  }

  RTAnyType data_type() const override { return TypedConverter<T>::type(); }

  bool operator()(label_t label, vid_t v) const {
    auto val = columns_[label].get_view(v);
    return ((val < to_) && !(val < from_));
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  T from_;
  T to_;
};

class VertexIdEQPredicateBeta : public SPVertexPredicate {
 public:
  VertexIdEQPredicateBeta(const GraphReadInterface& graph,
                          const std::string& val,
                          const common::DataType& data_type)
      : graph_(graph) {
    if (data_type == common::DataType::INT64) {
      data_type_ = RTAnyType::kI64Value;
      target_.set_i64(TypedConverter<int64_t>::typed_from_string(val));
    } else if (data_type == common::DataType::STRING) {
      data_type_ = RTAnyType::kStringValue;
      target_.set_string(val);
    }
  }
  ~VertexIdEQPredicateBeta() = default;

  SPVertexPredicateType type() const override {
    return SPVertexPredicateType::kIdEQ;
  }

  RTAnyType data_type() const override { return data_type_; }
  bool operator()(label_t label, vid_t v) const {
    auto id = graph_.GetVertexId(label, v);
    return id == target_;
  }

 private:
  const GraphReadInterface& graph_;
  RTAnyType data_type_;
  Any target_;
};

inline std::unique_ptr<SPVertexPredicate> parse_special_vertex_predicate(
    const common::Expression& expr, const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params) {
  // LOG(INFO) << "enter...";
  if (expr.operators_size() == 3) {
    // LT, EQ, GT, LE
    const common::ExprOpr& op0 = expr.operators(0);
    if (!op0.has_var()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op0.var().has_property()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op0.var().property().has_key()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!(op0.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    std::string property_name = op0.var().property().key().name();
    const common::ExprOpr& op1 = expr.operators(1);
    if (!(op1.item_case() == common::ExprOpr::kLogical)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    SPVertexPredicateType ptype;
    if (op1.logical() == common::Logical::LT) {
      ptype = SPVertexPredicateType::kPropertyLT;
    } else if (op1.logical() == common::Logical::GT) {
      ptype = SPVertexPredicateType::kPropertyGT;
    } else if (op1.logical() == common::Logical::EQ) {
      ptype = SPVertexPredicateType::kPropertyEQ;
    } else if (op1.logical() == common::Logical::LE) {
      ptype = SPVertexPredicateType::kPropertyLE;
    } else {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    const common::ExprOpr& op2 = expr.operators(2);
    if (!op2.has_param()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op2.param().has_data_type()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!(op2.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    std::string value_str = params.at(op2.param().name());
    if (property_name == "id") {
      if (ptype == SPVertexPredicateType::kPropertyEQ) {
        return std::unique_ptr<SPVertexPredicate>(new VertexIdEQPredicateBeta(
            graph, value_str, op2.param().data_type().data_type()));
      } else {
        LOG(INFO) << "AAAA";
        return nullptr;
      }
    } else {
      if (op2.param().data_type().data_type() == common::DataType::INT64) {
        if (ptype == SPVertexPredicateType::kPropertyLT) {
          return std::unique_ptr<SPVertexPredicate>(
              new VertexPropertyLTPredicateBeta<int64_t>(graph, property_name,
                                                         value_str));
        } else if (ptype == SPVertexPredicateType::kPropertyEQ) {
          return std::unique_ptr<SPVertexPredicate>(
              new VertexPropertyEQPredicateBeta<int64_t>(graph, property_name,
                                                         value_str));
        } else if (ptype == SPVertexPredicateType::kPropertyGT) {
          return std::unique_ptr<SPVertexPredicate>(
              new VertexPropertyGTPredicateBeta<int64_t>(graph, property_name,
                                                         value_str));
        } else if (ptype == SPVertexPredicateType::kPropertyLE) {
          return std::unique_ptr<SPVertexPredicate>(
              new VertexPropertyLEPredicateBeta<int64_t>(graph, property_name,
                                                         value_str));
        } else {
          LOG(INFO) << "AAAA";
          return nullptr;
        }
      } else if (op2.param().data_type().data_type() ==
                 common::DataType::STRING) {
        if (ptype == SPVertexPredicateType::kPropertyEQ) {
          return std::unique_ptr<SPVertexPredicate>(
              new VertexPropertyEQPredicateBeta<std::string_view>(
                  graph, property_name, value_str));
        } else {
          LOG(INFO) << "AAAA";
          return nullptr;
        }
      } else if (op2.param().data_type().data_type() ==
                 common::DataType::TIMESTAMP) {
        if (ptype == SPVertexPredicateType::kPropertyLT) {
          return std::unique_ptr<SPVertexPredicate>(
              new VertexPropertyLTPredicateBeta<Date>(graph, property_name,
                                                      value_str));
        } else if (ptype == SPVertexPredicateType::kPropertyEQ) {
          return std::unique_ptr<SPVertexPredicate>(
              new VertexPropertyEQPredicateBeta<Date>(graph, property_name,
                                                      value_str));
        } else if (ptype == SPVertexPredicateType::kPropertyGT) {
          return std::unique_ptr<SPVertexPredicate>(
              new VertexPropertyGTPredicateBeta<Date>(graph, property_name,
                                                      value_str));
        } else if (ptype == SPVertexPredicateType::kPropertyLE) {
          return std::unique_ptr<SPVertexPredicate>(
              new VertexPropertyLEPredicateBeta<Date>(graph, property_name,
                                                      value_str));
        } else {
          LOG(INFO) << "AAAA";
          return nullptr;
        }
      } else {
        LOG(INFO) << "AAAA";
        return nullptr;
      }
    }
  } else if (expr.operators_size() == 7) {
    // between
    const common::ExprOpr& op0 = expr.operators(0);
    if (!op0.has_var()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op0.var().has_property()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op0.var().property().has_key()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!(op0.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    std::string property_name = op0.var().property().key().name();

    const common::ExprOpr& op1 = expr.operators(1);
    if (!(op1.item_case() == common::ExprOpr::kLogical)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (op1.logical() != common::Logical::GE) {
      // LOG(INFO) << "AAAA";
      return nullptr;
    }

    const common::ExprOpr& op2 = expr.operators(2);
    if (!op2.has_param()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op2.param().has_data_type()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!(op2.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    std::string from_str = params.at(op2.param().name());

    const common::ExprOpr& op3 = expr.operators(3);
    if (!(op3.item_case() == common::ExprOpr::kLogical)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (op3.logical() != common::Logical::AND) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }

    const common::ExprOpr& op4 = expr.operators(4);
    if (!op4.has_var()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op4.var().has_property()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op4.var().property().has_key()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!(op4.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (property_name != op4.var().property().key().name()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }

    const common::ExprOpr& op5 = expr.operators(5);
    if (!(op5.item_case() == common::ExprOpr::kLogical)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (op5.logical() != common::Logical::LT) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }

    const common::ExprOpr& op6 = expr.operators(6);
    if (!op6.has_param()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op6.param().has_data_type()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!(op6.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    std::string to_str = params.at(op6.param().name());

    if (op2.param().data_type().data_type() !=
        op6.param().data_type().data_type()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }

    if (op2.param().data_type().data_type() == common::DataType::INT64) {
      return std::unique_ptr<SPVertexPredicate>(
          new VertexPropertyBetweenPredicateBeta<int64_t>(graph, property_name,
                                                          from_str, to_str));
    } else if (op2.param().data_type().data_type() ==
               common::DataType::TIMESTAMP) {
      return std::unique_ptr<SPVertexPredicate>(
          new VertexPropertyBetweenPredicateBeta<Date>(graph, property_name,
                                                       from_str, to_str));

    } else {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
  }

  LOG(INFO) << "AAAA - " << expr.operators_size();
  return nullptr;
}

enum class SPEdgePredicateType {
  kPropertyGT,
  kPropertyLT,
};

class SPEdgePredicate {
 public:
  virtual ~SPEdgePredicate() {}
  virtual SPEdgePredicateType type() const = 0;
  virtual RTAnyType data_type() const = 0;
};

template <typename T>
class EdgePropertyLTPredicate : public SPEdgePredicate {
 public:
  EdgePropertyLTPredicate(const std::string& target_str) {
    target_ = TypedConverter<T>::typed_from_string(target_str);
  }

  ~EdgePropertyLTPredicate() = default;

  SPEdgePredicateType type() const override {
    return SPEdgePredicateType::kPropertyLT;
  }

  RTAnyType data_type() const override { return TypedConverter<T>::type(); }

  bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
                  label_t edge_label, Direction dir, const T& edata) const {
    return edata < target_;
  }

  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t idx) const {
    return AnyConverter<T>::from_any(edata) < target_;
  }

 private:
  T target_;
};

template <typename T>
class EdgePropertyGTPredicate : public SPEdgePredicate {
 public:
  EdgePropertyGTPredicate(const std::string& target_str) {
    target_ = TypedConverter<T>::typed_from_string(target_str);
  }

  ~EdgePropertyGTPredicate() = default;

  SPEdgePredicateType type() const override {
    return SPEdgePredicateType::kPropertyGT;
  }

  RTAnyType data_type() const override { return TypedConverter<T>::type(); }

  bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
                  label_t edge_label, Direction dir, const T& edata) const {
    return target_ < edata;
  }

  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t idx) const {
    return target_ < AnyConverter<T>::from_any(edata);
  }

 private:
  T target_;
};

inline std::unique_ptr<SPEdgePredicate> parse_special_edge_predicate(
    const common::Expression& expr, const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params) {
  if (expr.operators_size() == 3) {
    // LT, GT
    const common::ExprOpr& op0 = expr.operators(0);
    if (!op0.has_var()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op0.var().has_property()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op0.var().property().has_key()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!(op0.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    std::string property_name = op0.var().property().key().name();

    const common::ExprOpr& op1 = expr.operators(1);
    if (!(op1.item_case() == common::ExprOpr::kLogical)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    SPEdgePredicateType ptype;
    if (op1.logical() == common::Logical::LT) {
      ptype = SPEdgePredicateType::kPropertyLT;
    } else if (op1.logical() == common::Logical::GT) {
      ptype = SPEdgePredicateType::kPropertyGT;
    } else {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    const common::ExprOpr& op2 = expr.operators(2);
    if (!op2.has_param()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!op2.param().has_data_type()) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    if (!(op2.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      LOG(INFO) << "AAAA";
      return nullptr;
    }
    std::string value_str = params.at(op2.param().name());

    if (op2.param().data_type().data_type() == common::DataType::INT64) {
      if (ptype == SPEdgePredicateType::kPropertyLT) {
        return std::unique_ptr<SPEdgePredicate>(
            new EdgePropertyLTPredicate<int64_t>(value_str));
      } else if (ptype == SPEdgePredicateType::kPropertyGT) {
        return std::unique_ptr<SPEdgePredicate>(
            new EdgePropertyGTPredicate<int64_t>(value_str));
      } else {
        LOG(INFO) << "AAAA";
        return nullptr;
      }
    } else if (op2.param().data_type().data_type() == common::DataType::INT32) {
      if (ptype == SPEdgePredicateType::kPropertyLT) {
        return std::unique_ptr<SPEdgePredicate>(
            new EdgePropertyLTPredicate<int>(value_str));
      } else if (ptype == SPEdgePredicateType::kPropertyGT) {
        return std::unique_ptr<SPEdgePredicate>(
            new EdgePropertyGTPredicate<int>(value_str));
      } else {
        LOG(INFO) << "AAAA";
        return nullptr;
      }
    } else if (op2.param().data_type().data_type() ==
               common::DataType::TIMESTAMP) {
      if (ptype == SPEdgePredicateType::kPropertyLT) {
        return std::unique_ptr<SPEdgePredicate>(
            new EdgePropertyLTPredicate<Date>(value_str));
      } else if (ptype == SPEdgePredicateType::kPropertyGT) {
        return std::unique_ptr<SPEdgePredicate>(
            new EdgePropertyGTPredicate<Date>(value_str));
      } else {
        LOG(INFO) << "AAAA";
        return nullptr;
      }
    }
  }
  LOG(INFO) << "AAAA - " << expr.operators_size();
  return nullptr;
}

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_OPERATORS_SPECIAL_PREDICATES_H_