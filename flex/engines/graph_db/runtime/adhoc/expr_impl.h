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

#ifndef RUNTIME_ADHOC_RUNTIME_EXPR_IMPL_H_
#define RUNTIME_ADHOC_RUNTIME_EXPR_IMPL_H_

#include <regex>
#include <stack>
#include "flex/proto_generated_gie/expr.pb.h"

#include "flex/engines/graph_db/runtime/adhoc/graph_interface.h"
#include "flex/engines/graph_db/runtime/adhoc/var.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"

namespace gs {

namespace runtime {

class ExprBase {
 public:
  virtual RTAny eval_path(size_t idx) const = 0;
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx) const = 0;
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx) const = 0;
  virtual RTAnyType type() const = 0;
  virtual RTAny eval_path(size_t idx, int) const { return eval_path(idx); }
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const {
    return eval_vertex(label, v, idx);
  }
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx, int) const {
    return eval_edge(label, src, dst, data, idx);
  }
  virtual std::shared_ptr<IContextColumnBuilder> builder() const {
    LOG(FATAL) << "not implemented";
    return nullptr;
  }

  virtual bool is_optional() const { return false; }

  virtual ~ExprBase() = default;
};

class ConstTrueExpr : public ExprBase {
 public:
  RTAny eval_path(size_t idx) const override { return RTAny::from_bool(true); }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    return RTAny::from_bool(true);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return RTAny::from_bool(true);
  }

  RTAnyType type() const override { return RTAnyType::kBoolValue; }
};

class ConstFalseExpr : public ExprBase {
 public:
  RTAny eval_path(size_t idx) const override { return RTAny::from_bool(false); }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    return RTAny::from_bool(false);
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return RTAny::from_bool(false);
  }

  RTAnyType type() const override { return RTAnyType::kBoolValue; }
};

template <typename T, typename GRAPH_IMPL>
class WithInExpr : public ExprBase {
 public:
  WithInExpr(const GraphInterface<GRAPH_IMPL>& txn, const Context& ctx,
             std::unique_ptr<ExprBase>&& key, const common::Value& array)
      : key_(std::move(key)) {
    if constexpr (std::is_same_v<T, int64_t>) {
      CHECK(array.item_case() == common::Value::kI64Array);
      size_t len = array.i64_array().item_size();
      for (size_t idx = 0; idx < len; ++idx) {
        container_.push_back(array.i64_array().item(idx));
      }
    } else if constexpr (std::is_same_v<T, int32_t>) {
      CHECK(array.item_case() == common::Value::kI32Array);
      size_t len = array.i32_array().item_size();
      for (size_t idx = 0; idx < len; ++idx) {
        container_.push_back(array.i32_array().item(idx));
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      CHECK(array.item_case() == common::Value::kStrArray);
      size_t len = array.str_array().item_size();
      for (size_t idx = 0; idx < len; ++idx) {
        container_.push_back(array.str_array().item(idx));
      }
    } else {
      LOG(FATAL) << "not implemented";
    }
  }

  RTAny eval_path(size_t idx) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(key_->eval_path(idx).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = TypedConverter<T>::to_typed(key_->eval_path(idx));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
  }

  RTAny eval_path(size_t idx, int) const override {
    auto any_val = key_->eval_path(idx, 0);
    if (any_val.is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(key_->eval_vertex(label, v, idx).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = TypedConverter<T>::to_typed(key_->eval_vertex(label, v, idx));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const override {
    auto any_val = key_->eval_vertex(label, v, idx, 0);
    if (any_val.is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, idx);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val =
          std::string(key_->eval_edge(label, src, dst, data, idx).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = TypedConverter<T>::to_typed(
          key_->eval_edge(label, src, dst, data, idx));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
    return RTAny::from_bool(false);
  }
  RTAnyType type() const override { return RTAnyType::kBoolValue; }
  bool is_optional() const override { return key_->is_optional(); }

  std::unique_ptr<ExprBase> key_;
  std::vector<T> container_;
};

template <typename GRAPH_IMPL>
class VariableExpr : public ExprBase {
 public:
  VariableExpr(const GraphInterface<GRAPH_IMPL>& txn, const Context& ctx,
               const common::Variable& pb, VarType var_type)
      : var_(txn, ctx, pb, var_type) {}

  RTAny eval_path(size_t idx) const override { return var_.get(idx); }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    return var_.get_vertex(label, v, idx);
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return var_.get_edge(label, src, dst, data, idx);
  }
  RTAnyType type() const override { return var_.type(); }

  RTAny eval_path(size_t idx, int) const override { return var_.get(idx, 0); }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const override {
    return var_.get_vertex(label, v, idx, 0);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, int) const override {
    return var_.get_edge(label, src, dst, data, idx, 0);
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return var_.builder();
  }

  bool is_optional() const override { return var_.is_optional(); }

 private:
  Var<GRAPH_IMPL> var_;
};

class UnaryLogicalExpr : public ExprBase {
 public:
  UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr, common::Logical logic);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

  bool is_optional() const override { return expr_->is_optional(); }

 private:
  std::unique_ptr<ExprBase> expr_;
  common::Logical logic_;
};
class LogicalExpr : public ExprBase {
 public:
  LogicalExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs,
              common::Logical logic);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAny eval_path(size_t idx, int) const override {
    if (lhs_->eval_path(idx, 0).is_null() ||
        rhs_->eval_path(idx, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const override {
    if (lhs_->eval_vertex(label, v, idx, 0).is_null() ||
        rhs_->eval_vertex(label, v, idx, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, idx);
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, int) const override {
    LOG(FATAL) << "not implemented";
    return RTAny();
  }

  RTAnyType type() const override;

  bool is_optional() const override {
    return lhs_->is_optional() || rhs_->is_optional();
  }

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  common::Logical logic_;
};

class ExtractExpr : public ExprBase {
 public:
  ExtractExpr(std::unique_ptr<ExprBase>&& expr, const common::Extract& extract);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  std::unique_ptr<ExprBase> expr_;
  const common::Extract extract_;
};
class ArithExpr : public ExprBase {
 public:
  ArithExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs,
            common::Arithmetic arith);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  common::Arithmetic arith_;
};

class ConstExpr : public ExprBase {
 public:
  ConstExpr(const RTAny& val);
  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  RTAny val_;
  std::string s;
};

class CaseWhenExpr : public ExprBase {
 public:
  CaseWhenExpr(
      std::vector<std::pair<std::unique_ptr<ExprBase>,
                            std::unique_ptr<ExprBase>>>&& when_then_exprs,
      std::unique_ptr<ExprBase>&& else_expr);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  std::vector<std::pair<std::unique_ptr<ExprBase>, std::unique_ptr<ExprBase>>>
      when_then_exprs_;
  std::unique_ptr<ExprBase> else_expr_;
};

class TupleExpr : public ExprBase {
 public:
  TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  std::vector<std::unique_ptr<ExprBase>> exprs_;
};

class MapExpr : public ExprBase {
 public:
  MapExpr(std::vector<std::string>&& keys,
          std::vector<std::unique_ptr<ExprBase>>&& values)
      : keys(std::move(keys)), value_exprs(std::move(values)) {
    CHECK(keys.size() == values.size());
  }

  RTAny eval_path(size_t idx) const override {
    std::vector<RTAny> ret;
    for (size_t i = 0; i < keys.size(); i++) {
      ret.push_back(value_exprs[i]->eval_path(idx));
    }
    values.emplace_back(ret);
    size_t id = values.size() - 1;
    auto map_impl = MapImpl::make_map_impl(&keys, &values[id]);
    auto map = Map::make_map(map_impl);
    return RTAny::from_map(map);
  }

  RTAny eval_path(size_t idx, int) const override {
    std::vector<RTAny> ret;
    for (size_t i = 0; i < keys.size(); i++) {
      ret.push_back(value_exprs[i]->eval_path(idx, 0));
    }
    values.emplace_back(ret);
    size_t id = values.size() - 1;
    auto map_impl = MapImpl::make_map_impl(&keys, &values[id]);
    auto map = Map::make_map(map_impl);
    return RTAny::from_map(map);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    LOG(FATAL) << "not implemented";
    return RTAny();
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    LOG(FATAL) << "not implemented";
    return RTAny();
  }

  RTAnyType type() const override { return RTAnyType::kMap; }

  bool is_optional() const override {
    for (auto& expr : value_exprs) {
      if (expr->is_optional()) {
        return true;
      }
    }
    return false;
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    auto builder = std::make_shared<MapValueColumnBuilder>();
    builder->set_keys(keys);
    return std::dynamic_pointer_cast<IContextColumnBuilder>(builder);
  }

 private:
  std::vector<std::string> keys;
  std::vector<std::unique_ptr<ExprBase>> value_exprs;
  mutable std::vector<std::vector<RTAny>> values;
};

LogicalExpr::LogicalExpr(std::unique_ptr<ExprBase>&& lhs,
                         std::unique_ptr<ExprBase>&& rhs, common::Logical logic)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)), logic_(logic) {}

RTAny LogicalExpr::eval_path(size_t idx) const {
  if (logic_ == common::Logical::LT) {
    bool ret = lhs_->eval_path(idx) < rhs_->eval_path(idx);
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::GT) {
    bool ret = rhs_->eval_path(idx) < lhs_->eval_path(idx);
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::GE) {
    bool ret = lhs_->eval_path(idx) < rhs_->eval_path(idx);
    return RTAny::from_bool(!ret);
  } else if (logic_ == common::Logical::LE) {
    bool ret = rhs_->eval_path(idx) < lhs_->eval_path(idx);
    return RTAny::from_bool(!ret);
  } else if (logic_ == common::Logical::EQ) {
    bool ret = (rhs_->eval_path(idx) == lhs_->eval_path(idx));
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::NE) {
    bool ret = (rhs_->eval_path(idx) == lhs_->eval_path(idx));
    return RTAny::from_bool(!ret);
  } else if (logic_ == common::Logical::AND) {
    bool ret =
        (rhs_->eval_path(idx).as_bool() && lhs_->eval_path(idx).as_bool());
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::OR) {
    bool ret =
        (rhs_->eval_path(idx).as_bool() || lhs_->eval_path(idx).as_bool());
    return RTAny::from_bool(ret);
  } else {
    LOG(FATAL) << "not support..." << static_cast<int>(logic_);
  }
  return RTAny::from_bool(false);
}

RTAny LogicalExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  if (logic_ == common::Logical::LT) {
    bool ret =
        lhs_->eval_vertex(label, v, idx) < rhs_->eval_vertex(label, v, idx);
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::GT) {
    bool ret =
        rhs_->eval_vertex(label, v, idx) < lhs_->eval_vertex(label, v, idx);
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::GE) {
    bool ret =
        lhs_->eval_vertex(label, v, idx) < rhs_->eval_vertex(label, v, idx);
    return RTAny::from_bool(!ret);
  } else if (logic_ == common::Logical::LE) {
    bool ret =
        rhs_->eval_vertex(label, v, idx) < lhs_->eval_vertex(label, v, idx);
    return RTAny::from_bool(!ret);
  } else if (logic_ == common::Logical::EQ) {
    bool ret =
        (rhs_->eval_vertex(label, v, idx) == lhs_->eval_vertex(label, v, idx));
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::NE) {
    bool ret =
        (rhs_->eval_vertex(label, v, idx) == lhs_->eval_vertex(label, v, idx));
    return RTAny::from_bool(!ret);
  } else if (logic_ == common::Logical::AND) {
    bool ret = (rhs_->eval_vertex(label, v, idx).as_bool() &&
                lhs_->eval_vertex(label, v, idx).as_bool());
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::REGEX) {
    std::string ret(lhs_->eval_vertex(label, v, idx).as_string());
    std::string rhs(rhs_->eval_vertex(label, v, idx).as_string());
    return RTAny::from_bool(std::regex_match(ret, std::regex(rhs)));

  } else if (logic_ == common::Logical::OR) {
    bool ret = (rhs_->eval_vertex(label, v, idx).as_bool() ||
                lhs_->eval_vertex(label, v, idx).as_bool());
    return RTAny::from_bool(ret);
  } else {
    LOG(FATAL) << "not support..." << static_cast<int>(logic_);
  }
  return RTAny::from_bool(false);
}

RTAny LogicalExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                             const Any& data, size_t idx) const {
  if (logic_ == common::Logical::LT) {
    bool ret = lhs_->eval_edge(label, src, dst, data, idx) <
               rhs_->eval_edge(label, src, dst, data, idx);
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::GT) {
    bool ret = rhs_->eval_edge(label, src, dst, data, idx) <
               lhs_->eval_edge(label, src, dst, data, idx);
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::GE) {
    bool ret = lhs_->eval_edge(label, src, dst, data, idx) <
               rhs_->eval_edge(label, src, dst, data, idx);
    return RTAny::from_bool(!ret);
  } else if (logic_ == common::Logical::LE) {
    bool ret = rhs_->eval_edge(label, src, dst, data, idx) <
               lhs_->eval_edge(label, src, dst, data, idx);
    return RTAny::from_bool(!ret);
  } else if (logic_ == common::Logical::EQ) {
    bool ret = (rhs_->eval_edge(label, src, dst, data, idx) ==
                lhs_->eval_edge(label, src, dst, data, idx));
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::NE) {
    bool ret = (rhs_->eval_edge(label, src, dst, data, idx) ==
                lhs_->eval_edge(label, src, dst, data, idx));
    return RTAny::from_bool(!ret);
  } else if (logic_ == common::Logical::AND) {
    bool ret = (rhs_->eval_edge(label, src, dst, data, idx).as_bool() &&
                lhs_->eval_edge(label, src, dst, data, idx).as_bool());
    return RTAny::from_bool(ret);
  } else if (logic_ == common::Logical::REGEX) {
    std::string ret(lhs_->eval_edge(label, src, dst, data, idx).as_string());
    std::string rhs(rhs_->eval_edge(label, src, dst, data, idx).as_string());
    return RTAny::from_bool(std::regex_match(ret, std::regex(rhs)));
  } else if (logic_ == common::Logical::OR) {
    bool ret = (rhs_->eval_edge(label, src, dst, data, idx).as_bool() ||
                lhs_->eval_edge(label, src, dst, data, idx).as_bool());
    return RTAny::from_bool(ret);
  } else {
    LOG(FATAL) << "not support..." << static_cast<int>(logic_);
  }
  return RTAny::from_bool(false);
}

RTAnyType LogicalExpr::type() const { return RTAnyType::kBoolValue; }

UnaryLogicalExpr::UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr,
                                   common::Logical logic)
    : expr_(std::move(expr)), logic_(logic) {}

RTAny UnaryLogicalExpr::eval_path(size_t idx) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(!expr_->eval_path(idx).as_bool());
  } else if (logic_ == common::Logical::ISNULL) {
    return RTAny::from_bool(expr_->eval_path(idx, 0).type() ==
                            RTAnyType::kNull);
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAny UnaryLogicalExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(!expr_->eval_vertex(label, v, idx).as_bool());
  } else if (logic_ == common::Logical::ISNULL) {
    return RTAny::from_bool(expr_->eval_vertex(label, v, idx, 0).is_null());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAny UnaryLogicalExpr::eval_edge(const LabelTriplet& label, vid_t src,
                                  vid_t dst, const Any& data,
                                  size_t idx) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(
        !expr_->eval_edge(label, src, dst, data, idx).as_bool());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAnyType UnaryLogicalExpr::type() const { return RTAnyType::kBoolValue; }

ArithExpr::ArithExpr(std::unique_ptr<ExprBase>&& lhs,
                     std::unique_ptr<ExprBase>&& rhs, common::Arithmetic arith)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)), arith_(arith) {}

RTAny ArithExpr::eval_path(size_t idx) const {
  switch (arith_) {
  case common::Arithmetic::ADD:
    return lhs_->eval_path(idx) + rhs_->eval_path(idx);
  case common::Arithmetic::SUB:
    return lhs_->eval_path(idx) - rhs_->eval_path(idx);
  case common::Arithmetic::DIV:
    return lhs_->eval_path(idx) / rhs_->eval_path(idx);
  default:
    LOG(FATAL) << "not support" << static_cast<int>(arith_);
  }
  return RTAny();
}

RTAny ArithExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  switch (arith_) {
  case common::Arithmetic::ADD:
    return lhs_->eval_path(idx) + rhs_->eval_path(idx);
  default:
    LOG(FATAL) << "not support";
  }
  return RTAny();
}

RTAny ArithExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& data, size_t idx) const {
  switch (arith_) {
  case common::Arithmetic::ADD:
    return lhs_->eval_path(idx) + rhs_->eval_path(idx);
  default:
    LOG(FATAL) << "not support";
  }
  return RTAny();
}

RTAnyType ArithExpr::type() const { return lhs_->type(); }

ConstExpr::ConstExpr(const RTAny& val) : val_(val) {
  if (val_.type() == RTAnyType::kStringValue) {
    s = val_.as_string();
    val_ = RTAny::from_string(s);
  }
}
RTAny ConstExpr::eval_path(size_t idx) const { return val_; }
RTAny ConstExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  return val_;
}
RTAny ConstExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& data, size_t idx) const {
  return val_;
}

RTAnyType ConstExpr::type() const { return val_.type(); }

ExtractExpr::ExtractExpr(std::unique_ptr<ExprBase>&& expr,
                         const common::Extract& extract)
    : expr_(std::move(expr)), extract_(extract) {}

static int32_t extract_year(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r((time_t*) (&micro_second), &tm);
  return tm.tm_year + 1900;
}

static int32_t extract_month(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r((time_t*) (&micro_second), &tm);
  return tm.tm_mon + 1;
}

static int32_t extract_day(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r((time_t*) (&micro_second), &tm);
  return tm.tm_mday;
}

static int32_t extract_time_from_milli_second(int64_t ms,
                                              common::Extract extract) {
  if (extract.interval() == common::Extract::YEAR) {
    return extract_year(ms);
  } else if (extract.interval() == common::Extract::MONTH) {
    return extract_month(ms);
  } else if (extract.interval() == common::Extract::DAY) {
    return extract_day(ms);
  } else {
    LOG(FATAL) << "not support";
  }
  return 0;
}

RTAny ExtractExpr::eval_path(size_t idx) const {
  auto ms = expr_->eval_path(idx).as_date32();
  int32_t val = extract_time_from_milli_second(ms, extract_);
  return RTAny::from_int32(val);
}

RTAny ExtractExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  auto ms = expr_->eval_vertex(label, v, idx).as_date32();
  int32_t val = extract_time_from_milli_second(ms, extract_);
  return RTAny::from_int32(val);
}

RTAny ExtractExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                             const Any& data, size_t idx) const {
  auto ms = expr_->eval_edge(label, src, dst, data, idx).as_date32();
  int32_t val = extract_time_from_milli_second(ms, extract_);
  return RTAny::from_int32(val);
}

RTAnyType ExtractExpr::type() const { return RTAnyType::kI32Value; }

CaseWhenExpr::CaseWhenExpr(
    std::vector<std::pair<std::unique_ptr<ExprBase>,
                          std::unique_ptr<ExprBase>>>&& when_then_exprs,
    std::unique_ptr<ExprBase>&& else_expr)
    : when_then_exprs_(std::move(when_then_exprs)),
      else_expr_(std::move(else_expr)) {}

RTAny CaseWhenExpr::eval_path(size_t idx) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_path(idx).as_bool()) {
      return pair.second->eval_path(idx);
    }
  }
  return else_expr_->eval_path(idx);
}

RTAny CaseWhenExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_vertex(label, v, idx).as_bool()) {
      return pair.second->eval_vertex(label, v, idx);
    }
  }
  return else_expr_->eval_vertex(label, v, idx);
}

RTAny CaseWhenExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const Any& data, size_t idx) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_edge(label, src, dst, data, idx).as_bool()) {
      return pair.second->eval_edge(label, src, dst, data, idx);
    }
  }
  return else_expr_->eval_edge(label, src, dst, data, idx);
}

RTAnyType CaseWhenExpr::type() const {
  RTAnyType type;
  bool null_able = false;
  if (when_then_exprs_.size() > 0) {
    if (when_then_exprs_[0].second->type() == RTAnyType::kNull) {
      null_able = true;
    } else {
      type = when_then_exprs_[0].second->type();
    }
  }
  if (else_expr_->type() == RTAnyType::kNull) {
    null_able = true;
  } else {
    type = else_expr_->type();
  }
  type.null_able_ = null_able;
  return type;
}

TupleExpr::TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs)
    : exprs_(std::move(exprs)) {}

RTAny TupleExpr::eval_path(size_t idx) const {
  std::vector<RTAny> ret;
  for (auto& expr : exprs_) {
    ret.push_back(expr->eval_path(idx));
  }
  return RTAny::from_tuple(std::move(ret));
}

RTAny TupleExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  std::vector<RTAny> ret;
  for (auto& expr : exprs_) {
    ret.push_back(expr->eval_vertex(label, v, idx));
  }
  return RTAny::from_tuple(std::move(ret));
}

RTAny TupleExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& data, size_t idx) const {
  std::vector<RTAny> ret;
  for (auto& expr : exprs_) {
    ret.push_back(expr->eval_edge(label, src, dst, data, idx));
  }
  return RTAny::from_tuple(std::move(ret));
}

RTAnyType TupleExpr::type() const { return RTAnyType::kTuple; }

static RTAny parse_const_value(const common::Value& val) {
  switch (val.item_case()) {
  case common::Value::kI32:
    return RTAny::from_int32(val.i32());
  case common::Value::kStr:
    return RTAny::from_string(val.str());
  case common::Value::kI64:
    return RTAny::from_int64(val.i64());
  case common::Value::kBoolean:
    return RTAny::from_bool(val.boolean());
  case common::Value::kNone:
    return RTAny(RTAnyType::kNull);
  case common::Value::kF64:
    return RTAny::from_double(val.f64());
  default:
    LOG(FATAL) << "not support for " << val.item_case();
  }
  return RTAny();
}

static RTAny parse_param(const common::DynamicParam& param,
                         const std::map<std::string, std::string>& input) {
  if (param.data_type().type_case() ==
      common::IrDataType::TypeCase::kDataType) {
    common::DataType dt = param.data_type().data_type();
    const std::string& name = param.name();
    if (dt == common::DataType::DATE32) {
      int64_t val = std::stoll(input.at(name));
      return RTAny::from_int64(val);
    } else if (dt == common::DataType::STRING) {
      const std::string& val = input.at(name);
      return RTAny::from_string(val);
    } else if (dt == common::DataType::INT32) {
      int val = std::stoi(input.at(name));
      return RTAny::from_int32(val);
    } else if (dt == common::DataType::INT64) {
      int64_t val = std::stoll(input.at(name));
      return RTAny::from_int64(val);
    }

    LOG(FATAL) << "not support type: " << common::DataType_Name(dt);
  }
  LOG(FATAL) << "graph data type not expected....";
  return RTAny();
}

static inline int get_proiority(const common::ExprOpr& opr) {
  switch (opr.item_case()) {
  case common::ExprOpr::kBrace: {
    return 17;
  }
  case common::ExprOpr::kExtract: {
    return 2;
  }
  case common::ExprOpr::kLogical: {
    switch (opr.logical()) {
    case common::Logical::AND:
      return 11;
    case common::Logical::OR:
      return 12;
    case common::Logical::NOT:
      return 2;
    case common::Logical::WITHIN:
    case common::Logical::WITHOUT:
      return 2;
    case common::Logical::EQ:
    case common::Logical::NE:
      return 7;
    case common::Logical::GE:
    case common::Logical::GT:
    case common::Logical::LT:
    case common::Logical::LE:
      return 6;
    case common::Logical::REGEX:
      return 2;
    default:
      return 16;
    }
  }
  case common::ExprOpr::kArith: {
    switch (opr.arith()) {
    case common::Arithmetic::ADD:
    case common::Arithmetic::SUB:
      return 4;
    case common::Arithmetic::MUL:
    case common::Arithmetic::DIV:
    case common::Arithmetic::MOD:
      return 3;
    default:
      return 16;
    }
  }
  default:
    return 16;
  }
  return 16;
}

template <typename GRAPH_IMPL>
static std::unique_ptr<ExprBase> build_expr(
    const GraphInterface<GRAPH_IMPL>& txn, const Context& ctx,
    const std::map<std::string, std::string>& params,
    std::stack<common::ExprOpr>& opr_stack, VarType var_type) {
  while (!opr_stack.empty()) {
    auto opr = opr_stack.top();
    opr_stack.pop();
    switch (opr.item_case()) {
    case common::ExprOpr::kConst: {
      if (opr.const_().item_case() == common::Value::kStr) {
        const std::string& str = opr.const_().str();
        return std::make_unique<ConstExpr>(RTAny::from_string(str));
      }
      return std::make_unique<ConstExpr>(parse_const_value(opr.const_()));
    }
    case common::ExprOpr::kParam: {
      return std::make_unique<ConstExpr>(parse_param(opr.param(), params));
    }
    case common::ExprOpr::kVar: {
      return std::make_unique<VariableExpr<GRAPH_IMPL>>(txn, ctx, opr.var(),
                                                        var_type);
    }
    case common::ExprOpr::kLogical: {
      if (opr.logical() == common::Logical::WITHIN) {
        auto lhs = opr_stack.top();
        opr_stack.pop();
        auto rhs = opr_stack.top();
        opr_stack.pop();
        CHECK(lhs.has_var());
        CHECK(rhs.has_const_());
        auto key = std::make_unique<VariableExpr<GRAPH_IMPL>>(
            txn, ctx, lhs.var(), var_type);
        if (key->type() == RTAnyType::kI64Value) {
          return std::make_unique<WithInExpr<int64_t, GRAPH_IMPL>>(
              txn, ctx, std::move(key), rhs.const_());
        } else if (key->type() == RTAnyType::kI32Value) {
          return std::make_unique<WithInExpr<int32_t, GRAPH_IMPL>>(
              txn, ctx, std::move(key), rhs.const_());
        } else if (key->type() == RTAnyType::kStringValue) {
          return std::make_unique<WithInExpr<std::string, GRAPH_IMPL>>(
              txn, ctx, std::move(key), rhs.const_());
        } else {
          LOG(FATAL) << "not support";
        }
      } else if (opr.logical() == common::Logical::NOT ||
                 opr.logical() == common::Logical::ISNULL) {
        auto lhs = build_expr(txn, ctx, params, opr_stack, var_type);
        return std::make_unique<UnaryLogicalExpr>(std::move(lhs),
                                                  opr.logical());
      } else {
        auto lhs = build_expr(txn, ctx, params, opr_stack, var_type);
        auto rhs = build_expr(txn, ctx, params, opr_stack, var_type);
        return std::make_unique<LogicalExpr>(std::move(lhs), std::move(rhs),
                                             opr.logical());
      }
      break;
    }
    case common::ExprOpr::kArith: {
      auto lhs = build_expr(txn, ctx, params, opr_stack, var_type);
      auto rhs = build_expr(txn, ctx, params, opr_stack, var_type);
      return std::make_unique<ArithExpr>(std::move(lhs), std::move(rhs),
                                         opr.arith());
    }
    case common::ExprOpr::kCase: {
      auto op = opr.case_();
      size_t len = op.when_then_expressions_size();
      std::vector<
          std::pair<std::unique_ptr<ExprBase>, std::unique_ptr<ExprBase>>>
          when_then_exprs;
      for (size_t i = 0; i < len; ++i) {
        auto when_expr = op.when_then_expressions(i).when_expression();
        auto then_expr = op.when_then_expressions(i).then_result_expression();
        when_then_exprs.emplace_back(
            parse_expression_impl(txn, ctx, params, when_expr, var_type),
            parse_expression_impl(txn, ctx, params, then_expr, var_type));
      }
      auto else_expr = parse_expression_impl(
          txn, ctx, params, op.else_result_expression(), var_type);
      return std::make_unique<CaseWhenExpr>(std::move(when_then_exprs),
                                            std::move(else_expr));
    }
    case common::ExprOpr::kExtract: {
      auto hs = build_expr(txn, ctx, params, opr_stack, var_type);
      return std::make_unique<ExtractExpr>(std::move(hs), opr.extract());
    }
    case common::ExprOpr::kVars: {
      auto op = opr.vars();
      std::vector<std::unique_ptr<ExprBase>> exprs;
      for (int i = 0; i < op.keys_size(); ++i) {
        exprs.push_back(std::make_unique<VariableExpr<GRAPH_IMPL>>(
            txn, ctx, op.keys(i), var_type));
      }
      return std::make_unique<TupleExpr>(std::move(exprs));
      // LOG(FATAL) << "not support" << opr.DebugString();
      // break;
    }
    case common::ExprOpr::kMap: {
      auto op = opr.map();
      std::vector<std::string> keys_vec;
      std::vector<std::unique_ptr<ExprBase>> exprs;
      for (int i = 0; i < op.key_vals_size(); ++i) {
        auto key = op.key_vals(i).key();
        auto val = op.key_vals(i).val();
        auto any = parse_const_value(key);
        CHECK(any.type() == RTAnyType::kStringValue);
        {
          auto str = any.as_string();
          keys_vec.push_back(std::string(str));
        }
        exprs.emplace_back(std::make_unique<VariableExpr<GRAPH_IMPL>>(
            txn, ctx, val,
            var_type));  // just for parse
      }
      if (exprs.size() > 0) {
        return std::make_unique<MapExpr>(std::move(keys_vec), std::move(exprs));
      }
      LOG(FATAL) << "not support" << opr.DebugString();
    }
    default:
      LOG(FATAL) << "not support" << opr.DebugString();
      break;
    }
  }
  return nullptr;
}
template <typename GRAPH_IMPL>
static std::unique_ptr<ExprBase> parse_expression_impl(
    const GraphInterface<GRAPH_IMPL>& txn, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const common::Expression& expr, VarType var_type) {
  std::stack<common::ExprOpr> opr_stack;
  std::stack<common::ExprOpr> opr_stack2;
  const auto& oprs = expr.operators();
  for (auto it = oprs.rbegin(); it != oprs.rend(); ++it) {
    switch ((*it).item_case()) {
    case common::ExprOpr::kBrace: {
      auto brace = (*it).brace();
      if (brace == common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE) {
        while (!opr_stack.empty() &&
               opr_stack.top().item_case() != common::ExprOpr::kBrace) {
          opr_stack2.push(opr_stack.top());
          opr_stack.pop();
        }
        CHECK(!opr_stack.empty());
        opr_stack.pop();
      } else if (brace == common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
        opr_stack.emplace(*it);
      }
      break;
    }
    case common::ExprOpr::kConst:
    case common::ExprOpr::kVar:
    case common::ExprOpr::kParam:
    case common::ExprOpr::kVars: {
      opr_stack2.push(*it);
      break;
    }
    case common::ExprOpr::kArith:
    case common::ExprOpr::kLogical: {
      // unary operator
      if ((*it).logical() == common::Logical::NOT ||
          (*it).logical() == common::Logical::ISNULL) {
        opr_stack2.push(*it);
        break;
      }

      while (!opr_stack.empty() &&
             get_proiority(opr_stack.top()) <= get_proiority(*it)) {
        opr_stack2.push(opr_stack.top());
        opr_stack.pop();
      }
      opr_stack.push(*it);
      break;
    }
    case common::ExprOpr::kExtract: {
      opr_stack2.push(*it);
      break;
    }
    case common::ExprOpr::kCase: {
      opr_stack2.push(*it);
      break;
    }
    case common::ExprOpr::kMap: {
      opr_stack2.push(*it);
      break;
    }
    default: {
      LOG(FATAL) << "not support" << (*it).DebugString();
      break;
    }
    }
  }
  while (!opr_stack.empty()) {
    opr_stack2.push(opr_stack.top());
    opr_stack.pop();
  }
  return build_expr(txn, ctx, params, opr_stack2, var_type);
}

template <typename GRAPH_IMPL>
std::unique_ptr<ExprBase> parse_expression(
    const GraphInterface<GRAPH_IMPL>& txn, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const common::Expression& expr, VarType var_type) {
  return parse_expression_impl(txn, ctx, params, expr, var_type);
}

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_RUNTIME_EXPR_IMPL_H_