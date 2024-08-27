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
#ifndef RUNTIME_ADHOC_PREDICATES_H_
#define RUNTIME_ADHOC_PREDICATES_H_

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/adhoc/expr.h"
#include "flex/engines/graph_db/runtime/adhoc/var.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/proto_generated_gie/expr.pb.h"

namespace gs {

namespace runtime {

struct GeneralPathPredicate {
  GeneralPathPredicate(const ReadTransaction& txn, const Context& ctx,
                       const std::map<std::string, std::string>& params,
                       const common::Expression& expr)
      : expr_(txn, ctx, params, expr, VarType::kPathVar) {}

  bool operator()(size_t idx) const {
    auto val = expr_.eval_path(idx);
    return val.as_bool();
  }

  Expr expr_;
};

struct GeneralVertexPredicate {
  GeneralVertexPredicate(const ReadTransaction& txn, const Context& ctx,
                         const std::map<std::string, std::string>& params,
                         const common::Expression& expr)
      : expr_(txn, ctx, params, expr, VarType::kVertexVar) {}

  bool operator()(label_t label, vid_t v, size_t path_idx) const {
    auto val = expr_.eval_vertex(label, v, path_idx);
    return val.as_bool();
  }

  Expr expr_;
};

struct ExactVertexPredicate {
  ExactVertexPredicate(label_t label, vid_t vid) : label_(label), vid_(vid) {}

  bool operator()(label_t label, vid_t vid, size_t path_idx) const {
    return (label == label_) && (vid == vid_);
  }

  label_t label_;
  vid_t vid_;
};

template <typename T>
struct VertexPropertyLTPredicate {
  VertexPropertyLTPredicate(const ReadTransaction& txn,
                            const std::string& property_name,
                            const RTAny& threshold) {
    label_t label_num = txn.schema().vertex_label_num();
    cols_.resize(label_num, nullptr);
    for (label_t l = 0; l < label_num; ++l) {
      auto col = std::dynamic_pointer_cast<TypedColumn<T>>(
          txn.get_vertex_property_column(l, property_name));
      if (col != nullptr) {
        cols_[l] = col.get();
      }
    }
    threshold_ = TypedConverter<T>::to_typed(threshold);
  }

  bool operator()(label_t label, vid_t vid) const {
    return cols_[label]->get_view(vid) < threshold_;
  }

  T threshold_;
  std::vector<const TypedColumn<T>*> cols_;
};

template <>
struct VertexPropertyLTPredicate<int64_t> {
  VertexPropertyLTPredicate(const ReadTransaction& txn,
                            const std::string& property_name,
                            const RTAny& threshold) {
    label_t label_num = txn.schema().vertex_label_num();
    cols_.resize(label_num, nullptr);
    date_cols_.resize(label_num, nullptr);
    for (label_t l = 0; l < label_num; ++l) {
      auto col = txn.get_vertex_property_column(l, property_name);
      if (col != nullptr) {
        auto casted_col0 = std::dynamic_pointer_cast<TypedColumn<int64_t>>(col);
        if (casted_col0 != nullptr) {
          cols_[l] = casted_col0.get();
        } else {
          auto casted_col1 = std::dynamic_pointer_cast<TypedColumn<Date>>(col);
          if (casted_col1 != nullptr) {
            date_cols_[l] = casted_col1.get();
          }
        }
      }
    }
    threshold_ = threshold.as_int64();
  }

  bool operator()(label_t label, vid_t vid) const {
    auto val = (cols_[label] == nullptr)
                   ? date_cols_[label]->get_view(vid).milli_second
                   : cols_[label]->get_view(vid);
    return val < threshold_;
  }

  int64_t threshold_;
  std::vector<const TypedColumn<int64_t>*> cols_;
  std::vector<const TypedColumn<Date>*> date_cols_;
};

struct GeneralEdgePredicate {
  GeneralEdgePredicate(const ReadTransaction& txn, const Context& ctx,
                       const std::map<std::string, std::string>& params,
                       const common::Expression& expr)
      : expr_(txn, ctx, params, expr, VarType::kEdgeVar) {}

  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t path_idx) const {
    auto val = expr_.eval_edge(label, src, dst, edata, path_idx);
    return val.as_bool();
  }

  Expr expr_;
};

struct DummyVertexPredicate {
  bool operator()(label_t label, vid_t v, size_t path_idx) const {
    return true;
  }
};

struct DummyEdgePredicate {
  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t path_idx) const {
    return true;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_PREDICATES_H_