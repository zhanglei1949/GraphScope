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

#include <limits>

#include "flex/engines/graph_db/runtime/adhoc/runtime.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/adhoc/var.h"
#include "flex/engines/graph_db/runtime/common/operators/order_by.h"

#include "flex/proto_generated_gie/algebra.pb.h"

namespace gs {

namespace runtime {

class GeneralComparer {
 public:
  GeneralComparer() : keys_num_(0) {}
  ~GeneralComparer() {}

  void add_keys(Var&& key, bool asc) {
    keys_.emplace_back(std::move(key));
    order_.push_back(asc);
    ++keys_num_;
  }

  bool operator()(size_t lhs, size_t rhs) const {
    for (size_t k = 0; k < keys_num_; ++k) {
      auto& v = keys_[k];
      auto asc = order_[k];
      RTAny lhs_val = v.get(lhs);
      RTAny rhs_val = v.get(rhs);
      if (lhs_val < rhs_val) {
        return asc;
      } else if (rhs_val < lhs_val) {
        return !asc;
      }
    }

    return lhs < rhs;
  }

 private:
  std::vector<Var> keys_;
  std::vector<bool> order_;
  size_t keys_num_;
};

Context eval_order_by(const algebra::OrderBy& opr, const ReadTransaction& txn,
                      Context&& ctx, bool enable_staged) {
#ifdef SINGLE_THREAD
  auto& op_cost = OpCost::get().table;
#endif
  int lower = 0;
  int upper = std::numeric_limits<int>::max();
  if (opr.has_limit()) {
    lower = std::max(lower, static_cast<int>(opr.limit().lower()));
    upper = std::min(upper, static_cast<int>(opr.limit().upper()));
  }

  GeneralComparer cmp;
  int keys_num = opr.pairs_size();
  CHECK_GE(keys_num, 1);
  double t0 = -grape::GetCurrentTime();
  bool staged_order_by = false;
  std::vector<size_t> picked_indices;
#if 1
  if (enable_staged) {
    if (ctx.row_num() <= static_cast<size_t>(upper)) {
      // LOG(INFO) << "row number of context <= upper, fallback";
    } else {
      if (opr.pairs(0).key().has_tag() &&
          opr.pairs(0).key().tag().item_case() ==
              common::NameOrId::ItemCase::kId) {
        int tag = opr.pairs(0).key().tag().id();
        if ((opr.pairs(0).order() == algebra::OrderBy_OrderingPair_Order::
                                         OrderBy_OrderingPair_Order_ASC) ||
            (opr.pairs(0).order() == algebra::OrderBy_OrderingPair_Order::
                                         OrderBy_OrderingPair_Order_DESC)) {
          bool asc = opr.pairs(0).order() ==
                     algebra::OrderBy_OrderingPair_Order::
                         OrderBy_OrderingPair_Order_ASC;
          auto col = ctx.get(tag);
          if (col != nullptr) {
            if (!opr.pairs(0).key().has_property()) {
              staged_order_by = col->order_by_limit(asc, upper, picked_indices);
              if (!staged_order_by) {
                LOG(INFO) << "column staged order by returns false, fallback";
              }
            } else if (col->column_type() == ContextColumnType::kVertex) {
              std::string prop_name =
                  opr.pairs(0).key().property().key().name();
              if (prop_name == "id") {
                staged_order_by = vertex_id_topN(
                    asc, upper, std::dynamic_pointer_cast<IVertexColumn>(col),
                    txn, picked_indices);
              } else {
                staged_order_by = vertex_property_topN(
                    asc, upper, std::dynamic_pointer_cast<IVertexColumn>(col),
                    txn, prop_name, picked_indices);
              }
            } else {
              LOG(INFO) << "first key is property of not vertex, fallback";
            }
          } else {
            LOG(INFO) << "the col of first key is null, fallback";
          }
        } else {
          LOG(INFO) << "first pair doesn't have tag";
        }
      } else {
        LOG(INFO) << "order is not asc or desc";
      }
    }
  }
#endif
  t0 += grape::GetCurrentTime();
#ifdef SINGLE_THREAD
  op_cost["order_by:preprocess"] += t0;
#endif
  for (int i = 0; i < keys_num; ++i) {
    const algebra::OrderBy_OrderingPair& pair = opr.pairs(i);
    Var v(txn, ctx, pair.key(), VarType::kPathVar);
    CHECK(pair.order() == algebra::OrderBy_OrderingPair_Order::
                              OrderBy_OrderingPair_Order_ASC ||
          pair.order() == algebra::OrderBy_OrderingPair_Order::
                              OrderBy_OrderingPair_Order_DESC);
    bool order =
        pair.order() ==
        algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC;
    cmp.add_keys(std::move(v), order);
  }

  if (!staged_order_by) {
    double t1 = -grape::GetCurrentTime();
    OrderBy::order_by_with_limit<GeneralComparer>(txn, ctx, cmp, lower, upper);
    t1 += grape::GetCurrentTime();
#ifdef SINGLE_THREAD
    op_cost["order_by:order_by"] += t0;
#endif
  } else {
    double t1 = -grape::GetCurrentTime();
    CHECK_GE(picked_indices.size(), upper);
    OrderBy::staged_order_by_with_limit<GeneralComparer>(txn, ctx, cmp, lower,
                                                         upper, picked_indices);
    t1 += grape::GetCurrentTime();
#ifdef SINGLE_THREAD
    op_cost["order_by:staged_order_by"] += t1;
#endif
  }
  return ctx;
}

}  // namespace runtime

}  // namespace gs