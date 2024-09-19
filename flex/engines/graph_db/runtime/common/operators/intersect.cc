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

#include "flex/engines/graph_db/runtime/common/operators/intersect.h"
#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

namespace gs {

namespace runtime {

static void ensure_sorted(std::shared_ptr<ValueColumn<size_t>> idx_col,
                          std::shared_ptr<IContextColumn> val_col) {
  auto& idx_col_ref = *idx_col;
  size_t row_num = idx_col_ref.size();
  for (size_t k = 1; k < row_num; ++k) {
    CHECK_GE(idx_col_ref.get_value(k), idx_col_ref.get_value(k - 1));
  }
}

Context Intersect::intersect(Context&& ctx,
                             std::vector<std::tuple<Context, int, int>>&& ctxs,
                             int alias) {
  std::vector<std::pair<std::shared_ptr<ValueColumn<size_t>>,
                        std::shared_ptr<IContextColumn>>>
      cols;
  for (auto& c : ctxs) {
    auto& this_ctx = std::get<0>(c);
    int idx_col = std::get<1>(c);
    int value_col = std::get<2>(c);
    cols.emplace_back(
        std::dynamic_pointer_cast<ValueColumn<size_t>>(this_ctx.get(idx_col)),
        this_ctx.get(value_col));
  }
  for (auto& pair : cols) {
    ensure_sorted(pair.first, pair.second);
  }
  if (cols.size() == 2) {
    auto& idx_col0 = *cols[0].first;
    auto& idx_col1 = *cols[1].first;

    size_t rn0 = idx_col0.size();
    size_t rn1 = idx_col1.size();

    CHECK(cols[0].second->column_type() == cols[1].second->column_type());
    if (cols[0].second->column_type() == ContextColumnType::kVertex) {
      auto vlist0_ptr =
          std::dynamic_pointer_cast<IVertexColumn>(cols[0].second);
      auto vlist1_ptr =
          std::dynamic_pointer_cast<IVertexColumn>(cols[1].second);
      if (vlist0_ptr->vertex_column_type() == VertexColumnType::kSingle &&
          vlist1_ptr->vertex_column_type() == VertexColumnType::kSingle) {
        auto& vlist0 = *std::dynamic_pointer_cast<SLVertexColumn>(vlist0_ptr);
        auto& vlist1 = *std::dynamic_pointer_cast<SLVertexColumn>(vlist1_ptr);

        std::vector<size_t> shuffle_offsets;
        SLVertexColumnBuilder builder(*vlist0.get_labels_set().begin());

        size_t idx0 = 0, idx1 = 0;
        std::set<vid_t> lhs_set;
        while (idx0 < rn0 && idx1 < rn1) {
          if (idx_col0.get_value(idx0) < idx_col1.get_value(idx1)) {
            ++idx0;
          } else if (idx_col0.get_value(idx0) > idx_col1.get_value(idx1)) {
            ++idx1;
          } else {
            lhs_set.clear();
            size_t common_index = idx_col0.get_value(idx0);
            while (idx_col0.get_value(idx0) == common_index) {
              lhs_set.insert(vlist0.get_vertex(idx0).second);
              ++idx0;
            }
            while (idx_col1.get_value(idx1) == common_index) {
              vid_t cur_v = vlist1.get_vertex(idx1).second;
              if (lhs_set.find(cur_v) != lhs_set.end()) {
                shuffle_offsets.push_back(common_index);
                builder.push_back_opt(cur_v);
              }
              ++idx1;
            }
          }
        }

        ctx.set_with_reshuffle(alias, builder.finish(), shuffle_offsets);
        return ctx;
      }
    }
  }

  LOG(FATAL) << "not support";
}
static Context left_outer_intersect(Context&& ctx, Context&& ctx0,
                                    Context&& ctx1, int key) {
  // specifically, this function is called when the first context is not
  // optional and the second context is optional
  auto& idx_col0 = ctx0.get_offsets();
  auto& idx_col1 = ctx1.get_offsets();
  auto& vlist0 = *(std::dynamic_pointer_cast<IVertexColumn>(ctx0.get(key)));
  auto& vlist1 = *(std::dynamic_pointer_cast<IVertexColumn>(ctx1.get(key)));

  std::vector<size_t> offset0, offset1;
  if (ctx0.row_num() == ctx.row_num()) {
    bool flag = true;
    for (size_t i = 0; i < idx_col0.size(); ++i) {
      if (idx_col0.get_value(i) != i) {
        flag = false;
        break;
      }
    }
    if (flag) {
      size_t j = 0;
      for (size_t i = 0; i < ctx0.row_num(); i++) {
        bool exist = false;
        for (; j < ctx1.row_num(); ++j) {
          if (idx_col1.get_value(j) != idx_col0.get_value(i)) {
            break;
          }
          if (vlist1.has_value(j) &&
              vlist0.get_vertex(i) == vlist1.get_vertex(j)) {
            exist = true;
            offset0.emplace_back(i);
            offset1.emplace_back(j);
          }
        }
        if (!exist) {
          offset0.emplace_back(i);
          offset1.emplace_back(std::numeric_limits<size_t>::max());
        }
      }
      ctx0.reshuffle(offset0);
      ctx1.optional_reshuffle(offset1);
      ctx.reshuffle(ctx0.get_offsets().data());
      for (size_t i = 0; i < ctx0.col_num() || i < ctx1.col_num(); ++i) {
        if (i < ctx0.col_num()) {
          if (ctx0.get(i) != nullptr) {
            ctx.set(i, ctx0.get(i));
          }
        }
        if (i < ctx1.col_num()) {
          if ((i >= ctx.col_num() || ctx.get(i) == nullptr) &&
              ctx1.get(i) != nullptr) {
            ctx.set(i, ctx1.get(i));
          }
        } else if (i >= ctx.col_num()) {
          ctx.set(i, nullptr);
        }
      }
      return ctx;
    }
  }

  LOG(FATAL) << "not support";
  return ctx;
}
static Context intersect_impl(Context&& ctx, std::vector<Context>&& ctxs,
                              int key) {
  if (ctxs[0].get(key)->column_type() == ContextColumnType::kVertex) {
    if (ctxs.size() == 2) {
      auto& vlist0 =
          *(std::dynamic_pointer_cast<IVertexColumn>(ctxs[0].get(key)));
      auto& vlist1 =
          *(std::dynamic_pointer_cast<IVertexColumn>(ctxs[1].get(key)));
      //      LOG(INFO) << vlist0.size() << " " << vlist1.size();
      //      LOG(INFO) << vlist0.is_optional() << " " << vlist1.is_optional();
      if (!vlist0.is_optional() && vlist1.is_optional()) {
        return left_outer_intersect(std::move(ctx), std::move(ctxs[0]),
                                    std::move(ctxs[1]), key);
      } else if (vlist0.is_optional() && !vlist1.is_optional()) {
        return left_outer_intersect(std::move(ctx), std::move(ctxs[1]),
                                    std::move(ctxs[0]), key);
      } else if (vlist0.is_optional() && vlist1.is_optional()) {
        //        LOG(INFO) << "both optional" << vlist0.size() << " " <<
        //        vlist1.size();
      }
      auto& idx_col0 = ctxs[0].get_offsets();
      auto& idx_col1 = ctxs[1].get_offsets();
      std::vector<size_t> offsets0(idx_col0.size()), offsets1(idx_col1.size());
      for (size_t k = 0; k < idx_col0.size(); ++k) {
        offsets0[k] = k;
      }
      for (size_t k = 0; k < idx_col1.size(); ++k) {
        offsets1[k] = k;
      }

      std::sort(offsets0.begin(), offsets0.end(),
                [&idx_col0, &vlist0](size_t a, size_t b) {
                  if (idx_col0.get_value(a) == idx_col0.get_value(b)) {
                    return vlist0.get_vertex(a) < vlist0.get_vertex(b);
                  }
                  return idx_col0.get_value(a) < idx_col0.get_value(b);
                });
      std::sort(offsets1.begin(), offsets1.end(),
                [&idx_col1, &vlist1](size_t a, size_t b) {
                  if (idx_col1.get_value(a) == idx_col1.get_value(b)) {
                    return vlist1.get_vertex(a) < vlist1.get_vertex(b);
                  }
                  return idx_col1.get_value(a) < idx_col1.get_value(b);
                });
      std::vector<size_t> shuffle_offsets;
      std::vector<size_t> shuffle_offsets_1;
      size_t idx0 = 0, idx1 = 0;
      while (idx0 < idx_col0.size() && idx1 < idx_col1.size()) {
        if (idx_col0.get_value(offsets0[idx0]) <
            idx_col1.get_value(offsets1[idx1])) {
          ++idx0;
        } else if (idx_col0.get_value(offsets0[idx0]) >
                   idx_col1.get_value(offsets1[idx1])) {
          ++idx1;
        } else {
          auto v0 = vlist0.get_vertex(offsets0[idx0]);
          size_t pre_idx1 = idx1;
          while (idx1 < idx_col1.size() &&
                 idx_col1.get_value(offsets1[idx1]) ==
                     idx_col0.get_value(offsets0[idx0])) {
            auto v1 = vlist1.get_vertex(offsets1[idx1]);
            if (v0 == v1) {
              shuffle_offsets.push_back(offsets0[idx0]);
              shuffle_offsets_1.push_back(offsets1[idx1]);
            } else if (v0 < v1) {
              break;
            } else {
              pre_idx1 = idx1;
            }
            ++idx1;
          }
          ++idx0;
          idx1 = pre_idx1;
        }
      }

      ctxs[0].reshuffle(shuffle_offsets);
      ctxs[1].reshuffle(shuffle_offsets_1);
      ctx.reshuffle(ctxs[0].get_offsets().data());

      for (size_t i = 0; i < ctxs[0].col_num() || i < ctxs[1].col_num(); ++i) {
        if (i < ctxs[0].col_num()) {
          if (ctxs[0].get(i) != nullptr) {
            ctx.set(i, ctxs[0].get(i));
          }
        }
        if (i < ctxs[1].col_num()) {
          if (ctxs[1].get(i) != nullptr) {
            ctx.set(i, ctxs[1].get(i));
          }
        } else if (i > ctx.col_num()) {
          ctx.set(i, nullptr);
        }
      }
      return ctx;
    }
  }
  LOG(FATAL) << "not support";
  return Context();
}

Context Intersect::intersect(Context&& ctx, std::vector<Context>&& ctxs,
                             int key) {
  return intersect_impl(std::move(ctx), std::move(ctxs), key);
}

}  // namespace runtime

}  // namespace gs
