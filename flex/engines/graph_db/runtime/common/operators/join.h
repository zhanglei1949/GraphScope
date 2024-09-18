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

#ifndef RUNTIME_COMMON_OPERATORS_JOIN_H_
#define RUNTIME_COMMON_OPERATORS_JOIN_H_

#include <vector>
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {
namespace runtime {

struct JoinParams {
  std::vector<int> left_columns;
  std::vector<int> right_columns;
  JoinKind join_type;
};

class Join {
 public:
  static Context join(Context&& ctx, Context&& ctx2, const JoinParams& params);

  template <typename PRED_T>
  static Context join(Context&& ctx, Context&& ctx2, const JoinParams& params,
                      PRED_T&& pred) {
    Context ret;
    std::vector<size_t> left_offsets, right_offsets;
    if (params.join_type == JoinKind::kInnerJoin) {
      for (size_t i = 0; i < ctx.row_num(); i++) {
        for (size_t j = 0; j < ctx2.row_num(); j++) {
          if (pred(i, j)) {
            left_offsets.push_back(i);
            right_offsets.push_back(j);
          }
        }
      }
      ctx.reshuffle(left_offsets);
      ctx2.reshuffle(right_offsets);
      for (size_t i = 0; i < ctx.col_num() || i < ctx2.col_num(); i++) {
        if (i < ctx.col_num() && ctx.get(i) != nullptr) {
          ret.set(i, ctx.get(i));
        }
        if (i < ctx2.col_num() && ctx2.get(i) != nullptr) {
          ret.set(i, ctx2.get(i));
        }
      }

      return ret;
    }
    LOG(FATAL) << "Unsupported join type";
    return ret;
  }
};
}  // namespace runtime
}  // namespace gs

#endif  // COMMON_OPERATORS_JOIN_H_