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

#include "flex/engines/graph_db/runtime/common/operators/dedup.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/var.h"

namespace gs {

namespace runtime {

Context eval_dedup(const algebra::Dedup& opr, const GraphReadInterface& graph,
                   Context&& ctx) {
  std::vector<size_t> keys;
  std::vector<std::function<RTAny(size_t)>> vars;
  int keys_num = opr.keys_size();
  bool flag = false;
  for (int k_i = 0; k_i < keys_num; ++k_i) {
    const common::Variable& key = opr.keys(k_i);

    int tag = -1;
    if (key.has_tag()) {
      tag = key.tag().id();
    }
    if (key.has_property()) {
      Var var(graph, ctx, key, VarType::kPathVar);
      vars.emplace_back([var](size_t i) { return var.get(i); });
      flag = true;
    } else {
      keys.push_back(tag);
    }
  }
  if (!flag) {
    Dedup::dedup(graph, ctx, keys);
  } else {
    Dedup::dedup(graph, ctx, keys, vars);
  }
  //  LOG(INFO) << "dedup row num:" << ctx.row_num();
  return ctx;
}

WriteContext eval_dedup(const algebra::Dedup& opr,
                        const GraphInsertInterface& graph, WriteContext&& ctx) {
  std::vector<size_t> keys;

  int keys_num = opr.keys_size();
  int row_num = ctx.row_num();
  if (row_num == 0) {
    return ctx;
  }

  for (int k_i = 0; k_i < keys_num; ++k_i) {
    const common::Variable& key = opr.keys(k_i);
    int tag = -1;
    CHECK(key.has_tag());
    tag = key.tag().id();
    keys.emplace_back(tag);
    CHECK(!key.has_property()) << "dedup not support property";
  }
  if (keys.size() == 2) {
    std::vector<
        std::tuple<WriteContext::WriteParams, WriteContext::WriteParams, int>>
        keys_tuples;
    for (int i = 0; i < ctx.row_num(); ++i) {
      keys_tuples.emplace_back(ctx.get(keys[0]).get(i), ctx.get(keys[1]).get(i),
                               i);
    }
    std::sort(keys_tuples.begin(), keys_tuples.end());
    std::vector<size_t> offsets;
    offsets.emplace_back(std::get<2>(keys_tuples[0]));
    for (int i = 1; i < ctx.row_num(); ++i) {
      if (!(std::get<0>(keys_tuples[i]) == std::get<0>(keys_tuples[i - 1]) &&
            std::get<1>(keys_tuples[i]) == std::get<1>(keys_tuples[i - 1]))) {
        offsets.emplace_back(std::get<2>(keys_tuples[i]));
      }
    }
    ctx.reshuffle(offsets);
    return ctx;
  } else if (keys.size() == 3) {
    std::vector<std::tuple<WriteContext::WriteParams, WriteContext::WriteParams,
                           WriteContext::WriteParams, int>>
        keys_tuples;
    for (int i = 0; i < ctx.row_num(); ++i) {
      keys_tuples.emplace_back(ctx.get(keys[0]).get(i), ctx.get(keys[1]).get(i),
                               ctx.get(keys[2]).get(i), i);
    }
    std::sort(keys_tuples.begin(), keys_tuples.end());
    std::vector<size_t> offsets;
    offsets.emplace_back(std::get<3>(keys_tuples[0]));
    for (int i = 1; i < ctx.row_num(); ++i) {
      if (!(std::get<0>(keys_tuples[i]) == std::get<0>(keys_tuples[i - 1]) &&
            std::get<1>(keys_tuples[i]) == std::get<1>(keys_tuples[i - 1]) &&
            std::get<2>(keys_tuples[i]) == std::get<2>(keys_tuples[i - 1]))) {
        offsets.emplace_back(std::get<3>(keys_tuples[i]));
      }
    }
    ctx.reshuffle(offsets);

    return ctx;
  } else {
    LOG(FATAL) << "dedup not support keys size:" << keys.size();
  }

  //  LOG(INFO) << "dedup row num:" << ctx.row_num();
  return ctx;
}

}  // namespace runtime

}  // namespace gs
