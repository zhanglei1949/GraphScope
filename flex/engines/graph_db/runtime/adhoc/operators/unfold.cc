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

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {
namespace runtime {
Context eval_unfold(const physical::Unfold& opr, Context&& ctx) {
  int key = opr.tag().value();
  int alias = opr.alias().value();
  auto col = ctx.get(key);
  CHECK(col->elem_type() == RTAnyType::kList);
  auto list_col = std::dynamic_pointer_cast<ListValueColumnBase>(col);
  auto [ptr, offsets] = list_col->unfold();

  ctx.set_with_reshuffle(alias, ptr, offsets);

  LOG(INFO) << ctx.row_num() << " " << offsets.size();
  return ctx;
}
}  // namespace runtime
}  // namespace gs