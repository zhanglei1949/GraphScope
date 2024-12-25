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

#include "flex/engines/graph_db/runtime/common/operators/scan.h"
#include <type_traits>

namespace gs {
namespace runtime {

Context Scan::find_vertex_with_id(const GraphReadInterface& graph,
                                  label_t label, const Any& pk, int alias,
                                  bool scan_oid) {
  if (scan_oid) {
    SLVertexColumnBuilder builder(label);
    vid_t vid;
    if (graph.GetVertexIndex(label, pk, vid)) {
      builder.push_back_opt(vid);
    }
    Context ctx;
    ctx.set(alias, builder.finish());
    return ctx;
  } else {
    SLVertexColumnBuilder builder(label);
    vid_t vid{};
    int64_t gid{};
    if (pk.type == PropertyType::kInt64) {
      gid = pk.AsInt64();
    } else if (pk.type == PropertyType::kInt32) {
      gid = pk.AsInt32();
    } else {
      LOG(FATAL) << "Unsupported primary key type";
    }
    if constexpr (std::is_same_v<vid_t, uint64_t> ||
                  std::is_same_v<vid_t, int64_t>) {
      LOG(FATAL) << "Unsupported vid type";
    }
    if (GlobalId::get_label_id(gid) == label) {
      vid = GlobalId::get_vid(gid);
    } else {
      LOG(ERROR) << "Global id " << gid << " does not match label " << label;
      return Context();
    }
    builder.push_back_opt(vid);
    Context ctx;
    ctx.set(alias, builder.finish());
    return ctx;
  }
}

}  // namespace runtime

}  // namespace gs
