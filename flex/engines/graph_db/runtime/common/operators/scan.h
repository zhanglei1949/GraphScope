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

#ifndef RUNTIME_COMMON_OPERATORS_SCAN_H_
#define RUNTIME_COMMON_OPERATORS_SCAN_H_

#include "flex/engines/graph_db/runtime/adhoc/operators/special_predicates.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {
struct ScanParams {
  int alias;
  std::vector<label_t> tables;
};
class Scan {
 public:
  template <typename PRED_T>
  static Context scan_vertex(const GraphReadInterface& graph,
                             const ScanParams& params,
                             const PRED_T& predicate) {
    Context ctx;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      SLVertexColumnBuilder builder(label);
      auto vertices = graph.GetVertexSet(label);
      for (auto vid : vertices) {
        if (predicate(label, vid)) {
          builder.push_back_opt(vid);
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
#if 0
      MLVertexColumnBuilder builder;

      for (auto label : params.tables) {
        auto vertices = graph.GetVertexSet(label);
        for (auto vid : vertices) {
          if (predicate(label, vid)) {
            builder.push_back_vertex({label, vid});
          }
        }
      }
#else
      MSVertexColumnBuilder builder;

      for (auto label : params.tables) {
        auto vertices = graph.GetVertexSet(label);
        builder.start_label(label);
        for (auto vid : vertices) {
          if (predicate(label, vid)) {
            builder.push_back_opt(vid);
          }
        }
      }
#endif
      ctx.set(params.alias, builder.finish());
    }
    return ctx;
  }

  static Context scan_vertex_with_special_vertex_predicate(
      const GraphReadInterface& graph, const ScanParams& params,
      const SPVertexPredicate& pred) {
    if (pred.type() == SPVertexPredicateType::kIdEQ) {
      return scan_vertex<VertexIdEQPredicateBeta>(
          graph, params, dynamic_cast<const VertexIdEQPredicateBeta&>(pred));
    } else {
      if (pred.data_type() == RTAnyType::kI64Value) {
        if (pred.type() == SPVertexPredicateType::kPropertyEQ) {
          return scan_vertex<VertexPropertyEQPredicateBeta<int64_t>>(
              graph, params,
              dynamic_cast<const VertexPropertyEQPredicateBeta<int64_t>&>(
                  pred));
        }
      } else if (pred.data_type() == RTAnyType::kStringValue) {
        if (pred.type() == SPVertexPredicateType::kPropertyEQ) {
          return scan_vertex<VertexPropertyEQPredicateBeta<std::string_view>>(
              graph, params,
              dynamic_cast<
                  const VertexPropertyEQPredicateBeta<std::string_view>&>(
                  pred));
        }
      }
    }
    LOG(FATAL) << "not impl... - " << static_cast<int>(pred.type());
    return Context();
  }

  template <typename PRED_T>
  static Context filter_gids(const GraphReadInterface& graph,
                             const ScanParams& params, const PRED_T& predicate,
                             const std::vector<int64_t>& gids) {
    Context ctx;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      SLVertexColumnBuilder builder(label);
      for (auto gid : gids) {
        vid_t vid = GlobalId::get_vid(gid);
        if (GlobalId::get_label_id(gid) == label && predicate(label, vid)) {
          builder.push_back_opt(vid);
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      MLVertexColumnBuilder builder;

      for (auto label : params.tables) {
        for (auto gid : gids) {
          vid_t vid = GlobalId::get_vid(gid);
          if (GlobalId::get_label_id(gid) == label && predicate(label, vid)) {
            builder.push_back_vertex({label, vid});
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    }
    return ctx;
  }

  template <typename KEY_T>
  static Context filter_gids_with_special_vertex_predicate(
      const GraphReadInterface& graph, const ScanParams& params,
      const SPVertexPredicate& predicate, const std::vector<KEY_T>& oids) {
    if (predicate.type() == SPVertexPredicateType::kIdEQ) {
      return filter_gids<VertexIdEQPredicateBeta>(
          graph, params,
          dynamic_cast<const VertexIdEQPredicateBeta&>(predicate), oids);
    }
    LOG(FATAL) << "not impl...";
    return Context();
  }

  template <typename PRED_T, typename KEY_T>
  static Context filter_oids(const GraphReadInterface& graph,
                             const ScanParams& params, const PRED_T& predicate,
                             const std::vector<KEY_T>& oids) {
    Context ctx;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      SLVertexColumnBuilder builder(label);
      for (auto oid : oids) {
        vid_t vid;
        if (graph.GetVertexIndex(label, oid, vid)) {
          if (predicate(label, vid)) {
            builder.push_back_opt(vid);
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      std::vector<std::pair<label_t, vid_t>> vids;

      for (auto label : params.tables) {
        for (auto oid : oids) {
          vid_t vid;
          if (graph.GetVertexIndex(label, oid, vid)) {
            if (predicate(label, vid)) {
              vids.emplace_back(label, vid);
            }
          }
        }
      }
      if (vids.size() == 1) {
        SLVertexColumnBuilder builder(vids[0].first);
        builder.push_back_opt(vids[0].second);
        ctx.set(params.alias, builder.finish());
      } else {
        MLVertexColumnBuilder builder;
        for (auto& pair : vids) {
          builder.push_back_vertex({pair.first, pair.second});
        }
        ctx.set(params.alias, builder.finish());
      }
    }
    return ctx;
  }

  template <typename KEY_T>
  static Context filter_oids_with_special_vertex_predicate(
      const GraphReadInterface& graph, const ScanParams& params,
      const SPVertexPredicate& predicate, const std::vector<KEY_T>& oids) {
    if (predicate.type() == SPVertexPredicateType::kIdEQ) {
      return filter_oids<VertexIdEQPredicateBeta>(
          graph, params,
          dynamic_cast<const VertexIdEQPredicateBeta&>(predicate), oids);
    }
    LOG(FATAL) << "not impl...";
    return Context();
  }

  // EXPR() is a function that returns the oid of the vertex
  template <typename EXPR>
  static Context find_vertex(const GraphReadInterface& graph, label_t label,
                             const EXPR& expr, int alias, bool scan_oid) {
    Context ctx;
    SLVertexColumnBuilder builder(label);
    if (scan_oid) {
      auto oid = expr();
      vid_t vid;
      if (graph.GetVertexIndex(label, oid, vid)) {
        builder.push_back_opt(vid);
      }
    } else {
      int64_t gid = expr();
      if (GlobalId::get_label_id(gid) == label) {
        builder.push_back_opt(GlobalId::get_vid(gid));
      } else {
        LOG(ERROR) << "Invalid label id: "
                   << static_cast<int>(GlobalId::get_label_id(gid));
      }
    }
    ctx.set(alias, builder.finish());
    return ctx;
  }

  static Context find_vertex_with_id(const GraphReadInterface& graph,
                                     label_t label, const Any& pk, int alias,
                                     bool scan_oid);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_SCAN_H_