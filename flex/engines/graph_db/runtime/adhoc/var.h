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

#ifndef RUNTIME_ADHOC_VAR_H_
#define RUNTIME_ADHOC_VAR_H_

#include "flex/engines/graph_db/runtime/adhoc/graph_interface.h"

#include "flex/engines/graph_db/runtime/common/accessors.h"
#include "flex/engines/graph_db/runtime/common/context.h"

#include "flex/proto_generated_gie/expr.pb.h"

namespace gs {

namespace runtime {

enum class VarType {
  kVertexVar,
  kEdgeVar,
  kPathVar,
};

class VarGetterBase {
 public:
  virtual ~VarGetterBase() = default;
  virtual RTAny eval_path(size_t idx) const = 0;
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx) const = 0;
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx) const = 0;
  virtual std::string name() const = 0;
};

template <typename GRAPH_IMPL>
class Var {
 public:
  Var(const GraphInterface<GRAPH_IMPL>& txn, const Context& ctx,
      const common::Variable& pb, VarType var_type)
      : getter_(nullptr) {
    int tag = -1;
    type_ = RTAnyType::kUnknown;
    if (pb.has_node_type()) {
      type_ = parse_from_ir_data_type(pb.node_type());
    }
    if (pb.has_tag()) {
      tag = pb.tag().id();
    }

    if (type_ == RTAnyType::kUnknown) {
      if (pb.has_tag()) {
        tag = pb.tag().id();
        CHECK(ctx.get(tag) != nullptr);
        type_ = ctx.get(tag)->elem_type();
      } else if (pb.has_property() &&
                 (pb.property().has_label() || pb.property().has_id())) {
        type_ = RTAnyType::kI64Value;
      } else {
        LOG(FATAL) << "not support";
      }
    }

    if (pb.has_tag() || var_type == VarType::kPathVar) {
      CHECK(ctx.get(tag) != nullptr) << "tag not found - " << tag;
      if (ctx.get(tag)->column_type() == ContextColumnType::kVertex) {
        if (pb.has_property()) {
          auto& pt = pb.property();
          if (pt.has_id()) {
            getter_ = std::make_shared<VertexGIdPathAccessor>(ctx, tag);
          } else if (pt.has_key()) {
            if (pt.key().name() == "id") {
              if (type_ == RTAnyType::kStringValue) {
                getter_ = std::make_shared<
                    VertexIdPathAccessor<std::string_view, GRAPH_IMPL>>(
                    txn, ctx, tag);
              } else if (type_ == RTAnyType::kI32Value) {
                getter_ =
                    std::make_shared<VertexIdPathAccessor<int32_t, GRAPH_IMPL>>(
                        txn, ctx, tag);
              } else if (type_ == RTAnyType::kI64Value) {
                getter_ =
                    std::make_shared<VertexIdPathAccessor<int64_t, GRAPH_IMPL>>(
                        txn, ctx, tag);
              } else {
                LOG(FATAL) << "not support for "
                           << static_cast<int>(type_.type_enum_);
              }
            } else {
              getter_ = create_vertex_property_path_accessor(
                  txn, ctx, tag, type_, pt.key().name());
            }
          } else if (pt.has_label()) {
            getter_ = create_vertex_label_path_accessor(ctx, tag);
          } else {
            LOG(FATAL) << "xxx, " << pt.item_case();
          }
        } else {
          getter_ = std::make_shared<VertexPathAccessor>(ctx, tag);
        }
      } else if (ctx.get(tag)->column_type() == ContextColumnType::kValue ||
                 ctx.get(tag)->column_type() ==
                     ContextColumnType::kOptionalValue) {
        getter_ = create_context_value_accessor(ctx, tag, type_);
      } else if (ctx.get(tag)->column_type() == ContextColumnType::kEdge) {
        if (pb.has_property()) {
          auto& pt = pb.property();
          if (pt.has_key()) {
            auto name = pt.key().name();
            getter_ =
                create_edge_property_path_accessor(txn, name, ctx, tag, type_);
          } else if (pt.has_label()) {
            getter_ = create_edge_label_path_accessor(ctx, tag);
          } else if (pt.has_id()) {
            getter_ = create_edge_global_id_path_accessor(ctx, tag);
          } else {
            LOG(FATAL) << "not support...";
          }
        } else {
          getter_ = std::make_shared<EdgeIdPathAccessor>(ctx, tag);
          // LOG(FATAL) << "not support for edge column - " << tag;
        }
      } else if (ctx.get(tag)->column_type() == ContextColumnType::kPath) {
        if (pb.has_property()) {
          auto& pt = pb.property();
          if (pt.has_len()) {
            getter_ = std::make_shared<PathLenPathAccessor>(ctx, tag);
          } else {
            LOG(FATAL) << "not support for path column - " << pt.DebugString();
          }
        } else {
          getter_ = std::make_shared<PathIdPathAccessor>(ctx, tag);
        }
      } else {
        LOG(FATAL) << "not support for " << ctx.get(tag)->column_info();
      }
    } else {
      if (var_type == VarType::kVertexVar) {
        if (pb.has_property()) {
          auto& pt = pb.property();
          if (pt.has_id()) {
            getter_ = std::make_shared<VertexGIdVertexAccessor>();
          } else if (pt.has_key()) {
            if (pt.key().name() == "id") {
              if (type_ == RTAnyType::kStringValue) {
                getter_ = std::make_shared<
                    VertexIdVertexAccessor<std::string_view, GRAPH_IMPL>>(txn);
              } else if (type_ == RTAnyType::kI32Value) {
                getter_ = std::make_shared<
                    VertexIdVertexAccessor<int32_t, GRAPH_IMPL>>(txn);
              } else if (type_ == RTAnyType::kI64Value) {
                getter_ = std::make_shared<
                    VertexIdVertexAccessor<int64_t, GRAPH_IMPL>>(txn);
              } else {
                LOG(FATAL) << "not support for "
                           << static_cast<int>(type_.type_enum_);
              }
            } else {
              getter_ = create_vertex_property_vertex_accessor(txn, type_,
                                                               pt.key().name());
            }
          } else if (pt.has_label()) {
            getter_ = std::make_shared<VertexLabelVertexAccessor>();
          } else {
            LOG(FATAL) << "xxx, " << pt.item_case();
          }
        } else {
          LOG(FATAL) << "not support";
        }
      } else if (var_type == VarType::kEdgeVar) {
        if (pb.has_property()) {
          auto& pt = pb.property();
          if (pt.has_key()) {
            auto name = pt.key().name();
            getter_ =
                create_edge_property_edge_accessor(txn, name, ctx, tag, type_);
          } else if (pt.has_label()) {
            getter_ = create_edge_label_edge_accessor();
          } else if (pt.has_id()) {
            getter_ = create_edge_global_id_edge_accessor();
          } else {
            LOG(FATAL) << "not support";
          }
        } else {
          LOG(FATAL) << "not support";
        }
      } else {
        LOG(FATAL) << "not support";
      }
    }
  }
  ~Var() {}

  RTAny get(size_t path_idx) const { return getter_->eval_path(path_idx); }
  RTAny get_vertex(label_t label, vid_t v, size_t idx) const {
    return getter_->eval_vertex(label, v, idx);
  }
  RTAny get_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                 const Any& data, size_t idx) const {
    return getter_->eval_edge(label, src, dst, data, idx);
  }

  RTAny get(size_t path_idx, int) const {
    return getter_->eval_path(path_idx, 0);
  }
  RTAny get_vertex(label_t label, vid_t v, size_t idx, int) const {
    return getter_->eval_vertex(label, v, idx, 0);
  }
  RTAny get_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                 const Any& data, size_t idx, int) const {
    return getter_->eval_edge(label, src, dst, data, idx, 0);
  }
  RTAnyType type() const { return type_; }
  bool is_optional() const { return getter_->is_optional(); }
  std::shared_ptr<IContextColumnBuilder> builder() const {
    return getter_->builder();
  }

 private:
  std::shared_ptr<IAccessor> getter_;
  RTAnyType type_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_VAR_H_