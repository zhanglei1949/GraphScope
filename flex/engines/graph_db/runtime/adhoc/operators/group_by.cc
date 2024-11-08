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
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/adhoc/var.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

namespace gs {

namespace runtime {

enum class AggrKind {
  kSum,
  kMin,
  kMax,
  kCount,
  kCountDistinct,
  kToSet,
  kFirst,
  kToList,
  kAvg,
};

AggrKind parse_aggregate(physical::GroupBy_AggFunc::Aggregate v) {
  if (v == physical::GroupBy_AggFunc::SUM) {
    return AggrKind::kSum;
  } else if (v == physical::GroupBy_AggFunc::MIN) {
    return AggrKind::kMin;
  } else if (v == physical::GroupBy_AggFunc::MAX) {
    return AggrKind::kMax;
  } else if (v == physical::GroupBy_AggFunc::COUNT) {
    return AggrKind::kCount;
  } else if (v == physical::GroupBy_AggFunc::COUNT_DISTINCT) {
    return AggrKind::kCountDistinct;
  } else if (v == physical::GroupBy_AggFunc::TO_SET) {
    return AggrKind::kToSet;
  } else if (v == physical::GroupBy_AggFunc::FIRST) {
    return AggrKind::kFirst;
  } else if (v == physical::GroupBy_AggFunc::TO_LIST) {
    return AggrKind::kToList;
  } else if (v == physical::GroupBy_AggFunc::AVG) {
    return AggrKind::kAvg;
  } else {
    LOG(FATAL) << "unsupport" << static_cast<int>(v);
    return AggrKind::kSum;
  }
}

struct AggFunc {
  AggFunc(const physical::GroupBy_AggFunc& opr, const GraphReadInterface& graph,
          const Context& ctx)
      : aggregate(parse_aggregate(opr.aggregate())),
        alias(-1),
        single_tag(false) {
    if (opr.has_alias()) {
      alias = opr.alias().value();
    }
    int var_num = opr.vars_size();
    for (int i = 0; i < var_num; ++i) {
      vars.emplace_back(graph, ctx, opr.vars(i), VarType::kPathVar);
    }

    if (var_num == 1) {
      const auto& v = opr.vars(0);
      if (v.has_tag() && !v.has_property() &&
          v.tag().item_case() == common::NameOrId::kId) {
        tag_id = v.tag().id();
        if (ctx.exist(tag_id) &&
            ctx.get(tag_id)->elem_type() == vars[0].type()) {
          single_tag = true;
        }
      }
    }
  }

  std::vector<Var> vars;
  AggrKind aggregate;
  int alias;

  bool single_tag;
  int tag_id;
};

struct AggKey {
  AggKey(const physical::GroupBy_KeyAlias& opr, const GraphReadInterface& graph,
         const Context& ctx)
      : key(graph, ctx, opr.key(), VarType::kPathVar),
        alias(-1),
        is_tag(false) {
    if (opr.has_alias()) {
      alias = opr.alias().value();
    }
    if (opr.key().has_tag() &&
        opr.key().tag().item_case() == common::NameOrId::kId &&
        !opr.key().has_property()) {
      is_tag = true;
      tag_id = opr.key().tag().id();
    }
  }

  Var key;
  int alias;

  bool is_tag;
  int tag_id;
};

std::pair<std::vector<std::vector<size_t>>, Context> generate_aggregate_indices(
    const Context& ctx, const std::vector<AggKey>& keys, size_t row_num,
    const std::vector<AggFunc>& functions) {
  size_t keys_num = keys.size();
#if 1
  if (keys_num == 1 && keys[0].is_tag) {
    auto key_column = ctx.get(keys[0].tag_id);
    if (key_column != nullptr) {
      auto ret = key_column->generate_aggregate_offset();
      if (ret.first != nullptr) {
        Context ret_ctx;
        ret_ctx.set(keys[0].alias, ret.first);
        ret_ctx.append_tag_id(keys[0].alias);

        // LOG(INFO) << "hit generate...";
        return std::make_pair(std::move(ret.second), std::move(ret_ctx));
      }
    }
  }
#endif

  std::vector<std::vector<size_t>> ret;
  std::vector<std::shared_ptr<IContextColumnBuilder>> keys_columns;
  std::unordered_map<std::string_view, size_t> sig_to_root;
  std::vector<std::vector<char>> root_list;
  std::vector<RTAny> keys_row(keys_num);
  for (size_t k_i = 0; k_i < keys_num; ++k_i) {
    auto type = keys[k_i].key.type();
    std::shared_ptr<IContextColumnBuilder> builder;
    if (type == RTAnyType::kList) {
      builder = keys[k_i].key.builder();
    } else if (type == RTAnyType::kPath) {
      builder = keys[k_i].key.builder();
    } else {
      if (type == RTAnyType::kVertex) {
        builder = keys[k_i].key.builder();
        CHECK(builder != nullptr);
      } else {
        builder = create_column_builder(type);
      }
    }
    keys_columns.push_back(builder);
  }

  for (size_t r_i = 0; r_i < row_num; ++r_i) {
    bool has_null{false};
    for (auto func : functions) {
      for (size_t v_i = 0; v_i < func.vars.size(); ++v_i) {
        if (func.vars[v_i].is_optional()) {
          if (func.vars[v_i].get(r_i, 0).is_null()) {
            has_null = true;
            break;
          }
        }
      }
      if (has_null) {
        break;
      }
    }

    std::vector<char> buf;
    ::gs::Encoder encoder(buf);
    for (size_t k_i = 0; k_i < keys_num; ++k_i) {
      keys_row[k_i] = keys[k_i].key.get(r_i);
      keys_row[k_i].encode_sig(keys[k_i].key.type(), encoder);
    }

    std::string_view sv(buf.data(), buf.size());
    auto iter = sig_to_root.find(sv);
    if (iter != sig_to_root.end()) {
      if (!has_null) {
        ret[iter->second].push_back(r_i);
      }
    } else {
      sig_to_root.emplace(sv, ret.size());
      root_list.emplace_back(std::move(buf));

      for (size_t k_i = 0; k_i < keys_num; ++k_i) {
        keys_columns[k_i]->push_back_elem(keys_row[k_i]);
      }

      std::vector<size_t> ret_elem;
      if (!has_null) {
        ret_elem.push_back(r_i);
      }
      ret.emplace_back(std::move(ret_elem));
    }
  }

  Context ret_ctx;
  for (size_t k_i = 0; k_i < keys_num; ++k_i) {
    auto col = keys_columns[k_i]->finish();
    ret_ctx.set(keys[k_i].alias, col);
    ret_ctx.append_tag_id(keys[k_i].alias);
  }

  return std::make_pair(std::move(ret), std::move(ret_ctx));
}

template <typename NT>
std::shared_ptr<IContextColumn> numeric_sum(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<NT> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    NT s = 0;
    for (auto idx : vec) {
      s += TypedConverter<NT>::to_typed(var.get(idx));
    }
    builder.push_back_opt(s);
  }
  return builder.finish();
}

template <typename NT>
std::shared_ptr<IContextColumn> typed_numeric_sum(
    std::shared_ptr<IContextColumn> input_column,
    const std::vector<std::vector<size_t>>& to_aggregate) {
  if (input_column == nullptr) {
    return nullptr;
  }
  ValueColumnBuilder<NT> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  auto casted_column = std::dynamic_pointer_cast<ValueColumn<NT>>(input_column);
  if (casted_column == nullptr) {
    return nullptr;
  }
  const auto& input_vec = casted_column->data();
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    NT s = 0;
    for (auto idx : vec) {
      s += input_vec[idx];
    }
    builder.push_back_opt(s);
  }
  return builder.finish();
}

template <typename NT>
std::shared_ptr<IContextColumn> numeric_count_distinct(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<int64_t> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    std::unordered_set<NT> s;
    for (auto idx : vec) {
      s.insert(TypedConverter<NT>::to_typed(var.get(idx)));
    }
    builder.push_back_opt(s.size());
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> vertex_count_distinct(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<int64_t> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    std::set<VertexRecord> s;
    for (auto idx : vec) {
      s.insert(var.get(idx).as_vertex());
    }
    builder.push_back_opt(s.size());
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> general_count_distinct(
    const std::vector<Var>& vars,
    const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<int64_t> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    std::set<std::string> s;
    std::vector<char> bytes;
    for (auto idx : vec) {
      bytes.clear();
      Encoder encoder(bytes);
      for (auto& var : vars) {
        var.get(idx).encode_sig(var.get(idx).type(), encoder);
        encoder.put_byte('#');
      }
      std::string str(bytes.begin(), bytes.end());
      s.insert(str);
    }
    builder.push_back_opt(s.size());
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> general_count(
    const std::vector<Var>& vars,
    const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<int64_t> builder;
  if (vars.size() == 1) {
    if (vars[0].is_optional()) {
      size_t col_size = to_aggregate.size();
      builder.reserve(col_size);
      for (size_t k = 0; k < col_size; ++k) {
        auto& vec = to_aggregate[k];
        int64_t s = 0;
        for (auto idx : vec) {
          if (vars[0].get(idx, 0).is_null()) {
            continue;
          }
          s += 1;
        }
        builder.push_back_opt(s);
      }
      auto ret = builder.finish();
      return ret;
    }
  }
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    builder.push_back_opt(vec.size());
  }
  auto ret = builder.finish();
  return ret;
}

std::shared_ptr<IContextColumn> vertex_first(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  MLVertexColumnBuilder builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    for (auto idx : vec) {
      builder.push_back_elem(var.get(idx));
      break;
    }
  }
  return builder.finish();
}

template <typename NT>
std::shared_ptr<IContextColumn> general_first(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<NT> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    for (auto idx : vec) {
      builder.push_back_elem(var.get(idx));
      break;
    }
  }
  return builder.finish();
}

template <typename NT>
std::shared_ptr<IContextColumn> general_min(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<NT> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    if (vec.size() == 0) {
      continue;
    }
    NT s = TypedConverter<NT>::to_typed(var.get(vec[0]));
    for (auto idx : vec) {
      s = std::min(s, TypedConverter<NT>::to_typed(var.get(idx)));
    }
    if constexpr (std::is_same<NT, std::string_view>::value) {
      builder.push_back_opt(std::string(s));
    } else {
      builder.push_back_opt(s);
    }
  }
  return builder.finish();
}

template <typename NT>
std::shared_ptr<IContextColumn> general_max(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<NT> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    if (vec.size() == 0) {
      continue;
    }
    NT s = TypedConverter<NT>::to_typed(var.get(vec[0]));
    for (auto idx : vec) {
      s = std::max(s, TypedConverter<NT>::to_typed(var.get(idx)));
    }
    if constexpr (std::is_same<NT, std::string_view>::value) {
      builder.push_back_opt(std::string(s));
    } else {
      builder.push_back_opt(s);
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> string_to_set(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ValueColumnBuilder<std::set<std::string>> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    std::set<std::string> elem;
    for (auto idx : vec) {
      elem.insert(std::string(var.get(idx).as_string()));
    }
    builder.push_back_opt(std::move(elem));
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> typed_vertex_to_set(
    std::shared_ptr<IContextColumn> input,
    const std::vector<std::vector<size_t>>& to_aggregate) {
  auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(input);
  if (vertex_col == nullptr) {
    return nullptr;
  }
  auto sl_vertex_col = std::dynamic_pointer_cast<SLVertexColumn>(vertex_col);
  if (sl_vertex_col == nullptr) {
    return nullptr;
  }
  const auto& vertex_col_ref = *sl_vertex_col;
  size_t col_size = to_aggregate.size();
  SetValueColumnBuilder<VertexRecord> builder(col_size);

  builder.reserve(col_size);

  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    auto set = builder.allocate_set();
    auto set_impl = dynamic_cast<SetImpl<VertexRecord>*>(set.impl_);
    for (auto idx : vec) {
      set_impl->insert(vertex_col_ref.get_vertex(idx));
    }
    builder.push_back_opt(set);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> vertex_to_set(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  size_t col_size = to_aggregate.size();
  SetValueColumnBuilder<VertexRecord> builder(col_size);
  builder.reserve(col_size);

  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];
    auto set = builder.allocate_set();
    auto set_impl = dynamic_cast<SetImpl<VertexRecord>*>(set.impl_);
    for (auto idx : vec) {
      set_impl->insert(var.get(idx));
    }
    builder.push_back_opt(set);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> tuple_to_list(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ListValueColumnBuilder<Tuple> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  std::vector<std::shared_ptr<ListImplBase>> impls;
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];

    std::vector<RTAny> elem;
    for (auto idx : vec) {
      elem.push_back(var.get(idx));
    }
    auto impl = ListImpl<Tuple>::make_list_impl(elem);
    auto list = List::make_list(impl);
    impls.emplace_back(impl);
    builder.push_back_opt(list);
  }
  builder.set_list_impls(impls);
  return builder.finish();
}

std::shared_ptr<IContextColumn> string_to_list(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ListValueColumnBuilder<std::string> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  std::vector<std::shared_ptr<ListImplBase>> impls;
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];

    std::vector<std::string> elem;
    for (auto idx : vec) {
      elem.push_back(std::string(var.get(idx).as_string()));
    }
    auto impl = ListImpl<std::string_view>::make_list_impl(std::move(elem));
    auto list = List::make_list(impl);
    impls.emplace_back(impl);
    builder.push_back_opt(list);
  }
  builder.set_list_impls(impls);
  return builder.finish();
}

std::shared_ptr<IContextColumn> vertex_to_list(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ListValueColumnBuilder<VertexRecord> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  std::vector<std::shared_ptr<ListImplBase>> impls;
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];

    std::vector<VertexRecord> elem;
    for (auto idx : vec) {
      elem.push_back(var.get(idx).as_vertex());
    }

    auto impl = ListImpl<VertexRecord>::make_list_impl(std::move(elem));
    auto list = List::make_list(impl);
    impls.emplace_back(impl);
    builder.push_back_opt(list);
  }
  builder.set_list_impls(impls);
  return builder.finish();
}

template <typename NT>
std::shared_ptr<IContextColumn> value_to_list(
    const Var& var, const std::vector<std::vector<size_t>>& to_aggregate) {
  ListValueColumnBuilder<NT> builder;
  size_t col_size = to_aggregate.size();
  builder.reserve(col_size);
  std::vector<std::shared_ptr<ListImplBase>> impls;
  for (size_t k = 0; k < col_size; ++k) {
    auto& vec = to_aggregate[k];

    std::vector<NT> elem;
    for (auto idx : vec) {
      elem.push_back(TypedConverter<NT>::to_typed(var.get(idx)));
    }
    auto impl = ListImpl<NT>::make_list_impl(std::move(elem));
    auto list = List::make_list(impl);
    impls.emplace_back(impl);
    builder.push_back_opt(list);
  }
  builder.set_list_impls(impls);
  return builder.finish();
}

std::shared_ptr<IContextColumn> apply_reduce(
    const Context& ctx, const AggFunc& func,
    const std::vector<std::vector<size_t>>& to_aggregate) {
  // LOG(INFO) << "enter reduce...";
  if (func.aggregate == AggrKind::kSum) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to sum is allowed";
    }
    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kI32Value) {
      if (func.single_tag) {
        auto ret = typed_numeric_sum<int>(ctx.get(func.tag_id), to_aggregate);
        if (ret != nullptr) {
          // LOG(INFO) << "hit reduce...";
          return ret;
        }
      }
      return numeric_sum<int>(var, to_aggregate);
    } else {
      LOG(FATAL) << "reduce on " << static_cast<int>(var.type().type_enum_)
                 << " is not supported...";
    }
  } else if (func.aggregate == AggrKind::kToSet) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to to_set is allowed";
    }
    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kStringValue) {
      return string_to_set(var, to_aggregate);
    } else if (var.type() == RTAnyType::kVertex) {
      if (func.single_tag) {
        auto ret = typed_vertex_to_set(ctx.get(func.tag_id), to_aggregate);
        if (ret != nullptr) {
          // LOG(INFO) << "hit reduce...";
          return ret;
        }
      }
      return vertex_to_set(var, to_aggregate);
    } else {
      LOG(FATAL) << "not support" << (int) var.type().type_enum_;
    }
  } else if (func.aggregate == AggrKind::kCountDistinct) {
    if (func.vars.size() == 1 && func.vars[0].type() == RTAnyType::kVertex) {
      const Var& var = func.vars[0];
      return vertex_count_distinct(var, to_aggregate);
    } else {
      return general_count_distinct(func.vars, to_aggregate);
    }
  } else if (func.aggregate == AggrKind::kCount) {
    // return general_count(to_aggregate);
    return general_count(func.vars, to_aggregate);
  } else if (func.aggregate == AggrKind::kFirst) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to first is allowed";
    }

    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kVertex) {
      return vertex_first(var, to_aggregate);
    } else if (var.type() == RTAnyType::kI64Value) {
      return general_first<int64_t>(var, to_aggregate);
    } else if (var.type() == RTAnyType::kDate32) {
      return general_first<Day>(var, to_aggregate);
    } else if (var.type() == RTAnyType::kTimestamp) {
      return general_first<Date>(var, to_aggregate);
    } else {
      LOG(FATAL) << "not support" << static_cast<int>(var.type().type_enum_);
    }
  } else if (func.aggregate == AggrKind::kMin) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to min is allowed";
    }

    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kI32Value) {
      return general_min<int>(var, to_aggregate);
    } else if (var.type() == RTAnyType::kStringValue) {
      return general_min<std::string_view>(var, to_aggregate);
    }
  } else if (func.aggregate == AggrKind::kMax) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to max is allowed";
    }

    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kI32Value) {
      return general_max<int>(var, to_aggregate);
    } else if (var.type() == RTAnyType::kI64Value) {
      return general_max<int64_t>(var, to_aggregate);
    } else if (var.type() == RTAnyType::kStringValue) {
      return general_max<std::string_view>(var, to_aggregate);
    }
  } else if (func.aggregate == AggrKind::kToList) {
    const Var& var = func.vars[0];
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to to_list is allowed";
    }
    if (var.type() == RTAnyType::kTuple) {
      return tuple_to_list(var, to_aggregate);
    } else if (var.type() == RTAnyType::kStringValue) {
      return string_to_list(var, to_aggregate);
    } else if (var.type() == RTAnyType::kVertex) {
      return vertex_to_list(var, to_aggregate);
    } else if (var.type() == RTAnyType::kI64Value) {
      return value_to_list<int64_t>(var, to_aggregate);
    } else if (var.type() == RTAnyType::kI32Value) {
      return value_to_list<int32_t>(var, to_aggregate);
    } else {
      LOG(FATAL) << "not support" << static_cast<int>(var.type().type_enum_);
    }
  } else if (func.aggregate == AggrKind::kAvg) {
    if (func.vars.size() != 1) {
      LOG(FATAL) << "only 1 variable to avg is allowed";
    }
    // LOG(FATAL) << "not support";
    const Var& var = func.vars[0];
    if (var.type() == RTAnyType::kI32Value) {
      ValueColumnBuilder<int32_t> builder;
      size_t col_size = to_aggregate.size();
      builder.reserve(col_size);
      for (size_t k = 0; k < col_size; ++k) {
        auto& vec = to_aggregate[k];
        int32_t s = 0;

        for (auto idx : vec) {
          s += TypedConverter<int32_t>::to_typed(var.get(idx));
        }
        builder.push_back_opt(s / vec.size());
      }
      return builder.finish();
    }
  }

  LOG(FATAL) << "unsupport " << static_cast<int>(func.aggregate);
  return nullptr;
}

Context eval_group_by(const physical::GroupBy& opr,
                      const GraphReadInterface& graph, Context&& ctx) {
  std::vector<AggFunc> functions;
  std::vector<AggKey> mappings;
  int func_num = opr.functions_size();
  for (int i = 0; i < func_num; ++i) {
    functions.emplace_back(opr.functions(i), graph, ctx);
  }

  int mappings_num = opr.mappings_size();
  if (mappings_num == 0) {
    Context ret;
    for (int i = 0; i < func_num; ++i) {
      std::vector<size_t> tmp;
      for (size_t _i = 0; _i < ctx.row_num(); ++_i) {
        tmp.emplace_back(_i);
      }
      auto new_col = apply_reduce(ctx, functions[i], {tmp});
      ret.set(functions[i].alias, new_col);
      ret.append_tag_id(functions[i].alias);
    }

    return ret;
  } else {
    for (int i = 0; i < mappings_num; ++i) {
      mappings.emplace_back(opr.mappings(i), graph, ctx);
    }

    auto keys_ret =
        generate_aggregate_indices(ctx, mappings, ctx.row_num(), functions);
    std::vector<std::vector<size_t>>& to_aggregate = keys_ret.first;

    Context& ret = keys_ret.second;

    // exclude null values
    if (func_num == 1 && functions[0].aggregate != AggrKind::kCount &&
        functions[0].aggregate != AggrKind::kCountDistinct &&
        functions[0].aggregate != AggrKind::kToList &&
        functions[0].aggregate != AggrKind::kToSet) {
      std::vector<size_t> tmp;
      std::vector<std::vector<size_t>> tmp_to_aggregate;
      for (size_t i = 0; i < to_aggregate.size(); ++i) {
        if (to_aggregate[i].size() == 0) {
          continue;
        }
        tmp_to_aggregate.emplace_back(to_aggregate[i]);
        tmp.emplace_back(i);
      }
      ret.reshuffle(tmp);
      std::swap(to_aggregate, tmp_to_aggregate);
    }

    for (int i = 0; i < func_num; ++i) {
      auto new_col = apply_reduce(ctx, functions[i], to_aggregate);
      ret.set(functions[i].alias, new_col);
      ret.append_tag_id(functions[i].alias);
    }
    return ret;
  }
}

}  // namespace runtime

}  // namespace gs